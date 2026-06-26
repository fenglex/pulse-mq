"""ZMQ PUB socket + PLAIN 认证 + 在线用户追踪。

v2 简化：单一 PUB socket，无需 ROUTER/XPUB。
api_keys 非空时自动开启 ZMQ PLAIN 认证。

ZAP handler 运行在 asyncio 事件循环中（与 PUB socket 同 context），
避免跨线程 inproc:// 的兼容性问题。

在线用户管理：
- 认证成功 → 加入 connected_users，拒绝重复连接（单用户单 SUB）
- PULL socket 监听断开通知 + 心跳帧（端口 = PUB 端口 + 1）
- 心跳保活：subscriber 定期发送心跳，超时未收到则自动移除（检测 kill/崩溃）
"""

from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable

import zmq
import zmq.asyncio
from loguru import logger

AuthCallback = Callable[[str, str, bool], Awaitable[None]]

# subscriber 控制帧 magic 前缀（与 subscriber 端保持一致）
_DISCONNECT_MAGIC = b"__pulse_disconnect__"
_KEEPALIVE_MAGIC = b"__pulse_keepalive__"


def _derive_disconnect_port(bind: str) -> int | None:
    """从 PUB bind 地址推导 disconnect PULL 端口（PUB 端口 + 1）。"""
    try:
        # tcp://*:5555 或 tcp://127.0.0.1:5555
        port_str = bind.rsplit(":", 1)[-1]
        port = int(port_str)
        if 1 <= port <= 65534:
            return port + 1
    except (ValueError, IndexError):
        pass
    return None


class AsyncZAPHandler:
    """ZMQ PLAIN 认证的 ZAP handler（asyncio 版）。

    与 PUB socket 共享同一个 zmq.asyncio.Context，
    在 asyncio 事件循环中处理 ZAP 请求。
    """

    def __init__(
        self,
        api_keys: dict[str, str],
        ctx: zmq.asyncio.Context,
        connected_users: set[str],
        last_seen: dict[str, float],
        on_auth: AuthCallback | None = None,
    ) -> None:
        self._api_keys = api_keys
        self._ctx = ctx
        self._connected_users = connected_users
        self._last_seen = last_seen
        self._on_auth = on_auth
        self._zap: zmq.asyncio.Socket | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """启动 ZAP handler。"""
        self._zap = self._ctx.socket(zmq.REP)
        self._zap.bind("inproc://zeromq.zap.01")
        self._task = asyncio.create_task(self._loop())
        logger.info("ZAP handler 启动: {} 个白名单用户", len(self._api_keys))

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        if self._zap is not None:
            self._zap.close(linger=100)
            self._zap = None

    async def _loop(self) -> None:
        """ZAP 请求处理循环（asyncio）。"""
        assert self._zap is not None
        while True:
            try:
                msg = await self._zap.recv_multipart()
            except zmq.ZMQError:
                break
            except asyncio.CancelledError:
                break

            # ZAP 请求帧格式:
            # [version, request_id, domain, address, identity, mechanism, ...credentials]
            if len(msg) < 7:
                await self._send_zap_reply(
                    msg[1] if len(msg) > 1 else b"",
                    b"400", b"Invalid ZAP request",
                )
                continue

            version = msg[0]
            request_id = msg[1]
            mechanism = msg[5]
            client_addr = msg[3].decode("utf-8", errors="replace") if len(msg) > 3 else "unknown"
            username = msg[6].decode("utf-8", errors="replace") if len(msg) > 6 else ""
            password = msg[7].decode("utf-8", errors="replace") if len(msg) > 7 else ""

            if mechanism != b"PLAIN":
                logger.warning(
                    "[SUB 认证失败] user={} addr={} auth=FAIL reason=not-PLAIN mechanism={}",
                    username or "<empty>", client_addr,
                    mechanism.decode("utf-8", "replace"),
                )
                await self._send_zap_reply(request_id, b"400", b"Not PLAIN")
                continue

            # 单用户限制：检查是否已在线
            if username in self._connected_users:
                logger.warning(
                    "[SUB 认证拒绝] user={} addr={} reason=already_connected "
                    "(每个用户仅允许一个 SUB 连接)",
                    username, client_addr,
                )
                await self._send_zap_reply(request_id, b"400", b"Already connected")
                continue

            # 白名单校验
            expected = self._api_keys.get(username)
            if expected is not None and expected == password:
                self._connected_users.add(username)
                self._last_seen[username] = time.monotonic()
                logger.info(
                    "[SUB 上线] user={} addr={} auth=OK (在线: {})",
                    username, client_addr, len(self._connected_users),
                )
                await self._notify_auth(username, client_addr, True)
                await self._send_zap_reply(
                    request_id, b"200", b"OK", user_id=username.encode(),
                )
            else:
                logger.warning(
                    "[SUB 认证失败] user={} addr={} auth=FAIL reason=invalid-credentials",
                    username or "<empty>", client_addr,
                )
                await self._notify_auth(username, client_addr, False)
                await self._send_zap_reply(request_id, b"400", b"Invalid credentials")

    async def _send_zap_reply(
        self,
        request_id: bytes,
        status_code: bytes,
        status_text: bytes,
        *,
        version: bytes = b"1.0",
        user_id: bytes = b"",
    ) -> None:
        """发送 ZAP 响应（6 帧），并保护 send 异常。

        ZAP 协议要求响应为 6 帧：
        [version, request_id, status_code, status_text, user_id, metadata]。

        历史问题：
        1) 响应未 await：zmq.asyncio socket 的 send_multipart 返回协程，
           未 await 则响应永不发送，SUB 认证永久挂死；
        2) send 异常未保护：一次 send 失败会让 _loop 整体退出，
           ZAP task 静默死亡，后续所有 SUB 认证全部失效。
        本方法统一 await + try/except，单次 send 失败仅记日志、不影响循环。
        """
        if self._zap is None:
            return
        try:
            await self._zap.send_multipart([
                version, request_id, status_code, status_text, user_id, b"",
            ])
        except (zmq.ZMQError, asyncio.CancelledError):
            logger.warning("ZAP 响应发送失败，忽略并继续处理后续请求", exc_info=True)

    async def _notify_auth(self, username: str, client_addr: str, success: bool) -> None:
        """调用认证事件回调，回调异常不影响认证流程。"""
        if self._on_auth is None:
            return
        try:
            await self._on_auth(username, client_addr, success)
        except Exception:
            logger.warning("认证回调执行异常", exc_info=True)


class ZmqPubTransport:
    """ZMQ PUB socket + PLAIN 认证 + 在线用户管理。"""

    def __init__(
        self,
        bind: str = "tcp://*:5555",
        api_keys: dict[str, str] | None = None,
        on_auth: AuthCallback | None = None,
        *,
        keepalive_timeout: float = 5.0,
    ) -> None:
        self._bind = bind
        self._api_keys = api_keys or {}
        self._on_auth = on_auth
        self._ctx: zmq.asyncio.Context | None = None
        self._pub: zmq.asyncio.Socket | None = None
        self._zap: AsyncZAPHandler | None = None

        # 在线用户追踪
        self._connected_users: set[str] = set()
        self._last_seen: dict[str, float] = {}  # username → time.monotonic()
        self._keepalive_timeout = keepalive_timeout

        # 断开通知 PULL socket
        self._disconnect_pull: zmq.asyncio.Socket | None = None
        self._disconnect_task: asyncio.Task | None = None
        self._disconnect_port: int | None = None
        # 心跳超时检测 task
        self._keepalive_task: asyncio.Task | None = None

    @property
    def connected_users(self) -> set[str]:
        """当前在线用户集合（只读副本）。"""
        return set(self._connected_users)

    @property
    def last_seen(self) -> dict[str, float]:
        """各用户最后心跳时间（monotonic 秒），供 Admin 展示。"""
        return dict(self._last_seen)

    @property
    def disconnect_port(self) -> int | None:
        """断开通知 PULL 端口（PUB 端口 + 1），供 subscriber 连接。"""
        return self._disconnect_port

    async def start(self) -> None:
        """启动 PUB socket，可选开启 PLAIN 认证。"""
        self._ctx = zmq.asyncio.Context()
        self._pub = self._ctx.socket(zmq.PUB)
        self._pub.setsockopt(zmq.SNDHWM, 0)  # 0=无上限，burst 模式不丢消息
        self._pub.setsockopt(zmq.LINGER, 1000)

        if self._api_keys:
            # ZAP handler 必须在 PUB bind 之前启动
            self._zap = AsyncZAPHandler(
                self._api_keys, self._ctx, self._connected_users,
                self._last_seen, self._on_auth,
            )
            await self._zap.start()
            self._pub.setsockopt(zmq.PLAIN_SERVER, 1)

        self._pub.bind(self._bind)
        logger.info("PUB socket 绑定到 {} (auth={})", self._bind, "on" if self._api_keys else "off")

        # 启动断开通知 PULL socket
        if self._api_keys:
            self._start_disconnect_pull()

    def _start_disconnect_pull(self) -> None:
        """启动 PULL socket 监听 subscriber 断开通知（端口 = PUB 端口 + 1）。"""
        if self._ctx is None:
            return
        port = _derive_disconnect_port(self._bind)
        if port is None:
            logger.warning("无法推导 disconnect PULL 端口，断开检测不可用 (bind={})", self._bind)
            return

        self._disconnect_port = port
        self._disconnect_pull = self._ctx.socket(zmq.PULL)
        self._disconnect_pull.setsockopt(zmq.LINGER, 0)

        # 推导 bind 地址：替换端口部分
        base = self._bind.rsplit(":", 1)[0]
        pull_bind = f"{base}:{port}"
        try:
            self._disconnect_pull.bind(pull_bind)
        except zmq.ZMQError:
            logger.warning("disconnect PULL 绑定失败 port={}，断开检测不可用", port)
            self._disconnect_pull.close(linger=0)
            self._disconnect_pull = None
            self._disconnect_port = None
            return

        self._disconnect_task = asyncio.create_task(self._disconnect_loop())
        logger.info("断开通知 PULL 监听 port={}", port)

        # 启动心跳超时检测
        self._keepalive_task = asyncio.create_task(self._keepalive_check_loop())
        logger.info("心跳超时检测启动 timeout={}s", self._keepalive_timeout)

    async def _disconnect_loop(self) -> None:
        """持续监听 subscriber 断开通知和心跳帧。"""
        assert self._disconnect_pull is not None
        try:
            while True:
                frames = await self._disconnect_pull.recv_multipart()
                if len(frames) < 2:
                    continue

                magic = frames[0]
                username = frames[1].decode("utf-8", errors="replace")

                if magic == _DISCONNECT_MAGIC:
                    # 断开帧：立即移除用户 + 清理 last_seen
                    if username in self._connected_users:
                        self._connected_users.discard(username)
                        self._last_seen.pop(username, None)
                        logger.info(
                            "[SUB 下线] user={} (在线: {})",
                            username, len(self._connected_users),
                        )
                elif magic == _KEEPALIVE_MAGIC:
                    # 心跳帧：更新 last_seen
                    self._last_seen[username] = time.monotonic()
        except (zmq.ZMQError, asyncio.CancelledError):
            return

    async def _keepalive_check_loop(self) -> None:
        """定期检查所有用户的最后心跳时间，超时则视为下线。"""
        try:
            while True:
                await asyncio.sleep(self._keepalive_timeout)
                now = time.monotonic()
                expired = [
                    user for user, last in self._last_seen.items()
                    if (now - last) > self._keepalive_timeout
                ]
                for user in expired:
                    self._connected_users.discard(user)
                    self._last_seen.pop(user, None)
                    logger.warning(
                        "[SUB 超时下线] user={} timeout={}s (在线: {})",
                        user, self._keepalive_timeout, len(self._connected_users),
                    )
        except asyncio.CancelledError:
            return

    def set_auth_callback(self, callback: AuthCallback | None) -> None:
        """设置认证事件回调。"""
        self._on_auth = callback
        if self._zap is not None:
            self._zap._on_auth = callback

    async def send(self, frames: list[bytes]) -> None:
        """广播一帧消息给所有 SUB。"""
        if self._pub is None:
            raise RuntimeError("Transport 未启动")
        await self._pub.send_multipart(frames)

    async def stop(self) -> None:
        """关闭 PUB socket 和 context。"""
        # 先关闭心跳检测 task
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except (asyncio.CancelledError, Exception):
                pass
            self._keepalive_task = None

        # 关闭 disconnect PULL
        if self._disconnect_task is not None:
            self._disconnect_task.cancel()
            try:
                await self._disconnect_task
            except (asyncio.CancelledError, Exception):
                pass
            self._disconnect_task = None
        if self._disconnect_pull is not None:
            self._disconnect_pull.close(linger=0)
            self._disconnect_pull = None

        if self._pub is not None:
            self._pub.close(linger=1000)
            self._pub = None
        if self._zap is not None:
            await self._zap.stop()
            self._zap = None
        if self._ctx is not None:
            self._ctx.term()
            self._ctx = None

        # 清理在线用户
        if self._connected_users:
            logger.info("关闭时在线用户: {}", ", ".join(sorted(self._connected_users)) or "无")
            self._connected_users.clear()
            self._last_seen.clear()
        logger.info("ZMQ PUB Transport 已关闭")
