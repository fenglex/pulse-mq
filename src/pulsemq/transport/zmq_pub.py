"""ZMQ PUB socket + PLAIN 认证。

v2 简化：单一 PUB socket，无需 ROUTER/XPUB。
api_keys 非空时自动开启 ZMQ PLAIN 认证。

ZAP handler 运行在 asyncio 事件循环中（与 PUB socket 同 context），
避免跨线程 inproc:// 的兼容性问题。
"""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Awaitable, Callable

import zmq
import zmq.asyncio

logger = logging.getLogger(__name__)

AuthCallback = Callable[[str, str, bool], Awaitable[None]]


def _notice(msg: str) -> None:
    """关键认证事件直接输出到 stderr，不依赖用户配置 logging。

    Python 默认 logging 无 handler 时，info/warning 级日志会被 lastResort
    吞掉，导致用户看不到 sub 上线/认证失败提示。这些是面向运维的关键可观测
    输出，应始终可见，故用 print 而非 logging。
    """
    print(msg, file=sys.stderr, flush=True)


class AsyncZAPHandler:
    """ZMQ PLAIN 认证的 ZAP handler（asyncio 版）。

    与 PUB socket 共享同一个 zmq.asyncio.Context，
    在 asyncio 事件循环中处理 ZAP 请求。
    """

    def __init__(
        self,
        api_keys: dict[str, str],
        ctx: zmq.asyncio.Context,
        on_auth: AuthCallback | None = None,
    ) -> None:
        self._api_keys = api_keys
        self._ctx = ctx
        self._on_auth = on_auth
        self._zap: zmq.asyncio.Socket | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """启动 ZAP handler。"""
        self._zap = self._ctx.socket(zmq.REP)
        self._zap.bind("inproc://zeromq.zap.01")
        self._task = asyncio.create_task(self._loop())
        logger.info("ZAP handler 启动: %d 个白名单用户", len(self._api_keys))

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
            # domain = msg[2]
            # address = msg[3] —— 客户端 ip:port
            # identity = msg[4]
            mechanism = msg[5]
            client_addr = msg[3].decode("utf-8", errors="replace") if len(msg) > 3 else "unknown"
            username = msg[6].decode("utf-8", errors="replace") if len(msg) > 6 else ""
            password = msg[7].decode("utf-8", errors="replace") if len(msg) > 7 else ""

            if mechanism != b"PLAIN":
                _notice(
                    f"[SUB 认证失败] user={username or '<empty>'} addr={client_addr} "
                    f"auth=FAIL reason=not-PLAIN mechanism={mechanism.decode('utf-8', 'replace')}"
                )
                await self._send_zap_reply(request_id, b"400", b"Not PLAIN")
                continue

            # 白名单校验
            expected = self._api_keys.get(username)
            if expected is not None and expected == password:
                _notice(f"[SUB 上线] user={username} addr={client_addr} auth=OK")
                await self._notify_auth(username, client_addr, True)
                await self._send_zap_reply(
                    request_id, b"200", b"OK", user_id=username.encode(),
                )
            else:
                _notice(
                    f"[SUB 认证失败] user={username or '<empty>'} addr={client_addr} "
                    f"auth=FAIL reason=invalid-credentials"
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
    """ZMQ PUB socket + PLAIN 认证。"""

    def __init__(
        self,
        bind: str = "tcp://*:5555",
        api_keys: dict[str, str] | None = None,
        on_auth: AuthCallback | None = None,
    ) -> None:
        self._bind = bind
        self._api_keys = api_keys or {}
        self._on_auth = on_auth
        self._ctx: zmq.asyncio.Context | None = None
        self._pub: zmq.asyncio.Socket | None = None
        self._zap: AsyncZAPHandler | None = None

    async def start(self) -> None:
        """启动 PUB socket，可选开启 PLAIN 认证。"""
        self._ctx = zmq.asyncio.Context()
        self._pub = self._ctx.socket(zmq.PUB)
        self._pub.setsockopt(zmq.SNDHWM, 0)  # 0=无上限，burst 模式不丢消息
        self._pub.setsockopt(zmq.LINGER, 1000)

        if self._api_keys:
            # ZAP handler 必须在 PUB bind 之前启动
            self._zap = AsyncZAPHandler(self._api_keys, self._ctx, self._on_auth)
            await self._zap.start()
            self._pub.setsockopt(zmq.PLAIN_SERVER, 1)

        self._pub.bind(self._bind)
        logger.info("PUB socket 绑定到 %s (auth=%s)", self._bind, "on" if self._api_keys else "off")

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
        if self._pub is not None:
            self._pub.close(linger=1000)
            self._pub = None
        if self._zap is not None:
            await self._zap.stop()
            self._zap = None
        if self._ctx is not None:
            self._ctx.term()
            self._ctx = None
        logger.info("ZMQ PUB Transport 已关闭")
