# src/pulsemq/transport/router.py
"""Transport：ROUTER/DEALER + ZAP PLAIN + monitor。唯一 import zmq 的模块。"""
from __future__ import annotations

import asyncio
import threading
from typing import Awaitable, Callable

import zmq
import zmq.asyncio

from pulsemq.logging_setup import logger

AuthCallback = Callable[[str, str, bool, "str | None"], Awaitable[None]]


class PlainAuthDict:
    """Spec 1 最简凭据源：明文 dict 白名单。Spec 2 替换为 security.CredentialStore。"""

    def __init__(self, credentials: dict[str, str] | None = None) -> None:
        self._creds = dict(credentials or {})

    def verify(self, username: str, password: str) -> tuple[bool, str | None]:
        if username not in self._creds:
            return False, "user_not_found"
        if self._creds[username] != password:
            return False, "invalid_password"
        return True, None


class AsyncZAPHandler:
    """inproc ZAP PLAIN 认证。沿用 v3.1.1 修复：统一 await + 异常保护。"""

    def __init__(self, ctx: zmq.asyncio.Context, auth: PlainAuthDict,
                 on_auth: AuthCallback | None = None) -> None:
        self._ctx = ctx
        self._auth = auth
        self._on_auth = on_auth
        self._socket: zmq.asyncio.Socket | None = None
        self._task: asyncio.Task | None = None
        self._stopping = False

    async def start(self) -> None:
        self._socket = self._ctx.socket(zmq.REP)
        self._socket.bind("inproc://zeromq.zap.01")
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._stopping = True
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        if self._socket:
            self._socket.close(linger=0)

    async def _loop(self) -> None:
        assert self._socket is not None
        while not self._stopping:
            try:
                msg = await self._socket.recv_multipart()
            except zmq.ContextTerminated:
                break
            except asyncio.CancelledError:
                break
            except Exception:
                continue
            await self._handle(msg)

    async def _handle(self, msg: list[bytes]) -> None:
        # ZAP 帧：version, request_id, domain, address, identity, mechanism, credentials...
        if len(msg) < 7:
            return
        request_id = msg[1]
        username = msg[6].decode("utf-8", "replace") if len(msg) > 6 else ""
        password = msg[7].decode("utf-8", "replace") if len(msg) > 7 else ""
        # verify 包含 bcrypt.checkpw（~200ms 同步阻塞），抛到线程池避免阻塞事件循环。
        loop = asyncio.get_running_loop()
        ok, reason = await loop.run_in_executor(None, self._auth.verify, username, password)
        status = b"200" if ok else b"400"
        text = b"OK" if ok else b"INVALID"
        await self._reply(request_id, status, text, user_id=username.encode() if ok else b"")
        if self._on_auth:
            try:
                await self._on_auth(username, "", ok, reason)
            except Exception:
                logger.exception("on_auth 回调异常")

    async def _reply(self, request_id: bytes, status: bytes, text: bytes,
                     *, user_id: bytes = b"") -> None:
        assert self._socket is not None
        # ZAP 响应必须为 6 帧：
        # [version, request_id, status_code, status_text, user_id, metadata]
        frames = [b"1.0", request_id, status, text, user_id, b""]
        try:
            await self._socket.send_multipart(frames)
        except Exception:
            pass  # 单次 send 失败不杀循环


class SyncZAPHandler:
    """同步 ZAP PLAIN 认证 handler（独立线程，用于同步数据面）。

    bcrypt.checkpw 在本线程中直接调用（~200ms 阻塞但不影响事件循环）。
    on_auth 回调通过 run_coroutine_threadsafe 调度到主线程事件循环。
    """

    def __init__(self, ctx: zmq.Context, auth: PlainAuthDict,
                 on_auth: AuthCallback | None = None,
                 loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._ctx = ctx
        self._auth = auth
        self._on_auth = on_auth
        self._async_loop = loop
        self._socket: zmq.Socket | None = None
        self._thread: threading.Thread | None = None
        self._running = False

    def start(self) -> None:
        self._socket = self._ctx.socket(zmq.REP)
        self._socket.bind("inproc://zeromq.zap.01")
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        # 不 close socket：Windows 上 bundled libzmq close 同步 ctx 的 socket
        # 触发 signaler Assertion failed。线程用 Poller 轮询，100ms 内退出。
        if self._thread:
            self._thread.join(timeout=5)
        self._socket = None

    def _loop(self) -> None:
        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)
        while self._running:
            events = dict(poller.poll(timeout=100))
            if self._socket in events:
                try:
                    msg = self._socket.recv_multipart(zmq.NOBLOCK)
                except zmq.Again:
                    continue
                self._handle(msg)

    def _handle(self, msg: list[bytes]) -> None:
        if len(msg) < 7:
            return
        request_id = msg[1]
        username = msg[6].decode("utf-8", "replace") if len(msg) > 6 else ""
        password = msg[7].decode("utf-8", "replace") if len(msg) > 7 else ""
        ok, reason = self._auth.verify(username, password)
        status = b"200" if ok else b"400"
        text = b"OK" if ok else b"INVALID"
        reply = [b"1.0", request_id, status, text,
                 username.encode() if ok else b"", b""]
        try:
            self._socket.send_multipart(reply)
        except Exception:
            pass
        if self._on_auth and self._async_loop:
            try:
                asyncio.run_coroutine_threadsafe(
                    self._on_auth(username, "", ok, reason), self._async_loop
                )
            except Exception:
                pass


class SyncDataThread:
    """同步数据面线程：ROUTER recv -> on_message 回调 -> ROUTER send。

    用 zmq.Poller 同时监听 ROUTER（recv）和 PULL（来自主线程的发送请求），
    所有 socket 操作都在本线程内完成，线程安全。
    主线程通过 send_from_main() 经 PUSH -> PULL 投递发送请求。
    """

    def __init__(self, ctx: zmq.Context, endpoint: str,
                 auth: PlainAuthDict | None = None,
                 on_auth: AuthCallback | None = None,
                 loop: asyncio.AbstractEventLoop | None = None,
                 sndhwm: int = 10000, rcvhwm: int = 10000) -> None:
        self._ctx = ctx
        self._endpoint = endpoint
        self._auth = auth
        self._on_auth = on_auth
        self._async_loop = loop
        self._sndhwm = sndhwm
        self._rcvhwm = rcvhwm
        self._socket: zmq.Socket | None = None
        self._zap: SyncZAPHandler | None = None
        self._pull: zmq.Socket | None = None
        self._push: zmq.Socket | None = None
        self._thread: threading.Thread | None = None
        self._running = False
        self._on_message: Callable[[bytes, bytes], None] | None = None

    def start(self, on_message: Callable[[bytes, bytes], None]) -> None:
        self._socket = self._ctx.socket(zmq.ROUTER)
        self._socket.setsockopt(zmq.LINGER, 1000)
        self._socket.setsockopt(zmq.ROUTER_MANDATORY, 1)
        self._socket.setsockopt(zmq.SNDHWM, self._sndhwm)
        self._socket.setsockopt(zmq.RCVHWM, self._rcvhwm)
        if self._auth is not None:
            self._socket.plain_server = True
            self._zap = SyncZAPHandler(self._ctx, self._auth,
                                       on_auth=self._on_auth, loop=self._async_loop)
            self._zap.start()
        self._socket.bind(self._endpoint)
        # PUSH/PULL 对：主线程 -> 数据面线程的发送请求通道
        pull_addr = f"inproc://sync_data_pull_{id(self)}"
        self._pull = self._ctx.socket(zmq.PULL)
        self._pull.bind(pull_addr)
        self._push = self._ctx.socket(zmq.PUSH)
        self._push.connect(pull_addr)
        self._on_message = on_message
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        poller = zmq.Poller()
        poller.register(self._socket, zmq.POLLIN)
        poller.register(self._pull, zmq.POLLIN)
        while self._running:
            events = dict(poller.poll(timeout=100))  # 100ms 超时以便检查 _running
            if self._socket in events:
                try:
                    parts = self._socket.recv_multipart(zmq.NOBLOCK)
                except zmq.Again:
                    pass
                else:
                    if len(parts) >= 2:
                        try:
                            self._on_message(parts[0], parts[-1])
                        except Exception:
                            pass
            if self._pull in events:
                while True:
                    try:
                        msg = self._pull.recv_multipart(zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    if len(msg) >= 2:
                        try:
                            self._socket.send_multipart([msg[0], msg[1]])
                        except Exception:
                            pass

    def send(self, ident: bytes, frame_bytes: bytes) -> None:
        """数据面线程内调用（从 on_message 回调）：直接通过 ROUTER socket 发送。"""
        self._socket.send_multipart([ident, frame_bytes])

    def send_from_main(self, ident: bytes, frame_bytes: bytes) -> None:
        """主线程调用：通过 PUSH -> PULL 投递到数据面线程。"""
        self._push.send_multipart([ident, frame_bytes])

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        if self._zap:
            self._zap.stop()
        # 不 close socket：Windows 上 close 同步 ctx 的 socket 触发
        # signaler Assertion failed。线程用 Poller 轮询，100ms 内退出。
        self._socket = None
        self._pull = None
        self._push = None


class Transport:
    """数据面/控制面 ROUTER(serve) 或 DEALER(client)。"""

    def __init__(self, ctx: zmq.asyncio.Context | None = None,
                 *, sndhwm: int = 10000, rcvhwm: int = 10000) -> None:
        self._ctx = ctx or zmq.asyncio.Context.instance()
        self._sockets: dict[str, zmq.asyncio.Socket] = {}
        self._zaps: list[AsyncZAPHandler] = []
        self._monitors: list[zmq.asyncio.Socket] = []
        self._monitor_tasks: list[asyncio.Task] = []
        self._on_monitor: Callable[[str], Awaitable[None]] | None = None
        # ZAP 是 context 级单例：同一 ctx 上所有 plain_server=True 的 socket
        # 共享同一个 inproc://zeromq.zap.01 REP socket。仅在首次 auth bind 时启动。
        self._zap_started = False
        self._sndhwm = sndhwm
        self._rcvhwm = rcvhwm
        # 同步数据面线程（低延迟模式）；None 表示未启用
        self._sync_data: SyncDataThread | None = None

    def set_monitor_callback(self, cb: Callable[[str], Awaitable[None]]) -> None:
        self._on_monitor = cb

    # ---- 同步数据面（低延迟模式）----

    def bind_sync_data(self, endpoint: str,
                       *, auth: PlainAuthDict | None = None,
                       on_auth: AuthCallback | None = None,
                       on_message: Callable[[bytes, bytes], None] | None = None,
                       loop: asyncio.AbstractEventLoop | None = None) -> None:
        """启动同步数据面线程（独立 zmq.Context + 独立线程）。

        与异步 bind() 完全隔离：使用单独的同步 ctx，ZAP 各自独立。
        on_message 回调在数据面线程中执行，可调用 send_sync_direct() 转发消息。
        """
        sync_ctx = zmq.Context()
        self._sync_data = SyncDataThread(
            ctx=sync_ctx, endpoint=endpoint, auth=auth, on_auth=on_auth,
            loop=loop, sndhwm=self._sndhwm, rcvhwm=self._rcvhwm,
        )
        self._sync_data.start(on_message)

    def send_sync_direct(self, ident: bytes, frame_bytes: bytes) -> None:
        """数据面线程内调用（从 on_message 回调）：直接通过 ROUTER socket 发送。"""
        if self._sync_data:
            self._sync_data.send(ident, frame_bytes)

    def send_sync_data(self, ident: bytes, frame_bytes: bytes) -> None:
        """主线程调用（内置 producer）：通过 PUSH -> PULL 投递到数据面线程。"""
        if self._sync_data:
            self._sync_data.send_from_main(ident, frame_bytes)

    # ---- 异步 bind/connect ----

    async def bind(self, endpoint: str, role: str,
                   *, auth: PlainAuthDict | None = None,
                   on_auth: AuthCallback | None = None) -> None:
        sock = self._ctx.socket(zmq.ROUTER)
        sock.setsockopt(zmq.LINGER, 1000)
        sock.setsockopt(zmq.ROUTER_MANDATORY, 1)
        sock.setsockopt(zmq.SNDHWM, self._sndhwm)
        sock.setsockopt(zmq.RCVHWM, self._rcvhwm)
        if auth is not None:
            sock.plain_server = True
            # ZAP REP socket 绑定的是 inproc 单例端点，同一 ctx 只能 bind 一次。
            # 多个 ROUTER socket（数据面/控制面）共享同一 ZAP handler。
            # ZAP 是 ctx 单例：仅首次 auth bind 创建 handler，故 on_auth 必须在
            # 首次（数据面）bind 时提供；后续（控制面）bind 的 on_auth 被忽略。
            if not self._zap_started:
                zap = AsyncZAPHandler(self._ctx, auth, on_auth=on_auth)
                await zap.start()
                self._zaps.append(zap)
                self._zap_started = True
        sock.bind(endpoint)
        self._sockets[role] = sock

    async def connect(self, endpoint: str, role: str,
                      credentials: tuple[str, str] | None = None,
                      *, monitor: bool = True,
                      identity: bytes | None = None) -> None:
        sock = self._ctx.socket(zmq.DEALER)
        sock.setsockopt(zmq.LINGER, 1000)
        sock.setsockopt(zmq.SNDHWM, self._sndhwm)
        sock.setsockopt(zmq.RCVHWM, self._rcvhwm)
        # 显式设置 identity：让同一 client 的数据面/控制面两个 DEALER
        # 在各自 ROUTER 上呈现相同的 bytes identity，这样 server 的
        # routing 表（以 control 面的 ident 为 key）能直接用于数据面转发。
        if identity is not None:
            sock.setsockopt(zmq.IDENTITY, identity)
        if credentials:
            username, password = credentials
            sock.plain_username = username.encode("utf-8")
            sock.plain_password = password.encode("utf-8")
        if monitor:
            mon = sock.get_monitor_socket(
                zmq.EVENT_CONNECTED | zmq.EVENT_DISCONNECTED
                | zmq.EVENT_HANDSHAKE_FAILED_AUTH | zmq.EVENT_HANDSHAKE_SUCCEEDED
            )
            self._monitors.append(mon)
            self._monitor_tasks.append(asyncio.create_task(self._monitor_loop(mon)))
        sock.connect(endpoint)
        self._sockets[role] = sock

    async def _monitor_loop(self, mon: zmq.asyncio.Socket) -> None:
        import struct
        while True:
            try:
                evt = await mon.recv_multipart()
            except (asyncio.CancelledError, zmq.ContextTerminated):
                break
            except Exception:
                continue
            # pyzmq monitor 帧：[event:uint16, address:bytes]
            event = 0
            if evt and len(evt[0]) >= 2:
                event = struct.unpack("=H", evt[0][:2])[0]
            kind = (
                "connected" if event == zmq.EVENT_CONNECTED
                else "disconnected" if event == zmq.EVENT_DISCONNECTED
                else "auth_failed" if event == zmq.EVENT_HANDSHAKE_FAILED_AUTH
                else "handshake_ok" if event == zmq.EVENT_HANDSHAKE_SUCCEEDED
                else "other"
            )
            if self._on_monitor:
                try:
                    await self._on_monitor(kind)
                except Exception:
                    logger.exception("monitor 回调异常")

    def _socket_for(self, role: str) -> zmq.asyncio.Socket:
        """按 role 取 socket；role 不存在时，若仅有一个 socket 则回退到它。

        这让 client 侧（单 DEALER）无需每次显式传 role 即可 send/recv。
        """
        sock = self._sockets.get(role)
        if sock is not None:
            return sock
        if len(self._sockets) == 1:
            return next(iter(self._sockets.values()))
        raise KeyError(role)

    async def send(self, identity: bytes, frame_bytes: bytes, *, role: str = "server_ingress") -> None:
        sock = self._socket_for(role)
        if identity:
            await sock.send_multipart([identity, frame_bytes])
        else:
            await sock.send(frame_bytes)

    async def recv(self, role: str = "server_ingress") -> tuple[bytes, bytes]:
        sock = self._socket_for(role)
        parts = await sock.recv_multipart()
        if len(parts) == 2:
            return parts[0], parts[1]
        # DEALER 收到单帧
        return b"", parts[0]

    async def close(self) -> None:
        # 同步数据面先关（独立线程 + 独立 ctx）
        if self._sync_data:
            self._sync_data.stop()
            self._sync_data = None
        # monitor 先于业务 socket 关闭
        for t in self._monitor_tasks:
            t.cancel()
        for t in self._monitor_tasks:
            try:
                await t
            except Exception:
                pass
        self._monitor_tasks.clear()
        for m in self._monitors:
            m.close(linger=0)
        self._monitors.clear()
        for s in self._sockets.values():
            s.close(linger=1000)
        self._sockets.clear()
        for z in self._zaps:
            await z.stop()
        self._zaps.clear()
