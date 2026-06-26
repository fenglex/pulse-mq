# src/pulsemq/transport/router.py
"""Transport：ROUTER/DEALER + ZAP PLAIN + monitor。唯一 import zmq 的模块。"""
from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

import zmq
import zmq.asyncio

from pulsemq.logging_setup import logger

AuthCallback = Callable[[str, str, bool], Awaitable[None]]


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
        ok, reason = self._auth.verify(username, password)
        status = b"200" if ok else b"400"
        text = b"OK" if ok else b"INVALID"
        await self._reply(request_id, status, text, user_id=username.encode() if ok else b"")
        if self._on_auth:
            try:
                await self._on_auth(username, "", ok)
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


class Transport:
    """数据面/控制面 ROUTER(serve) 或 DEALER(client)。"""

    def __init__(self, ctx: zmq.asyncio.Context | None = None) -> None:
        self._ctx = ctx or zmq.asyncio.Context.instance()
        self._sockets: dict[str, zmq.asyncio.Socket] = {}
        self._zaps: list[AsyncZAPHandler] = []
        self._monitors: list[zmq.asyncio.Socket] = []
        self._monitor_tasks: list[asyncio.Task] = []
        self._on_monitor: Callable[[str], Awaitable[None]] | None = None
        # ZAP 是 context 级单例：同一 ctx 上所有 plain_server=True 的 socket
        # 共享同一个 inproc://zeromq.zap.01 REP socket。仅在首次 auth bind 时启动。
        self._zap_started = False

    def set_monitor_callback(self, cb: Callable[[str], Awaitable[None]]) -> None:
        self._on_monitor = cb

    async def bind(self, endpoint: str, role: str,
                   *, auth: PlainAuthDict | None = None) -> None:
        sock = self._ctx.socket(zmq.ROUTER)
        sock.setsockopt(zmq.LINGER, 1000)
        sock.setsockopt(zmq.ROUTER_MANDATORY, 1)
        if auth is not None:
            sock.plain_server = True
            # ZAP REP socket 绑定的是 inproc 单例端点，同一 ctx 只能 bind 一次。
            # 多个 ROUTER socket（数据面/控制面）共享同一 ZAP handler。
            if not self._zap_started:
                zap = AsyncZAPHandler(self._ctx, auth)
                await zap.start()
                self._zaps.append(zap)
                self._zap_started = True
        sock.bind(endpoint)
        self._sockets[role] = sock

    async def connect(self, endpoint: str, role: str,
                      credentials: tuple[str, str] | None = None,
                      *, monitor: bool = True) -> None:
        sock = self._ctx.socket(zmq.DEALER)
        sock.setsockopt(zmq.LINGER, 1000)
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
