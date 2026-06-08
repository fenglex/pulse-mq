"""ZMQ PUB socket + PLAIN 认证。

v2 简化：单一 PUB socket，无需 ROUTER/XPUB。
api_keys 非空时自动开启 ZMQ PLAIN 认证。

ZAP handler 运行在 asyncio 事件循环中（与 PUB socket 同 context），
避免跨线程 inproc:// 的兼容性问题。
"""

from __future__ import annotations

import asyncio
import logging

import zmq
import zmq.asyncio

logger = logging.getLogger(__name__)


class AsyncZAPHandler:
    """ZMQ PLAIN 认证的 ZAP handler（asyncio 版）。

    与 PUB socket 共享同一个 zmq.asyncio.Context，
    在 asyncio 事件循环中处理 ZAP 请求。
    """

    def __init__(self, api_keys: dict[str, str], ctx: zmq.asyncio.Context) -> None:
        self._api_keys = api_keys
        self._ctx = ctx
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
                self._zap.send_multipart([
                    msg[1] if len(msg) > 1 else b"",
                    b"400",
                    b"Invalid ZAP request",
                    b"",
                    b"",
                ])
                continue

            version = msg[0]
            request_id = msg[1]
            # domain = msg[2]
            # address = msg[3]
            # identity = msg[4]
            mechanism = msg[5]
            username = msg[6].decode("utf-8", errors="replace") if len(msg) > 6 else ""
            password = msg[7].decode("utf-8", errors="replace") if len(msg) > 7 else ""

            if mechanism != b"PLAIN":
                self._zap.send_multipart([version, request_id, b"400", b"Not PLAIN", b"", b""])
                continue

            # 白名单校验
            expected = self._api_keys.get(username)
            if expected is not None and expected == password:
                self._zap.send_multipart([version, request_id, b"200", b"OK", username.encode(), b""])
            else:
                logger.warning("ZAP 拒绝: username=%s", username)
                self._zap.send_multipart([version, request_id, b"400", b"Invalid credentials", b"", b""])


class ZmqPubTransport:
    """ZMQ PUB socket + PLAIN 认证。"""

    def __init__(self, bind: str = "tcp://*:5555", api_keys: dict[str, str] | None = None) -> None:
        self._bind = bind
        self._api_keys = api_keys or {}
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
            self._zap = AsyncZAPHandler(self._api_keys, self._ctx)
            await self._zap.start()
            self._pub.setsockopt(zmq.PLAIN_SERVER, 1)

        self._pub.bind(self._bind)
        logger.info("PUB socket 绑定到 %s (auth=%s)", self._bind, "on" if self._api_keys else "off")

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
