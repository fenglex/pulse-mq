"""ZMQ ROUTER + XPUB 传输适配器。

ROUTER socket: 接收客户端 DEALER 消息（控制路径）
XPUB socket:  广播给 SUB 订阅者（数据路径）
"""

from __future__ import annotations

import logging
from typing import Awaitable, Callable

import zmq
import zmq.asyncio

from pulsemq.config import BrokerConfig

logger = logging.getLogger(__name__)


class ZmqTransport:
    """ZMQ 传输层，管理 ROUTER + XPUB 两个 socket。"""

    def __init__(self, config: BrokerConfig):
        self._config = config
        self._ctx: zmq.asyncio.Context | None = None
        self._router: zmq.asyncio.Socket | None = None
        self._xpub: zmq.asyncio.Socket | None = None

    async def start(self) -> None:
        """启动 ZMQ socket 并绑定。"""
        self._ctx = zmq.asyncio.Context()

        # ROUTER socket：接收客户端消息
        self._router = self._ctx.socket(zmq.ROUTER)
        self._router.setsockopt(zmq.RCVHWM, self._config.zmq_rcvhwm)
        self._router.setsockopt(zmq.SNDHWM, self._config.zmq_sndhwm)
        self._router.setsockopt(zmq.IMMEDIATE, 1)
        # 心跳配置
        self._router.setsockopt(zmq.HEARTBEAT_IVL, self._config.zmq_heartbeat_ivl)
        self._router.setsockopt(zmq.HEARTBEAT_TIMEOUT, self._config.zmq_heartbeat_timeout)
        self._router.setsockopt(zmq.HEARTBEAT_TTL, self._config.zmq_heartbeat_ttl)
        self._router.setsockopt(zmq.ROUTER_MANDATORY, 0)
        self._router.bind(self._config.bind)
        logger.info("ROUTER 绑定到 %s", self._config.bind)

        # XPUB socket：广播给订阅者
        self._xpub = self._ctx.socket(zmq.XPUB)
        self._xpub.setsockopt(zmq.SNDHWM, self._config.zmq_sndhwm)
        self._xpub.setsockopt(zmq.IMMEDIATE, 1)
        self._xpub.bind(self._config.xpub_bind)
        logger.info("XPUB 绑定到 %s", self._config.xpub_bind)

    async def recv(self) -> list[bytes]:
        """接收一条 ROUTER 消息（6 帧）。"""
        if self._router is None:
            raise RuntimeError("Transport 未启动")
        frames = await self._router.recv_multipart()
        return frames

    async def send(self, identity: bytes, frames: list[bytes]) -> None:
        """通过 ROUTER 发送消息给特定客户端。"""
        if self._router is None:
            raise RuntimeError("Transport 未启动")
        await self._router.send_multipart([identity, b""] + frames)

    async def broadcast(self, frames: list[bytes]) -> None:
        """通过 XPUB 广播消息给所有订阅者。"""
        if self._xpub is None:
            raise RuntimeError("Transport 未启动")
        await self._xpub.send_multipart(frames)

    async def stop(self) -> None:
        """关闭 ZMQ socket 和 context。"""
        if self._router is not None:
            self._router.close(linger=0)
            self._router = None
        if self._xpub is not None:
            self._xpub.close(linger=0)
            self._xpub = None
        if self._ctx is not None:
            self._ctx.term()
            self._ctx = None
        logger.info("ZMQ Transport 已关闭")
