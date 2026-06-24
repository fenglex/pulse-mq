"""PulseSubscriber: 订阅端客户端。

用法:
    sub = PulseSubscriber("tcp://host:5555", username="user1", password="pulse_sk_xxx")
    async with sub:
        async for msg in sub.subscribe("sh_market_data"):
            print(msg.topic, msg.payload, msg.timestamp_ns)
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import AsyncIterator

import zmq
import zmq.asyncio

from pulsemq.protocol.frames import PulseMessage, decode

logger = logging.getLogger(__name__)


class PulseSubscriberError(Exception):
    """订阅端异常基类。"""


class AuthenticationError(PulseSubscriberError):
    """PLAIN 认证被拒绝（ZAP 返回 400）。

    Attributes:
        username: 尝试认证的用户名。
        address: 发布端地址（tcp://host:port）。
    """

    def __init__(self, message: str, username: str = "", address: str = "") -> None:
        super().__init__(message)
        self.username = username
        self.address = address


class ConnectionLostError(PulseSubscriberError):
    """连接意外断开（心跳超时或 TCP 断开）。"""


class PulseSubscriber:
    """订阅端客户端。"""

    def __init__(
        self,
        address: str = "tcp://localhost:5555",
        *,
        username: str = "",
        password: str = "",
        receive_timeout: int | None = None,
    ) -> None:
        self._address = address
        self._username = username
        self._password = password
        self._receive_timeout = receive_timeout
        self._ctx: zmq.asyncio.Context | None = None
        self._sub: zmq.asyncio.Socket | None = None
        self._closed_by_user = False

    async def connect(self) -> None:
        """连接 PUB socket，PLAIN 认证。"""
        self._ctx = zmq.asyncio.Context()
        self._sub = self._ctx.socket(zmq.SUB)
        self._sub.setsockopt(zmq.RCVHWM, 0)  # 0=无上限
        if self._receive_timeout is not None:
            self._sub.setsockopt(zmq.RCVTIMEO, self._receive_timeout)

        if self._username:
            self._sub.setsockopt(zmq.PLAIN_USERNAME, self._username.encode())
            self._sub.setsockopt(zmq.PLAIN_PASSWORD, self._password.encode())

        self._sub.connect(self._address)
        logger.info("Subscriber 连接到 %s (auth=%s)", self._address, "on" if self._username else "off")

    async def subscribe(
        self, *topics: str,
        heartbeat_timeout: float = 90.0,
    ) -> AsyncIterator[PulseMessage]:
        """订阅 topic，返回异步迭代器。

        Args:
            *topics: 要订阅的 topic 列表。
            heartbeat_timeout: 心跳超时时间（秒），默认 90s。
                设为 0 禁用心跳检测。超时后抛 ConnectionLostError。
        """
        if self._sub is None:
            raise RuntimeError("Subscriber 未连接")

        for t in topics:
            self._sub.setsockopt(zmq.SUBSCRIBE, t.encode("utf-8"))
            logger.info("订阅 topic: %s", t)

        # 心跳检测：自动订阅内部心跳 topic，用 ZMQ RCVTIMEO 做轮询
        _hb_enabled = heartbeat_timeout > 0
        if _hb_enabled:
            self._sub.setsockopt(zmq.SUBSCRIBE, b"__pulse_hb__")
            last_recv: float | None = None  # 第一条消息到达后才开始计时
            if self._receive_timeout is None:
                self._sub.setsockopt(zmq.RCVTIMEO, 1000)
            logger.info("心跳检测已启用 (timeout=%.0fs)", heartbeat_timeout)

        while True:
            try:
                frames = await self._sub.recv_multipart()

                if len(frames) != 4:
                    continue

                # 检查是否为 PING 帧（内部心跳，不暴露给用户）
                if _hb_enabled:
                    msg_type = frames[1][0]
                    if msg_type == 0x02:  # MsgType.PING
                        last_recv = time.monotonic()
                        continue

                # DATA 帧：刷新心跳计时器
                if _hb_enabled:
                    last_recv = time.monotonic()

                yield decode(frames)

            except zmq.ZMQError as e:
                # RCVTIMEO 超时（EAGAIN）→ 检查心跳
                if _hb_enabled and e.errno == zmq.EAGAIN:
                    if last_recv is not None:
                        elapsed = time.monotonic() - last_recv
                        if elapsed > heartbeat_timeout:
                            raise ConnectionLostError(
                                f"心跳超时: 已 {elapsed:.0f}s 未收到消息"
                            )
                    continue
                if self._closed_by_user:
                    break
                raise ConnectionLostError("与 Publisher 的连接已断开")
            except asyncio.CancelledError:
                # 调用方取消迭代：清理 socket 后重新抛出，遵守 asyncio 取消协议
                if self._sub is not None:
                    self._closed_by_user = True
                    self._sub.close(linger=0)
                    self._sub = None
                raise

    async def close(self) -> None:
        """关闭连接。"""
        if self._sub is not None:
            self._closed_by_user = True
            self._sub.close(linger=1000)
            self._sub = None
        if self._ctx is not None:
            self._ctx.term()
            self._ctx = None
        logger.info("Subscriber 已关闭")

    # ---- 上下文管理器 ----

    async def __aenter__(self) -> PulseSubscriber:
        await self.connect()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()
