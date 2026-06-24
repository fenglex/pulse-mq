"""PulseSubscriber: 订阅端客户端。

用法:
    sub = PulseSubscriber("tcp://host:5555", username="user1", password="pulse_sk_xxx")
    async with sub:
        async for msg in sub.subscribe("sh_market_data"):
            print(msg.topic, msg.payload, msg.timestamp_ns)

认证失败（凭证错误）时，subscribe() 会打 error 日志并自动结束迭代，
``async for`` 自然退出，**无需用户 try/except**：

    async with sub:
        async for msg in sub.subscribe(...):   # 认证失败时这里会自动结束
            ...
    # 看到 [SUB 认证失败] 日志即知道是凭证问题
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import AsyncIterator

import zmq
import zmq.asyncio

from pulsemq.protocol.frames import PulseMessage, decode
from pulsemq.protocol.msg_type import MsgType

logger = logging.getLogger(__name__)


class PulseSubscriber:
    """订阅端客户端。"""

    def __init__(
        self,
        address: str = "tcp://localhost:5555",
        *,
        username: str = "",
        password: str = "",
    ) -> None:
        self._address = address
        self._username = username
        self._password = password
        self._ctx: zmq.asyncio.Context | None = None
        self._sub: zmq.asyncio.Socket | None = None
        # 握手监控 socket：仅开启 PLAIN 认证时启用，监听握手成功 + 各类失败事件
        self._mon: zmq.asyncio.Socket | None = None
        # 握手结果通知事件：后台 monitor task 置位，主循环 recv 时竞争检测。
        # None=未决，"ok"=成功，"fail"=失败（凭证错误/非 PLAIN/协议错误等）。
        self._handshake_result: str | None = None
        self._handshake_evt: asyncio.Event | None = None

    async def connect(self) -> None:
        """连接 PUB socket，PLAIN 认证。"""
        self._ctx = zmq.asyncio.Context()
        self._sub = self._ctx.socket(zmq.SUB)
        self._sub.setsockopt(zmq.RCVHWM, 0)  # 0=无上限

        if self._username:
            self._sub.setsockopt(zmq.PLAIN_USERNAME, self._username.encode())
            self._sub.setsockopt(zmq.PLAIN_PASSWORD, self._password.encode())

        self._sub.connect(self._address)
        if self._username:
            # 仅开启认证时启用 monitor：监听握手结果（成功 + 各类失败）。
            # pyzmq 在 ZAP 拒绝时 recv 不抛错，只能靠 monitor 检测，否则会卡死。
            # 掩码用位 OR 组合多个事件：
            #   HANDSHAKE_SUCCEEDED    认证通过（含 PLAIN ZAP 成功）
            #   HANDSHAKE_FAILED_AUTH  凭证错误
            #   HANDSHAKE_FAILED_PROTOCOL / NO_DETAIL  非 PLAIN 等其它失败
            handshake_mask = (
                zmq.EVENT_HANDSHAKE_SUCCEEDED
                | zmq.EVENT_HANDSHAKE_FAILED_AUTH
                | zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL
                | zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL
            )
            self._mon = self._sub.get_monitor_socket(handshake_mask)
            logger.info(
                "Subscriber 连接到 %s (auth=on, user=%s)", self._address, self._username
            )
        else:
            logger.info("Subscriber 连接到 %s (auth=off)", self._address)

    async def subscribe(self, *topics: str) -> AsyncIterator[PulseMessage]:
        """订阅 topic，返回异步迭代器。

        认证场景下：握手成功会打 info 日志（[SUB 上线]），
        握手失败（凭证错误/非 PLAIN 等）打 error 日志并自动结束迭代，
        ``async for`` 自然退出，**无需用户 try/except**。
        """
        if self._sub is None:
            raise RuntimeError("Subscriber 未连接")

        for t in topics:
            self._sub.setsockopt(zmq.SUBSCRIBE, t.encode("utf-8"))
            logger.info("订阅 topic: %s", t)

        # 认证场景：启动后台 monitor 监听握手结果
        mon_task: asyncio.Task | None = None
        if self._mon is not None:
            self._handshake_evt = asyncio.Event()
            mon_task = asyncio.create_task(self._watch_handshake())

        try:
            while True:
                if self._handshake_evt is not None:
                    # 认证场景：recv 和"握手结果"事件竞争，谁先就绪处理谁。
                    # 否则 recv 会无限阻塞，握手失败/成功均无法被检查到。
                    recv_task = asyncio.ensure_future(self._sub.recv_multipart())
                    evt_task = asyncio.ensure_future(self._handshake_evt.wait())
                    try:
                        await asyncio.wait(
                            [recv_task, evt_task],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if self._handshake_evt.is_set():
                            # 握手结果已出（由 _watch_handshake 设置 _handshake_result
                            # 并打日志）。失败则结束迭代，成功则继续正常接收。
                            if self._handshake_result == "fail":
                                break
                            # 成功：日志已由 _watch_handshake 打印，继续 recv。
                            # 把 evt 标记为 None 后续走纯 recv 路径，避免重复竞争。
                            self._handshake_evt = None
                            # 等待已发起的 recv_task（若有）或继续下一轮
                            if recv_task.done() and not recv_task.cancelled():
                                frames = recv_task.result()
                            else:
                                continue
                        else:
                            frames = recv_task.result()
                    finally:
                        # 清理未完成的 task，避免 awaitable 泄漏
                        for t in (recv_task, evt_task):
                            if not t.done():
                                t.cancel()
                                with contextlib.suppress(asyncio.CancelledError, Exception):
                                    await t
                else:
                    # 无认证场景 或 握手已成功：直接 recv
                    try:
                        frames = await self._sub.recv_multipart()
                    except zmq.ZMQError:
                        break
                    except asyncio.CancelledError:
                        if self._sub is not None:
                            self._sub.close(linger=0)
                            self._sub = None
                        raise

                if len(frames) == 4:
                    # 过滤心跳帧（PING 控制帧）：meta[0] 为消息类型，PING 不交付给用户。
                    # 心跳是协议控制帧，空 payload 经反序列化会崩，也不属于业务消息流。
                    if frames[1][0] == MsgType.PING:
                        continue
                    yield decode(frames)
        finally:
            if mon_task is not None:
                mon_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await mon_task
            self._handshake_evt = None
            self._handshake_result = None

    async def _watch_handshake(self) -> None:
        """后台监听 monitor socket，收到握手结果事件就 set 通知 + 打日志。

        事件类型区分：
        - EVENT_HANDSHAKE_SUCCEEDED：PLAIN 认证通过 → 打 info「[SUB 上线]」
        - 任意 HANDSHAKE_FAILED_*：认证失败 → 打 error「[SUB 认证失败]」

        日志在此统一打印（成功/失败都打），subscribe() 主循环只据
        _handshake_result 决定是否结束迭代，避免重复打日志。
        """
        assert self._mon is not None
        assert self._handshake_evt is not None
        try:
            # monitor 事件帧格式：[uint16 event | uint32 value]（小端）
            data = await self._mon.recv_multipart()
            # 取出事件码（前 2 字节小端 uint16）
            event_code = int.from_bytes(data[0][:2], "little")
            if event_code == zmq.EVENT_HANDSHAKE_SUCCEEDED:
                self._handshake_result = "ok"
                logger.info(
                    "[SUB 上线] 认证成功，订阅就绪 (user=%r, addr=%r)",
                    self._username, self._address,
                )
            else:
                # 任意失败：凭证错误 / 非 PLAIN / 协议错误
                self._handshake_result = "fail"
                logger.error(
                    "[SUB 认证失败] PLAIN 握手被服务端拒绝，已停止订阅 "
                    "(user=%r, addr=%r)。请检查 username/password 或联系 publisher 管理员。",
                    self._username, self._address,
                )
            self._handshake_evt.set()
        except (zmq.ZMQError, asyncio.CancelledError):
            # socket 关闭或任务取消：正常退出
            return

    async def close(self) -> None:
        """关闭连接。"""
        # ⚠️ monitor socket 必须在它监控的 SUB socket 之前关闭，
        # 否则 ctx.term() 会卡死（pyzmq 已知行为）
        if self._mon is not None:
            self._mon.close(linger=0)
            self._mon = None
        if self._sub is not None:
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
