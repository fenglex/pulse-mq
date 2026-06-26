"""PulseSubscriber: 订阅端客户端。

用法:
    sub = PulseSubscriber("tcp://host:5555", username="user1", password="pulse_sk_xxx")
    async with sub:
        async for msg in sub.subscribe("sh_market_data"):
            print(msg.topic, msg.payload, msg.timestamp_ns)

连接生命周期（上线/认证失败/断线）的关键事件通过 loguru 输出，
确保始终可见。

认证失败或 publisher 停止时，subscribe() 会自动结束迭代，
``async for`` 自然退出，**无需用户 try/except**：

    async with sub:
        async for msg in sub.subscribe(...):   # 认证失败/断线时自动结束
            ...
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import AsyncIterator

import zmq
import zmq.asyncio
from loguru import logger

from pulsemq.protocol.frames import PulseMessage, decode
from pulsemq.protocol.msg_type import MsgType

# 控制帧 magic 前缀（与 transport 端保持一致）
_DISCONNECT_MAGIC = b"__pulse_disconnect__"
_KEEPALIVE_MAGIC = b"__pulse_keepalive__"

# 关键连接事件始终监听的 monitor 掩码（位 OR）：
#   HANDSHAKE_SUCCEEDED        握手成功（含 PLAIN 认证通过）
#   HANDSHAKE_FAILED_AUTH      凭证错误
#   HANDSHAKE_FAILED_PROTOCOL  非 PLAIN 等协议失败
#   HANDSHAKE_FAILED_NO_DETAIL 其它握手失败
#   DISCONNECTED               连接断开（publisher 停止 / 网络中断）
_MONITOR_MASK = (
    zmq.EVENT_HANDSHAKE_SUCCEEDED
    | zmq.EVENT_HANDSHAKE_FAILED_AUTH
    | zmq.EVENT_HANDSHAKE_FAILED_PROTOCOL
    | zmq.EVENT_HANDSHAKE_FAILED_NO_DETAIL
    | zmq.EVENT_DISCONNECTED
)


def _derive_disconnect_address(address: str) -> str | None:
    """从 PUB 连接地址推导 disconnect PULL 地址（端口 + 1）。"""
    try:
        # tcp://host:5555 → tcp://host:5556
        base, port_str = address.rsplit(":", 1)
        port = int(port_str)
        if 1 <= port <= 65534:
            return f"{base}:{port + 1}"
    except (ValueError, IndexError):
        pass
    return None


class PulseSubscriber:
    """订阅端客户端。"""

    def __init__(
        self,
        address: str = "tcp://localhost:5555",
        *,
        username: str = "",
        password: str = "",
        keepalive_interval: float = 1.0,
    ) -> None:
        self._address = address
        self._username = username
        self._password = password
        self._keepalive_interval = keepalive_interval
        self._ctx: zmq.asyncio.Context | None = None
        self._sub: zmq.asyncio.Socket | None = None
        # 连接监控 socket：始终启用（无论是否认证），监听握手结果 + 断线事件
        self._mon: zmq.asyncio.Socket | None = None
        # 连接事件信号：后台 monitor task 置位，主循环竞争检测。
        # _event_kind 标记事件类型："ok"=握手成功, "fail"=认证失败, "disconnect"=断线
        self._event_kind: str | None = None
        self._event_sig: asyncio.Event | None = None
        # 控制通道：持久化 PUSH socket（心跳 + 断开通知）
        self._ctrl_ctx: zmq.asyncio.Context | None = None
        self._ctrl_push: zmq.asyncio.Socket | None = None
        self._ctrl_addr: str | None = None

    async def connect(self) -> None:
        """连接 PUB socket，可选 PLAIN 认证。"""
        self._ctx = zmq.asyncio.Context()
        self._sub = self._ctx.socket(zmq.SUB)
        self._sub.setsockopt(zmq.RCVHWM, 0)  # 0=无上限

        # 设置 PLAIN 认证参数
        # 只有当用户提供了凭证时才设置 PLAIN credentials
        # 注意：ZMQ PLAIN 认证的已知限制：
        #   - 如果 publisher 启用 PLAIN_SERVER 但 subscriber 未设置凭证，会绕过认证
        #   - 因此连接到有认证的 publisher 时，必须提供 username/password
        if self._username:
            self._sub.setsockopt(zmq.PLAIN_USERNAME, self._username.encode())
            self._sub.setsockopt(zmq.PLAIN_PASSWORD, self._password.encode() if self._password else b"x")

        # 始终启用 monitor：监听握手结果 + 断线。
        # 即使无认证，publisher 停止时也能靠 EVENT_DISCONNECTED 检测到并结束迭代，
        # 避免 recv 无限阻塞卡死用户代码。
        self._mon = self._sub.get_monitor_socket(_MONITOR_MASK)
        self._sub.connect(self._address)

        # 初始化控制通道 PUSH socket（心跳 + 断开通知）
        self._ctrl_addr = _derive_disconnect_address(self._address)
        if self._username and self._ctrl_addr is not None:
            self._ctrl_ctx = zmq.asyncio.Context()
            self._ctrl_push = self._ctrl_ctx.socket(zmq.PUSH)
            self._ctrl_push.setsockopt(zmq.LINGER, 500)
            self._ctrl_push.setsockopt(zmq.SNDHWM, 10)
            self._ctrl_push.connect(self._ctrl_addr)

        if self._username:
            logger.info("[SUB] 连接到 {} (auth=on, user={})", self._address, self._username)
        else:
            logger.info("[SUB] 连接到 {} (auth=off)", self._address)

    async def subscribe(self, *topics: str) -> AsyncIterator[PulseMessage]:
        """订阅 topic，返回异步迭代器。

        认证失败（凭证错误/非 PLAIN 等）或 publisher 停止/断线时：
        打提示并自动结束迭代，``async for`` 自然退出，
        **无需用户 try/except**。
        """
        if self._sub is None:
            raise RuntimeError("Subscriber 未连接")

        for t in topics:
            self._sub.setsockopt(zmq.SUBSCRIBE, t.encode("utf-8"))
            logger.info("订阅 topic: {}", t)

        # 启动后台 monitor：持续监听握手结果 + 断线事件
        mon_task: asyncio.Task | None = None
        if self._mon is not None:
            self._event_sig = asyncio.Event()
            mon_task = asyncio.create_task(self._watch_events())

        # 启动心跳 task
        hb_task: asyncio.Task | None = None
        if self._username and self._ctrl_push is not None:
            hb_task = asyncio.create_task(self._keepalive_loop())

        stop = False
        try:
            while True:
                if self._event_sig is not None:
                    # 有事件信号：recv 与事件竞争，谁先就绪处理谁。
                    # 否则纯 recv 会无限阻塞，握手失败/断线无法被检查到。
                    recv_task = asyncio.ensure_future(self._sub.recv_multipart())
                    sig_task = asyncio.ensure_future(self._event_sig.wait())
                    try:
                        await asyncio.wait(
                            [recv_task, sig_task],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if self._event_sig.is_set():
                            kind = self._event_kind
                            if kind in ("fail", "disconnect"):
                                # 认证失败 或 断线：已由 _watch_events 打日志，
                                # 结束迭代。
                                stop = True
                                break
                            # kind == "ok"：握手成功，继续接收。
                            # 重置信号，继续监听后续事件（如断线）。
                            self._event_sig = asyncio.Event()
                            if recv_task.done() and not recv_task.cancelled():
                                frames = recv_task.result()
                            else:
                                continue
                        else:
                            frames = recv_task.result()
                    finally:
                        # 清理未完成的 task，避免 awaitable 泄漏
                        for t in (recv_task, sig_task):
                            if not t.done():
                                t.cancel()
                                with contextlib.suppress(asyncio.CancelledError, Exception):
                                    await t
                else:
                    # 无 monitor（理论上不会到这里，connect 总会建 monitor）
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
            if hb_task is not None:
                hb_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await hb_task
            if mon_task is not None:
                mon_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await mon_task
            self._event_sig = None
            self._event_kind = None
            if stop:
                logger.info("[SUB] 订阅已结束")

    async def _watch_events(self) -> None:
        """后台持续监听 monitor socket，按事件类型分发并记录日志。

        事件处理：
        - EVENT_HANDSHAKE_SUCCEEDED：认证通过 → [SUB 上线] 认证成功
        - 任意 HANDSHAKE_FAILED_*：认证失败 → [SUB 认证失败]，置 fail
        - EVENT_DISCONNECTED：连接断开 → [SUB 断线]，置 disconnect

        主循环 subscribe() 据 _event_kind 决定是否结束迭代。
        """
        assert self._mon is not None
        assert self._event_sig is not None
        try:
            while True:
                # monitor 事件帧格式：第 1 帧前 2 字节为事件码（小端 uint16）
                data = await self._mon.recv_multipart()
                event_code = int.from_bytes(data[0][:2], "little")
                if event_code == zmq.EVENT_HANDSHAKE_SUCCEEDED:
                    self._event_kind = "ok"
                    logger.info(
                        "[SUB 上线] 认证成功，订阅就绪 (user={!r}, addr={!r})",
                        self._username, self._address,
                    )
                elif event_code == zmq.EVENT_DISCONNECTED:
                    self._event_kind = "disconnect"
                    logger.warning(
                        "[SUB 断线] 与 publisher 的连接已断开 (addr={!r})",
                        self._address,
                    )
                else:
                    # 任意握手失败：凭证错误 / 非 PLAIN / 协议错误
                    self._event_kind = "fail"
                    logger.error(
                        "[SUB 认证失败] PLAIN 握手被服务端拒绝，已停止订阅 "
                        "(user={!r}, addr={!r})。请检查 username/password 或联系 publisher 管理员。",
                        self._username, self._address,
                    )
                self._event_sig.set()
        except (zmq.ZMQError, asyncio.CancelledError):
            # socket 关闭或任务取消：正常退出
            return

    async def _keepalive_loop(self) -> None:
        """定期发送心跳帧，使 publisher 能检测异常下线。"""
        assert self._ctrl_push is not None
        try:
            while True:
                await asyncio.sleep(self._keepalive_interval)
                try:
                    await self._ctrl_push.send_multipart(
                        [_KEEPALIVE_MAGIC, self._username.encode("utf-8")]
                    )
                except (zmq.ZMQError, asyncio.CancelledError):
                    break
                except Exception:
                    pass  # 单次发送失败不影响
        except asyncio.CancelledError:
            return

    async def _send_disconnect_notification(self) -> None:
        """发送断开连接通知给 publisher（通过持久化 PUSH socket）。"""
        if not self._username or self._ctrl_push is None:
            return
        try:
            await self._ctrl_push.send_multipart(
                [_DISCONNECT_MAGIC, self._username.encode("utf-8")]
            )
            logger.debug("[SUB] 已发送断开通知 user={}", self._username)
            # 等待 publisher 处理断开通知，避免同一用户立即重连时被误判为"已在线"
            await asyncio.sleep(0.05)
        except Exception:
            # 发送失败不影响关闭流程
            pass

    async def close(self) -> None:
        """关闭连接。"""
        # 发送断开通知（在关闭任何 socket 之前）
        await self._send_disconnect_notification()

        # 关闭控制通道 PUSH socket
        if self._ctrl_push is not None:
            self._ctrl_push.close(linger=500)
            self._ctrl_push = None
        if self._ctrl_ctx is not None:
            self._ctrl_ctx.term()
            self._ctrl_ctx = None

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
        logger.info("[SUB] 已关闭")

    # ---- 上下文管理器 ----

    async def __aenter__(self) -> PulseSubscriber:
        await self.connect()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()
