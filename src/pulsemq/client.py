# src/pulsemq/client.py
"""Client / ProducerClient / ConsumerClient。

启动硬失败 + 运行期（Spec 1：单次连接，重连由 Task 12b 接管）。

启动认证检测采用 monitor-based 设计（非 brief 的"握手成功即认证成功"）：
- 在数据面 connect 时开启 ZMQ monitor，监听握手期事件。
- ``handshake_ok`` → PLAIN 认证通过，继续控制面 connect + REGISTER。
- ``auth_failed`` → 抛 ``AuthenticationError``（exit 3）。
- 超时 / 其他事件 → 视为服务器不可达，抛 ``ClientStartupError``（exit 4）。

Spec 1 已知限制（见 task-12-report）：
- 控制面回复匹配是朴素的：REGISTER/SUBSCRIBE 各做一次 ``recv("control")``，
  心跳 ack 是 fire-and-forget，可能在控制面 socket 上堆积并和下一次
  register/subscribe 的 recv 串扰。单客户端 e2e 场景下可接受。
"""
from __future__ import annotations

import asyncio
import functools
import inspect
import uuid
from typing import Any, Awaitable, Callable

from pulsemq.control import ControlCmd
from pulsemq.errors import (AuthenticationError, ClientStartupError,
                            ConnectionError)
from pulsemq.logging_setup import log_event, logger
from pulsemq.protocol import frames
from pulsemq.transport.router import Transport

# 启动时等待 monitor 认证裁定的最长秒数。超时即视为服务器不可达。
_STARTUP_MONITOR_TIMEOUT = 5.0
# REGISTER 控制帧回复的超时秒数。
_REGISTER_REPLY_TIMEOUT = 3.0
# 心跳间隔（秒）。
_HEARTBEAT_INTERVAL = 1.0


def require_connected(func):
    """要求 _connected 且 _authenticated，否则抛 ConnectionError。"""

    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        if not self._connected or not self._authenticated:
            raise ConnectionError("Client 未连接或未认证，无法执行操作")
        return await func(self, *args, **kwargs)

    return wrapper


class Client:
    """PulseMQ 客户端：发布 + 订阅。

    子类 ProducerClient / ConsumerClient 通过覆写屏蔽对应能力。
    """

    def __init__(
        self,
        data_endpoint: str = "tcp://localhost:5555",
        control_endpoint: str = "tcp://localhost:5556",
        username: str = "",
        password: str = "",
        client_id: str | None = None,
    ) -> None:
        self._data_endpoint = data_endpoint
        self._control_endpoint = control_endpoint
        self._username = username
        self._password = password
        self._client_id = client_id or uuid.uuid4().hex
        self._transport = Transport()
        self._connected = False
        self._authenticated = False
        self._registered = False
        # pattern -> callback（同步或异步均可）
        self._subscriptions: dict[str, Callable] = {}
        self._recv_task: asyncio.Task | None = None
        self._hb_task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        # 启动期 monitor 裁定 future：由 _on_startup_monitor resolve。
        self._startup_event: asyncio.Future | None = None
        # 生命周期回调（可选）。
        self.on_connected: Callable[[], Awaitable[None]] | None = None
        self.on_disconnected: Callable[[], Awaitable[None]] | None = None
        self.on_reconnecting: Callable[[], Awaitable[None]] | None = None

    # ------------------------------------------------------------------ start

    async def start(self) -> None:
        """启动客户端：连接数据面 + 控制面 + 注册 + 启动后台循环。

        失败模式（硬失败，向上传播）：
        - 密码错误 → ``AuthenticationError``（exit 3）。
        - 服务器不可达 / 握手超时 → ``ClientStartupError``（exit 4）。
        - REGISTER 被拒 / 超时 → ``ClientStartupError``（exit 4）。
        """
        creds = (self._username, self._password) if self._username else None
        # 数据面/控制面两个 DEALER 共用同一 bytes identity，使 server 的
        # routing（以 control 面 ident 为 key）能直接转发到数据面 DEALER。
        ident = self._client_id.encode("utf-8")

        # ---- 数据面：DEALER + PLAIN + monitor，等待认证裁定 ----
        self._startup_event = asyncio.get_running_loop().create_future()
        self._transport.set_monitor_callback(self._on_startup_monitor)
        await self._transport.connect(
            self._data_endpoint, "consumer", credentials=creds,
            monitor=True, identity=ident,
        )

        kind: str | None
        try:
            kind = await asyncio.wait_for(
                self._startup_event, timeout=_STARTUP_MONITOR_TIMEOUT
            )
        except asyncio.TimeoutError:
            kind = None

        if kind == "handshake_ok":
            self._connected = True
            self._authenticated = True
        elif kind == "auth_failed":
            # 先关闭半开的 transport 再抛，避免泄漏 socket。
            await self._transport.close()
            raise AuthenticationError(
                f"认证失败（用户名/密码错误）: {self._username}",
                reason="invalid_password",
            )
        else:
            # None / 超时 / 其他事件 → 服务器不可达。
            await self._transport.close()
            raise ClientStartupError(
                f"连接数据面失败: {self._data_endpoint}",
                reason="CONNECT_FAILED",
                address=self._data_endpoint,
                username=self._username,
            )

        # ---- 控制面：DEALER + PLAIN，不开 monitor（控制面复用数据面认证态）----
        try:
            await self._transport.connect(
                self._control_endpoint, "control",
                credentials=creds, monitor=False, identity=ident,
            )
        except Exception as e:
            await self._transport.close()
            raise ClientStartupError(
                f"连接控制面失败: {self._control_endpoint}",
                reason="CONTROL_CONNECT_FAILED",
                address=self._control_endpoint,
                username=self._username,
            ) from e

        # ---- REGISTER ----
        await self._register()

        # ---- 恢复既有订阅（重连场景；首次启动时为空）----
        for pattern in list(self._subscriptions):
            await self._send_subscribe(pattern)

        # ---- 启动后台循环 ----
        self._recv_task = asyncio.create_task(self._recv_loop())
        self._hb_task = asyncio.create_task(self._heartbeat_loop())

        if self.on_connected:
            try:
                await self.on_connected()
            except Exception:
                logger.exception("on_connected 回调异常")

    async def _on_startup_monitor(self, kind: str) -> None:
        """启动期 monitor 回调：仅 auth-outcome 事件 resolve startup future。

        ``connected``/``disconnected``/``other`` 被忽略 —— 服务器宕机会
        表现为超时，进而由 ``start`` 抛 ClientStartupError。
        """
        if kind in ("handshake_ok", "auth_failed") and self._startup_event is not None \
                and not self._startup_event.done():
            self._startup_event.set_result(kind)

    # -------------------------------------------------------------- register

    async def _register(self) -> None:
        """在控制面发送 REGISTER 并等待回复。

        - 超时 → ``ClientStartupError(reason="REGISTER_REJECTED")``。
        - result != "OK" → ``ClientStartupError(reason=result)``。
        """
        req = frames.encode_control(
            ControlCmd.REGISTER,
            {
                "client_id": self._client_id,
                "username": self._username,
                "endpoint": self._data_endpoint,
                "roles": [],
                "topics": list(self._subscriptions),
            },
        )
        await self._transport.send(b"", req, role="control")
        try:
            _, reply = await asyncio.wait_for(
                self._transport.recv("control"), timeout=_REGISTER_REPLY_TIMEOUT
            )
        except asyncio.TimeoutError as e:
            raise ClientStartupError(
                "REGISTER 超时无回复",
                reason="REGISTER_REJECTED",
                address=self._control_endpoint,
                username=self._username,
            ) from e
        msg = frames.decode_control(reply)
        result = msg.payload.get("result", "")
        if result != "OK":
            raise ClientStartupError(
                f"REGISTER 被拒: {result}",
                reason=result,
                address=self._control_endpoint,
                username=self._username,
            )
        self._registered = True
        log_event("INFO", "CLIENT", username=self._username, action="online")

    # ------------------------------------------------------------- subscribe

    async def _send_subscribe(self, pattern: str) -> None:
        """发送 SUBSCRIBE 控制帧；回复 fire-and-forget（不阻塞，容错超时）。"""
        req = frames.encode_control(
            ControlCmd.SUBSCRIBE,
            {"client_id": self._client_id, "topic": pattern},
        )
        await self._transport.send(b"", req, role="control")
        # 排空服务端的 SUBSCRIBE ack，避免它在控制面 socket 堆积串扰。
        # 已知限制：若此刻刚好有一帧心跳 ack 抢先到达，本 recv 会把它当成
        # SUBSCRIBE ack 消费掉；Spec 1 单客户端 e2e 不影响正确性。
        try:
            await asyncio.wait_for(
                self._transport.recv("control"), timeout=0.5
            )
        except asyncio.TimeoutError:
            pass
        except Exception:
            logger.debug("SUBSCRIBE 排空 ack 失败", exc_info=True)

    @require_connected
    async def subscribe(self, topic_pattern: str, callback: Callable) -> None:
        self._subscriptions[topic_pattern] = callback
        await self._send_subscribe(topic_pattern)

    # ---------------------------------------------------------------- publish

    @require_connected
    async def publish(self, topic: str, data: Any) -> None:
        frame = frames.encode(topic, data)
        await self._transport.send(b"", frame, role="consumer")

    # -------------------------------------------------------------- recv loop

    async def _recv_loop(self) -> None:
        """消费数据面帧，按 topic 前缀匹配分发给订阅回调。"""
        while not self._stop.is_set():
            try:
                _, frame_bytes = await self._transport.recv("consumer")
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("client 数据面 recv 异常")
                continue
            try:
                msg = frames.decode(frame_bytes)
            except Exception:
                logger.debug("client 帧解码失败，丢弃")
                continue
            for pattern, cb in list(self._subscriptions.items()):
                if _matches(pattern, msg.topic):
                    try:
                        if inspect.iscoroutinefunction(cb):
                            await cb(msg)
                        else:
                            cb(msg)
                    except Exception:
                        logger.exception("订阅回调异常")

    # -------------------------------------------------------- heartbeat loop

    async def _heartbeat_loop(self) -> None:
        """周期发送 HEARTBEAT 控制帧；ack fire-and-forget。"""
        while not self._stop.is_set():
            try:
                hb = frames.encode_control(
                    ControlCmd.HEARTBEAT, {"client_id": self._client_id}
                )
                await self._transport.send(b"", hb, role="control")
            except Exception:
                logger.debug("心跳发送失败", exc_info=True)
            await asyncio.sleep(_HEARTBEAT_INTERVAL)

    # ------------------------------------------------------------------- stop

    async def stop(self) -> None:
        """优雅停机：取消后台任务 + DISCONNECT + 关闭 transport。"""
        self._stop.set()
        for t in (self._recv_task, self._hb_task):
            if t:
                t.cancel()
        for t in (self._recv_task, self._hb_task):
            if t:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        self._recv_task = None
        self._hb_task = None
        if self._registered:
            try:
                disc = frames.encode_control(
                    ControlCmd.DISCONNECT, {"client_id": self._client_id}
                )
                await self._transport.send(b"", disc, role="control")
            except Exception:
                logger.debug("DISCONNECT 发送失败", exc_info=True)
        await self._transport.close()
        self._connected = False
        self._authenticated = False
        self._registered = False
        if self.on_disconnected:
            try:
                await self.on_disconnected()
            except Exception:
                logger.exception("on_disconnected 回调异常")


def _matches(pattern: str, topic: str) -> bool:
    """topic 前缀匹配，与 routing.SubscriptionTable._matches 一致。

    ``foo.*`` 匹配 ``foo`` 和 ``foo.<anything>``；否则精确匹配。
    """
    if pattern.endswith(".*"):
        prefix = pattern[:-2]
        return topic == prefix or topic.startswith(prefix + ".")
    return pattern == topic


class ProducerClient(Client):
    """只发布。支持 producer 调度装饰器（复用 ProducerManager）。"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        from pulsemq.producers.manager import ProducerManager

        self._producer_mgr = ProducerManager()

    def producer(
        self,
        topic: str,
        *,
        interval: float = 5.0,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
    ) -> Callable:
        """注册一个定时 producer：回调返回的数据发布到 topic。"""

        def deco(fn):
            self._producer_mgr.register(
                fn,
                name=topic,
                interval=interval,
                cache_size=cache_size,
                serializer=serializer,
                compression=compression,
                inject_sender=False,
            )
            return fn

        return deco

    def burst_producer(
        self,
        topic: str,
        *,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
    ) -> Callable:
        """注册一个 burst producer：无间隔连续发送，用于极限性能测试。"""

        def deco(fn):
            self._producer_mgr.register_burst(
                fn,
                name=topic,
                cache_size=cache_size,
                serializer=serializer,
                compression=compression,
                inject_sender=False,
            )
            return fn

        return deco

    async def _on_produce(self, spec, data) -> None:
        # spec.name == topic（注册时以 topic 为 name）
        await self.publish(spec.name, data)

    async def run_forever(self) -> None:
        """连接 + 认证 + 注册，启动所有 producer 调度，运行直到 stop()。"""
        await self.start()
        try:
            await self._producer_mgr.start_all(self._on_produce, sender_factory=None)
            await self._stop.wait()  # stop() 设置 _stop 后退出
        finally:
            await self._producer_mgr.stop_all()

    async def subscribe(self, topic_pattern: str, callback: Callable) -> None:  # type: ignore[override]
        raise NotImplementedError("ProducerClient 不支持订阅")


class ConsumerClient(Client):
    """只订阅，屏蔽 publish。"""

    async def publish(self, topic: str, data: Any) -> None:  # type: ignore[override]
        raise NotImplementedError("ConsumerClient 不支持发布")
