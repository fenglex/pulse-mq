# src/pulsemq/client.py
"""Client / ProducerClient / ConsumerClient。

启动硬失败 + 运行期自动重连（Spec 1 §8.3）。

启动认证检测采用 monitor-based 设计（非 brief 的"握手成功即认证成功"）：
- 在数据面 connect 时开启 ZMQ monitor，监听握手期事件。
- ``handshake_ok`` → PLAIN 认证通过，继续控制面 connect + REGISTER。
- ``auth_failed`` → 抛 ``AuthenticationError``（exit 3）。
- 超时 / 其他事件 → 视为服务器不可达，抛 ``ClientStartupError``（exit 4）。

运行期（启动成功后）：
- monitor 回调切换为 ``_on_runtime_monitor``，监听 ``disconnected``。
- 断线 → ``_reconnect_loop`` 指数退避（初始 1s，×2，封顶 30s）：
  新 Transport → PLAIN 重认证 → REGISTER（同 client_id）→ 恢复订阅 →
  重启 recv/heartbeat 循环。业务层无需重新 subscribe()。
- 重连时 auth_failed → ``AuthenticationError``（exit 3）。
- ALREADY_ONLINE / 其他暂态 → 退避重试（服务端心跳扫描会清理 stale 记录）。

Spec 1 已知限制（见 task-12-report）：
- 控制面回复匹配是朴素的：REGISTER/SUBSCRIBE 各做一次 ``recv("control")``，
  心跳 ack 是 fire-and-forget，可能在控制面 socket 上堆积并和下一次
  register/subscribe 的 recv 串扰。单客户端 e2e 场景下可接受。

Spec 1 显式偏差：ALREADY_ONLINE 重试
- Spec 1 §8.3 规定重连时 REGISTER 收到 ALREADY_ONLINE 应退出码 4。但服务端的
  ``OnlineRegistry`` 以 username 为唯一键，断网后 stale 记录要等心跳超时扫描
  （``heartbeat_timeout = 6.0s``）才会清理。若一遇到 ALREADY_ONLINE 立即退出 4，
  自动重连将形同虚设——网络闪断后任何重连都会被 6s 内尚未释放的旧条目击落。
- 因此当前实现把 ALREADY_ONLINE（及任何 REGISTER/控制面失败）视为暂态失败，
  退避重试，待心跳扫描释放 username 后自然成功。重试行为本身不改；本偏差仅
  记录此处与 §8.3 文字的分歧。待服务端支持 reconnect 触发的快速 stale 条目
  驱逐后再回到 exit 4 的严格行为。
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
# 运行期重连参数（Spec 1 §8.3）：指数退避，初始 1s，×2，封顶 30s。
_RECONNECT_INITIAL_DELAY = 1.0
_RECONNECT_BACKOFF_MULTIPLIER = 2.0
_RECONNECT_MAX_DELAY = 30.0
# 重连时单次等待 monitor 认证裁定的超时秒数。
_RECONNECT_MONITOR_TIMEOUT = 5.0


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
        *,
        heartbeat_interval: float = _HEARTBEAT_INTERVAL,
        reconnect_initial_delay: float = _RECONNECT_INITIAL_DELAY,
        reconnect_max_delay: float = _RECONNECT_MAX_DELAY,
        reconnect_backoff_multiplier: float = _RECONNECT_BACKOFF_MULTIPLIER,
        reconnect_monitor_timeout: float = _RECONNECT_MONITOR_TIMEOUT,
        startup_timeout: float = _STARTUP_MONITOR_TIMEOUT,
        register_reply_timeout: float = _REGISTER_REPLY_TIMEOUT,
    ) -> None:
        self._data_endpoint = data_endpoint
        self._control_endpoint = control_endpoint
        self._username = username
        self._password = password
        self._client_id = client_id or uuid.uuid4().hex
        self._heartbeat_interval = heartbeat_interval
        self._reconnect_initial_delay = reconnect_initial_delay
        self._reconnect_max_delay = reconnect_max_delay
        self._reconnect_backoff_multiplier = reconnect_backoff_multiplier
        self._reconnect_monitor_timeout = reconnect_monitor_timeout
        self._startup_timeout = startup_timeout
        self._register_reply_timeout = register_reply_timeout
        self._transport = Transport()
        self._connected = False
        self._authenticated = False
        self._registered = False
        # pattern -> callback（同步或异步均可）
        self._subscriptions: dict[str, Callable] = {}
        # pattern -> header_only 标记：True 表示回调只接收 FrameHeader，跳过完整 decode
        self._sub_header_only: dict[str, bool] = {}
        self._recv_task: asyncio.Task | None = None
        self._hb_task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        # 启动期 monitor 裁定 future：由 _on_startup_monitor resolve。
        self._startup_event: asyncio.Future | None = None
        # 运行期重连状态机（Spec 1 §8.3）。
        self._reconnecting = False
        self._reconnect_task: asyncio.Task | None = None
        # 重连时发生的致命错误（如认证失败）。后台 _reconnect_loop 不直接 raise
        # （会被 asyncio GC 吞掉），而是存到这里，由 run_forever/stop 在主任务
        # 上下文重新抛出，从而让 CLI 经 exit_code_for 拿到 exit 3。
        self._reconnect_fatal: AuthenticationError | None = None
        # 角色标记（监控用）：子类 ProducerClient/ConsumerClient 覆写。
        self._roles: list[str] = ["publisher", "subscriber"]
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
            self._data_endpoint, "data", credentials=creds,
            monitor=True, identity=ident,
        )

        kind: str | None
        try:
            kind = await asyncio.wait_for(
                self._startup_event, timeout=self._startup_timeout
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

        # 启动成功后，切换到运行期 monitor 回调（接管断线重连）。
        self._transport.set_monitor_callback(self._on_runtime_monitor)

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

    # ------------------------------------------------- 运行期重连状态机 §8.3

    async def _on_runtime_monitor(self, kind: str) -> None:
        """运行期 monitor 回调：仅在 ``disconnected`` 且未在重连时触发一次重连。

        幂等保护：``_reconnecting`` 标志避免多次 disconnected 事件派生多个重连任务。
        """
        if kind != "disconnected":
            return
        if self._reconnecting or self._stop.is_set():
            return
        self._reconnecting = True
        log_event("INFO", "CLIENT", username=self._username, action="reconnecting")
        if self.on_reconnecting:
            try:
                await self.on_reconnecting()
            except Exception:
                logger.exception("on_reconnecting 回调异常")
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    async def _on_reconnect_monitor(self, kind: str) -> None:
        """重连期 monitor 回调：与启动期同形，仅在 auth-outcome 事件 resolve future。"""
        if kind in ("handshake_ok", "auth_failed") and self._startup_event is not None \
                and not self._startup_event.done():
            self._startup_event.set_result(kind)

    async def _cancel_bg_tasks(self) -> None:
        """取消并等待 recv/heartbeat 后台任务（吞掉 CancelledError/异常）。"""
        for t in (self._recv_task, self._hb_task):
            if t is not None:
                t.cancel()
        for t in (self._recv_task, self._hb_task):
            if t is not None:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        self._recv_task = None
        self._hb_task = None

    async def _reconnect_loop(self) -> None:
        """运行期重连状态机（Spec 1 §8.3）。

        断线后指数退避（初始 1s，×2，封顶 30s）尝试重连：
        新建 Transport → PLAIN 认证 → REGISTER（同 client_id）→ 恢复订阅 →
        重启 recv/heartbeat 循环。

        - 重连时认证失败（auth_failed）→ **不在此后台任务内 raise**（会被
          asyncio GC 吞掉）；改为把 ``AuthenticationError`` 存到
          ``self._reconnect_fatal``，set ``_stop`` 后 return。run_forever /
          stop 路径在主任务上下文重新抛出，使 CLI 经 ``exit_code_for`` 拿到
          exit 3。
        - ALREADY_ONLINE：Spec 1 偏差（见模块 docstring 与下方注释），退避重试。
        - 超时/其他暂态失败 → 退避重试。
        - ``_stop`` 触发后立即退出（优雅停机优先）。
        - 中途被取消（``CancelledError``，属 BaseException）→ 关闭尚未提交的
          in-flight transport，避免 socket/monitor 任务泄漏。
        """
        loop = asyncio.get_running_loop()
        creds = (self._username, self._password) if self._username else None
        ident = self._client_id.encode("utf-8")
        delay = self._reconnect_initial_delay

        # 清理旧 transport 与后台任务（保留 _subscriptions 作为恢复源）。
        await self._cancel_bg_tasks()
        self._connected = False
        self._authenticated = False
        self._registered = False
        try:
            await self._transport.close()
        except Exception:
            logger.debug("重连前旧 transport 关闭失败", exc_info=True)

        # 本次重连尚未完全成功提交的新 transport。它指向 ``new_transport`` 直到
        # 完整成功路径走完才置 None；这样 pre-handshake 与 post-handshake 取消
        # 都能在下面 ``except BaseException`` 里关掉同一个 in_flight，避免
        # 半连接的 socket/monitor 任务泄漏（详见 round-2 Fix B）。
        in_flight: Transport | None = None
        try:
            while not self._stop.is_set():
                new_transport = Transport()
                in_flight = new_transport
                # 准备本次重连的认证裁定 future。
                self._startup_event = loop.create_future()
                new_transport.set_monitor_callback(self._on_reconnect_monitor)
                try:
                    await new_transport.connect(
                        self._data_endpoint, "data",
                        credentials=creds, monitor=True, identity=ident,
                    )
                except Exception:
                    await self._safe_close(in_flight)
                    in_flight = None
                    await self._backoff_sleep(delay)
                    delay = min(delay * self._reconnect_backoff_multiplier,
                                self._reconnect_max_delay)
                    continue

                # 等待认证裁定。
                kind: str | None
                try:
                    kind = await asyncio.wait_for(
                        self._startup_event,
                        timeout=self._reconnect_monitor_timeout,
                    )
                except asyncio.TimeoutError:
                    kind = None

                if kind == "auth_failed":
                    # 重连时凭据无效 → 致命错误。**不在后台任务内 raise**
                    # （会被 asyncio GC 吞掉，进程不会 exit 3）。存到实例，
                    # 触发 _stop 让 run_forever 主循环退出并在主上下文重抛。
                    await self._safe_close(in_flight)
                    in_flight = None
                    log_event("ERROR", "CLIENT",
                              username=self._username,
                              action="reconnect_auth_failed")
                    self._reconnect_fatal = AuthenticationError(
                        f"重连认证失败（用户名/密码错误）: {self._username}",
                        reason="invalid_password",
                    )
                    self._stop.set()
                    return
                if kind != "handshake_ok":
                    # 超时/其他暂态失败 → 退避重试。
                    await self._safe_close(in_flight)
                    in_flight = None
                    await self._backoff_sleep(delay)
                    delay = min(delay * self._reconnect_backoff_multiplier,
                                self._reconnect_max_delay)
                    continue

                # 认证通过：把 new_transport 提交给 self._transport，使 _register
                # /_send_subscribe 能用。**注意**：in_flight 此时仍指向
                # new_transport（== self._transport），不在此处置 None——只有完整
                # 成功路径走完才置 None。这样 post-handshake 取消（CancelledError
                # 属 BaseException，绕过 except Exception）也能在 except
                # BaseException 中关掉它（round-2 Fix B）。
                self._transport = new_transport
                try:
                    await self._transport.connect(
                        self._control_endpoint, "control",
                        credentials=creds, monitor=False, identity=ident,
                    )
                    await self._register()
                    # 恢复订阅（不重新调用业务 subscribe()）。
                    for pattern in list(self._subscriptions):
                        await self._send_subscribe(pattern)
                except Exception:
                    # Spec 1 偏差：§8.3 规定重连 REGISTER 收到 ALREADY_ONLINE
                    # 应 exit 4，但我们退避重试。原因：服务端的 stale 记录要等
                    # 心跳超时扫描（~6s）才释放，立即 exit 4 会让任何网络闪断后
                    # 的重连被旧条目击落，自动重连形同虚设。待服务端支持
                    # reconnect 触发的快速 stale 条目驱逐后再回到严格 exit 4。
                    logger.debug("重连阶段 REGISTER/订阅恢复失败，将退避重试",
                                 exc_info=True)
                    # in_flight == self._transport == new_transport，关掉它。
                    await self._safe_close(in_flight)
                    self._transport = Transport()  # 占位，避免 stop/close 拿到坏的
                    in_flight = None
                    await self._backoff_sleep(delay)
                    delay = min(delay * self._reconnect_backoff_multiplier,
                                self._reconnect_max_delay)
                    continue

                # 成功：重启后台循环，恢复运行期 monitor，清掉之前的致命错误。
                self._reconnect_fatal = None
                self._connected = True
                self._authenticated = True
                self._recv_task = asyncio.create_task(self._recv_loop())
                self._hb_task = asyncio.create_task(self._heartbeat_loop())
                self._transport.set_monitor_callback(self._on_runtime_monitor)
                self._reconnecting = False
                log_event("INFO", "CLIENT",
                          username=self._username, action="reconnected")
                # 完整成功：现在才把 in_flight 置 None。此后 self._transport 由
                # 运行期 monitor / stop() 负责生命周期，except BaseException 不
                # 应再关它（否则会和 stop() 的 close() 双关）。
                in_flight = None
                return
        except BaseException:
            # 包括 CancelledError（Py3.8+ 属 BaseException，普通 except Exception
            # 捕不到）。若 in_flight 仍非 None，说明本次重连尚未走完整成功路径
            # ——可能是 pre-handshake 取消（in_flight == new_transport，尚未提交给
            # self._transport）或 post-handshake 取消（in_flight == self._transport
            # == new_transport）。两种情况都关闭 in_flight 以防 socket/任务泄漏，
            # 然后 re-raise（不吞 CancelledError，stop() 才能真正取消本任务）。
            if in_flight is not None:
                await self._safe_close(in_flight)
            raise
        finally:
            # 兜底：确保 _reconnecting 不会卡住（若 stop 触发提前退出）。
            self._reconnecting = False

    async def _backoff_sleep(self, delay: float) -> None:
        """退避 sleep，``_stop`` 触发时立即返回（不阻塞优雅停机）。"""
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    async def _safe_close(self, transport: Transport) -> None:
        try:
            await transport.close()
        except Exception:
            logger.debug("重连 cleanup 关闭 transport 失败", exc_info=True)

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
                "roles": list(self._roles),
                "topics": list(self._subscriptions),
            },
        )
        await self._transport.send(b"", req, role="control")
        try:
            _, reply = await asyncio.wait_for(
                self._transport.recv("control"), timeout=self._register_reply_timeout
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

    async def subscribe(self, topic_pattern: str, callback: Callable,
                        *, header_only: bool = False) -> None:
        """订阅 topic 模式。

        Args:
            topic_pattern: 主题模式（支持 ``foo.*`` 前缀通配）。
            callback: 消息回调。``header_only=False`` 时接收 ``PulseMessage``，
                ``header_only=True`` 时接收 ``FrameHeader``（跳过完整 decode，降低延迟）。
            header_only: 仅需 topic/record_count/timestamp_ns 时设 True，跳过反序列化。
        """
        self._subscriptions[topic_pattern] = callback
        self._sub_header_only[topic_pattern] = header_only
        # 仅在已连接时立即发送；未连接时缓存，start() 末尾会 flush（A3）
        if self._connected:
            await self._send_subscribe(topic_pattern)

    # ---------------------------------------------------------------- publish

    @require_connected
    async def publish(self, topic: str, data: Any, *,
                      serializer: str | None = None,
                      compression: str = "none",
                      data_type: int | None = None) -> None:
        frame = frames.encode(topic, data, serializer=serializer,
                              compression=compression, data_type=data_type)
        await self._transport.send(b"", frame, role="data")

    # -------------------------------------------------------------- recv loop

    async def _recv_loop(self) -> None:
        """消费数据面帧，按 topic 前缀匹配分发给订阅回调。

        先 decode_header 用 topic 快速过滤，不匹配的帧跳过完整 decode。
        header_only 回调只接收 FrameHeader，跳过完整 decode 以降低延迟。
        """
        while not self._stop.is_set():
            try:
                _, frame_bytes = await self._transport.recv("data")
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("client 数据面 recv 异常")
                continue
            try:
                hdr = frames.decode_header(frame_bytes)
            except Exception:
                logger.debug("client 帧头部解码失败，丢弃")
                continue
            # 快速过滤：先用 header 的 topic 匹配订阅
            matched = [(cb, self._sub_header_only.get(p, False))
                       for p, cb in list(self._subscriptions.items())
                       if _matches(p, hdr.topic)]
            if not matched:
                continue
            # 有非 header_only 回调时才做完整 decode
            need_decode = any(not ho for _, ho in matched)
            msg = None
            if need_decode:
                try:
                    msg = frames.decode(frame_bytes)
                except Exception:
                    logger.debug("client 帧解码失败，丢弃")
                    continue
            for cb, ho in matched:
                try:
                    target = hdr if ho else msg
                    if inspect.iscoroutinefunction(cb):
                        await cb(target)
                    else:
                        cb(target)
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
            await asyncio.sleep(self._heartbeat_interval)

    # ----------------------------------------------------------- run_forever

    async def _wait_stop_and_raise_fatal(self) -> None:
        """等待 _stop 被设置；退出时若有重连致命错误则重新抛出。

        ``_reconnect_loop`` 是后台任务，直接 raise 会被 asyncio GC 吞掉，因此
        它把致命错误（如认证失败）存到 ``self._reconnect_fatal`` 并 set ``_stop``。
        本方法在主任务上下文检查并重抛，使 CLI 经 ``exit_code_for`` 拿到 exit 3。
        """
        try:
            await self._stop.wait()
        finally:
            fatal = self._reconnect_fatal
            if fatal is not None:
                self._reconnect_fatal = None
                raise fatal

    def _install_signal_handlers(self) -> None:
        """注册 SIGINT/SIGTERM -> _stop.set，Windows 静默跳过（A4）。"""
        import signal
        loop = asyncio.get_running_loop()
        for sig_name in ("SIGINT", "SIGTERM"):
            sig = getattr(signal, sig_name, None)
            if sig is None:
                continue  # Windows 无 SIGTERM
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except (NotImplementedError, RuntimeError):
                break  # Windows 不支持 add_signal_handler

    async def run_forever(self) -> None:
        """连接 + 注册，运行直到 stop() 或重连致命错误。

        替代手写 asyncio.sleep 的维持模式。重连遇到致命错误（如认证失败）时，
        在主任务上下文重新抛出，使 CLI 经 exit_code_for 拿到 exit 3。
        """
        await self.start()
        self._install_signal_handlers()
        try:
            await self._wait_stop_and_raise_fatal()
        finally:
            await self.stop()

    # ------------------------------------------------------------------- stop

    async def stop(self) -> None:
        """优雅停机：取消后台任务（含重连任务）+ DISCONNECT + 关闭 transport。"""
        self._stop.set()
        # 先取消重连任务（若正在重连），避免它继续派生新 transport。
        for t in (self._reconnect_task, self._recv_task, self._hb_task):
            if t:
                t.cancel()
        for t in (self._reconnect_task, self._recv_task, self._hb_task):
            if t:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        self._reconnect_task = None
        self._recv_task = None
        self._hb_task = None
        self._reconnecting = False
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
        self._roles = ["publisher"]
        from pulsemq.producers.manager import ProducerManager

        self._producer_mgr = ProducerManager()

    def producer(
        self,
        topic: str,
        *,
        interval: float = 5.0,
        serializer: str = "msgpack",
        compression: str = "none",
    ) -> Callable:
        """注册一个定时 producer：回调返回的数据发布到 topic。"""

        def deco(fn):
            self._producer_mgr.register(
                fn,
                name=topic,
                interval=interval,
                serializer=serializer,
                compression=compression,
            )
            return fn

        return deco

    def burst_producer(
        self,
        topic: str,
        *,
        serializer: str = "msgpack",
        compression: str = "none",
    ) -> Callable:
        """注册一个 burst producer：无间隔连续发送，用于极限性能测试。"""

        def deco(fn):
            self._producer_mgr.register_burst(
                fn,
                name=topic,
                serializer=serializer,
                compression=compression,
            )
            return fn

        return deco

    async def _on_produce(self, spec, data) -> None:
        # spec.name == topic（注册时以 topic 为 name）
        await self.publish(spec.name, data,
                           serializer=spec.serializer,
                           compression=spec.compression)

    async def run_forever(self) -> None:
        """连接 + 认证 + 注册，启动所有 producer 调度，运行直到 stop()。

        ProducerClient 在基类 ``run_forever`` 框架内插入 ``ProducerManager``
        的 ``start_all/stop_all``：致命错误重抛交给基类
        ``_wait_stop_and_raise_fatal`` 统一处理（A1+A2）。
        """
        await self.start()
        self._install_signal_handlers()
        try:
            await self._producer_mgr.start_all(self._on_produce)
            await self._wait_stop_and_raise_fatal()
        finally:
            await self._producer_mgr.stop_all()
            await self.stop()

    async def subscribe(self, topic_pattern: str, callback: Callable,
                        *, header_only: bool = False) -> None:  # type: ignore[override]
        raise NotImplementedError("ProducerClient 不支持订阅")


class ConsumerClient(Client):
    """只订阅，屏蔽 publish。"""

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._roles = ["subscriber"]

    async def publish(self, topic: str, data: Any) -> None:  # type: ignore[override]
        raise NotImplementedError("ConsumerClient 不支持发布")
