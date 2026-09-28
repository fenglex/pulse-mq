# src/pulsemq/client.py
"""Client / ProducerClient / ConsumerClient。

启动硬失败 + 运行期自动重连（Spec 1 §8.3）。

消费模式（9.2.4 起唯一）：多进程 worker 池。
- 主进程 recv 循环批量排空接收 + 轻量头部解析 + key 路由，payload 解码
  与用户回调全部在 worker 进程执行（共享内存环 + spawn 进程，绕开 GIL 真并行）；
- workers=1（默认）即"一个主进程接收 + 一个 worker 进程处理"；
- 回调必须为模块级可导入函数（pickle 按引用传递），同步或异步均可；
  worker 内不共享主进程内存，lambda/闭包不支持。
旧的两线程解码队列（_DropQueue）与事件循环内联分发已于 9.2.4 移除。

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
import itertools
import random
import threading
import time
import uuid
import zmq
from typing import Any, Awaitable, Callable

from pulsemq.control import ControlCmd
from pulsemq.errors import (AuthenticationError, ClientStartupError,                            ConnectionError, PublishAckTimeout)
from pulsemq.logging_setup import log_event, logger
from pulsemq.protocol import frames
from pulsemq.protocol.msg_type import MsgType
from pulsemq.transport.router import Transport

# 启动时等待 monitor 认证裁定的最长秒数。超时即视为服务器不可达。
_STARTUP_MONITOR_TIMEOUT = 5.0
# REGISTER 控制帧回复的超时秒数。
_REGISTER_REPLY_TIMEOUT = 3.0
# 客户端控制面发送超时（9.2.1）：服务端管道满时不阻塞客户端循环。
_CONTROL_SEND_TIMEOUT = 2.0

# 接收循环单次唤醒的最大排空帧数：摊薄事件循环往返；上限防止批过大
# 挤占心跳/控制面调度（4 核机器上 256 帧 × ~µs 级处理 ≈ 亚毫秒，安全）
_RECV_DRAIN_MAX = 256
# 心跳间隔（秒）。
_HEARTBEAT_INTERVAL = 1.0
# 运行期重连参数（Spec 1 §8.3）：指数退避，初始 1s，×2，封顶 30s。
_RECONNECT_INITIAL_DELAY = 1.0
_RECONNECT_BACKOFF_MULTIPLIER = 2.0
_RECONNECT_MAX_DELAY = 30.0
# 重连时单次等待 monitor 认证裁定的超时秒数。
_RECONNECT_MONITOR_TIMEOUT = 5.0


class _ProcStats:
    """自上次心跳以来的 per-topic 处理统计（线程安全），供心跳上报。

    - ``record_recv``：接收延迟（recv 线程，帧到达时间 - 帧内时间戳）。
    - ``record_proc``：处理耗时（decode + 回调执行；inline 模式在事件循环线程，
      worker 模式在解码线程）。
    - ``drain``：心跳线程取走并清零，折算为速率/均值后随 HEARTBEAT 上报。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._count: dict[str, int] = {}    # 接收并匹配的消息条数
        self._recv_ns: dict[str, int] = {}  # 接收延迟累计（ns）
        self._proc_ns: dict[str, int] = {}  # 处理耗时累计（ns）
        self._proc_n: dict[str, int] = {}   # 完成处理的消息条数
        self._last_drain = time.time_ns()

    def record_recv(self, topic: str, latency_ns: int) -> None:
        with self._lock:
            self._count[topic] = self._count.get(topic, 0) + 1
            self._recv_ns[topic] = self._recv_ns.get(topic, 0) + latency_ns

    def merge_recv(self, topic: str, total_ns: int, count: int) -> None:
        """worker 进程聚合上报的批量合并（total/count 形式）。"""
        with self._lock:
            self._count[topic] = self._count.get(topic, 0) + count
            self._recv_ns[topic] = self._recv_ns.get(topic, 0) + total_ns

    def merge_proc(self, topic: str, total_ns: int, count: int) -> None:
        with self._lock:
            self._proc_n[topic] = self._proc_n.get(topic, 0) + count
            self._proc_ns[topic] = self._proc_ns.get(topic, 0) + total_ns

    def record_proc(self, topic: str, duration_ns: int) -> None:
        with self._lock:
            self._proc_n[topic] = self._proc_n.get(topic, 0) + 1
            self._proc_ns[topic] = self._proc_ns.get(topic, 0) + duration_ns

    def drain(self) -> dict[str, dict]:
        """取走并清零，返回 {topic: {count, rate, recv_avg_ns, proc_avg_ns}}。

        rate 按实际流逝时间（两次 drain 间隔）折算，比名义心跳间隔更准。
        空窗口（无消息且无待发 proc）返回空 dict，心跳里省略 proc 字段。

        9.2.7 修复：遍历"新接收 ∪ 待发处理统计"的并集。worker 统计 ≥1s
        批量 flush，通常比 recv 晚一个心跳窗口到达——只遍历 count 会把
        proc-only 窗口的数据静默吞掉（proc_n 已清零，永不补发），表现为
        admin API 偶发/持续缺 proc_avg_ms。
        """
        with self._lock:
            now = time.time_ns()
            elapsed = max(1e-9, (now - self._last_drain) / 1e9)
            self._last_drain = now
            count, recv_ns = self._count, self._recv_ns
            proc_ns, proc_n = self._proc_ns, self._proc_n
            self._count = {}
            self._recv_ns = {}
            self._proc_ns = {}
            self._proc_n = {}
        out: dict[str, dict] = {}
        for topic in set(count) | set(proc_n):
            n = count.get(topic, 0)
            entry: dict = {"count": n, "rate": round(n / elapsed, 2)}
            if n > 0:
                entry["recv_avg_ns"] = recv_ns.get(topic, 0) // n
            pn = proc_n.get(topic, 0)
            if pn > 0:
                entry["proc_avg_ns"] = proc_ns.get(topic, 0) // pn
            out[topic] = entry
        return out


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

    订阅侧（9.2.4 唯一消费模式）：多进程 worker 池——主进程接收 + key 路由，
    worker 进程解码 + 回调。start() 时已有订阅即创建池；运行期动态 subscribe()
    时懒创建。参数 workers/key/worker_ring_mb/worker_init 见 __init__。

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
        sndhwm: int = 10000,
        rcvhwm: int = 10000,
        latency_sample_rate: float = 0.01,
        buffer_policy: str | None = None,
        buffer_cfg: dict | None = None,
        workers: int = 1,
        key: str | None = None,
        worker_ring_mb: int = 64,
        worker_init=None,
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
        self._sndhwm = sndhwm
        self._rcvhwm = rcvhwm
        self._latency_sample_rate = latency_sample_rate
        # 服务端订阅者缓冲策略：None=直发（默认，旧行为）；"drop_old"/"conflate"
        # 随 REGISTER 协商。旧服务端忽略该字段，向后兼容。
        self._buffer_policy = buffer_policy
        self._buffer_cfg = buffer_cfg or {}
        # 9.2.4 唯一消费模式：多进程 worker 池（主进程接收+路由，worker 进程
        # 解码+回调）。workers=1 即"一个主进程 + 一个 worker 进程"。
        self._workers = max(1, int(workers))
        self._key_mode = key
        self._worker_ring_bytes = int(worker_ring_mb) * 1024 * 1024
        self._worker_init = worker_init
        # 确认发布：ack_token（4B per-publisher 计数器）-> Future
        # （recv 循环收到 PUBLISH_ACK 时 resolve）
        self._ack_counter = itertools.count(1)
        self._pending_acks: dict[int, asyncio.Future] = {}
        # 消费端缺口检测（9.2.1）：per-topic 最近 seq + 累计缺失 + 心跳上报基线
        self._gap_last: dict[str, int] = {}
        self._gap_missing: dict[str, int] = {}
        self._gap_reported: dict[str, int] = {}
        # per-topic 处理统计（接收延迟 + 处理耗时），随心跳上报服务端。
        self._proc_stats = _ProcStats()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._transport = Transport(sndhwm=self._sndhwm, rcvhwm=self._rcvhwm)
        self._connected = False
        self._authenticated = False
        self._registered = False
        # pattern -> callback（模块级可导入函数，同步或异步均可）
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
        # 多进程消费池（9.2.4 唯一消费模式）：start() 时有订阅即创建，
        # 运行期动态 subscribe 时按需懒创建。纯发布客户端不创建。
        self._pool = None
        self._route_drops: dict[str, int] = {}
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

        # ---- 多进程消费池（9.2.4 唯一消费模式）：已有订阅即创建 ----
        self._loop = asyncio.get_running_loop()
        if self._subscriptions:
            self._ensure_pool()

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
                new_transport = Transport(sndhwm=self._sndhwm, rcvhwm=self._rcvhwm)
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
                    self._transport = Transport(sndhwm=self._sndhwm, rcvhwm=self._rcvhwm)  # 占位，避免 stop/close 拿到坏的
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

    async def _recv_control_reply(self, expected_id: str, timeout: float) -> dict:
        """循环 recv 控制面回复直到 request_id 匹配；丢弃不匹配的帧（C3）。

        兼容旧 server（reply 无 request_id）：退化为直接返回。
        """
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise asyncio.TimeoutError()
            _, reply = await asyncio.wait_for(
                self._transport.recv("control"), timeout=remaining
            )
            msg = frames.decode_control(reply)
            rid = msg.payload.get("request_id")
            if rid == expected_id or rid is None:
                return msg.payload

    async def _register(self) -> None:
        """在控制面发送 REGISTER 并等待回复。

        - 超时 → ``ClientStartupError(reason="REGISTER_REJECTED")``。
        - result != "OK" → ``ClientStartupError(reason=result)``。
        """
        req_id = uuid.uuid4().hex
        register_payload = {
            "client_id": self._client_id,
            "username": self._username,
            "endpoint": self._data_endpoint,
            "roles": list(self._roles),
            "topics": list(self._subscriptions),
            "request_id": req_id,
        }
        if self._buffer_policy:
            register_payload["buffer"] = {
                "policy": self._buffer_policy, **self._buffer_cfg,
            }
        req = frames.encode_control(ControlCmd.REGISTER, register_payload)
        await self._transport.send(b"", req, role="control")
        try:
            payload = await self._recv_control_reply(req_id, self._register_reply_timeout)
        except asyncio.TimeoutError as e:
            raise ClientStartupError(
                "REGISTER 超时无回复",
                reason="REGISTER_REJECTED",
                address=self._control_endpoint,
                username=self._username,
            ) from e
        result = payload.get("result", "")
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
        """发送 SUBSCRIBE 控制帧；按 request_id 匹配回复（C3），容错超时。"""
        req_id = uuid.uuid4().hex
        req = frames.encode_control(
            ControlCmd.SUBSCRIBE,
            {"client_id": self._client_id, "topic": pattern, "request_id": req_id},
        )
        try:
            await asyncio.wait_for(
                self._transport.send(b"", req, role="control"),
                timeout=_CONTROL_SEND_TIMEOUT)
        except asyncio.TimeoutError:
            logger.debug("SUBSCRIBE 发送超时（控制面繁忙），跳过 ack 排空")
        except Exception:
            logger.debug("SUBSCRIBE 发送失败", exc_info=True)
            return
        try:
            await asyncio.wait_for(
                self._recv_control_reply(req_id, 0.5),
                timeout=0.5 + _CONTROL_SEND_TIMEOUT)
        except asyncio.TimeoutError:
            pass
        except Exception:
            logger.debug("SUBSCRIBE 排空 ack 失败", exc_info=True)

    async def _send_unsubscribe(self, pattern: str) -> None:
        """发送 UNSUBSCRIBE 控制帧；按 request_id 匹配回复（C3），容错超时。"""
        req_id = uuid.uuid4().hex
        req = frames.encode_control(
            ControlCmd.UNSUBSCRIBE,
            {"client_id": self._client_id, "topic": pattern, "request_id": req_id},
        )
        try:
            await asyncio.wait_for(
                self._transport.send(b"", req, role="control"),
                timeout=_CONTROL_SEND_TIMEOUT)
        except asyncio.TimeoutError:
            logger.debug("UNSUBSCRIBE 发送超时（控制面繁忙），跳过 ack 排空")
        except Exception:
            logger.debug("UNSUBSCRIBE 发送失败", exc_info=True)
            return
        try:
            await asyncio.wait_for(
                self._recv_control_reply(req_id, 0.5),
                timeout=0.5 + _CONTROL_SEND_TIMEOUT)
        except asyncio.TimeoutError:
            pass
        except Exception:
            logger.debug("UNSUBSCRIBE 排空 ack 失败", exc_info=True)

    async def unsubscribe(self, topic_pattern: str) -> None:
        """取消订阅（动态退订，幂等）。

        立即停止本地分发（worker 池同步移除该模式），并向服务端发送
        UNSUBSCRIBE（服务端停止路由）。未连接时仅移除本地订阅——下次
        start() 不会自动恢复该订阅。退订不存在的模式为空操作。
        """
        self._subscriptions.pop(topic_pattern, None)
        self._sub_header_only.pop(topic_pattern, None)
        if self._pool is not None:
            self._pool.remove_subscription(topic_pattern)
        if self._connected:
            await self._send_unsubscribe(topic_pattern)

    def _validate_worker_callback(self, cb, what: str = "回调") -> None:
        """多进程模式的回调约束：可跨进程按引用传递（pickle），同步或异步均可。

        lambda/闭包/局部函数无法被 pickle 按引用传递到 worker 进程（即便用
        cloudpickle 序列化，闭包捕获的变量也是拷贝而非共享内存），必须在
        模块顶层定义。
        """
        import pickle
        try:
            pickle.dumps(cb)
        except Exception as e:
            raise ValueError(
                f"多进程消费模式的{what}必须为模块级可导入函数"
                "（lambda/闭包无法跨进程传递；跨进程回调不共享主进程内存）；"
                "请把回调放到模块顶层") from e

    def _ensure_pool(self) -> None:
        """创建并启动多进程消费池（幂等）。阻塞约 0.3s（等待 worker attach）。"""
        if self._pool is not None:
            return
        for pattern, cb in self._subscriptions.items():
            self._validate_worker_callback(cb, f"订阅 {pattern!r} 的回调")
        if self._worker_init is not None:
            self._validate_worker_callback(self._worker_init, "worker_init")
        from pulsemq.worker_pool import WorkerPool
        subs = [(p, cb, self._sub_header_only.get(p, False))
                for p, cb in self._subscriptions.items()]
        self._pool = WorkerPool(
            self._workers, self._worker_ring_bytes, subs,
            self._key_mode, self._worker_init)
        self._pool.start()

    async def subscribe(self, topic_pattern: str, callback: Callable,
                        *, header_only: bool = False) -> None:
        """订阅 topic 模式。

        回调在 worker 进程内执行（9.2.4 唯一消费模式）：必须是模块级可导入
        函数（同步或异步均可），且不与主进程共享内存——通过回调副作用收集
        结果时请写入文件/队列等进程外介质。

        Args:
            topic_pattern: 主题模式（支持 ``foo.*`` 前缀通配）。
            callback: 消息回调。``header_only=False`` 时接收 ``PulseMessage``，
                ``header_only=True`` 时接收 ``FrameHeader``（跳过完整 decode，降低延迟）。
            header_only: 仅需 topic/record_count/timestamp_ns 时设 True，跳过反序列化。
        """
        self._validate_worker_callback(
            callback, f"订阅 {topic_pattern!r} 的回调")
        self._subscriptions[topic_pattern] = callback
        self._sub_header_only[topic_pattern] = header_only
        # 仅在已连接时立即生效；未连接时缓存，start() 末尾会 flush（A3）
        if self._connected:
            if self._pool is None:
                # 运行期首次订阅：懒创建消费池（含全部既有订阅）
                self._ensure_pool()
            else:
                self._pool.add_subscription(topic_pattern, callback, header_only)
            await self._send_subscribe(topic_pattern)

    # ---------------------------------------------------------------- publish

    @require_connected
    async def publish(self, topic: str, data: Any, *,
                      serializer: str | None = None,
                      compression: str = "none",
                      data_type: int | None = None,
                      confirm: bool = False,
                      ack_timeout: float = 5.0) -> int | None:
        """发布一条消息。

        confirm=True（9.2.1+）：等服务端 PUBLISH_ACK，返回服务端分配的
        per-topic 序号。语义 = 服务端已接受该帧（含路由），不代表订阅者已
        送达。上行帧头部携带 4B ack_token（flags bit6），超时抛
        ``PublishAckTimeout``。
        """
        if not confirm:
            frame = frames.encode(topic, data, serializer=serializer,
                                  compression=compression, data_type=data_type)
            await self._transport.send(b"", frame, role="data")
            return None
        ack_token = next(self._ack_counter) & 0xFFFFFFFF
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_acks[ack_token] = fut
        try:
            frame = frames.encode(topic, data, serializer=serializer,
                                  compression=compression, data_type=data_type,
                                  ack_token=ack_token)
            await asyncio.wait_for(
                self._transport.send(b"", frame, role="data"),
                timeout=ack_timeout)
            payload = await asyncio.wait_for(fut, timeout=ack_timeout)
            return int(payload.get("seq") or 0)
        except asyncio.TimeoutError:
            raise PublishAckTimeout(
                f"确认发布超时（{ack_timeout}s）topic={topic} "
                f"ack_token={ack_token}") from None
        finally:
            self._pending_acks.pop(ack_token, None)

    def gap_stats(self) -> dict[str, dict[str, int]]:
        """消费端缺口统计快照（9.2.1）：topic -> {missing, last_seq}。

        missing = 按帧头 seq 检测的累计缺失帧数（断线期间不计入 last_seq，
        重连后从新 seq 继续检测）。与心跳上报累计值一致。
        """
        return {
            topic: {"missing": self._gap_missing.get(topic, 0),
                    "last_seq": self._gap_last.get(topic, 0)}
            for topic in set(self._gap_last) | set(self._gap_missing)
        }

    def _track_gap(self, topic: str, seq: int) -> None:
        """帧头 seq 缺口检测（recv 线程内联，数据面热路径轻量操作）。"""
        last = self._gap_last.get(topic)
        if last is not None and seq > last + 1:
            self._gap_missing[topic] = self._gap_missing.get(topic, 0) + (seq - last - 1)
        if last is None or seq > last:
            self._gap_last[topic] = seq

    def _drain_gap_report(self) -> dict[str, int]:
        """取自上次心跳以来新增缺口（心跳上报用），不重置累计值。"""
        delta: dict[str, int] = {}
        for topic, total in self._gap_missing.items():
            d = total - self._gap_reported.get(topic, 0)
            if d > 0:
                delta[topic] = d
            self._gap_reported[topic] = total
        return delta

    # -------------------------------------------------------------- recv loop

    async def _recv_loop(self) -> None:
        """消费数据面帧：批量排空接收 + 轻量头部解析 + 按 key 路由进池。

        9.2.7 批量化：一次事件循环唤醒后 DONTWAIT 连收（最多 _RECV_DRAIN_MAX
        帧）成批处理，摊薄逐帧 await 的事件循环往返；批内为纯同步热路径
        （无 await），接收统计按 topic 聚合一次合并。完整 decode + 回调仍在
        worker 进程执行（绕开 GIL，真并行）。池未创建（纯发布客户端）时
        数据帧直接丢弃。
        """
        while not self._stop.is_set():
            try:
                _, first = await self._transport.recv("data")
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("client 数据面 recv 异常")
                continue
            batch = [first]
            for _ in range(_RECV_DRAIN_MAX - 1):
                try:
                    _, fb = await self._transport.recv_nowait("data")
                except zmq.Again:
                    break
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("client 数据面排空异常")
                    break
                batch.append(fb)
            reports = self._process_batch(batch)
            for topic, latency_ns in reports:
                try:
                    rep = frames.encode_control(
                        ControlCmd.LATENCY_REPORT,
                        {"topic": topic, "latency_ns": latency_ns},
                    )
                    # 超时保护（9.2.1）：控制面管道满时不阻塞 recv 循环
                    await asyncio.wait_for(
                        self._transport.send(b"", rep, role="control"),
                        timeout=_CONTROL_SEND_TIMEOUT)
                except Exception:
                    logger.debug("延迟回传发送失败", exc_info=True)

    def _process_batch(self, batch: list[bytes]) -> list[tuple[str, int]]:
        """同步处理一批帧（热路径，无 await）。

        逐帧：peek 头部（不构造 FrameHeader）→ 控制帧分发（PUBLISH_ACK）/
        缺口检测 → 池路由；接收延迟统计按 topic 聚合，批尾一次
        merge_recv（替代逐帧 record_recv 加锁）。返回待发送的延迟采样
        回传 (topic, latency_ns) 列表，由调用方批后统一发送。
        """
        pool = self._pool
        gap_last = self._gap_last
        gap_missing = self._gap_missing
        route_drops = self._route_drops
        recv_acc: dict[str, list[int]] = {}
        reports: list[tuple[str, int]] = []
        do_sample = self._latency_sample_rate > 0
        now_ns = time.time_ns()
        for frame_bytes in batch:
            try:
                topic, seq, ts_ns, msg_type = frames.peek_topic_seq(frame_bytes)
            except Exception:
                logger.debug("client 帧头部解码失败，丢弃")
                continue
            # 9.2.1：数据面 socket 上的控制帧（PUBLISH_ACK）→ resolve 等待方
            if msg_type == MsgType.CONTROL:
                try:
                    msg = frames.decode_control(frame_bytes)
                except Exception:
                    logger.debug("client 数据面控制帧解码失败，丢弃")
                    continue
                if msg.cmd == ControlCmd.PUBLISH_ACK:
                    ack_fut = self._pending_acks.get(msg.payload.get("ack_token"))
                    if ack_fut is not None and not ack_fut.done():
                        ack_fut.set_result(msg.payload)
                continue
            # 消费端缺口检测（v3 帧头恒带 seq，服务端已改写；与 _track_gap 同构）
            last = gap_last.get(topic)
            if last is not None and seq > last + 1:
                gap_missing[topic] = gap_missing.get(topic, 0) + (seq - last - 1)
            if last is None or seq > last:
                gap_last[topic] = seq
            # 端到端延迟采样回传（批内仅记录，批后统一发送）
            if do_sample and random.random() < self._latency_sample_rate:
                reports.append((topic, time.time_ns() - ts_ns))
            # 多进程消费池路由（9.2.4）：匹配/解码/回调全在 worker 进程
            if pool is None:
                continue  # 纯发布客户端：未订阅，数据帧丢弃
            if pool.route(topic, frame_bytes):
                acc = recv_acc.get(topic)
                if acc is None:
                    recv_acc[topic] = [now_ns - ts_ns, 1]
                else:
                    acc[0] += now_ns - ts_ns
                    acc[1] += 1
            else:
                route_drops[topic] = route_drops.get(topic, 0) + 1
                # 环满丢弃的帧 seq 已被观测——回退基线，
                # 让后续帧的缺口计数把它包含进来（否则漏报）
                if gap_last.get(topic) == seq:
                    gap_last[topic] = seq - 1
        for topic, (total_ns, cnt) in recv_acc.items():
            self._proc_stats.merge_recv(topic, total_ns, cnt)
        return reports

    # -------------------------------------------------------- heartbeat loop

    async def _heartbeat_loop(self) -> None:
        """周期发送 HEARTBEAT 控制帧；ack fire-and-forget。

        心跳携带 credit（全部环空闲量，服务端据此做信用流控）与自上次心跳
        以来的 per-topic 丢弃量（环满 drop-new 计数），供服务端 DropStats
        聚合监控。worker 统计（处理速率/耗时）一并聚合上报。
        """
        while not self._stop.is_set():
            try:
                payload: dict = {"client_id": self._client_id}
                if self._pool is not None:
                    # 9.2.4 多进程池：credit = 全环空闲字节/观测最大帧（保守帧数）
                    payload["credit"] = self._pool.free_credit()
                    drops = dict(self._route_drops)
                    self._route_drops.clear()
                    for st in self._pool.drain_stats():
                        for t, (total, cnt) in st.get("recv", {}).items():
                            self._proc_stats.merge_recv(t, total, cnt)
                        for t, (total, cnt) in st.get("proc", {}).items():
                            self._proc_stats.merge_proc(t, total, cnt)
                    if drops:
                        payload["drops"] = drops
                # 消费端缺口上报（9.2.1）：自上次心跳以来的新增缺失量
                gaps = self._drain_gap_report()
                if gaps:
                    payload["gaps"] = gaps
                # 处理统计：per-topic 处理速率 + 接收/处理延迟均值（自上次心跳以来）
                proc = self._proc_stats.drain()
                if proc:
                    payload["proc"] = proc
                hb = frames.encode_control(ControlCmd.HEARTBEAT, payload)
                # 超时保护（9.2.1）：服务端控制管道满时不阻塞心跳循环
                await asyncio.wait_for(
                    self._transport.send(b"", hb, role="control"),
                    timeout=_CONTROL_SEND_TIMEOUT)
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
        # 停止多进程消费池（排空 ~150ms 后退出 worker）
        if self._pool is not None:
            self._pool.stop()
            self._pool = None
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
    """只订阅，屏蔽 publish。

    9.2.4 起消费端只有一种模式——多进程消费池（默认 workers=1，即
    "一个主进程接收 + 一个 worker 进程处理"）：

    - 主进程 recv 循环批量排空接收（一次唤醒 DONTWAIT 连收），只做轻量
      头部解析 + key 路由（零 payload 解码），worker 进程经共享内存环取帧、
      解码并执行回调，真并行（绕开 GIL）；
      基准见性能报告第七章（4 worker 6~23 倍于旧单进程模式）；
    - workers=N：N 个 worker 进程；key：默认 None = 最短队列分发（不保序；
      worker 等速时即轮询，慢 worker 自动降载）；"topic" = 同 topic 恒定
      落同 worker 且保序（零解码成本，jump consistent hash——worker 数
      变化时仅 ~1/n 换位）；payload 字段名
      （如 "symbol"）= 按字段值路由（接收侧需解码 payload，有代价）；
      或 callable(payload, topic)->str 自定义提取（运行在主进程，lambda 可用）；
    - 回调（含异步回调）必须是模块级可导入函数：跨进程按引用传递，
      lambda/闭包不支持——worker 内不共享主进程内存，副作用需落到
      进程外介质（文件/队列等）；worker_init(worker_index) 做每 worker 初始化；
    - 心跳上报 credit（全部环空闲量）与丢帧（环满 drop-new 计数）；
    - 9.2.1 默认开启服务端缓冲（buffer_policy="drop_old"）：消费端处理
      不过来时服务端代为积压（默认上限 10 万条 / 10MB / 30s，任一触发即
      开始丢弃最旧），把"静默丢帧"变成"可观测的积压 + 超龄过期"。
      上限可通过 buffer_cfg 调小（不能超过服务端上限）；传
      buffer_policy=None 恢复直发。
    """

    def __init__(self, *args,
                 buffer_policy: str | None = "drop_old",
                 buffer_cfg: dict | None = None,
                 workers: int = 1,
                 key: str | None = None,
                 worker_ring_mb: int = 64,
                 worker_init=None,
                 **kwargs) -> None:
        if buffer_cfg is None:
            buffer_cfg = {"max_age_s": 30.0}
        super().__init__(*args, buffer_policy=buffer_policy,
                         buffer_cfg=buffer_cfg, workers=workers, key=key,
                         worker_ring_mb=worker_ring_mb, worker_init=worker_init,
                         **kwargs)
        self._roles = ["subscriber"]

    async def publish(self, topic: str, data: Any) -> None:  # type: ignore[override]
        raise NotImplementedError("ConsumerClient 不支持发布")
