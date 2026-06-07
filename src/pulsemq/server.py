"""服务端启动器：组装各层并启动消息主循环。"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
import time

import msgpack
import zmq

from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.auth.permission import PermissionService
from pulsemq.auth.zap_handler import PulseMQZAPHandler
from pulsemq.config import ServerConfig, load_config
from pulsemq.engine.engine import Engine
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.pipeline import (
    AuthInterceptor,
    AuthError,
    InterceptorChain,
    MonitorInterceptor,
    PermissionInterceptor,
    PermissionError,
    PipelineContext,
)
from pulsemq.engine.router import MessageRouter
from pulsemq.event_loop import install_event_loop
from pulsemq.monitoring.admin_server import AdminServer
from pulsemq.monitoring.api import MetricsHTTPServer
from pulsemq.monitoring.client_tracker import ClientTracker
from pulsemq.monitoring.minute import MinuteAggregator
from pulsemq.monitoring.realtime import RealtimeMetrics, TopicMetricsRegistry
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType
from pulsemq.storage.database import init_db, parse_db_url
from pulsemq.storage.sqlite_perm import SqlitePermGroupRepo
from pulsemq.storage.sqlite_user import SqliteUserRepo
from pulsemq.storage.sqlite_stats import SQLiteStatsRepo
from pulsemq.transport.zmq_transport import ZmqTransport

logger = logging.getLogger(__name__)


class PulseServer:
    """PulseMQ 服务端。"""

    def __init__(self, config: ServerConfig | None = None):
        self._config = config or load_config()
        self._router = MessageRouter()

        # 实时监控指标
        self._realtime_metrics = RealtimeMetrics()
        self._minute_aggregator = MinuteAggregator(retention_days=self._config.stats_retention_days)
        self._monitor = MonitorInterceptor(
            realtime_metrics=self._realtime_metrics,
            minute_aggregator=self._minute_aggregator,
        )
        self._metrics_http = MetricsHTTPServer(
            bind=self._config.metrics_bind,
            snapshot_fn=self._realtime_metrics.snapshot,
        )

        # Transport
        self._transport = ZmqTransport(self._config)

        # 存储
        db_path = parse_db_url(self._config.db_url)
        self._db_conn = init_db(db_path)
        self._user_repo = SqliteUserRepo(self._db_conn)
        self._perm_repo = SqlitePermGroupRepo(self._db_conn)

        # Phase 6: topic 统计仓库 (独立 DB, 7 天 TTL)
        stats_db_path = parse_db_url(self._config.stats_db_url)
        self._stats_repo = SQLiteStatsRepo(
            stats_db_path, retention_days=self._config.stats_retention_days
        )

        # 认证
        self._auth_store = AuthMemoryStore()
        self._perm_service = PermissionService(
            self._perm_repo, user_repo=self._user_repo
        )

        # 注入 auth_store 引用到 router（用于连接计数）
        self._router._auth_store = self._auth_store

        # ZAP Handler
        self._zap_handler = PulseMQZAPHandler(
            auth_store=self._auth_store,
            user_lookup_fn=self._user_repo.get_by_api_key,
        )

        # 拦截器链：Monitor 在最外层，记录所有成功/失败
        interceptors: list = [self._monitor]                   # 外层：记录延迟和错误
        if self._config.auth_enabled:
            interceptors.append(AuthInterceptor(self._auth_store))         # 认证
            interceptors.append(PermissionInterceptor(self._perm_service)) # 权限
        pipeline = InterceptorChain(interceptors)

        # Phase 4: 客户端追踪器
        self._client_tracker = ClientTracker()

        # Phase 5: topic 维度 1-min 监控注册表
        self._topic_metrics = TopicMetricsRegistry()

        # 处理器
        self._handlers = MessageHandlers(
            router=self._router,
            send_fn=self._transport.send,
            broadcast_fn=self._transport.broadcast,
            pipeline=pipeline,
            default_ser=self._config.default_serializer,
            default_comp=self._config.default_compressor,
            client_tracker=self._client_tracker,
            topic_metrics=self._topic_metrics,
        )

        # Engine
        self._engine = Engine(
            transport=self._transport,
            handlers=self._handlers,
            config=self._config,
        )

        self._running = False
        # Phase 6: 后台任务引用
        self._stats_cleanup_task: asyncio.Task | None = None
        self._stats_minute_task: asyncio.Task | None = None
        # Phase 8: 服务端启动时间 (给 AdminServer 用)
        self._server_start_time: float = time.time()

        # Phase 8: AdminServer (REST + SSE + Web UI)
        self._admin_server = AdminServer(
            bind=self._config.admin_bind,
            client_tracker=self._client_tracker,
            topic_metrics=self._topic_metrics,
            realtime_metrics=self._realtime_metrics,
            stats_repo=self._stats_repo,
            user_repo=self._user_repo,
            perm_service=self._perm_service,
            perm_repo=self._perm_repo,
            snapshot_fn=self._realtime_metrics.snapshot,
            start_time=self._server_start_time,
        )

    async def start(self) -> None:
        """启动服务端。"""
        logger.info("事件循环: %s", type(asyncio.get_event_loop()).__name__)

        await self._transport.start()

        # 注册 ZMQ 事件监听（连接/断开）
        try:
            self._transport._router.setsockopt(zmq.ROUTER_NOTIFY, zmq.NOTIFY_DISCONNECT)
        except (zmq.ZMQError, AttributeError):
            pass  # 旧版 pyzmq 不支持 ROUTER_NOTIFY

        # 启动监控
        if self._config.metrics_enabled:
            await self._metrics_http.start()
            await self._minute_aggregator.start()

        # Phase 8: 启动 AdminServer (REST + SSE + Web UI)
        if self._config.admin_enabled:
            await self._admin_server.start()

        # Phase 6: 启动 topic_stats 后台任务 (清理 + 分钟落库)
        self._stats_cleanup_task = await self._stats_repo.start_cleanup_task(
            interval_seconds=300.0,
        )
        self._stats_minute_task = asyncio.create_task(self._stats_minute_loop())

        logger.info(
            "PulseMQ 服务端启动: ROUTER=%s, XPUB=%s",
            self._config.bind, self._config.xpub_bind,
        )

        self._running = True

        # 启动事件监听协程 + 引擎主循环 + 指标同步 + topic 清理
        await asyncio.gather(
            self._event_loop(),
            self._engine.run(),
            self._metrics_sync_loop(),
            self._topic_cleanup_loop(),
            return_exceptions=True,
        )

    async def stop(self) -> None:
        """优雅停止服务端：停止接收 → drain 缓冲 → 关闭传输。"""
        self._running = False
        # Phase 6: 停止 stats 后台任务
        for task in (self._stats_minute_task, self._stats_cleanup_task):
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass
        self._stats_minute_task = None
        self._stats_cleanup_task = None
        # 先停止引擎（等待后台任务完成）
        await self._engine.stop()
        # drain 双缓冲残余消息
        drained = await self._engine._drain_buffers()
        if drained > 0:
            logger.info("优雅关闭: drain %d 条缓冲消息", drained)
        if self._config.metrics_enabled:
            await self._minute_aggregator.stop()
            await self._metrics_http.stop()
        # Phase 8: 关闭 AdminServer
        if self._config.admin_enabled:
            try:
                await self._admin_server.stop()
            except Exception as e:
                logger.debug("admin_server stop 异常: %s", e)
        await self._transport.stop(linger_ms=2000)
        if self._db_conn:
            self._db_conn.close()
        # Phase 6: 关闭 stats 仓库
        try:
            self._stats_repo.close()
        except Exception as e:
            logger.debug("stats_repo close 异常: %s", e)
        logger.info("PulseMQ 服务端已停止")

    async def _event_loop(self) -> None:
        """监听 ZMQ 连接/断开事件，管理认证和资源清理。"""
        monitor_socket = self._transport._router.get_monitor_socket(
            zmq.EVENT_CONNECTED | zmq.EVENT_DISCONNECTED
        )
        if monitor_socket is None:
            # 某些 pyzmq 版本不支持 get_monitor_socket，跳过
            logger.debug("ZMQ monitor socket 不可用，跳过事件监听")
            return

        try:
            while self._running:
                try:
                    event = await monitor_socket.recv_multipart()
                    if len(event) < 2:
                        continue
                    # event[0] = 事件类型（2 bytes）, event[1] = 地址
                    event_type = int.from_bytes(event[0][:2], "little")
                    address = event[1]

                    if event_type & zmq.EVENT_CONNECTED:
                        await self._on_connected(address)
                    elif event_type & zmq.EVENT_DISCONNECTED:
                        await self._on_disconnected(address)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.debug("事件监听异常: %s", e)
                    await asyncio.sleep(0.1)
        finally:
            monitor_socket.close()

    async def _metrics_sync_loop(self) -> None:
        """定期同步引擎和缓冲区指标到 RealtimeMetrics。"""
        while self._running:
            try:
                # 同步 DualBuffer 丢弃计数
                self._realtime_metrics.dropped_messages = (
                    self._engine.dual_buffer.dropped_total
                )
                # 同步背压状态
                self._realtime_metrics.backpressure = (
                    self._engine._pending_tasks
                    > self._engine._max_concurrency * self._engine._backpressure_threshold
                )
                # 同步引擎指标
                metrics = self._engine.metrics
                self._realtime_metrics.update_engine_metrics(
                    batch_size=metrics.effective_batch_size,
                    pending_tasks=metrics.pending_tasks,
                    concurrency_usage=metrics.concurrency_usage,
                )
            except Exception as e:
                logger.debug("指标同步异常: %s", e)
            await asyncio.sleep(1.0)

    async def _stats_minute_loop(self) -> None:
        """每分钟把 TopicMetricsRegistry 当前快照写入 SQLite。

        1) 等到下一个整分钟边界
        2) 遍历 topic_metrics 所有 topic, 写入 (minute_ts, msg_count, p50, p99, max_lat, peak)
        3) 落库后 reset_window 让指标重新计数
        """
        while self._running:
            try:
                # 等到下一分钟边界
                now = time.time()
                next_minute = (int(now) // 60 + 1) * 60
                await asyncio.sleep(max(0.1, next_minute - now))

                minute_ts = int(next_minute)  # 整分钟秒
                topics = self._topic_metrics.list_topics()
                for m in topics:
                    if m.msg_count_1min == 0:
                        # 空窗口: 跳过, 不写 (避免噪声)
                        continue
                    await self._stats_repo.upsert_minute(
                        topic=m.topic,
                        minute_ts=minute_ts,
                        msg_count=m.msg_count_1min,
                        p50=m.latency_p50_1min,
                        p99=m.latency_p99_1min,
                        max_lat=m.latency_max_1min,
                        peak_in_flight=self._topic_metrics.peak_in_flight(m.topic),
                    )
                    # 落库后重置窗口
                    self._topic_metrics.reset_window(m.topic)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("stats 分钟落库异常: %s", e)

    async def _topic_cleanup_loop(self) -> None:
        """定期清理无订阅者且无缓冲消息的空闲 topic。"""
        while self._running:
            try:
                await asyncio.sleep(300)  # 每 5 分钟执行一次
                topics = list(self._router._topics.keys())
                cleaned = 0
                for topic_name in topics:
                    info = self._router._topics.get(topic_name)
                    if info is None:
                        continue
                    # 无订阅者 + 缓冲已空 → 移除
                    subs = self._router._topic_subscribers.get(topic_name, set())
                    buf = self._router._buffers.get(topic_name)
                    if not subs and (buf is None or len(buf) == 0):
                        self._router._topics.pop(topic_name, None)
                        self._router.remove_topic_buffer(topic_name)
                        cleaned += 1
                if cleaned > 0:
                    logger.info("Topic 清理: 移除 %d 个空闲 topic", cleaned)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("Topic 清理异常: %s", e)

    async def _on_connected(self, address: bytes) -> None:
        """连接建立：查找用户信息 → 注入 AuthMemoryStore → 推送 AUTH 元信息。"""
        # 通过 ZAP handler 查找（ZAP 可能在 IO 线程中已注册）
        user = self._auth_store.get_user(address)
        if user is None:
            # 未通过 ZAP 认证的连接，尝试用默认 api_key 查找
            # （简化模式：允许默认 admin 连接）
            if self._config.auth_enabled:
                return
            # auth_disabled 模式下注入 admin
            db_user = await self._user_repo.get_by_api_key(self._config.default_admin_key)
            if db_user:
                from pulsemq.models import AuthUser
                user = AuthUser(
                    user_id=db_user.id,
                    role=db_user.role,
                    groups=[],
                    api_key=db_user.api_key,
                    namespace=db_user.namespace,
                )
                self._auth_store.register(address, user)

        if user is not None:
            # 推送 AUTH 元信息
            await self._push_auth_info(address, user)
            # 更新实时监控
            self._realtime_metrics.active_connections = len(
                self._auth_store._identity_user
            )
            # Phase 4: 客户端追踪 - 注册新连接
            self._client_tracker.on_connect(address, user.user_id)
            logger.info("连接建立: user_id=%s role=%s", user.user_id, user.role)

    async def _on_disconnected(self, address: bytes) -> None:
        """连接断开：清理认证 + 订阅 + 连接映射。"""
        user = self._auth_store.unregister(address)
        if user is not None:
            self._router.remove_identity(address)
            self._monitor.remove_identity(address)
            self._realtime_metrics.active_connections = len(
                self._auth_store._identity_user
            )
            self._realtime_metrics.active_subscriptions = self._router.subscription_count()
            # Phase 4: 客户端追踪 - 移除断开连接
            self._client_tracker.on_disconnect(address)
            logger.info("连接断开: user_id=%s", user.user_id)

    async def _push_auth_info(self, identity: bytes, user) -> None:
        """推送 AUTH 元信息给客户端。"""
        auth_info = {
            "user_id": user.user_id,
            "role": user.role,
            "namespace": user.namespace,
            "groups": user.groups,
            "server_time": time.time(),
        }
        payload = FrameCodec.encode_payload(auth_info, "msgpack", "none")
        frames = FrameCodec.encode(MsgType.AUTH, "", 0, payload, "msgpack", "none")
        try:
            await self._transport.send(identity, frames)
        except Exception as e:
            logger.debug("AUTH 推送失败: %s", e)

    @property
    def engine(self) -> Engine:
        return self._engine

    @property
    def router(self) -> MessageRouter:
        return self._router

    @property
    def monitor(self) -> MonitorInterceptor:
        return self._monitor

    @property
    def realtime_metrics(self) -> RealtimeMetrics:
        return self._realtime_metrics

    @property
    def client_tracker(self) -> ClientTracker:
        return self._client_tracker

    @property
    def topic_metrics(self) -> TopicMetricsRegistry:
        return self._topic_metrics

    @property
    def stats_repo(self) -> SQLiteStatsRepo:
        return self._stats_repo

    @property
    def admin_server(self) -> AdminServer:
        return self._admin_server


def main() -> None:
    """CLI 入口: pulse-mq 命令。"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = load_config()

    # 安装事件循环（必须在 asyncio.new_event_loop 之前）
    # Windows → SelectorEventLoop; Linux/macOS → uvloop（如已安装）
    loop_type = install_event_loop(config.use_uvloop)
    logger.info("事件循环: %s", loop_type)

    server = PulseServer(config)

    loop = asyncio.new_event_loop()

    def _shutdown():
        loop.call_soon_threadsafe(lambda: asyncio.ensure_future(server.stop()))

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown)

    try:
        loop.run_until_complete(server.start())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(server.stop())
        loop.close()


if __name__ == "__main__":
    main()
