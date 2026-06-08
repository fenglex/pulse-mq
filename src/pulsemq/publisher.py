"""PulsePublisher: v2 入口类（编排各层）。

纯 pub → sub 架构，单进程 publisher：
- ZMQ PUB socket 广播
- Producer 回调注册 + 并发调度
- 流量统计 + Admin 后台
- Topic 缓存

用法:
    pub = PulsePublisher()

    @pub.producer(name="sh_market", interval=5.0)
    async def sh_market():
        return fetch_data()

    pub.start()  # 阻塞运行
"""

from __future__ import annotations

import asyncio
import logging
import time
from functools import wraps
from typing import Any, Callable, Awaitable

from pulsemq.admin.server import AdminServer
from pulsemq.cache.topic_buffer import TopicBufferRegistry
from pulsemq.config import PublisherConfig, load_config
from pulsemq.producers.manager import ProducerManager
from pulsemq.protocol import frames as frame_codec
from pulsemq.stats.storage import StatsStorage
from pulsemq.stats.traffic import TrafficStats
from pulsemq.transport.zmq_pub import ZmqPubTransport

logger = logging.getLogger(__name__)


class PulsePublisher:
    """PulseMQ v2 Publisher 入口类。"""

    def __init__(
        self,
        config: PublisherConfig | None = None,
        *,
        bind: str | None = None,
        admin_bind: str | None = None,
        api_keys: dict[str, str] | None = None,
    ) -> None:
        self._config = config or load_config()
        # 参数覆盖
        if bind:
            self._config.bind = bind
        if admin_bind:
            self._config.admin_bind = admin_bind
        self._explicit_api_keys = api_keys

        # 内部组件
        self._transport: ZmqPubTransport | None = None
        self._producer_mgr = ProducerManager()
        self._buffers = TopicBufferRegistry()
        self._traffic = TrafficStats(self._config.stats_retention_minutes)
        self._storage: StatsStorage | None = None
        self._admin: AdminServer | None = None

        self._start_time: float = 0
        self._running = False

    # ---- Producer 注册 ----

    def producer(
        self,
        name: str,
        *,
        interval: float = 5.0,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
    ) -> Callable:
        """装饰器：注册 async producer。"""
        def decorator(fn: Callable[[], Awaitable[Any]]) -> Callable[[], Awaitable[Any]]:
            self._producer_mgr.register(
                callback=fn,
                name=name,
                interval=interval,
                cache_size=cache_size,
                serializer=serializer,
                compression=compression,
            )
            return fn
        return decorator

    def register_producer(
        self,
        fn: Callable[[], Awaitable[Any]],
        *,
        name: str,
        interval: float = 5.0,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
    ) -> None:
        """直接注册 async producer。"""
        self._producer_mgr.register(
            callback=fn,
            name=name,
            interval=interval,
            cache_size=cache_size,
            serializer=serializer,
            compression=compression,
        )

    def add_api_key(self, username: str, password: str) -> None:
        """编程式添加 API Key。需要在 start() 前调用。"""
        if self._explicit_api_keys is None:
            self._explicit_api_keys = {}
        self._explicit_api_keys[username] = password

    # ---- 启动 ----

    def start(self) -> None:
        """阻塞启动 publisher。"""
        asyncio.run(self._run())

    async def start_async(self) -> None:
        """异步启动 publisher（方便嵌入其他 asyncio 程序）。"""
        await self._run()

    async def _run(self) -> None:
        """主运行循环。"""
        self._running = True
        self._start_time = time.time()

        # 确定最终 api_keys
        api_keys = self._explicit_api_keys or self._config.api_keys

        # 初始化传输层
        self._transport = ZmqPubTransport(
            bind=self._config.bind,
            api_keys=api_keys,
        )
        await self._transport.start()

        # 初始化统计存储
        self._storage = StatsStorage(self._config.stats_db)
        self._storage.connect()

        # 为所有 producer 创建 topic 缓存
        for name, spec in self._producer_mgr.specs.items():
            self._buffers.get_or_create(name, spec.cache_size)

        # 初始化 Admin 后台
        self._admin = AdminServer(
            bind=self._config.admin_bind,
            traffic_stats=self._traffic,
            topic_buffers=self._buffers,
            stats_storage=self._storage,
            snapshot_fn=self._system_snapshot,
            start_time=self._start_time,
        )
        await self._admin.start()

        # 启动分钟滚动任务
        roll_task = asyncio.create_task(self._minute_roll_loop())

        # 启动所有 producer
        await self._producer_mgr.start_all(self._on_produce)

        logger.info("PulsePublisher 运行中 (bind=%s, admin=%s)", self._config.bind, self._config.admin_bind)

        try:
            # 等待运行结束
            while self._running:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            await self._shutdown(roll_task)

    async def _shutdown(self, roll_task: asyncio.Task) -> None:
        """优雅关闭。"""
        self._running = False
        await self._producer_mgr.stop_all()
        roll_task.cancel()
        try:
            await roll_task
        except asyncio.CancelledError:
            pass

        # 最后一次分钟滚动 + 落库
        archived = self._traffic.roll_minute()
        if self._storage and archived:
            self._storage.save_minutes_batch(archived)

        if self._admin:
            await self._admin.stop()
        if self._transport:
            await self._transport.stop()
        if self._storage:
            self._storage.close()
        logger.info("PulsePublisher 已关闭")

    async def _on_produce(self, spec: Any, data: Any) -> None:
        """Producer 回调返回数据后的处理流程。"""
        try:
            # 1. 推断类型 + record_count
            record_count = self._infer_record_count(data)
            payload_obj = self._prepare_payload(data)

            # 2. 序列化 + 压缩 + 编码帧
            encoded_frames = frame_codec.encode(
                topic=spec.name,
                data=payload_obj,
                serializer=spec.serializer,
                compression=spec.compression,
                record_count=record_count,
            )

            # 3. 并行分发
            await self._transport.send(encoded_frames)

            # 4. 同步操作：缓存 + 统计
            ts_ns = frame_codec._TS_STRUCT.unpack(encoded_frames[2])[0]
            self._buffers.get_or_create(spec.name, spec.cache_size).append(ts_ns, encoded_frames)
            self._traffic.record(spec.name, record_count, len(encoded_frames[3]))

        except Exception:
            logger.warning("Producer %s 消息处理异常", spec.name, exc_info=True)

    @staticmethod
    def _infer_record_count(data: Any) -> int:
        """推断记录数。"""
        if isinstance(data, list):
            return len(data)
        if hasattr(data, "__len__") and hasattr(data, "columns"):
            # DataFrame
            return len(data)
        return 1

    @staticmethod
    def _prepare_payload(data: Any) -> Any:
        """预处理数据为可序列化格式。"""
        try:
            import pandas as pd
            if isinstance(data, pd.DataFrame):
                return data.to_dict(orient="records")
            if isinstance(data, list):
                # 检查是否包含 DataFrame
                result = []
                for item in data:
                    if isinstance(item, pd.DataFrame):
                        result.extend(item.to_dict(orient="records"))
                    else:
                        result.append(item)
                return result
        except ImportError:
            pass
        return data

    def _system_snapshot(self) -> dict:
        """系统快照（给 admin SSE 用）。"""
        return {
            "start_time": self._start_time,
            "producer_count": len(self._producer_mgr.specs),
        }

    # ---- 分钟滚动 ----

    async def _minute_roll_loop(self) -> None:
        """每分钟执行一次：归档统计 → SQLite 落库。"""
        while self._running:
            now = time.time()
            next_minute = (int(now) // 60 + 1) * 60
            await asyncio.sleep(next_minute - now)
            if not self._running:
                break

            archived = self._traffic.roll_minute()
            if self._storage and archived:
                self._storage.save_minutes_batch(archived)

            # 每小时清理过期数据
            if int(next_minute) % 3600 < 70:
                if self._storage:
                    self._storage.cleanup()


def main() -> None:
    """CLI 入口点。提供最小示例 publisher。"""
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        stream=sys.stderr,
    )
    pub = PulsePublisher()
    print("PulseMQ Publisher v2 — 零配置模式启动", file=sys.stderr)
    print("用法: 参考 PulsePublisher 文档注册 producer", file=sys.stderr)
    pub.start()
