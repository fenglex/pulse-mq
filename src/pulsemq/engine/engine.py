"""Engine 消息主循环：单条派发 + 信号量并发 + 背压。

核心设计：
- 主循环只负责最快把消息从 socket 取出来，不等待处理完成
- 单条消息派发，PUB 走快速路径，其他走拦截器链
- 信号量控制总并发，pending_tasks 超阈值暂停 recv（背压）
- 双缓冲：高负载入双缓冲，低负载直接派发
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from pulsemq.config import ServerConfig
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.overload import DualBuffer
from pulsemq.engine.pipeline import PipelineContext
from pulsemq.engine.pool import MessageContextPool
from pulsemq.protocol.msg_type import MsgType
from pulsemq.transport.zmq_transport import ZmqTransport

import zmq

logger = logging.getLogger(__name__)


@dataclass
class EngineMetrics:
    """引擎运行指标。"""

    pending_tasks: int = 0
    concurrency_usage: float = 0.0
    backpressure_events: int = 0
    total_messages: int = 0
    total_errors: int = 0


class Engine:
    """消息主循环引擎。"""

    def __init__(
        self,
        transport: ZmqTransport,
        handlers: MessageHandlers,
        config: ServerConfig,
    ):
        self._transport = transport
        self._handlers = handlers
        self._config = config

        # 并发控制
        self._sem = asyncio.Semaphore(config.max_concurrency)
        self._pending_tasks: int = 0
        self._max_concurrency = config.max_concurrency

        # 背压
        self._backpressure_threshold: float = config.backpressure_threshold
        self._backpressure_events: int = 0

        # 对象池
        self._ctx_pool = MessageContextPool(size=config.object_pool_size)

        # 双缓冲过载保护
        self._dual_buffer = DualBuffer(
            data_buffer_size=config.data_buffer_size,
            ctrl_buffer_size=config.ctrl_buffer_size,
        )

        # 后台任务集合（防止 GC 回收未完成 task）
        self._background_tasks: set[asyncio.Task] = set()

        # PUB 快速路径开关（auth 关闭时启用）
        self._pub_fast_path = not config.auth_enabled

        # #7 优化：broadcast 解耦队列
        # 注意: v1 zmq_io_threading 后, broadcast_queue 已在 ZmqTransport.BroadcastThread
        # 这里保留 broadcast_queue/broadcast_task 字段为空, 仅供 stop() 调用
        # 主循环直接 await transport.broadcast() (内部入 transport 线程安全队列)
        self._broadcast_queue: asyncio.Queue[list[bytes] | None] | None = None
        self._broadcast_task: asyncio.Task | None = None

        # ---- 累积 metrics 计数器 (无锁, 单写者: Engine 主循环) ----
        # 替代每条消息的 tracker.on_pub/topic_metrics.record 加锁调用
        # 后台 _metrics_flush_loop 每 60s 把累计值推送到 trackers
        self._cum_msg_total: int = 0
        self._cum_per_topic: dict[str, int] = {}
        self._cum_per_client: dict[bytes, int] = {}
        self._cum_latency_sum: float = 0.0
        self._cum_latency_count: int = 0
        self._cum_latency_max: float = 0.0
        self._flush_task: asyncio.Task | None = None
        self._flush_interval_s: float = 60.0

        # 运行状态
        self._running = False
        self._metrics = EngineMetrics()

    async def run(self) -> None:
        """启动消息主循环。

        v1 改造: zmq IO 已线程化, 主循环 recv/broadcast 都不再有 zmq await。
        - recv: await transport.recv() (内部 to_thread 包装 recv_queue.get)
        - broadcast: await transport.broadcast() (内部入 thread-safe 队列)
        """
        self._running = True

        # 注入 Engine 引用 (用于 dispatch_pub_fast 累积 metrics)
        self._handlers.set_engine(self)

        # 启动 metrics 累积 flush 后台 task
        self._flush_task = asyncio.create_task(self._metrics_flush_loop())

        logger.info(
            "Engine 启动: max_concurrency=%d, fast_path=%s",
            self._max_concurrency, self._pub_fast_path,
        )

        while self._running:
            try:
                # 1. 检查背压
                if self._pending_tasks > self._max_concurrency * self._backpressure_threshold:
                    self._backpressure_events += 1
                    logger.debug("背压触发: pending=%d", self._pending_tasks)
                    await asyncio.sleep(0.001)  # 短暂让出
                    continue

                # 2. 先消费双缓冲
                consumed = await self._drain_buffers()
                if consumed > 0:
                    continue  # 优先处理缓冲区

                # 3. recv 第一条（阻塞等待）
                frames = await self._transport.recv()

                # 4. 根据负载决定走双缓冲还是直接派发
                load_ratio = self._pending_tasks / self._max_concurrency if self._max_concurrency else 0
                if load_ratio > self._backpressure_threshold:
                    # 高负载：入双缓冲，由后续 drain 消费
                    self._dual_buffer.enqueue(frames)
                else:
                    # 5. 单条派发（PUB 走 fast path, 其他走 _process_single）
                    await self._dispatch_one(frames)

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Engine 消息循环异常")
                if self._running:
                    continue
                break

    async def stop(self) -> None:
        """停止引擎，等待后台任务完成。"""
        self._running = False

        # 注意: v1 zmq_io_threading 后, broadcast_loop 已被 BroadcastThread 取代
        # 这里不再需要取消 _broadcast_task (其始终为 None)
        if self._broadcast_task is not None:
            self._broadcast_task.cancel()
            try:
                await asyncio.wait_for(self._broadcast_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            self._broadcast_task = None
        self._broadcast_queue = None

        # 等待后台任务完成（最多 5 秒）
        if self._background_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._background_tasks, return_exceptions=True),
                    timeout=5.0,
                )
            except asyncio.TimeoutError:
                # 超时取消剩余任务
                for task in list(self._background_tasks):
                    task.cancel()

        # 停止 metrics flush task, 最后做一次 flush
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except (asyncio.CancelledError, Exception):
                pass
            self._flush_task = None
        # 收尾 flush (把残留累计值推给 trackers)
        self._flush_metrics_once()

    async def _drain_buffers(self) -> int:
        """消费双缓冲中的消息（控制优先）。"""
        consumed = 0

        # 先消费控制消息
        for frames in self._dual_buffer.drain_ctrl():
            await self._process_single(frames)
            consumed += 1

        # 再消费数据消息
        for frames in self._dual_buffer.drain_data():
            await self._process_single(frames)
            consumed += 1

        return consumed

    async def _dispatch_one(self, frames: list[bytes]) -> None:
        """派发单条消息。

        优先走 PUB 快速路径 (绕过拦截器链),否则走 _process_single (含拦截器链)。
        """
        if self._pub_fast_path and self._is_pub_frames(frames):
            try:
                await self._handlers.dispatch_pub_fast(frames)
                self._metrics.total_messages += 1
            except Exception as e:
                self._metrics.total_errors += 1
                logger.debug("快速路径处理错误: %s", e)
            return
        # 拦截器链路径: SUB/UNSUB/PING/QUERY/非 PUB 走这里
        await self._process_single(frames)
    @staticmethod
    def _is_pub_frames(frames: list[bytes]) -> bool:
        """快速判断 ROUTER 帧是否为 PUB 消息（不完整解码）。"""
        # 6 帧: [identity, delimiter, topic, meta, rc, payload] → meta 在 frames[3]
        # 5 帧: [identity, topic, meta, rc, payload] → meta 在 frames[2]
        try:
            meta = frames[3] if len(frames) == 6 else frames[2]
            return len(meta) >= 1 and meta[0] == MsgType.PUB
        except (IndexError, TypeError):
            return False

    async def _process_single(self, frames: list[bytes]) -> None:
        """处理单条消息（通过 handlers.dispatch，含拦截器链）。"""
        try:
            await self._handlers.dispatch(frames)
            self._metrics.total_messages += 1
        except Exception as e:
            self._metrics.total_errors += 1
            logger.debug("消息处理错误: %s", e)

    @property
    def metrics(self) -> EngineMetrics:
        self._metrics.pending_tasks = self._pending_tasks
        self._metrics.concurrency_usage = (
            self._pending_tasks / self._max_concurrency
            if self._max_concurrency > 0 else 0
        )
        return self._metrics

    @property
    def dual_buffer(self) -> DualBuffer:
        return self._dual_buffer

    # ---- 累积 metrics (无锁) ----

    def record_msg(self, topic: str, identity: bytes, latency_ms: float = 0.0) -> None:
        """记录一条消息到累积计数器 (无锁, 单写者: Engine 主循环)。

        替代每条调用 tracker.on_pub() / topic_metrics.record() 的加锁开销。
        后台 _metrics_flush_loop 每 60s 把累计值推送给 trackers。
        """
        self._cum_msg_total += 1
        self._cum_per_topic[topic] = self._cum_per_topic.get(topic, 0) + 1
        self._cum_per_client[identity] = self._cum_per_client.get(identity, 0) + 1
        if latency_ms > 0.0:
            self._cum_latency_sum += latency_ms
            self._cum_latency_count += 1
            if latency_ms > self._cum_latency_max:
                self._cum_latency_max = latency_ms

    async def _metrics_flush_loop(self) -> None:
        """后台 task: 每 60s 推送累积 metrics 到 topic/client trackers。

        监控粒度 = 分钟; 每分钟一次的更新足够, 不需要在每条消息上争锁。
        """
        try:
            while self._running:
                await asyncio.sleep(self._flush_interval_s)
                if not self._running:
                    break
                self._flush_metrics_once()
        except asyncio.CancelledError:
            # stop() 时取消, 此时 stop() 末尾会再调一次 _flush_metrics_once()
            pass
        except Exception:
            logger.exception("metrics flush 异常")

    def _flush_metrics_once(self) -> None:
        """一次性推送累积值到 topic/client trackers, 然后清零。"""
        if self._cum_msg_total == 0:
            return

        # 推 topic metrics
        if self._topic_metrics is not None:
            avg_ms = (
                self._cum_latency_sum / self._cum_latency_count
                if self._cum_latency_count > 0
                else 0.0
            )
            max_ms = self._cum_latency_max
            for topic, count in self._cum_per_topic.items():
                try:
                    self._topic_metrics.flush_minute(topic, count, avg_ms, max_ms)
                except Exception:
                    logger.debug("topic flush_minute 失败: %s", topic)

        # 推 client tracker
        if self._client_tracker is not None:
            for identity, count in self._cum_per_client.items():
                try:
                    self._client_tracker.flush_minute(identity, count)
                except Exception:
                    logger.debug("client flush_minute 失败: %s=%s", identity, count)

        # 清零
        self._cum_msg_total = 0
        self._cum_per_topic.clear()
        self._cum_per_client.clear()
        self._cum_latency_sum = 0.0
        self._cum_latency_count = 0
        self._cum_latency_max = 0.0
