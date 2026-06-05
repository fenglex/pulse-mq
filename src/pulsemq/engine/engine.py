"""Engine 消息主循环：自适应批处理 + 同 topic 有序 + 信号量并发 + 背压。

核心设计：
- 主循环只负责最快把消息从 socket 取出来，不等待处理完成
- 按 topic 分组，同 topic 串行保序，不同 topic 并发
- 信号量控制总并发，pending_tasks 超阈值暂停 recv（背压）
- 自适应批大小：低负载=1（即时处理），高负载渐进增大
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field

from pulsemq.config import BrokerConfig
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.overload import DualBuffer
from pulsemq.engine.pipeline import PipelineContext
from pulsemq.engine.pool import MessageContextPool
from pulsemq.protocol.frames import FrameCodec
from pulsemq.transport.zmq_transport import ZmqTransport

logger = logging.getLogger(__name__)


@dataclass
class EngineMetrics:
    """引擎运行指标。"""

    effective_batch_size: int = 1
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
        config: BrokerConfig,
    ):
        self._transport = transport
        self._handlers = handlers
        self._config = config

        # 并发控制
        self._sem = asyncio.Semaphore(config.max_concurrency)
        self._pending_tasks: int = 0
        self._max_concurrency = config.max_concurrency

        # 自适应批大小
        self._effective_batch_size: int = 1
        self._max_batch_size: int = config.max_batch_size
        self._drain_timeout_ms: int = config.drain_timeout_ms
        self._batch_history: list[int] = []
        self._adapt_window: int = 10

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

        # 运行状态
        self._running = False
        self._metrics = EngineMetrics()

    async def run(self) -> None:
        """启动消息主循环。"""
        self._running = True
        logger.info(
            "Engine 启动: max_concurrency=%d, max_batch_size=%d",
            self._max_concurrency, self._max_batch_size,
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
                batch = [frames]

                # 4. 排空缓冲区
                await self._drain_socket(batch)

                # 5. 按 topic 分组派发
                await self._dispatch_batch(batch)

                # 6. 更新自适应批大小
                self._adapt_batch_size(len(batch))

            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Engine 消息循环异常")
                if self._running:
                    continue
                break

    async def stop(self) -> None:
        """停止引擎。"""
        self._running = False

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

    async def _drain_socket(self, batch: list) -> None:
        """排空 ZMQ socket 缓冲区到 batch。"""
        timeout = self._drain_timeout_ms / 1000.0
        effective_max = self._effective_batch_size - 1  # 已有 1 条

        for _ in range(effective_max):
            try:
                frames = await asyncio.wait_for(
                    self._transport.recv(), timeout=timeout
                )
                batch.append(frames)
            except asyncio.TimeoutError:
                break

    async def _dispatch_batch(self, batch: list) -> None:
        """按 topic 分组派发，同 topic 串行，不同 topic 并发。"""
        if not batch:
            return

        # 按 (identity, topic) 分组 — 同连接同 topic 串行
        groups: dict[tuple[bytes, str], list[list[bytes]]] = defaultdict(list)
        for frames in batch:
            decoded = FrameCodec.decode_server(frames)
            key = (decoded.identity, decoded.topic)
            groups[key].append(frames)

        # 每组创建一个 task
        tasks = []
        for key, group_frames in groups.items():
            task = asyncio.create_task(self._process_group(group_frames))
            tasks.append(task)

        # 等待所有 task 完成
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_group(self, frames_list: list[list[bytes]]) -> None:
        """处理同一 topic 的一批消息（串行保证有序）。"""
        async with self._sem:
            self._pending_tasks += 1
            try:
                for frames in frames_list:
                    await self._process_single(frames)
            finally:
                self._pending_tasks -= 1

    async def _process_single(self, frames: list[bytes]) -> None:
        """处理单条消息（通过 handlers.dispatch，含拦截器链）。"""
        try:
            await self._handlers.dispatch(frames)
            self._metrics.total_messages += 1
        except Exception as e:
            self._metrics.total_errors += 1
            logger.debug("消息处理错误: %s", e)

    def _adapt_batch_size(self, actual: int) -> None:
        """自适应调整批大小。"""
        self._batch_history.append(actual)
        if len(self._batch_history) < self._adapt_window:
            return

        # 最近 N 次都排满了 → 增大批
        if all(h >= self._effective_batch_size * 0.8 for h in self._batch_history):
            self._effective_batch_size = min(
                self._effective_batch_size * 2, self._max_batch_size
            )
        # 最近 N 次都收不满 → 减小
        elif all(h < 2 for h in self._batch_history):
            self._effective_batch_size = max(self._effective_batch_size // 2, 1)

        self._batch_history.clear()

    @property
    def metrics(self) -> EngineMetrics:
        self._metrics.effective_batch_size = self._effective_batch_size
        self._metrics.pending_tasks = self._pending_tasks
        self._metrics.concurrency_usage = (
            self._pending_tasks / self._max_concurrency
            if self._max_concurrency > 0 else 0
        )
        return self._metrics

    @property
    def dual_buffer(self) -> DualBuffer:
        return self._dual_buffer
