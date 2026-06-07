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
        # 主循环把 broadcast_frames 放入队列，由独立协程发送到 XPUB
        # 这样主循环不被 XPUB 发送阻塞，recv 和 broadcast 并行
        self._broadcast_queue: asyncio.Queue[list[bytes] | None] | None = None
        self._broadcast_task: asyncio.Task | None = None

        # 运行状态
        self._running = False
        self._metrics = EngineMetrics()

    async def run(self) -> None:
        """启动消息主循环。"""
        self._running = True

        # #7 优化：启动 broadcast 消费协程
        # 无限队列：主循环 put_nowait 不阻塞，broadcast_loop 按自己节奏消费
        self._broadcast_queue = asyncio.Queue(maxsize=0)  # 0 = unlimited
        self._broadcast_task = asyncio.create_task(self._broadcast_loop())

        # 注入解耦队列到 handlers
        self._handlers.set_broadcast_queue(self._broadcast_queue)

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

        # 直接取消 broadcast 协程（不等队列清空）
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

    async def _broadcast_loop(self) -> None:
        """#7 优化：独立协程消费 broadcast 队列，发送到 XPUB。

        主循环把 broadcast 帧放入 _broadcast_queue，此协程异步消费。
        同时消费 XPUB 的 SUBSCRIBE/UNSUBSCRIBE 确认消息。
        """
        assert self._broadcast_queue is not None
        transport = self._transport
        xpub = transport._xpub

        while self._running:
            try:
                # 消费 XPUB 的订阅/取消确认消息（避免接收缓冲区满）
                if xpub is not None:
                    try:
                        while await xpub.poll(timeout=0, flags=zmq.POLLIN):
                            await xpub.recv_multipart()
                    except Exception:
                        pass

                # 等待 broadcast 帧
                frames = await self._broadcast_queue.get()
                if frames is None:
                    break  # 哨兵，停止
                await transport.broadcast(frames)
                self._broadcast_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("broadcast 协程异常")

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
