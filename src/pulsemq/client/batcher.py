"""客户端 Batcher: 攒批发送 (count + time 双触发, first-of-both)。

行为 (first-of-both):
  - 队列长度 >= batch_size → flush
  - 距离首次入队 >= batch_interval_ms → flush
  - 距离上次 flush >= batch_max_wait_ms (硬上限) → flush

同一批共享 ser/comp, 必要时按 (ser, comp) 分组 flush.
单条直发模式: batch_size == 1 时 add() 直接调用 send_fn, 不入队。
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


# send_fn 签名: async (items, comp, topic) -> None
# items: list[(ser_fmt, comp, payload_bytes), ...] - 每条带 ser/comp 信息
# comp: 整个批次的统一压缩算法 (BATCH 协议外层 msgpack 包装的 comp)
# topic: 该批次的统一 topic (Batcher 单 topic 设计)
#   实际集成时由 async_client 把 (data, ser_fmt, comp) 预序列化为 bytes.
BatcherSendFn = Callable[[list[tuple[str, str, bytes]], str, str], Awaitable[None]]


class Batcher:
    """攒批发送器 (first-of-both 触发)。"""

    def __init__(
        self,
        send_fn: BatcherSendFn,
        ser_fmt: str = "msgpack",
        comp: str = "none",
        batch_size: int = 1,
        batch_interval_ms: float = 10.0,
        batch_max_wait_ms: float = 50.0,
    ):
        """初始化 Batcher。

        Args:
            send_fn: 实际发送函数, 签名 async (items_list, comp) -> None.
                      items_list 是 list[(ser_fmt, payload_bytes), ...].
                      comp 是整个批次的统一压缩算法 (BATCH 协议在 msgpack 包装层做压缩).
            ser_fmt: 默认序列化格式 (仅用于 _direct 模式直发, 批量模式由 add() 传入).
            comp: 默认压缩算法 (仅用于 _direct 模式直发, 批量模式由 add() 传入).
            batch_size: 触发 flush 的批量大小, == 1 时退化为单条直发。
            batch_interval_ms: 距离首次入队多少毫秒后必须 flush。
            batch_max_wait_ms: 距离上次 flush 多少毫秒后强制 flush (硬上限)。

        Note:
            批量模式下, 每条入队 payload 携带自己的 ser_fmt, 批次共享 comp.
            BATCH 协议外层 msgpack 包装的 comp 来自 add() 的 comp 参数.
        """
        if batch_size < 1:
            raise ValueError(f"batch_size 必须 >= 1, 收到 {batch_size}")
        if batch_interval_ms < 0:
            raise ValueError(f"batch_interval_ms 必须 >= 0, 收到 {batch_interval_ms}")
        if batch_max_wait_ms < 0:
            raise ValueError(f"batch_max_wait_ms 必须 >= 0, 收到 {batch_max_wait_ms}")
        if send_fn is None:
            raise ValueError("send_fn 不能为空")

        self._send_fn = send_fn
        self._ser_fmt = ser_fmt  # 仅 _direct 模式使用
        self._comp = comp  # 仅 _direct 模式使用
        self._batch_size = batch_size
        self._batch_interval_ms = batch_interval_ms
        self._batch_max_wait_ms = batch_max_wait_ms

        # 单条直发模式: batch_size == 1
        self._direct = (batch_size == 1)

        # 内部状态
        # _queue 存 (ser_fmt, comp, payload_bytes) tuple, 批次内可混合
        self._queue: list[tuple[str, str, bytes]] = []
        # 当前批次对应 topic (Batcher 单 topic 假设; 切换 topic 时强制 flush)
        self._current_topic: str = ""
        # 当前批次统一 comp (BATCH 外层 msgpack 包装的 comp)
        self._current_comp: str = "none"
        self._first_enqueue_at: float = 0.0
        # 初始 last_flush_at 用 monotonic() 启动值, 避免"距离上次 flush"在
        # 第 1 条 add 时就满足 max_wait 触发条件 (单调时间, 不依赖 epoch)
        self._last_flush_at: float = time.monotonic()

        # max_wait 后台定时器
        self._max_wait_task: asyncio.Task | None = None

        # 锁: 保护 _queue / _current_topic / _current_comp / 时间字段
        self._lock = asyncio.Lock()

        # 关闭标志
        self._closed = False

    @property
    def batch_size(self) -> int:
        return self._batch_size

    @property
    def ser_fmt(self) -> str:
        return self._ser_fmt

    @property
    def comp(self) -> str:
        return self._comp

    @property
    def pending(self) -> int:
        """当前队列长度 (用于测试)。"""
        return len(self._queue)

    @property
    def current_topic(self) -> str:
        """当前批次对应 topic (空表示队列空)。"""
        return self._current_topic

    async def add(self, payload: bytes, topic: str = "", ser_fmt: str = "", comp: str = "") -> None:
        """入队一条 payload (应是 ser_fmt 序列化后的 bytes)。

        Args:
            payload: 序列化后的 bytes。
            topic: 该 payload 对应 topic. 队列非空时若 topic 与当前 topic
                   不一致, 强制 flush 当前批次再开新批次 (Batcher 单 topic 设计).
            ser_fmt: 该 payload 的序列化格式 (在 _direct 模式与批量模式均透传).
            comp: 该 payload / 该批次的压缩算法 (批量模式统一 BATCH 外层 comp).

        满足任一条件则 flush:
          1. 队列长度 >= batch_size
          2. 距离首次入队 >= batch_interval_ms
          3. 距离上次 flush >= batch_max_wait_ms (硬上限)
          4. topic 切换
        """
        if self._closed:
            raise RuntimeError("Batcher 已关闭")

        # 缺省时用 Batcher 构造时的默认值
        ser_fmt = ser_fmt or self._ser_fmt
        comp = comp or self._comp

        if self._direct:
            # 单条直发: 不入队, 立即发送. topic/ser/comp 透传给 send_fn
            await self._send_fn([(ser_fmt, comp, payload)], comp, topic)
            return

        should_flush = False
        # 记录 topic 切换: 标志位, 用于在 lock 外 flush 后再入新 item
        topic_changed = False
        async with self._lock:
            now = time.monotonic()
            # topic 切换: 先 flush 当前批次, 不入新 item (等 flush 后再入)
            if self._queue and topic and topic != self._current_topic:
                should_flush = True
                topic_changed = True
            else:
                # 正常路径: 直接入队
                if not self._queue:
                    self._first_enqueue_at = now
                    self._current_topic = topic
                    self._current_comp = comp
                self._queue.append((ser_fmt, comp, payload))

                # 条件 1: 数量触发
                if len(self._queue) >= self._batch_size:
                    should_flush = True
                # 条件 2: 距离首次入队时间触发
                elif (now - self._first_enqueue_at) * 1000.0 >= self._batch_interval_ms:
                    should_flush = True
                # 条件 3: 距离上次 flush 硬上限触发
                elif (now - self._last_flush_at) * 1000.0 >= self._batch_max_wait_ms:
                    should_flush = True

        if should_flush:
            await self.flush()
        if topic_changed:
            # 把新 item 入到新 topic 的新批次
            async with self._lock:
                if not self._queue:
                    self._first_enqueue_at = time.monotonic()
                    self._current_topic = topic
                    self._current_comp = comp
                self._queue.append((ser_fmt, comp, payload))

    async def flush(self) -> None:
        """强制 flush 当前所有批次。

        空队列时 noop (除更新 _last_flush_at 外)。
        """
        if self._direct:
            # 单条直发模式无队列可 flush
            return

        async with self._lock:
            if not self._queue:
                # 即使空也更新 last_flush_at, 让 max_wait 计时从此刻重新开始
                self._last_flush_at = time.monotonic()
                return
            items = self._queue
            topic = self._current_topic
            comp = self._current_comp
            self._queue = []
            self._current_topic = ""
            self._current_comp = "none"
            self._last_flush_at = time.monotonic()
            self._first_enqueue_at = 0.0

        try:
            # items 已经是 list[(ser_fmt, comp, payload_bytes)]
            await self._send_fn(items, comp, topic)
        except Exception:
            logger.exception("Batcher flush 失败 (%d 条)", len(items))
            raise

    async def close(self) -> None:
        """关闭: 停止定时器, flush 残留。

        重复 close 幂等。
        """
        if self._closed:
            return
        self._closed = True
        # flush 残留
        try:
            await self.flush()
        except Exception:
            logger.debug("Batcher close 时 flush 失败 (忽略)")
        # 取消 max_wait 后台 task
        if self._max_wait_task is not None and not self._max_wait_task.done():
            self._max_wait_task.cancel()
            try:
                await self._max_wait_task
            except (asyncio.CancelledError, Exception):
                pass
        self._max_wait_task = None
