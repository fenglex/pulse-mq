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


# send_fn 签名: async (payloads_list, ser_fmt, comp) -> None
# payloads_list 是 list[Any] (各元素已是各 ser_fmt 序列化后的 bytes).
#   实际集成时由 async_client 把 (data, ser_fmt, comp) 预序列化为 bytes.
BatcherSendFn = Callable[[list[bytes], str, str], Awaitable[None]]


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
            send_fn: 实际发送函数, 签名 async (payloads_list, ser_fmt, comp) -> None.
                      payloads_list 是已按 ser_fmt 序列化的 bytes 列表.
            ser_fmt: 默认序列化格式。
            comp: 默认压缩算法。
            batch_size: 触发 flush 的批量大小, == 1 时退化为单条直发。
            batch_interval_ms: 距离首次入队多少毫秒后必须 flush。
            batch_max_wait_ms: 距离上次 flush 多少毫秒后强制 flush (硬上限)。
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
        self._ser_fmt = ser_fmt
        self._comp = comp
        self._batch_size = batch_size
        self._batch_interval_ms = batch_interval_ms
        self._batch_max_wait_ms = batch_max_wait_ms

        # 单条直发模式: batch_size == 1
        self._direct = (batch_size == 1)

        # 内部状态
        self._queue: list[bytes] = []
        self._first_enqueue_at: float = 0.0
        # 初始 last_flush_at 用 monotonic() 启动值, 避免"距离上次 flush"在
        # 第 1 条 add 时就满足 max_wait 触发条件 (单调时间, 不依赖 epoch)
        self._last_flush_at: float = time.monotonic()

        # max_wait 后台定时器
        self._max_wait_task: asyncio.Task | None = None

        # 锁: 保护 _queue / _first_enqueue_at / _last_flush_at
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

    async def add(self, payload: bytes) -> None:
        """入队一条 payload (应是 ser_fmt 序列化后的 bytes)。

        满足任一条件则 flush:
          1. 队列长度 >= batch_size
          2. 距离首次入队 >= batch_interval_ms
          3. 距离上次 flush >= batch_max_wait_ms (硬上限)
        """
        if self._closed:
            raise RuntimeError("Batcher 已关闭")

        if self._direct:
            # 单条直发: 不入队, 立即发送
            await self._send_fn([payload], self._ser_fmt, self._comp)
            return

        should_flush = False
        async with self._lock:
            now = time.monotonic()
            if not self._queue:
                self._first_enqueue_at = now
            self._queue.append(payload)

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
            payloads = self._queue
            self._queue = []
            self._last_flush_at = time.monotonic()
            self._first_enqueue_at = 0.0

        try:
            await self._send_fn(payloads, self._ser_fmt, self._comp)
        except Exception:
            logger.exception("Batcher flush 失败 (%d 条)", len(payloads))
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
