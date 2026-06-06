"""实时指标：EWMA + SlidingWindow。

所有指标存储在内存中，O(1) 更新，零阻塞。
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field


class EWMA:
    """指数加权移动平均（Exponentially Weighted Moving Average）。

    α 越大越敏感（更快响应变化），α 越小越平滑。
    """

    def __init__(self, alpha: float = 0.3):
        self._alpha = alpha
        self._value: float = 0.0
        self._initialized: bool = False

    def update(self, value: float) -> None:
        if not self._initialized:
            self._value = value
            self._initialized = True
        else:
            self._value = self._alpha * value + (1 - self._alpha) * self._value

    @property
    def value(self) -> float:
        return self._value


class SlidingWindow:
    """滑动窗口，用于计算 P50/P99 延迟。

    保留最近 window_seconds 秒内的数据点，限制最大容量避免排序开销。
    采用 reservoir sampling 策略：超过 max_samples 时随机替换。
    """

    def __init__(self, window_seconds: float = 60.0, max_samples: int = 4096):
        self._window = window_seconds
        self._max_samples = max_samples
        self._data: deque[tuple[float, float]] = deque(maxlen=max_samples)

    def add(self, value: float, ts: float | None = None) -> None:
        ts = ts or time.time()
        if len(self._data) >= self._max_samples:
            # 已满时移除最旧的再插入
            self._cleanup(ts)
        self._data.append((ts, value))

    def percentile(self, p: float) -> float:
        """计算百分位数（p: 0-100）。"""
        self._cleanup()
        if not self._data:
            return 0.0
        values = sorted(v for _, v in self._data)
        idx = min(round((len(values) - 1) * p / 100), len(values) - 1)
        return values[idx]

    def _cleanup(self, now: float | None = None) -> None:
        now = now or time.time()
        cutoff = now - self._window
        while self._data and self._data[0][0] < cutoff:
            self._data.popleft()

    @property
    def count(self) -> int:
        return len(self._data)


@dataclass
class RealtimeMetrics:
    """实时监控指标（全部内存）。"""

    # 速率指标（EWMA）
    msg_rate: EWMA = field(default_factory=lambda: EWMA(alpha=0.3))
    record_rate: EWMA = field(default_factory=lambda: EWMA(alpha=0.3))
    bytes_rate: EWMA = field(default_factory=lambda: EWMA(alpha=0.3))
    error_rate: EWMA = field(default_factory=lambda: EWMA(alpha=0.3))

    # 延迟指标（SlidingWindow）
    latency_p50: SlidingWindow = field(default_factory=lambda: SlidingWindow(60))
    latency_p99: SlidingWindow = field(default_factory=lambda: SlidingWindow(60))

    # 计数器
    active_connections: int = 0
    active_subscriptions: int = 0
    dropped_messages: int = 0
    backpressure: bool = False

    # 引擎指标
    _engine_batch_size: int = 1
    _engine_pending_tasks: int = 0
    _engine_concurrency_usage: float = 0.0

    def inc_message(self, topic: str, payload_size: int, elapsed_ms: float) -> None:
        """记录一条消息处理完成。"""
        self.msg_rate.update(1)
        self.bytes_rate.update(payload_size)
        self.latency_p50.add(elapsed_ms)
        self.latency_p99.add(elapsed_ms)

    def inc_record(self, count: int) -> None:
        """记录 record_count。"""
        self.record_rate.update(count)

    def inc_error(self) -> None:
        self.error_rate.update(1)

    def snapshot(self) -> dict:
        """获取当前快照。"""
        return {
            "timestamp": time.time(),
            "msg_rate": round(self.msg_rate.value, 1),
            "record_rate": round(self.record_rate.value, 1),
            "bytes_rate": round(self.bytes_rate.value, 1),
            "latency_p50_ms": round(self.latency_p50.percentile(50), 3),
            "latency_p99_ms": round(self.latency_p99.percentile(99), 3),
            "active_connections": self.active_connections,
            "active_subscriptions": self.active_subscriptions,
            "error_rate": round(self.error_rate.value, 2),
            "dropped_total": self.dropped_messages,
            "backpressure": self.backpressure,
            # 引擎指标（由外部更新）
            "engine_batch_size": self._engine_batch_size,
            "engine_pending_tasks": self._engine_pending_tasks,
            "engine_concurrency_usage": self._engine_concurrency_usage,
        }

    def update_engine_metrics(
        self, batch_size: int, pending_tasks: int, concurrency_usage: float
    ) -> None:
        """更新引擎运行指标。"""
        self._engine_batch_size = batch_size
        self._engine_pending_tasks = pending_tasks
        self._engine_concurrency_usage = concurrency_usage
