"""实时指标：EWMA + SlidingWindow + Topic 维度 1-min 监控。

所有指标存储在内存中，O(1) 更新，零阻塞。
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any


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
    实现策略：
    - 底层 deque(maxlen=max_samples): 容量满后自动丢弃最旧数据 (FIFO 截断)
    - 计算百分位时调用 _cleanup() 移除 window 之外的所有过期样本
    注：与"reservoir sampling"不同；当前实现是 FIFO + 滑动窗口。
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
            "engine_pending_tasks": self._engine_pending_tasks,
            "engine_concurrency_usage": self._engine_concurrency_usage,
        }

    def update_engine_metrics(
        self, pending_tasks: int, concurrency_usage: float
    ) -> None:
        """更新引擎运行指标。"""
        self._engine_pending_tasks = pending_tasks
        self._engine_concurrency_usage = concurrency_usage


# ---- Phase 5: Topic 维度 1-min 滑动窗口监控 ----


@dataclass
class TopicMetrics:
    """单个 topic 的 1-min 监控指标。

    - msg_count_1min: 1min 内消息计数 (只增不主动衰减, 60s 后由 registry 滚动重置)
    - msg_rate_1min: EWMA 平滑的 msg/s
    - latency_p50/p99/p999/p_max_1min: 1min 滑动窗口延迟 (ms)
    - in_flight: 当前正在处理的消息数
    - backpressure: in_flight 超过阈值
    - last_msg_ts: 最近一条消息的接收时间戳
    """

    topic: str
    msg_count_1min: int = 0
    msg_rate_1min: float = 0.0
    latency_p50_1min: float = 0.0
    latency_p99_1min: float = 0.0
    latency_p999_1min: float = 0.0
    latency_max_1min: float = 0.0
    in_flight: int = 0
    backpressure: bool = False
    last_msg_ts: float = 0.0


class TopicMetricsRegistry:
    """Topic 维度 1-min 滑动窗口指标注册表。

    用法:
        reg = TopicMetricsRegistry(window_seconds=60.0, backpressure_threshold=1000)
        reg.record("team-a.mkt", latency_ms=2.5)
        m = reg.get("team-a.mkt")
    """

    def __init__(
        self,
        window_seconds: float = 60.0,
        backpressure_threshold: int = 1000,
        max_samples: int = 4096,
    ):
        self._window = window_seconds
        self._backpressure_threshold = backpressure_threshold
        self._max_samples = max_samples
        # topic -> TopicMetrics 状态
        self._topics: dict[str, TopicMetrics] = {}
        # topic -> EWMA (msg_rate)
        self._ewma: dict[str, EWMA] = {}
        # topic -> SlidingWindow (p50/p99/p999/max)
        self._w_p50: dict[str, SlidingWindow] = {}
        self._w_p99: dict[str, SlidingWindow] = {}
        self._w_p999: dict[str, SlidingWindow] = {}
        self._w_max: dict[str, SlidingWindow] = {}
        # topic -> 窗口起点 (秒)
        self._window_start: dict[str, float] = {}
        # topic -> 当前 in_flight (峰值供 minute 落库用)
        self._in_flight_peak: dict[str, int] = {}

    def _ensure_topic(self, topic: str) -> TopicMetrics:
        m = self._topics.get(topic)
        if m is None:
            m = TopicMetrics(topic=topic)
            self._topics[topic] = m
            self._ewma[topic] = EWMA(alpha=0.3)
            self._w_p50[topic] = SlidingWindow(self._window, self._max_samples)
            self._w_p99[topic] = SlidingWindow(self._window, self._max_samples)
            self._w_p999[topic] = SlidingWindow(self._window, self._max_samples)
            self._w_max[topic] = SlidingWindow(self._window, self._max_samples)
            self._window_start[topic] = time.time()
            self._in_flight_peak[topic] = 0
        return m

    def _maybe_roll_window(self, topic: str, m: TopicMetrics) -> None:
        """若超过 window_seconds, 滚动重置计数 (EWMA 状态保留)。"""
        start = self._window_start.get(topic, 0)
        if start > 0 and (time.time() - start) >= self._window:
            m.msg_count_1min = 0
            m.in_flight = 0
            self._in_flight_peak[topic] = 0
            self._window_start[topic] = time.time()

    def record(self, topic: str, latency_ms: float) -> None:
        """记录一条消息 (按 topic 维度)。"""
        m = self._ensure_topic(topic)
        self._maybe_roll_window(topic, m)

        m.msg_count_1min += 1
        m.last_msg_ts = time.time()
        # 延迟加到 3 个滑动窗口 (p50/p99/p999/max 共享底层数据,
        # 但分位数计算用同一个 window 即可; 我们分开 SlidingWindow 避免冲突)
        self._w_p50[topic].add(latency_ms, ts=m.last_msg_ts)
        self._w_p99[topic].add(latency_ms, ts=m.last_msg_ts)
        self._w_p999[topic].add(latency_ms, ts=m.last_msg_ts)
        self._w_max[topic].add(latency_ms, ts=m.last_msg_ts)
        # 速率 EWMA
        self._ewma[topic].update(1)
        m.msg_rate_1min = self._ewma[topic].value
        # 背压
        m.backpressure = m.in_flight > self._backpressure_threshold

    def inc_in_flight(self, topic: str) -> None:
        """递增 in_flight (处理开始时)。"""
        m = self._ensure_topic(topic)
        m.in_flight += 1
        if m.in_flight > self._in_flight_peak.get(topic, 0):
            self._in_flight_peak[topic] = m.in_flight
        m.backpressure = m.in_flight > self._backpressure_threshold

    def dec_in_flight(self, topic: str) -> None:
        """递减 in_flight (处理完成时)。"""
        m = self._topics.get(topic)
        if m is not None and m.in_flight > 0:
            m.in_flight -= 1
            m.backpressure = m.in_flight > self._backpressure_threshold

    def get(self, topic: str) -> TopicMetrics:
        """获取 topic 的当前指标 (无则返回默认值)。"""
        m = self._ensure_topic(topic)
        self._maybe_roll_window(topic, m)
        # 每次取时刷新分位数
        m.latency_p50_1min = self._w_p50[topic].percentile(50)
        m.latency_p99_1min = self._w_p99[topic].percentile(99)
        m.latency_p999_1min = self._w_p999[topic].percentile(99.9)
        # max: 取当前窗口内最大值
        wmax = self._w_max[topic]
        if wmax.count > 0:
            # 沿用 percentile(100) 即可拿到 max (按窗口数据)
            m.latency_max_1min = wmax.percentile(100)
        m.msg_rate_1min = self._ewma[topic].value
        return m

    def list_topics(self) -> list[TopicMetrics]:
        """列出所有已知 topic 的当前指标快照。"""
        out: list[TopicMetrics] = []
        for topic in list(self._topics.keys()):
            out.append(self.get(topic))
        return out

    def peak_in_flight(self, topic: str) -> int:
        """获取 topic 上一窗口的 in_flight 峰值 (供 SQLite 落库)。"""
        return self._in_flight_peak.get(topic, 0)

    def reset_window(self, topic: str) -> None:
        """手动滚动 topic 的窗口 (供分钟落库后调用)。"""
        m = self._topics.get(topic)
        if m is not None:
            m.msg_count_1min = 0
            m.in_flight = 0
            self._in_flight_peak[topic] = 0
            self._window_start[topic] = time.time()

    def snapshot(self) -> dict:
        """给 AdminServer 的全量快照。

        Returns:
            {
              "topics": [{...}, ...],
              "topic_count": int,
            }
        """
        topics = self.list_topics()
        return {
            "topic_count": len(topics),
            "topics": [
                {
                    "topic": m.topic,
                    "msg_count_1min": m.msg_count_1min,
                    "msg_rate_1min": round(m.msg_rate_1min, 2),
                    "latency_p50_1min": round(m.latency_p50_1min, 3),
                    "latency_p99_1min": round(m.latency_p99_1min, 3),
                    "latency_p999_1min": round(m.latency_p999_1min, 3),
                    "latency_max_1min": round(m.latency_max_1min, 3),
                    "in_flight": m.in_flight,
                    "backpressure": m.backpressure,
                    "last_msg_ts": m.last_msg_ts,
                }
                for m in topics
            ],
        }

    def to_dict(self, topic: str) -> dict[str, Any]:
        """单 topic dict (供 SQLite 落库用)。"""
        m = self.get(topic)
        return {
            "topic": m.topic,
            "msg_count": m.msg_count_1min,
            "p50": m.latency_p50_1min,
            "p99": m.latency_p99_1min,
            "p999": m.latency_p999_1min,
            "max_latency": m.latency_max_1min,
            "peak_in_flight": self.peak_in_flight(topic),
        }
