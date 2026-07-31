"""端到端延迟统计：采样 + 固定桶直方图 + P50/P95/P99。"""
from __future__ import annotations

import bisect
import random
import threading
import time
from collections import deque
from dataclasses import dataclass

# 桶上界（ns）：0.05 / 0.1 / 0.5 / 1 / 5 / 10 / 50 ms
# 末桶为 [50ms, +inf)，需要一个有限上界用于分位线性插值的代表值。
_BUCKET_BOUNDS_NS = [50_000, 100_000, 500_000, 1_000_000,
                     5_000_000, 10_000_000, 50_000_000]
# 每个桶的下界（ns）：第 0 桶下界为 0，其余为前一桶上界
_BUCKET_LOWER_NS = [0] + _BUCKET_BOUNDS_NS
# 末桶有限上界（ns）：取末上界的 2 倍作为尾桶代表区间终点，使插值有定义。
_BUCKET_UPPER_NS = _BUCKET_BOUNDS_NS + [_BUCKET_BOUNDS_NS[-1] * 2]


class LatencyStats:
    """采样 + 固定桶直方图延迟统计（线程安全）。

    分位数采用桶内线性插值：当目标分位落入某桶内部时，按该桶内目标位置
    在 [下界, 上界] 之间线性插值，得到比固定代表值更准确的估计。
    """

    def __init__(self, sample_rate: float = 0.01) -> None:
        self._rate = max(0.0, min(1.0, sample_rate))
        # 桶计数：末桶为 [last_bound, +inf)
        self._counts = [0] * (len(_BUCKET_BOUNDS_NS) + 1)
        self._total = 0
        self._lock = threading.Lock()

    def should_sample(self) -> bool:
        if self._rate >= 1.0:
            return True
        if self._rate <= 0.0:
            return False
        return random.random() < self._rate

    def record(self, latency_ns: int) -> None:
        with self._lock:
            # bisect_left: latency < bound[0] -> 0；latency >= 末上界 -> len(bounds)
            idx = bisect.bisect_left(_BUCKET_BOUNDS_NS, latency_ns)
            if idx >= len(_BUCKET_BOUNDS_NS):
                idx = len(_BUCKET_BOUNDS_NS)  # 落入末桶
            self._counts[idx] += 1
            self._total += 1

    def _percentile_ms(self, pct: float) -> float:
        with self._lock:
            if self._total == 0:
                return 0.0
            # pct ∈ (0, 1]，目标位置（1-based 排序中的目标序号）
            target = pct * self._total
            running = 0
            for i, c in enumerate(self._counts):
                prev = running
                running += c
                if running >= target:
                    # 目标落入第 i 个桶。按桶内位置线性插值：
                    # 桶内偏移 = (target - prev) / c （∈ (0, 1]）
                    if c <= 0:
                        continue
                    frac = (target - prev) / c
                    lo = _BUCKET_LOWER_NS[i]
                    hi = _BUCKET_UPPER_NS[i]
                    return (lo + frac * (hi - lo)) / 1_000_000.0
            return _BUCKET_UPPER_NS[-1] / 1_000_000.0

    def percentiles(self) -> dict:
        return {
            "p50_ms": self._percentile_ms(0.50),
            "p95_ms": self._percentile_ms(0.95),
            "p99_ms": self._percentile_ms(0.99),
        }

    def snapshot(self) -> dict:
        p = self.percentiles()
        p["count"] = self._total
        return p


@dataclass
class MinuteLatency:
    """一个 topic 一分钟的延迟快照。"""
    timestamp: int       # 整分钟秒
    p50_ms: float
    p95_ms: float
    p99_ms: float
    count: int           # 本分钟采样命中数


class LatencyStatsRegistry:
    """按 topic + 分钟窗口的延迟统计（线程安全）。

    线程模型：数据面线程写 record()（半程），控制面协程写 record()（全程回传），
    主线程协程写 roll_minute()，admin 线程读 snapshot()/get_history()。
    用 threading.Lock 保护（record 仅在采样命中时执行，lock 开销可接受）。
    """

    def __init__(self, sample_rate: float = 0.01, retention_minutes: int = 480) -> None:
        self._rate = max(0.0, min(1.0, sample_rate))
        self._retention = retention_minutes
        self._current: dict[str, LatencyStats] = {}
        self._history: dict[str, deque[MinuteLatency]] = {}
        self._lock = threading.Lock()

    def should_sample(self) -> bool:
        if self._rate >= 1.0:
            return True
        if self._rate <= 0.0:
            return False
        return random.random() < self._rate

    def record(self, topic: str, latency_ns: int) -> None:
        with self._lock:
            ls = self._current.get(topic)
            if ls is None:
                # 内部不再采样，由 registry 的 should_sample 控制
                ls = LatencyStats(sample_rate=1.0)
                self._current[topic] = ls
            ls.record(latency_ns)

    def roll_minute(self) -> None:
        """整分钟归档：_current 各 topic 算分位 -> MinuteLatency 追加 _history。"""
        ts = int(time.time()) // 60 * 60
        with self._lock:
            for topic, ls in self._current.items():
                snap = ls.snapshot()
                if snap.get("count", 0) > 0:
                    ml = MinuteLatency(timestamp=ts, p50_ms=snap["p50_ms"],
                                       p95_ms=snap["p95_ms"], p99_ms=snap["p99_ms"],
                                       count=snap["count"])
                    dq = self._history.get(topic)
                    if dq is None:
                        dq = deque(maxlen=self._retention)
                        self._history[topic] = dq
                    dq.append(ml)
            self._current.clear()

    def snapshot(self) -> dict[str, dict]:
        """各 topic 当前进行中的延迟快照。"""
        with self._lock:
            return {t: ls.snapshot() for t, ls in self._current.items()}

    def get_history(self, topic: str, minutes: int = 60) -> list[dict]:
        """近 N 分钟延迟序列（给折线图）。"""
        dq = self._history.get(topic)
        if not dq:
            return []
        return [{"timestamp": m.timestamp, "p50_ms": m.p50_ms, "p95_ms": m.p95_ms,
                 "p99_ms": m.p99_ms, "count": m.count}
                for m in list(dq)[-minutes:]]
