"""端到端延迟统计：采样 + 固定桶直方图 + P50/P95/P99。"""
from __future__ import annotations

import bisect
import random
import threading

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
