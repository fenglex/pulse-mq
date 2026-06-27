import pytest
from pulsemq.stats.latency import LatencyStats


def test_sample_rate_controls_sampling(monkeypatch):
    s = LatencyStats(sample_rate=0.0)
    assert s.should_sample() is False
    s2 = LatencyStats(sample_rate=1.0)
    assert s2.should_sample() is True


def test_record_and_percentiles_full_sampling():
    s = LatencyStats(sample_rate=1.0)
    # 100 个样本，线性分布 0..99ms（ns）
    for i in range(100):
        s.record(i * 1_000_000)  # i ms
    p = s.percentiles()
    assert p["p50_ms"] >= 0
    assert p["p95_ms"] >= p["p50_ms"]
    assert p["p99_ms"] >= p["p95_ms"]
    # p50 应在 50ms 附近（桶估算）
    assert 40 <= p["p50_ms"] <= 60


def test_no_record_when_not_sampled():
    s = LatencyStats(sample_rate=0.0)
    for i in range(1000):
        if s.should_sample():
            s.record(i * 1_000_000)
    assert s.percentiles()["p99_ms"] == 0.0  # 无样本


def test_snapshot_shape():
    s = LatencyStats(sample_rate=1.0)
    s.record(100_000)
    snap = s.snapshot()
    assert set(snap) == {"p50_ms", "p95_ms", "p99_ms", "count"}
    assert snap["count"] == 1
