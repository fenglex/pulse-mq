import pytest
from pulsemq.stats.latency import LatencyStats, LatencyStatsRegistry


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


def test_registry_record_and_snapshot():
    """LatencyStatsRegistry 按 topic 记录并快照。"""
    reg = LatencyStatsRegistry(sample_rate=1.0, retention_minutes=480)
    assert reg.should_sample()  # rate=1.0 总是 True
    reg.record("market.tick", 1_000_000)
    reg.record("market.tick", 2_000_000)
    snap = reg.snapshot()
    assert "market.tick" in snap
    assert snap["market.tick"]["count"] == 2


def test_registry_roll_minute_appends_history():
    """roll_minute 归档当前分钟到 history。"""
    reg = LatencyStatsRegistry(sample_rate=1.0)
    reg.record("market.tick", 1_000_000)
    reg.roll_minute()
    hist = reg.get_history("market.tick", 60)
    assert len(hist) == 1
    assert "p50_ms" in hist[0]
    # roll 后 current 清空
    assert reg.snapshot().get("market.tick") is None


def test_registry_history_capped_at_retention():
    """history deque 按 retention_minutes 截断。"""
    reg = LatencyStatsRegistry(sample_rate=1.0, retention_minutes=3)
    for _ in range(5):
        reg.record("t", 500_000)
        reg.roll_minute()
    assert len(reg.get_history("t", 100)) == 3  # maxlen=3
