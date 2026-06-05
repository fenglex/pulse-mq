"""监控模块测试：EWMA + SlidingWindow + RealtimeMetrics + HTTP API。"""

import pytest
from pulsemq.monitoring.realtime import EWMA, SlidingWindow, RealtimeMetrics


class TestEWMA:
    def test_first_update(self):
        ewma = EWMA(alpha=0.3)
        ewma.update(100)
        assert ewma.value == 100

    def test_smooth(self):
        ewma = EWMA(alpha=0.5)
        ewma.update(0)
        ewma.update(100)
        assert 40 < ewma.value <= 100

    def test_converges(self):
        ewma = EWMA(alpha=0.3)
        for _ in range(100):
            ewma.update(50)
        assert abs(ewma.value - 50) < 1


class TestSlidingWindow:
    def test_add_and_percentile(self):
        w = SlidingWindow(window_seconds=60)
        for v in [10, 20, 30, 40, 50]:
            w.add(v)
        p50 = w.percentile(50)
        assert 20 <= p50 <= 30  # 5 个值的中间区域
        assert w.percentile(99) == 50
        assert w.percentile(0) == 10

    def test_empty(self):
        w = SlidingWindow()
        assert w.percentile(50) == 0.0


class TestRealtimeMetrics:
    def test_inc_message(self):
        m = RealtimeMetrics()
        m.inc_message("test.topic", 1024, 0.5)
        m.inc_message("test.topic", 512, 1.0)
        assert m.msg_rate.value > 0
        snap = m.snapshot()
        assert snap["msg_rate"] > 0
        assert "latency_p50_ms" in snap
        assert "timestamp" in snap

    def test_snapshot_structure(self):
        m = RealtimeMetrics()
        snap = m.snapshot()
        expected_keys = [
            "timestamp", "msg_rate", "record_rate", "bytes_rate",
            "latency_p50_ms", "latency_p99_ms", "active_connections",
            "active_subscriptions", "error_rate", "dropped_total", "backpressure",
        ]
        for key in expected_keys:
            assert key in snap, f"缺少 key: {key}"
