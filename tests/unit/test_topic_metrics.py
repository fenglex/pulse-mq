"""TopicMetrics / TopicMetricsRegistry 单测。

覆盖:
- TopicMetrics 默认值
- record() 增计数、加延迟、刷新 EWMA
- in_flight inc/dec + 背压阈值触发
- get() 刷新分位数
- list_topics() 列出所有 topic
- reset_window() 清零
- window 滚动: window_seconds 过期后 msg_count 重置
- snapshot 字段完整
- peak_in_flight 跟踪峰值
- 不存在的 topic get() 返回默认值
"""

from __future__ import annotations

import time

import pytest

from pulsemq.monitoring.realtime import TopicMetrics, TopicMetricsRegistry


# ---- TopicMetrics 默认值 ----


def test_topic_metrics_defaults():
    """默认 TopicMetrics 全部为 0/False/空。"""
    m = TopicMetrics(topic="t.a")
    assert m.topic == "t.a"
    assert m.msg_count_1min == 0
    assert m.msg_rate_1min == 0.0
    assert m.latency_p50_1min == 0.0
    assert m.latency_p99_1min == 0.0
    assert m.latency_p999_1min == 0.0
    assert m.latency_max_1min == 0.0
    assert m.in_flight == 0
    assert m.backpressure is False
    assert m.last_msg_ts == 0.0


# ---- record ----


def test_record_increments_count_and_updates_ewma():
    """record() 增 msg_count, 触发 EWMA。"""
    reg = TopicMetricsRegistry()
    reg.record("t.a", latency_ms=1.0)
    reg.record("t.a", latency_ms=2.0)
    reg.record("t.a", latency_ms=3.0)
    m = reg.get("t.a")
    assert m.msg_count_1min == 3
    assert m.msg_rate_1min > 0  # EWMA 已 update
    assert m.last_msg_ts > 0


def test_record_separate_topics_isolated():
    """不同 topic 计数隔离。"""
    reg = TopicMetricsRegistry()
    reg.record("t.a", latency_ms=1.0)
    reg.record("t.b", latency_ms=2.0)
    reg.record("t.a", latency_ms=3.0)
    assert reg.get("t.a").msg_count_1min == 2
    assert reg.get("t.b").msg_count_1min == 1


def test_record_populates_latency_percentiles():
    """record 后 get() 拿到非 0 分位数。"""
    reg = TopicMetricsRegistry()
    for v in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
        reg.record("t.a", latency_ms=float(v))
    m = reg.get("t.a")
    # 10 个样本的 p50 ≈ 5.5, p99 ≈ 9.91
    assert 4.0 <= m.latency_p50_1min <= 7.0
    assert m.latency_p99_1min >= 9.0
    assert m.latency_max_1min == 10.0


# ---- in_flight + 背压 ----


def test_inc_dec_in_flight():
    """inc/dec 正确增减 in_flight。"""
    reg = TopicMetricsRegistry(backpressure_threshold=10)
    reg.inc_in_flight("t.a")
    reg.inc_in_flight("t.a")
    assert reg.get("t.a").in_flight == 2
    reg.dec_in_flight("t.a")
    assert reg.get("t.a").in_flight == 1


def test_backpressure_triggered_above_threshold():
    """in_flight > threshold 触发 backpressure=True。"""
    reg = TopicMetricsRegistry(backpressure_threshold=3)
    for _ in range(4):
        reg.inc_in_flight("t.a")
    assert reg.get("t.a").backpressure is True


def test_backpressure_clears_below_threshold():
    """in_flight 降回阈值以下时 backpressure=False。"""
    reg = TopicMetricsRegistry(backpressure_threshold=3)
    for _ in range(5):
        reg.inc_in_flight("t.a")
    for _ in range(5):
        reg.dec_in_flight("t.a")
    assert reg.get("t.a").backpressure is False


def test_dec_in_flight_does_not_go_negative():
    """in_flight 不会减到负数。"""
    reg = TopicMetricsRegistry()
    reg.dec_in_flight("t.a")  # 之前 0
    assert reg.get("t.a").in_flight == 0
    reg.dec_in_flight("t.a")
    assert reg.get("t.a").in_flight == 0


# ---- list / get ----


def test_get_unknown_topic_returns_default():
    """未 record 的 topic 也能 get, 返回默认 (0 值)。"""
    reg = TopicMetricsRegistry()
    m = reg.get("never-recorded")
    assert m.topic == "never-recorded"
    assert m.msg_count_1min == 0
    assert m.backpressure is False


def test_list_topics_returns_all_recorded():
    """list_topics 列出所有 record 过的 topic。"""
    reg = TopicMetricsRegistry()
    reg.record("t.a", 1.0)
    reg.record("t.b", 2.0)
    reg.record("t.c", 3.0)
    topics = reg.list_topics()
    names = {m.topic for m in topics}
    assert names == {"t.a", "t.b", "t.c"}


# ---- 窗口滚动 ----


def test_window_rolls_after_timeout():
    """window_seconds 过期后 msg_count 重置为 0。"""
    reg = TopicMetricsRegistry(window_seconds=0.1)
    reg.record("t.a", 1.0)
    reg.record("t.a", 2.0)
    assert reg.get("t.a").msg_count_1min == 2
    time.sleep(0.15)
    # 触发窗口滚动
    m = reg.get("t.a")
    assert m.msg_count_1min == 0


# ---- reset_window ----


def test_reset_window_clears_count():
    """reset_window 显式重置 msg_count。"""
    reg = TopicMetricsRegistry()
    reg.record("t.a", 1.0)
    reg.record("t.a", 2.0)
    reg.inc_in_flight("t.a")
    reg.inc_in_flight("t.a")
    reg.reset_window("t.a")
    m = reg.get("t.a")
    assert m.msg_count_1min == 0
    assert m.in_flight == 0


# ---- peak_in_flight ----


def test_peak_in_flight_tracks_max():
    """peak_in_flight 跟踪窗口内 in_flight 峰值。"""
    reg = TopicMetricsRegistry()
    for _ in range(5):
        reg.inc_in_flight("t.a")
    assert reg.peak_in_flight("t.a") == 5
    # 减回去
    for _ in range(3):
        reg.dec_in_flight("t.a")
    # 峰值仍是 5
    assert reg.peak_in_flight("t.a") == 5


# ---- snapshot ----


def test_snapshot_field_set():
    """snapshot 包含 topic_count 和 topics 列表。"""
    reg = TopicMetricsRegistry()
    reg.record("t.a", 1.0)
    reg.record("t.b", 2.0)
    s = reg.snapshot()
    assert s["topic_count"] == 2
    assert isinstance(s["topics"], list)
    assert len(s["topics"]) == 2
    for entry in s["topics"]:
        assert "topic" in entry
        assert "msg_count_1min" in entry
        assert "msg_rate_1min" in entry
        assert "latency_p50_1min" in entry
        assert "latency_p99_1min" in entry
        assert "in_flight" in entry
        assert "backpressure" in entry


# ---- to_dict ----


def test_to_dict_for_sqlite():
    """to_dict 返回 SQLite 落库所需字段。"""
    reg = TopicMetricsRegistry()
    reg.record("t.a", 5.0)
    d = reg.to_dict("t.a")
    assert d["topic"] == "t.a"
    assert d["msg_count"] == 1
    assert d["p50"] >= 0
    assert d["p99"] >= 0
    assert d["max_latency"] >= 0
    assert d["peak_in_flight"] == 0
