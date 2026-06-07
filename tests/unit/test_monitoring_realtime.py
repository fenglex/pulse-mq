"""RealtimeMetrics / EWMA / SlidingWindow 单测。

覆盖:
- EWMA 初始化行为 (首次 update 等于输入值)
- EWMA 平滑 (alpha 越大越敏感)
- SlidingWindow 百分位 (p50/p99) 边界、空数据、超出窗口清理
- RealtimeMetrics 计数: msg/record/error/latency 聚合
- RealtimeMetrics.snapshot 字段完整
- RealtimeMetrics.update_engine_metrics 桥接引擎指标
"""

from __future__ import annotations

import time

import pytest

from pulsemq.monitoring.realtime import EWMA, RealtimeMetrics, SlidingWindow


# ---- EWMA ----


def test_ewma_first_update_equals_value():
    """首次 update 把值原封不动写入（_initialized 路径）。"""
    e = EWMA(alpha=0.3)
    e.update(42.0)
    assert e.value == 42.0


def test_ewma_smooths_toward_recent():
    """alpha 越大越偏向新值。"""
    e = EWMA(alpha=1.0)
    e.update(0.0)
    e.update(100.0)
    # alpha=1 时应完全等于最新一次
    assert e.value == 100.0


def test_ewma_alpha_blend():
    """alpha=0.5: 第二次 update 后续 50% 新 + 50% 旧。"""
    e = EWMA(alpha=0.5)
    e.update(0.0)
    e.update(10.0)
    # 0.5 * 10 + 0.5 * 0 = 5
    assert e.value == 5.0


def test_ewma_low_alpha_smooths_more():
    """alpha 越小越平滑: 一次小扰动后值变化小。"""
    e_low = EWMA(alpha=0.1)
    e_high = EWMA(alpha=0.9)
    # 同样先 update(0), 然后 update(100)
    e_low.update(0.0)
    e_low.update(100.0)
    e_high.update(0.0)
    e_high.update(100.0)
    assert e_low.value < e_high.value


# ---- SlidingWindow ----


def test_sliding_window_empty_percentile_returns_zero():
    """空窗口: 任何百分位返回 0.0。"""
    w = SlidingWindow(window_seconds=60.0, max_samples=128)
    assert w.percentile(50) == 0.0
    assert w.percentile(99) == 0.0
    assert w.count == 0


def test_sliding_window_p50_single_value():
    """单点窗口 p50 等于该点。"""
    w = SlidingWindow(window_seconds=60.0, max_samples=128)
    w.add(7.5)
    assert w.percentile(50) == 7.5
    assert w.count == 1


def test_sliding_window_percentile_ordering():
    """p50 <= p99 (单调非降)。"""
    w = SlidingWindow(window_seconds=60.0, max_samples=128)
    for v in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]:
        w.add(v)
    p50 = w.percentile(50)
    p99 = w.percentile(99)
    assert p50 <= p99


def test_sliding_window_expires_old_samples():
    """超过 window_seconds 的样本被 _cleanup 移除。"""
    w = SlidingWindow(window_seconds=1.0, max_samples=128)
    # 注入一个"很旧"的样本
    old_ts = time.time() - 10.0
    w.add(99.0, ts=old_ts)
    # 再注入一个新样本
    w.add(1.0)
    # 旧样本应在 percentile 计算时被清理
    assert w.percentile(50) == 1.0
    # count 只反映当前未过期的数据
    assert w.count == 1


def test_sliding_window_max_samples_capped():
    """max_samples 上限: 超出时不无限增长。"""
    w = SlidingWindow(window_seconds=60.0, max_samples=10)
    for v in range(50):
        w.add(float(v))
    assert w.count <= 10


# ---- RealtimeMetrics ----


def test_realtime_metrics_default_state():
    """默认构造: 所有 EWMA=0, 窗口空, 计数器 0, backpressure=False。"""
    m = RealtimeMetrics()
    assert m.msg_rate.value == 0.0
    assert m.record_rate.value == 0.0
    assert m.bytes_rate.value == 0.0
    assert m.error_rate.value == 0.0
    assert m.active_connections == 0
    assert m.active_subscriptions == 0
    assert m.dropped_messages == 0
    assert m.backpressure is False
    assert m.latency_p50.count == 0
    assert m.latency_p99.count == 0


def test_inc_message_accumulates():
    """inc_message 应同时更新 msg_rate / bytes_rate / latency 窗口。"""
    m = RealtimeMetrics()
    m.inc_message("topic.a", payload_size=100, elapsed_ms=2.5)
    m.inc_message("topic.b", payload_size=200, elapsed_ms=3.5)
    # msg_rate 已被 update 至少一次, 当前值非 0
    assert m.msg_rate.value > 0
    assert m.bytes_rate.value > 0
    # latency 窗口记到 2 个样本
    assert m.latency_p50.count == 2
    assert m.latency_p99.count == 2


def test_inc_record_updates_record_rate():
    """inc_record(count) 把 record_count 注入 EWMA。"""
    m = RealtimeMetrics()
    m.inc_record(5)
    # EWMA 首次 update 5 → value=5
    assert m.record_rate.value == 5.0


def test_inc_error_updates_error_rate():
    """inc_error 让 error_rate 非 0。"""
    m = RealtimeMetrics()
    m.inc_error()
    assert m.error_rate.value == 1.0


def test_snapshot_has_all_keys():
    """snapshot 应返回完整字段集。"""
    m = RealtimeMetrics()
    s = m.snapshot()
    expected = {
        "timestamp",
        "msg_rate",
        "record_rate",
        "bytes_rate",
        "latency_p50_ms",
        "latency_p99_ms",
        "active_connections",
        "active_subscriptions",
        "error_rate",
        "dropped_total",
        "backpressure",
        "engine_pending_tasks",
        "engine_concurrency_usage",
    }
    assert expected.issubset(s.keys()), f"缺失字段: {expected - set(s.keys())}"


def test_snapshot_reflects_updates():
    """snapshot 应反映 inc_* 后状态。"""
    m = RealtimeMetrics()
    m.inc_message("t", 10, 1.0)
    m.inc_record(2)
    m.inc_error()
    s = m.snapshot()
    assert s["msg_rate"] > 0
    assert s["record_rate"] > 0
    assert s["error_rate"] > 0
    assert s["dropped_total"] == 0


def test_update_engine_metrics_propagates():
    """update_engine_metrics 应写入 snapshot 字段。"""
    m = RealtimeMetrics()
    m.update_engine_metrics(pending_tasks=3, concurrency_usage=0.42)
    s = m.snapshot()
    assert s["engine_pending_tasks"] == 3
    assert s["engine_concurrency_usage"] == 0.42
    # engine_batch_size 字段已移除
    assert "engine_batch_size" not in s


def test_backpressure_flag_toggleable():
    """backpressure 可独立设置。"""
    m = RealtimeMetrics()
    assert m.backpressure is False
    m.backpressure = True
    assert m.snapshot()["backpressure"] is True


def test_active_subscriptions_incremented_manually():
    """active_subscriptions 是简单 int, 由调用方维护。"""
    m = RealtimeMetrics()
    m.active_subscriptions = 5
    assert m.snapshot()["active_subscriptions"] == 5
