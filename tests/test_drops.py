"""DropStats 单元测试。

9.2.4：消费端唯一模式为多进程池，原 _DropQueue（两线程解码队列）已删除；
环满 drop-new 丢弃语义的单元覆盖见 test_worker_pool.py（ShmRing 满丢弃 +
write_fail 计数）。
"""
from __future__ import annotations

import threading

from pulsemq.stats.drops import DropStats


# ---------------------------------------------------------------------------
# DropStats
# ---------------------------------------------------------------------------

def test_drop_stats_record_and_snapshot():
    ds = DropStats(retention_minutes=60)
    ds.record("topic_a", 5)
    ds.record("topic_a", 3)
    ds.record("topic_b", 2)
    snap = ds.snapshot()
    assert snap["topic_a"]["drops_current"] == 8
    assert snap["topic_b"]["drops_current"] == 2


def test_drop_stats_roll_minute():
    ds = DropStats(retention_minutes=60)
    ds.record("topic_a", 10)
    ds.roll_minute()
    snap = ds.snapshot()
    # roll 后 current 清零，last_min 有值
    assert snap["topic_a"]["drops_current"] == 0
    assert snap["topic_a"]["drops_last_min"] == 10
    assert snap["topic_a"]["drops_1h_total"] == 10


def test_drop_stats_1h_total_accumulates():
    ds = DropStats(retention_minutes=60)
    ds.record("t", 5)
    ds.roll_minute()
    ds.record("t", 3)
    snap = ds.snapshot()
    assert snap["t"]["drops_current"] == 3       # 当前分钟
    assert snap["t"]["drops_last_min"] == 5      # 上一分钟
    assert snap["t"]["drops_1h_total"] == 8      # 5 + 3


def test_drop_stats_ignore_zero():
    ds = DropStats(retention_minutes=60)
    ds.record("t", 0)  # 应被忽略
    snap = ds.snapshot()
    assert "t" not in snap


def test_drop_stats_thread_safety():
    """并发 record 不丢数据。"""
    ds = DropStats(retention_minutes=60)

    def worker():
        for _ in range(1000):
            ds.record("t", 1)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    snap = ds.snapshot()
    assert snap["t"]["drops_current"] == 4000
