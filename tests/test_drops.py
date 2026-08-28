"""_DropQueue + DropStats 单元测试。"""
from __future__ import annotations

import threading
from types import SimpleNamespace

from pulsemq.stats.drops import DropStats


# ---------------------------------------------------------------------------
# _DropQueue
# ---------------------------------------------------------------------------

def _item(topic: str):
    """构造队列元素 (frame_bytes, hdr, matched)。"""
    hdr = SimpleNamespace(topic=topic)
    return (b"frame", hdr, [])


def test_drop_queue_basic_put_get():
    from pulsemq.client import _DropQueue
    q = _DropQueue(maxlen=10)
    q.put(_item("a"))
    item = q.get(timeout=1.0)
    assert item is not None
    assert item[1].topic == "a"


def test_drop_queue_drops_oldest_when_full():
    from pulsemq.client import _DropQueue
    q = _DropQueue(maxlen=3)
    q.put(_item("t1"))
    q.put(_item("t2"))
    q.put(_item("t3"))
    # 队列满，put 第 4 条应丢弃 t1
    q.put(_item("t4"))
    drops = q.drain_drops()
    assert drops == {"t1": 1}
    # 队列中应剩 t2, t3, t4
    items = []
    while True:
        item = q.get(timeout=0.5)
        if item is None:
            break
        items.append(item[1].topic)
    assert items == ["t2", "t3", "t4"]


def test_drop_queue_per_topic_count():
    from pulsemq.client import _DropQueue
    q = _DropQueue(maxlen=2)
    q.put(_item("hot"))
    q.put(_item("hot"))
    q.put(_item("hot"))  # 丢弃第 1 个 hot
    q.put(_item("hot"))  # 丢弃第 2 个 hot
    drops = q.drain_drops()
    assert drops == {"hot": 2}


def test_drop_queue_drain_resets():
    from pulsemq.client import _DropQueue
    q = _DropQueue(maxlen=1)
    q.put(_item("a"))
    q.put(_item("b"))  # 丢弃 a
    drops1 = q.drain_drops()
    assert drops1 == {"a": 1}
    drops2 = q.drain_drops()
    assert drops2 == {}


def test_drop_queue_close():
    from pulsemq.client import _DropQueue
    q = _DropQueue(maxlen=5)
    q.put(_item("a"))
    q.close()
    assert q.put(_item("b")) is False  # 关闭后 put 返回 False
    # 已入队的项仍可取出（drain 语义）
    item = q.get(timeout=0.5)
    assert item is not None
    assert item[1].topic == "a"
    # 队列空后返回 None
    assert q.get(timeout=0.5) is None


def test_drop_queue_multi_topic_drops():
    from pulsemq.client import _DropQueue
    q = _DropQueue(maxlen=2)
    q.put(_item("topic_a"))
    q.put(_item("topic_b"))
    q.put(_item("topic_a"))  # 丢弃 topic_a
    q.put(_item("topic_b"))  # 丢弃 topic_b
    drops = q.drain_drops()
    assert drops == {"topic_a": 1, "topic_b": 1}


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
