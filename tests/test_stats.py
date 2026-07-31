"""流量统计单元测试。"""

from __future__ import annotations

import time

from pulsemq.stats.traffic import TrafficStats, MinuteSlot
from pulsemq.stats.storage import StatsStorage


class TestTrafficStats:
    def test_record_basic(self) -> None:
        ts = TrafficStats()
        ts.record("topic_a", record_count=5, payload_size=1024)
        snap = ts.snapshot()
        assert "topic_a" in snap
        assert snap["topic_a"]["msg_count"] == 1
        assert snap["topic_a"]["record_count"] == 5
        assert snap["topic_a"]["bytes_total"] == 1024

    def test_record_multiple_topics(self) -> None:
        ts = TrafficStats()
        ts.record("topic_a", 3, 100)
        ts.record("topic_b", 7, 200)
        ts.record("topic_a", 2, 50)
        snap = ts.snapshot()
        assert snap["topic_a"]["msg_count"] == 2
        assert snap["topic_a"]["record_count"] == 5
        assert snap["topic_b"]["record_count"] == 7

    def test_roll_minute(self) -> None:
        ts = TrafficStats()
        ts.record("topic_a", 10, 1000)
        archived = ts.roll_minute()
        # 没过一分钟，不会归档
        assert archived == {}

    def test_get_history_empty(self) -> None:
        ts = TrafficStats()
        history = ts.get_history("nonexistent")
        assert history == []

    def test_all_topics_snapshot(self) -> None:
        ts = TrafficStats()
        ts.record("topic_a", 5, 100)
        ts.record("topic_b", 3, 200)
        snap = ts.all_topics_snapshot()
        assert "topic_a" in snap
        assert "topic_b" in snap
        assert snap["topic_a"]["record_count_current"] == 5


class TestStatsStorage:
    def test_save_and_load(self) -> None:
        storage = StatsStorage(":memory:")
        storage.connect()

        slot = MinuteSlot(timestamp=100, msg_count=10, record_count=50, bytes_total=1024)
        storage.save_minute("test_topic", slot)

        history = storage.load_history("test_topic", since_ts=0)
        assert len(history) == 1
        assert history[0]["msg_count"] == 10
        assert history[0]["record_count"] == 50

        storage.close()

    def test_save_batch(self) -> None:
        storage = StatsStorage(":memory:")
        storage.connect()

        data = {
            "topic_a": MinuteSlot(timestamp=100, msg_count=5, record_count=10, bytes_total=500),
            "topic_b": MinuteSlot(timestamp=100, msg_count=3, record_count=6, bytes_total=300),
        }
        storage.save_minutes_batch(data)

        ha = storage.load_history("topic_a", 0)
        hb = storage.load_history("topic_b", 0)
        assert len(ha) == 1
        assert len(hb) == 1

        storage.close()

    def test_cleanup(self) -> None:
        storage = StatsStorage(":memory:")
        storage.connect()

        # 插入旧数据
        old_ts = int(time.time()) - 8 * 86400
        storage.save_minute("old_topic", MinuteSlot(timestamp=old_ts, msg_count=1))

        # 插入新数据
        new_ts = int(time.time())
        storage.save_minute("new_topic", MinuteSlot(timestamp=new_ts, msg_count=2))

        deleted = storage.cleanup(retention_days=7)
        assert deleted >= 1
        assert len(storage.load_history("new_topic", 0)) == 1

        storage.close()

