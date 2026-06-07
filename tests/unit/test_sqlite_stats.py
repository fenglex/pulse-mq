"""SQLiteStatsRepo 单测。

覆盖:
- 建表: 初始化后表存在
- upsert_minute: 写入 + 冲突覆盖
- get_topic_history: since_ts 过滤 + 升序
- list_all_topics: DISTINCT topic
- cleanup_expired: 删过期行 + 返回行数
- start_cleanup_task: 取消正常, 不抛错
- retention_days 默认 7
- close: 释放连接
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import time

import pytest
import pytest_asyncio

from pulsemq.storage.sqlite_stats import SQLiteStatsRepo


@pytest_asyncio.fixture
async def repo():
    """每个测试一个临时 db。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    r = SQLiteStatsRepo(path)
    try:
        yield r
    finally:
        r.close()
        os.unlink(path)


# ---- 构造 ----


def test_ctor_creates_table():
    """初始化后 topic_stats 表存在。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    try:
        r = SQLiteStatsRepo(path)
        # 验证表存在
        row = r._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='topic_stats'"
        ).fetchone()
        assert row is not None
        # 索引也存在
        idx = r._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_topic_stats_minute_ts'"
        ).fetchone()
        assert idx is not None
        r.close()
    finally:
        os.unlink(path)


def test_ctor_default_retention_is_7_days():
    """默认 retention_days=7。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    try:
        r = SQLiteStatsRepo(path)
        assert r._retention_days == 7
        r.close()
    finally:
        os.unlink(path)


def test_ctor_custom_retention():
    """自定义 retention_days。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    try:
        r = SQLiteStatsRepo(path, retention_days=14)
        assert r._retention_days == 14
        r.close()
    finally:
        os.unlink(path)


# ---- upsert_minute ----


@pytest.mark.asyncio
async def test_upsert_inserts_new_row(repo: SQLiteStatsRepo):
    """upsert 新行, get_topic_history 应能读到。"""
    now = int(time.time())
    await repo.upsert_minute(
        "t.a", minute_ts=now, msg_count=10,
        p50=1.0, p99=5.0, max_lat=10.0, peak_in_flight=3,
    )
    rows = await repo.get_topic_history("t.a", since_ts=now - 60)
    assert len(rows) == 1
    assert rows[0]["topic"] == "t.a"
    assert rows[0]["minute_ts"] == now
    assert rows[0]["msg_count"] == 10
    assert rows[0]["latency_p50_ms"] == 1.0
    assert rows[0]["latency_p99_ms"] == 5.0
    assert rows[0]["latency_max_ms"] == 10.0
    assert rows[0]["peak_in_flight"] == 3


@pytest.mark.asyncio
async def test_upsert_conflict_overwrites(repo: SQLiteStatsRepo):
    """UNIQUE(topic, minute_ts) 冲突时覆盖, 不新增行。"""
    now = int(time.time())
    await repo.upsert_minute("t.a", now, 10, 1.0, 5.0, 10.0, 3)
    await repo.upsert_minute("t.a", now, 99, 2.0, 9.0, 50.0, 7)
    rows = await repo.get_topic_history("t.a", since_ts=now - 60)
    # 仍只 1 行, 字段被覆盖
    assert len(rows) == 1
    assert rows[0]["msg_count"] == 99
    assert rows[0]["latency_p50_ms"] == 2.0
    assert rows[0]["peak_in_flight"] == 7


@pytest.mark.asyncio
async def test_upsert_different_minute_keeps_both(repo: SQLiteStatsRepo):
    """不同 minute_ts 的两行独立。"""
    now = int(time.time())
    await repo.upsert_minute("t.a", now, 10, 1.0, 5.0, 10.0, 3)
    await repo.upsert_minute("t.a", now + 60, 20, 2.0, 6.0, 12.0, 4)
    rows = await repo.get_topic_history("t.a", since_ts=now - 60)
    assert len(rows) == 2
    assert rows[0]["minute_ts"] == now
    assert rows[1]["minute_ts"] == now + 60


# ---- get_topic_history ----


@pytest.mark.asyncio
async def test_get_history_filters_by_since_ts(repo: SQLiteStatsRepo):
    """since_ts 之前的记录被过滤。"""
    old = int(time.time()) - 3600
    new = int(time.time())
    await repo.upsert_minute("t.a", old, 5, 1.0, 2.0, 3.0, 1)
    await repo.upsert_minute("t.a", new, 10, 1.0, 2.0, 3.0, 1)
    rows = await repo.get_topic_history("t.a", since_ts=new - 60)
    assert len(rows) == 1
    assert rows[0]["minute_ts"] == new


@pytest.mark.asyncio
async def test_get_history_empty_topic(repo: SQLiteStatsRepo):
    """不存在的 topic 返回空列表。"""
    rows = await repo.get_topic_history("never-recorded", since_ts=0)
    assert rows == []


@pytest.mark.asyncio
async def test_get_history_ordered_ascending(repo: SQLiteStatsRepo):
    """返回结果按 minute_ts 升序。"""
    base = int(time.time())
    for offset in [120, 0, 60, 30]:  # 故意乱序
        await repo.upsert_minute("t.a", base + offset, 1, 1.0, 1.0, 1.0, 1)
    rows = await repo.get_topic_history("t.a", since_ts=base)
    ts_list = [r["minute_ts"] for r in rows]
    assert ts_list == sorted(ts_list)


# ---- list_all_topics ----


@pytest.mark.asyncio
async def test_list_all_topics_distinct(repo: SQLiteStatsRepo):
    """DISTINCT topic 列表。"""
    now = int(time.time())
    await repo.upsert_minute("t.a", now, 1, 1.0, 1.0, 1.0, 1)
    await repo.upsert_minute("t.b", now, 1, 1.0, 1.0, 1.0, 1)
    await repo.upsert_minute("t.a", now + 60, 1, 1.0, 1.0, 1.0, 1)  # 重复 topic
    topics = await repo.list_all_topics()
    assert topics == ["t.a", "t.b"]


@pytest.mark.asyncio
async def test_list_all_topics_empty(repo: SQLiteStatsRepo):
    """空表返回空列表。"""
    topics = await repo.list_all_topics()
    assert topics == []


# ---- cleanup_expired ----


@pytest.mark.asyncio
async def test_cleanup_expired_removes_old_rows(repo: SQLiteStatsRepo):
    """retention_days 之前的行被删, 返回删除行数。"""
    # 插入 8 天前的行
    old_ts = int(time.time()) - 8 * 86400
    await repo.upsert_minute("t.a", old_ts, 1, 1.0, 1.0, 1.0, 1)
    # 插入 1 天前的行
    recent_ts = int(time.time()) - 86400
    await repo.upsert_minute("t.b", recent_ts, 1, 1.0, 1.0, 1.0, 1)

    n = await repo.cleanup_expired()
    assert n == 1  # 仅 8 天前的被删

    # 1 天前的应还在
    rows = await repo.get_topic_history("t.b", since_ts=0)
    assert len(rows) == 1
    # 8 天前的应被删
    rows = await repo.get_topic_history("t.a", since_ts=0)
    assert rows == []


@pytest.mark.asyncio
async def test_cleanup_expired_returns_count(repo: SQLiteStatsRepo):
    """返回删除行数。"""
    old_ts = int(time.time()) - 10 * 86400
    for i in range(5):
        await repo.upsert_minute(f"t.{i}", old_ts - i * 60, 1, 1.0, 1.0, 1.0, 1)
    n = await repo.cleanup_expired()
    assert n == 5


@pytest.mark.asyncio
async def test_cleanup_expired_no_old_rows_returns_zero(repo: SQLiteStatsRepo):
    """无过期行时返回 0。"""
    now = int(time.time())
    await repo.upsert_minute("t.a", now, 1, 1.0, 1.0, 1.0, 1)
    n = await repo.cleanup_expired()
    assert n == 0


# ---- start_cleanup_task ----


@pytest.mark.asyncio
async def test_start_cleanup_task_can_be_cancelled():
    """后台清理 task 可正常取消, 不抛错。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    try:
        r = SQLiteStatsRepo(path)
        # 短间隔, 启动后立即取消
        task = await r.start_cleanup_task(interval_seconds=0.05)
        await asyncio.sleep(0.02)  # 让它跑一次
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass  # 预期
        # task 状态为 cancelled
        assert task.cancelled()
        r.close()
    finally:
        os.unlink(path)


# ---- close ----


def test_close_releases_connection():
    """close 后连接应被关闭。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    try:
        r = SQLiteStatsRepo(path)
        r.close()
        # _conn 设为 None
        assert r._conn is None
    finally:
        os.unlink(path)
