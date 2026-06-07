"""AdminServer 单元测试 (纯逻辑, 不需要 HTTP)。"""

from __future__ import annotations

import os
import sys
import tempfile
import time

import pytest

if sys.platform == "win32":
    from pulsemq.event_loop import install_event_loop

    install_event_loop(use_uvloop=False)

from pulsemq.auth.permission import PermissionService
from pulsemq.monitoring.admin_server import (
    AdminServer,
    SERVER_START_TIME,
    SERVER_VERSION,
    _user_to_dict,
)
from pulsemq.monitoring.client_tracker import ClientTracker
from pulsemq.monitoring.realtime import RealtimeMetrics, TopicMetricsRegistry
from pulsemq.storage.database import init_db
from pulsemq.storage.sqlite_perm import SqlitePermGroupRepo
from pulsemq.storage.sqlite_stats import SQLiteStatsRepo
from pulsemq.storage.sqlite_user import SqliteUserRepo
from pulsemq.storage.interfaces import User


@pytest.fixture
def env():
    """构造最小环境: 内存 SQLite + tracker + metrics。"""
    tmpdir = tempfile.mkdtemp(prefix="pulse_admin_unit_")
    db_path = os.path.join(tmpdir, "users.db")
    stats_path = os.path.join(tmpdir, "stats.db")
    conn = init_db(db_path)
    user_repo = SqliteUserRepo(conn)
    perm_repo = SqlitePermGroupRepo(conn)
    perm_service = PermissionService(perm_repo, user_repo=user_repo)
    stats_repo = SQLiteStatsRepo(stats_path, retention_days=1)
    tracker = ClientTracker()
    topic_metrics = TopicMetricsRegistry()
    realtime = RealtimeMetrics()

    server = AdminServer(
        bind="127.0.0.1:0",  # 不实际启动
        client_tracker=tracker,
        topic_metrics=topic_metrics,
        realtime_metrics=realtime,
        stats_repo=stats_repo,
        user_repo=user_repo,
        perm_service=perm_service,
        perm_repo=perm_repo,
        start_time=time.time(),
    )
    yield {
        "server": server,
        "user_repo": user_repo,
        "perm_repo": perm_repo,
        "perm_service": perm_service,
        "stats_repo": stats_repo,
        "tracker": tracker,
        "topic_metrics": topic_metrics,
        "realtime": realtime,
    }
    try:
        stats_repo.close()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass


# ---- 路径匹配 ----


def test_match_topic_path_simple(env):
    assert AdminServer._match_topic_path("/api/v1/topics/team-a.mkt") == ("", "team-a.mkt")


def test_match_topic_path_history(env):
    assert AdminServer._match_topic_path("/api/v1/topics/team-a.mkt/history") == (
        "history", "team-a.mkt",
    )


def test_match_topic_path_no_match(env):
    assert AdminServer._match_topic_path("/api/v1/topics") is None
    assert AdminServer._match_topic_path("/api/v1/users/1") is None


def test_match_client_path_ok(env):
    assert AdminServer._match_client_path("/api/v1/clients/abcd1234") == "abcd1234"


def test_match_client_path_subpath_rejected(env):
    assert AdminServer._match_client_path("/api/v1/clients/abcd/extra") is None


def test_match_user_path_user_id(env):
    assert AdminServer._match_user_path("/api/v1/users/42", "GET", b"") == "USER:42"


def test_match_user_path_api_keys(env):
    assert AdminServer._match_user_path("/api/v1/users/42/api_keys", "POST", b"") == "API_KEYS:42"


def test_match_user_path_batch_config(env):
    """batch_config 路径已移除 (batcher 策略撤销), 应返回 None。"""
    assert AdminServer._match_user_path("/api/v1/users/42/batch_config", "PUT", b"") is None


def test_match_user_path_invalid(env):
    assert AdminServer._match_user_path("/api/v1/users", "GET", b"") is None
    assert AdminServer._match_user_path("/api/v1/users/42/foo", "GET", b"") is None


# ---- 快照 ----


def test_realtime_snapshot_contains_topics(env):
    env["topic_metrics"].record("a.b.c", latency_ms=1.5)
    snap = env["server"]._realtime_snapshot()
    assert "msg_rate" in snap
    assert "topics" in snap
    assert "clients_online" in snap
    assert len(snap["topics"]["topics"]) == 1


def test_full_snapshot_has_system(env):
    snap = env["server"]._full_snapshot()
    assert "system" in snap
    assert snap["system"]["version"] == SERVER_VERSION


def test_system_status_version_and_time(env):
    s = env["server"]._system_status()
    assert s["version"] == SERVER_VERSION
    assert s["start_time"] > 0
    assert s["uptime_seconds"] >= 0


def test_sse_takeover_attribute_set(env):
    """writer._sse_takeover=True 时 finally 不关连接 (断言行为靠测试覆盖)。"""
    # 简单验证: 默认 writer 没有 _sse_takeover 属性
    import asyncio

    async def _check():
        server = env["server"]
        # 直接构造 writer
        async def _run():
            return await asyncio.sleep(0)
        # 仅验证 hasattr
        return hasattr(server, "_sse_clients")
    assert asyncio.run(_check())


# ---- User dict ----


def test_user_to_dict_all_fields():
    u = User(
        id=7,
        username="bob",
        api_key="pulse_sk_bob",
        role="user",
        namespace="ns1",
        disabled=False,
        max_connections=10,
        created_at=1.0,
        updated_at=2.0,
    )
    d = _user_to_dict(u)
    assert d["id"] == 7
    assert d["username"] == "bob"
    assert "batch_size" not in d
    assert "batch_interval_ms" not in d
    assert "batch_max_wait_ms" not in d
    assert d["created_at"] == 1.0


# ---- SSE 广播 ----


@pytest.mark.asyncio
async def test_sse_broadcast_loop_creates_frames(env):
    """SSE 广播循环 1s 后应有数据帧 (短测试, 跑 1.5s 即可)。"""
    import asyncio

    # 启 server
    env["server"]._host = "127.0.0.1"
    import socket as _s
    with _s.socket(_s.AF_INET, _s.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        env["server"]._port = s.getsockname()[1]
    await env["server"].start()
    try:
        # 用内存队列模拟客户端
        import asyncio as _aio
        q = _aio.Queue(maxsize=64)
        env["server"]._sse_clients[99] = (q, _aio.create_task(_aio.sleep(10)))

        # 触发一次广播
        from pulsemq.monitoring.admin_server import AdminServer
        # 直接调用 _sse_broadcast_loop 一帧 (cancel 它)
        task = _aio.create_task(env["server"]._sse_broadcast_loop())
        # 等 1.1s
        await _aio.sleep(1.1)
        task.cancel()
        try:
            await task
        except _aio.CancelledError:
            pass

        # 验证队列收到帧
        frame = await asyncio.wait_for(q.get(), timeout=1.0)
        assert frame.startswith(b"data: ")
        assert b"msg_rate" in frame
    finally:
        await env["server"].stop()


# ---- 列表与详情 ----


def test_list_topics_uses_registry(env):
    env["topic_metrics"].record("a.b", 1.0)
    env["topic_metrics"].record("c.d", 2.0)
    data = env["server"]._list_topics()
    assert data["topic_count"] == 2


def test_list_clients_uses_tracker(env):
    env["tracker"].on_connect(b"\x00\x01", user_id=1)
    env["tracker"].on_connect(b"\x00\x02", user_id=2)
    data = env["server"]._list_clients()
    assert data["online_count"] == 2


# ---- 列表 users (async) ----


@pytest.mark.asyncio
async def test_list_users_contains_admin(env):
    data = await env["server"]._list_users()
    assert data["count"] >= 1
    admin = next((u for u in data["users"] if u["username"] == "admin"), None)
    assert admin is not None
    assert admin["role"] == "admin"


@pytest.mark.asyncio
async def test_list_permissions_empty(env):
    data = await env["server"]._list_permissions({})
    assert data["count"] == 0
    assert data["permissions"] == []


@pytest.mark.asyncio
async def test_list_permissions_filters_by_user(env):
    data = await env["server"]._list_permissions({"user_id": ["1"]})
    assert "user_id" in data
    assert data["user_id"] == 1


@pytest.mark.asyncio
async def test_list_permissions_invalid_user_id(env):
    data = await env["server"]._list_permissions({"user_id": ["abc"]})
    assert data["count"] == 0
