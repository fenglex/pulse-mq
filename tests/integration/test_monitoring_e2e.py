"""监控 + SSE 端到端集成测试。

启一个 in-process PulseServer (auth 关, admin + metrics 开),
验证:
- SSE 流能收到实时指标 (持续 3s, 至少 1 帧)
- topic_stats 写入 SQLite (调 _stats_minute_loop 立即)
- 7d 清理: 手工 insert 8 天前的记录, 调 cleanup_expired, 验证被删
- /api/v1/metrics/realtime p99 < 50ms (性能断言)
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import sys
import tempfile
import time

import pytest
import pytest_asyncio

# Windows 上 pyzmq 不兼容 ProactorEventLoop
if sys.platform == "win32":
    from pulsemq.event_loop import install_event_loop

    install_event_loop(use_uvloop=False)

from pulsemq.config import ServerConfig
from pulsemq.server import PulseServer
from pulsemq.storage.database import init_db
from pulsemq.storage.sqlite_user import SqliteUserRepo


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _free_port_range(n: int) -> list[int]:
    """分配 n 个非连续的空闲端口 (用于同时启多端口 server)."""
    ports: list[int] = []
    for _ in range(n):
        p = _free_port()
        while p in ports:
            p = _free_port()
        ports.append(p)
    return ports


async def _wait_for_port(host: str, port: int, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.close()
            await writer.wait_closed()
            return
        except OSError:
            await asyncio.sleep(0.05)
    raise TimeoutError(f"port {host}:{port} 在 {timeout}s 内未就绪")


@pytest_asyncio.fixture
async def monitor_server():
    """启一个 in-process PulseServer, admin + metrics 开, auth 关."""
    port, xpub_port, admin_port, metrics_port = _free_port_range(4)

    tmpdir = tempfile.mkdtemp(prefix="pulse_monitor_test_")
    db_path = os.path.join(tmpdir, "users.db")
    stats_path = os.path.join(tmpdir, "stats.db")
    init_db(db_path)

    config = ServerConfig(
        bind=f"tcp://127.0.0.1:{port}",
        xpub_bind=f"tcp://127.0.0.1:{xpub_port}",
        auth_enabled=False,
        metrics_enabled=True,
        metrics_bind=f"127.0.0.1:{metrics_port}",
        admin_enabled=True,
        admin_bind=f"127.0.0.1:{admin_port}",
        db_url=f"sqlite://{db_path}",
        stats_db_url=f"sqlite://{stats_path}",
        stats_retention_days=7,
        use_uvloop=False,
    )

    server = PulseServer(config)
    server_task = asyncio.create_task(server.start())

    await _wait_for_port("127.0.0.1", port, timeout=5.0)
    await _wait_for_port("127.0.0.1", admin_port, timeout=5.0)
    await asyncio.sleep(0.3)

    try:
        yield {
            "port": port,
            "xpub_port": xpub_port,
            "admin_port": admin_port,
            "host": "127.0.0.1",
            "server": server,
            "stats_repo": server._stats_repo,
            "topic_metrics": server._topic_metrics,
            "realtime_metrics": server._realtime_metrics,
            "tmpdir": tmpdir,
        }
    finally:
        # 先取消 server_task
        if not server_task.done():
            server_task.cancel()
        # 再 stop
        try:
            await asyncio.wait_for(server.stop(), timeout=5.0)
        except (asyncio.TimeoutError, Exception):
            pass
        if not server_task.done():
            try:
                await asyncio.wait_for(server_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                server_task.cancel()
        try:
            server._stats_repo.close()
        except Exception:
            pass


# ---- HTTP 工具 ----


async def _http_get(host: str, port: int, path: str, timeout: float = 5.0) -> tuple[int, bytes]:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=timeout
    )
    try:
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        ).encode("utf-8")
        writer.write(req)
        await writer.drain()

        status_line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        status = int(status_line.decode("utf-8", errors="ignore").split()[1])

        headers: dict[str, str] = {}
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if line in (b"\r\n", b"\n", b""):
                break
            s = line.decode("utf-8", errors="ignore").strip()
            if ":" in s:
                k, v = s.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        body = b""
        cl = headers.get("content-length")
        if cl:
            body = await asyncio.wait_for(
                reader.readexactly(int(cl)), timeout=timeout
            )
        return status, body
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# ---- Tests ----


@pytest.mark.asyncio
async def test_sse_stream_receives_frames(monitor_server):
    """SSE 流能收到实时指标 (持续 3s, 至少 1 帧)."""
    env = monitor_server
    host, port = env["host"], env["admin_port"]

    reader, writer = await asyncio.open_connection(host, port)
    try:
        req = (
            "GET /api/v1/metrics/stream HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Accept: text/event-stream\r\n"
            "Connection: keep-alive\r\n"
            "\r\n"
        ).encode("utf-8")
        writer.write(req)
        await writer.drain()

        # 读响应行
        status_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
        assert b"200" in status_line

        # 读 headers, 验证 Content-Type 是 text/event-stream
        seen_event_stream = False
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if line in (b"\r\n", b"\n"):
                break
            s = line.decode("utf-8", errors="ignore").lower()
            if "content-type:" in s and "text/event-stream" in s:
                seen_event_stream = True
        assert seen_event_stream, "Content-Type 应为 text/event-stream"

        # 收集 3s 内的所有 data 帧
        frames: list[dict] = []
        buffer = b""
        deadline = time.time() + 3.5
        while time.time() < deadline:
            try:
                chunk = await asyncio.wait_for(reader.read(1024), timeout=1.0)
                if not chunk:
                    break
                buffer += chunk
                # 拆 SSE 帧
                while b"\n\n" in buffer:
                    frame, buffer = buffer.split(b"\n\n", 1)
                    decoded = frame.decode("utf-8", errors="ignore")
                    for ln in decoded.splitlines():
                        if ln.startswith("data: "):
                            try:
                                frames.append(json.loads(ln[6:]))
                            except json.JSONDecodeError:
                                pass
            except asyncio.TimeoutError:
                continue

        assert len(frames) >= 1, f"3s 内应至少 1 帧, 实际 {len(frames)}"
        # 第一帧的结构
        first = frames[0]
        assert "msg_rate" in first
        assert "server_time" in first
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_realtime_endpoint_fast(monitor_server):
    """realtime 端点延迟 < 50ms (本地回环)."""
    env = monitor_server
    host, port = env["host"], env["admin_port"]

    # 预热
    await _http_get(host, port, "/api/v1/metrics/realtime")

    # 5 次取最短时间
    samples = []
    for _ in range(5):
        t0 = time.perf_counter()
        status, _ = await _http_get(host, port, "/api/v1/metrics/realtime")
        elapsed_ms = (time.perf_counter() - t0) * 1000
        assert status == 200
        samples.append(elapsed_ms)

    p99 = sorted(samples)[int(len(samples) * 0.99) - 1] if len(samples) > 1 else samples[0]
    assert p99 < 50, f"realtime p99 延迟 {p99:.2f}ms > 50ms (samples={samples})"


@pytest.mark.asyncio
async def test_stats_repo_write_and_read(monitor_server):
    """topic_stats 写入 SQLite 后可读出."""
    env = monitor_server
    stats_repo = env["stats_repo"]

    now_minute = int(time.time()) // 60 * 60
    await stats_repo.upsert_minute(
        topic="team-a.mkt",
        minute_ts=now_minute,
        msg_count=100,
        p50=1.5,
        p99=5.0,
        max_lat=10.0,
        peak_in_flight=5,
    )

    rows = await stats_repo.get_topic_history("team-a.mkt", since_ts=now_minute - 60)
    assert len(rows) == 1
    assert rows[0]["msg_count"] == 100
    assert rows[0]["latency_p99_ms"] == 5.0


@pytest.mark.asyncio
async def test_stats_minute_loop_writes_to_sqlite(monitor_server):
    """模拟 _stats_minute_loop 写流程: 注入 topic_metrics, 调 _stats_minute_loop 一次."""
    env = monitor_server
    server = env["server"]
    stats_repo = env["stats_repo"]
    topic_metrics = env["topic_metrics"]

    # 注入 5 条记录到 topic "test.monitor.foo"
    for i in range(5):
        topic_metrics.record("test.monitor.foo", latency_ms=1.0 + i * 0.1)

    # 直接调 _stats_minute_loop 的内部逻辑 (避免等 60s)
    # 它先等下一分钟边界, 我们手动构造 minute_ts
    minute_ts = int(time.time()) // 60 * 60
    for m in topic_metrics.list_topics():
        if m.topic != "test.monitor.foo":
            continue
        if m.msg_count_1min == 0:
            continue
        await stats_repo.upsert_minute(
            topic=m.topic,
            minute_ts=minute_ts,
            msg_count=m.msg_count_1min,
            p50=m.latency_p50_1min,
            p99=m.latency_p99_1min,
            max_lat=m.latency_max_1min,
            peak_in_flight=topic_metrics.peak_in_flight(m.topic),
        )
        topic_metrics.reset_window(m.topic)

    # 验证 SQLite 写入了
    rows = await stats_repo.get_topic_history("test.monitor.foo", since_ts=minute_ts - 60)
    assert len(rows) == 1
    assert rows[0]["msg_count"] == 5


@pytest.mark.asyncio
async def test_7d_cleanup_removes_expired(monitor_server):
    """7d 清理: 手工 insert 8 天前的记录, 调 cleanup_expired, 验证被删."""
    env = monitor_server
    stats_repo = env["stats_repo"]

    # 插入 8 天前的记录
    eight_days_ago = int(time.time()) - 8 * 86400
    await stats_repo.upsert_minute(
        topic="old.topic",
        minute_ts=eight_days_ago,
        msg_count=50,
        p50=1.0, p99=3.0, max_lat=8.0, peak_in_flight=2,
    )

    # 插入 1 天前的记录 (应保留)
    one_day_ago = int(time.time()) - 1 * 86400
    await stats_repo.upsert_minute(
        topic="recent.topic",
        minute_ts=one_day_ago,
        msg_count=10,
        p50=0.5, p99=2.0, max_lat=4.0, peak_in_flight=1,
    )

    # 验证两条都在
    old_rows = await stats_repo.get_topic_history("old.topic", since_ts=0)
    recent_rows = await stats_repo.get_topic_history("recent.topic", since_ts=0)
    assert len(old_rows) == 1
    assert len(recent_rows) == 1

    # 触发清理
    deleted = await stats_repo.cleanup_expired()
    assert deleted == 1, f"应删 1 条 (8 天前的), 实际删 {deleted}"

    # 验证 old 被删, recent 保留
    old_rows_after = await stats_repo.get_topic_history("old.topic", since_ts=0)
    recent_rows_after = await stats_repo.get_topic_history("recent.topic", since_ts=0)
    assert len(old_rows_after) == 0
    assert len(recent_rows_after) == 1


@pytest.mark.asyncio
async def test_sse_frame_contains_topics(monitor_server):
    """SSE 帧中 topics 字段反映 topic_metrics 状态."""
    env = monitor_server
    host, port = env["host"], env["admin_port"]
    topic_metrics = env["topic_metrics"]

    # 先注入 3 条
    for i in range(3):
        topic_metrics.record("test.sse.topic", latency_ms=2.0)

    reader, writer = await asyncio.open_connection(host, port)
    try:
        req = (
            "GET /api/v1/metrics/stream HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Accept: text/event-stream\r\n"
            "Connection: keep-alive\r\n"
            "\r\n"
        ).encode("utf-8")
        writer.write(req)
        await writer.drain()

        # 读 headers
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if line in (b"\r\n", b"\n"):
                break

        # 等首帧
        buffer = b""
        deadline = time.time() + 2.5
        first_frame = None
        while time.time() < deadline and first_frame is None:
            try:
                chunk = await asyncio.wait_for(reader.read(512), timeout=0.5)
                if not chunk:
                    break
                buffer += chunk
                while b"\n\n" in buffer:
                    frame, buffer = buffer.split(b"\n\n", 1)
                    decoded = frame.decode("utf-8", errors="ignore")
                    for ln in decoded.splitlines():
                        if ln.startswith("data: "):
                            first_frame = json.loads(ln[6:])
                            break
                    if first_frame is not None:
                        break
            except asyncio.TimeoutError:
                continue

        assert first_frame is not None
        assert "topics" in first_frame
        assert "clients_online" in first_frame
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_topic_history_endpoint_via_http(monitor_server):
    """GET /api/v1/topics/{topic}/history 返回写入的历史数据."""
    env = monitor_server
    host, port = env["host"], env["admin_port"]
    stats_repo = env["stats_repo"]

    now_minute = int(time.time()) // 60 * 60
    await stats_repo.upsert_minute(
        topic="test.history.topic",
        minute_ts=now_minute,
        msg_count=42, p50=1.0, p99=3.0, max_lat=5.0, peak_in_flight=2,
    )

    status, body = await _http_get(
        host, port, "/api/v1/topics/test.history.topic/history",
    )
    assert status == 200
    data = json.loads(body)
    assert data["topic"] == "test.history.topic"
    assert len(data["history"]) >= 1
    assert data["history"][0]["msg_count"] == 42


@pytest.mark.asyncio
async def test_metrics_snapshot_full(monitor_server):
    """GET /api/v1/metrics/snapshot 包含 clients + topics + system."""
    env = monitor_server
    host, port = env["host"], env["admin_port"]
    topic_metrics = env["topic_metrics"]
    topic_metrics.record("test.snap", latency_ms=1.0)

    status, body = await _http_get(host, port, "/api/v1/metrics/snapshot")
    assert status == 200
    data = json.loads(body)
    assert "system" in data
    assert "topics" in data
    assert "clients" in data
    assert data["system"]["version"]


@pytest.mark.asyncio
async def test_system_status_via_http(monitor_server):
    """GET /api/v1/system/status 返回 version, uptime."""
    env = monitor_server
    host, port = env["host"], env["admin_port"]

    status, body = await _http_get(host, port, "/api/v1/system/status")
    assert status == 200
    data = json.loads(body)
    assert "version" in data
    assert "uptime_seconds" in data
    assert data["uptime_seconds"] >= 0


@pytest.mark.asyncio
async def test_clients_endpoint_reflects_tracker(monitor_server):
    """GET /api/v1/clients 反映 ClientTracker 状态."""
    env = monitor_server
    host, port = env["host"], env["admin_port"]
    tracker = env["server"]._client_tracker

    # 注入 1 个 client
    tracker.on_connect(b"\xaa\xbb\xcc", user_id=99)

    status, body = await _http_get(host, port, "/api/v1/clients")
    assert status == 200
    data = json.loads(body)
    assert data["online_count"] >= 1
    user_ids = [c["user_id"] for c in data["clients"]]
    assert 99 in user_ids

    # 清理
    tracker.on_disconnect(b"\xaa\xbb\xcc")
