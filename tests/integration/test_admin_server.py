"""AdminServer (Phase 8) 集成测试。

- 用 AdminServer 单独启动 (不依赖 PulseServer), 注入最小依赖 (ClientTracker / TopicMetrics / StatsRepo)
- 用 asyncio.open_connection 走真实 HTTP 协议验证各端点
- 覆盖: 静态 HTML / JSON API / POST/PUT/DELETE / SSE 流
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

if sys.platform == "win32":
    from pulsemq.event_loop import install_event_loop

    install_event_loop(use_uvloop=False)

from pulsemq.auth.permission import PermissionService
from pulsemq.monitoring.admin_server import AdminServer
from pulsemq.monitoring.client_tracker import ClientTracker
from pulsemq.monitoring.realtime import RealtimeMetrics, TopicMetricsRegistry
from pulsemq.storage.database import init_db
from pulsemq.storage.sqlite_perm import SqlitePermGroupRepo
from pulsemq.storage.sqlite_stats import SQLiteStatsRepo
from pulsemq.storage.sqlite_user import SqliteUserRepo


def _free_port() -> int:
    """找一个当前空闲的 TCP 端口。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ---- 异步 HTTP 工具 ----


class HTTPResponse:
    def __init__(self, status: int, reason: str, headers: dict, body: bytes):
        self.status = status
        self.reason = reason
        self.headers = headers
        self.body = body

    @property
    def json(self) -> dict:
        return json.loads(self.body.decode("utf-8")) if self.body else {}


async def _http_request(
    host: str,
    port: int,
    method: str,
    path: str,
    body: bytes | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 5.0,
) -> HTTPResponse:
    """发一个完整 HTTP 请求并解析响应 (Content-Length 闭合)."""
    hdrs = {"Host": f"{host}:{port}", "Connection": "close"}
    if headers:
        hdrs.update(headers)
    if body is not None:
        hdrs["Content-Length"] = str(len(body))

    lines = [f"{method} {path} HTTP/1.1"]
    for k, v in hdrs.items():
        lines.append(f"{k}: {v}")
    raw = ("\r\n".join(lines) + "\r\n\r\n").encode("utf-8")
    if body:
        raw += body

    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=timeout
    )
    try:
        writer.write(raw)
        await writer.drain()

        # 读响应行
        status_line = await asyncio.wait_for(reader.readline(), timeout=timeout)
        if not status_line:
            return HTTPResponse(0, "", {}, b"")
        parts = status_line.decode("utf-8", errors="ignore").strip().split(" ", 2)
        if len(parts) < 2:
            return HTTPResponse(0, "", {}, b"")
        status = int(parts[1])
        reason = parts[2] if len(parts) > 2 else ""

        # 读 headers
        resp_headers: dict[str, str] = {}
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if line in (b"\r\n", b"\n", b""):
                break
            s = line.decode("utf-8", errors="ignore").strip()
            if ":" in s:
                k, v = s.split(":", 1)
                resp_headers[k.strip().lower()] = v.strip()

        # 读 body
        body_bytes = b""
        cl = resp_headers.get("content-length")
        if cl:
            try:
                body_bytes = await asyncio.wait_for(
                    reader.readexactly(int(cl)), timeout=timeout
                )
            except asyncio.IncompleteReadError as e:
                body_bytes = e.partial
        elif resp_headers.get("transfer-encoding", "").lower() == "chunked":
            # chunked 模式: 简单解析
            while True:
                size_line = await asyncio.wait_for(reader.readline(), timeout=timeout)
                if not size_line:
                    break
                size_str = size_line.decode("utf-8", errors="ignore").strip().split(";")[0]
                try:
                    size = int(size_str, 16)
                except ValueError:
                    break
                if size == 0:
                    await reader.readline()  # 末尾空行
                    break
                chunk = await asyncio.wait_for(reader.readexactly(size), timeout=timeout)
                body_bytes += chunk
                await reader.readline()  # chunk 末尾 CRLF

        return HTTPResponse(status, reason, resp_headers, body_bytes)
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# ---- Fixtures ----


@pytest_asyncio.fixture
async def admin_env():
    """起一个最小 AdminServer, 用临时 SQLite, 随机端口。"""
    port = _free_port()
    tmpdir = tempfile.mkdtemp(prefix="pulse_admin_test_")
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
        bind=f"127.0.0.1:{port}",
        client_tracker=tracker,
        topic_metrics=topic_metrics,
        realtime_metrics=realtime,
        stats_repo=stats_repo,
        user_repo=user_repo,
        perm_service=perm_service,
        perm_repo=perm_repo,
        start_time=time.time(),
    )
    await server.start()
    # 等 50ms 让 listen 完全生效
    await asyncio.sleep(0.05)
    try:
        yield {
            "port": port,
            "host": "127.0.0.1",
            "server": server,
            "user_repo": user_repo,
            "perm_repo": perm_repo,
            "perm_service": perm_service,
            "stats_repo": stats_repo,
            "tracker": tracker,
            "topic_metrics": topic_metrics,
            "realtime": realtime,
            "tmpdir": tmpdir,
        }
    finally:
        await server.stop()
        try:
            stats_repo.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


# ---- 静态首页 ----


@pytest.mark.asyncio
async def test_get_index_returns_html(admin_env):
    resp = await _http_request(admin_env["host"], admin_env["port"], "GET", "/")
    assert resp.status == 200
    assert "text/html" in resp.headers.get("content-type", "")
    body = resp.body.decode("utf-8")
    assert "PulseMQ" in body
    assert "管理后台" in body
    assert "EventSource" in body  # SSE 客户端代码


@pytest.mark.asyncio
async def test_get_index_html_alias(admin_env):
    resp = await _http_request(admin_env["host"], admin_env["port"], "GET", "/index.html")
    assert resp.status == 200
    assert "PulseMQ" in resp.body.decode("utf-8")


# ---- Realtime / Snapshot ----


@pytest.mark.asyncio
async def test_metrics_realtime_returns_json(admin_env):
    resp = await _http_request(admin_env["host"], admin_env["port"], "GET", "/api/v1/metrics/realtime")
    assert resp.status == 200
    data = resp.json
    assert "msg_rate" in data
    assert "topics" in data
    assert "server_time" in data


@pytest.mark.asyncio
async def test_metrics_snapshot_returns_json(admin_env):
    resp = await _http_request(admin_env["host"], admin_env["port"], "GET", "/api/v1/metrics/snapshot")
    assert resp.status == 200
    data = resp.json
    assert "system" in data
    assert data["system"]["version"]


@pytest.mark.asyncio
async def test_system_status(admin_env):
    resp = await _http_request(admin_env["host"], admin_env["port"], "GET", "/api/v1/system/status")
    assert resp.status == 200
    data = resp.json
    assert "version" in data
    assert "start_time" in data
    assert "uptime_seconds" in data
    assert data["uptime_seconds"] >= 0


@pytest.mark.asyncio
async def test_healthz(admin_env):
    resp = await _http_request(admin_env["host"], admin_env["port"], "GET", "/healthz")
    assert resp.status == 200
    assert resp.json.get("status") == "ok"


# ---- Topics ----


@pytest.mark.asyncio
async def test_list_topics_empty(admin_env):
    resp = await _http_request(admin_env["host"], admin_env["port"], "GET", "/api/v1/topics")
    assert resp.status == 200
    data = resp.json
    assert "topics" in data
    assert isinstance(data["topics"], list)


@pytest.mark.asyncio
async def test_get_single_topic(admin_env):
    # 先注入一个 topic 指标
    admin_env["topic_metrics"].record("team-a.mkt.sh", latency_ms=2.5)
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET",
        "/api/v1/topics/team-a.mkt.sh",
    )
    assert resp.status == 200
    data = resp.json
    assert data["topic"] == "team-a.mkt.sh"
    assert data["msg_count_1min"] >= 1


@pytest.mark.asyncio
async def test_get_topic_history_empty(admin_env):
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET",
        "/api/v1/topics/team-a.mkt.sh/history",
    )
    assert resp.status == 200
    data = resp.json
    assert data["topic"] == "team-a.mkt.sh"
    assert data["history"] == []


@pytest.mark.asyncio
async def test_topic_history_contains_data(admin_env):
    # 写入一条
    now = int(time.time()) // 60 * 60
    await admin_env["stats_repo"].upsert_minute(
        topic="team-a.mkt.sh",
        minute_ts=now,
        msg_count=42,
        p50=1.5,
        p99=5.2,
        max_lat=10.0,
        peak_in_flight=3,
    )
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET",
        "/api/v1/topics/team-a.mkt.sh/history",
    )
    assert resp.status == 200
    data = resp.json
    assert len(data["history"]) >= 1
    assert data["history"][0]["msg_count"] == 42


# ---- Clients ----


@pytest.mark.asyncio
async def test_list_clients_empty(admin_env):
    resp = await _http_request(admin_env["host"], admin_env["port"], "GET", "/api/v1/clients")
    assert resp.status == 200
    data = resp.json
    assert "clients" in data
    assert data["online_count"] == 0


@pytest.mark.asyncio
async def test_client_detail_404(admin_env):
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET",
        "/api/v1/clients/deadbeef",
    )
    assert resp.status == 404


@pytest.mark.asyncio
async def test_client_invalid_identity_400(admin_env):
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET",
        "/api/v1/clients/not-hex-zzzz",
    )
    assert resp.status == 400


@pytest.mark.asyncio
async def test_client_detail_ok(admin_env):
    # 注册一个客户端
    admin_env["tracker"].on_connect(b"\xab\xcd", user_id=7)
    admin_env["tracker"].on_sub(b"\xab\xcd", "team-a.mkt")
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET",
        "/api/v1/clients/ab cd",  # 注: HTTP 中应 hex, 不带空格
    )
    # 改用纯 hex
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET",
        "/api/v1/clients/abcd",
    )
    assert resp.status == 200
    data = resp.json
    assert data["user_id"] == 7
    assert "team-a.mkt" in data["subscribed_topics"]


# ---- Users ----


@pytest.mark.asyncio
async def test_users_empty(admin_env):
    resp = await _http_request(admin_env["host"], admin_env["port"], "GET", "/api/v1/users")
    assert resp.status == 200
    data = resp.json
    # init_db 插入了默认 admin, 所以至少有 1 个
    assert data["count"] >= 1


@pytest.mark.asyncio
async def test_create_user_ok(admin_env):
    body = json.dumps({
        "username": "alice_test",
        "role": "user",
        "namespace": "team-a",
        "max_connections": 5,
    }).encode("utf-8")
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "POST", "/api/v1/users",
        body=body,
        headers={"Content-Type": "application/json"},
    )
    assert resp.status == 201
    data = resp.json
    assert data["username"] == "alice_test"
    assert data["id"] is not None
    assert data["max_connections"] == 5


@pytest.mark.asyncio
async def test_create_user_missing_username(admin_env):
    body = b'{"role": "user"}'
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "POST", "/api/v1/users",
        body=body,
        headers={"Content-Type": "application/json"},
    )
    assert resp.status == 400


@pytest.mark.asyncio
async def test_create_user_invalid_json(admin_env):
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "POST", "/api/v1/users",
        body=b"not json",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status == 400


@pytest.mark.asyncio
async def test_get_user_by_id(admin_env):
    # admin 是默认用户 (id=1)
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET", "/api/v1/users/1",
    )
    assert resp.status == 200
    data = resp.json
    assert data["id"] == 1
    assert data["username"] == "admin"


@pytest.mark.asyncio
async def test_get_user_not_found(admin_env):
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET", "/api/v1/users/99999",
    )
    assert resp.status == 404


@pytest.mark.asyncio
async def test_update_user_put(admin_env):
    body = json.dumps({"max_connections": 99, "namespace": "test"}).encode("utf-8")
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "PUT", "/api/v1/users/1",
        body=body,
        headers={"Content-Type": "application/json"},
    )
    assert resp.status == 200
    data = resp.json
    assert data["max_connections"] == 99
    assert data["namespace"] == "test"


@pytest.mark.asyncio
async def test_delete_user(admin_env):
    # 先创建
    body = json.dumps({"username": "to_delete"}).encode("utf-8")
    create_resp = await _http_request(
        admin_env["host"], admin_env["port"], "POST", "/api/v1/users",
        body=body, headers={"Content-Type": "application/json"},
    )
    uid = create_resp.json["id"]

    # 删
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "DELETE", f"/api/v1/users/{uid}",
    )
    assert resp.status == 200

    # 验证已删
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET", f"/api/v1/users/{uid}",
    )
    assert resp.status == 404


@pytest.mark.asyncio
async def test_regen_api_key(admin_env):
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "POST",
        "/api/v1/users/1/api_keys",
    )
    assert resp.status == 200
    data = resp.json
    assert "api_key" in data
    assert data["api_key"].startswith("pulse_sk_")
    # 新 key 应当与默认的不同
    assert data["api_key"] != "pulse_sk_admin_default"


# ---- Batch Config ----


@pytest.mark.asyncio
async def test_get_batch_config(admin_env):
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET",
        "/api/v1/users/1/batch_config",
    )
    assert resp.status == 200
    data = resp.json
    assert "batch_size" in data
    assert "batch_interval_ms" in data
    assert "batch_max_wait_ms" in data


@pytest.mark.asyncio
async def test_put_batch_config(admin_env):
    body = json.dumps({
        "batch_size": 200,
        "batch_interval_ms": 100,
        "batch_max_wait_ms": 500,
    }).encode("utf-8")
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "PUT",
        "/api/v1/users/1/batch_config",
        body=body, headers={"Content-Type": "application/json"},
    )
    assert resp.status == 200
    # 验证 GET 返回新值
    resp2 = await _http_request(
        admin_env["host"], admin_env["port"], "GET",
        "/api/v1/users/1/batch_config",
    )
    data = resp2.json
    assert data["batch_size"] == 200
    assert data["batch_interval_ms"] == 100
    assert data["batch_max_wait_ms"] == 500


@pytest.mark.asyncio
async def test_put_batch_config_invalid(admin_env):
    body = json.dumps({"batch_size": 0}).encode("utf-8")
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "PUT",
        "/api/v1/users/1/batch_config",
        body=body, headers={"Content-Type": "application/json"},
    )
    assert resp.status == 400


# ---- Permissions ----


@pytest.mark.asyncio
async def test_list_permissions_empty(admin_env):
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET", "/api/v1/permissions",
    )
    assert resp.status == 200
    data = resp.json
    assert "permissions" in data
    assert isinstance(data["permissions"], list)


@pytest.mark.asyncio
async def test_grant_and_revoke_permission(admin_env):
    # Grant pub
    body = json.dumps({
        "user_id": 1,
        "action": "pub",
        "topic_pattern": "team-a.>",
    }).encode("utf-8")
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "POST", "/api/v1/permissions",
        body=body, headers={"Content-Type": "application/json"},
    )
    assert resp.status == 201, f"got {resp.status} {resp.body!r}"
    data = resp.json
    assert data["granted"]["topic_pattern"] == "team-a.>"

    # 验证 list 能看到
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET",
        "/api/v1/permissions?user_id=1",
    )
    data = resp.json
    assert data["count"] >= 1

    # Revoke
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "DELETE",
        "/api/v1/permissions?user_id=1&topic_pattern=team-a.%3E&action=pub",
    )
    assert resp.status == 200, f"got {resp.status} {resp.body!r}"


@pytest.mark.asyncio
async def test_grant_invalid_action(admin_env):
    body = json.dumps({
        "user_id": 1,
        "action": "hack",
        "topic_pattern": "team-a.>",
    }).encode("utf-8")
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "POST", "/api/v1/permissions",
        body=body, headers={"Content-Type": "application/json"},
    )
    assert resp.status == 400


# ---- SSE ----


@pytest.mark.asyncio
async def test_metrics_stream_sse(admin_env):
    """SSE: 至少收到一帧 data: <json>."""
    reader, writer = await asyncio.open_connection(
        admin_env["host"], admin_env["port"],
    )
    try:
        req = (
            "GET /api/v1/metrics/stream HTTP/1.1\r\n"
            f"Host: {admin_env['host']}:{admin_env['port']}\r\n"
            "Accept: text/event-stream\r\n"
            "Connection: keep-alive\r\n"
            "\r\n"
        ).encode("utf-8")
        writer.write(req)
        await writer.drain()

        # 读响应行 + headers
        status_line = await asyncio.wait_for(reader.readline(), timeout=5.0)
        assert b"200" in status_line
        # headers
        seen_ct = False
        while True:
            line = await asyncio.wait_for(reader.readline(), timeout=5.0)
            if line in (b"\r\n", b"\n"):
                break
            s = line.decode("utf-8", errors="ignore").lower()
            if "content-type:" in s and "text/event-stream" in s:
                seen_ct = True
        assert seen_ct, "缺少 text/event-stream content-type"

        # 读首帧 (可能是注释行 : connected 或首条 data)
        # 等待最多 2s 收一帧 data
        data_frame = None
        deadline = time.time() + 2.5
        buffer = b""
        while time.time() < deadline and data_frame is None:
            try:
                chunk = await asyncio.wait_for(reader.read(256), timeout=1.0)
                if not chunk:
                    break
                buffer += chunk
                # 拆分 SSE 帧
                while b"\n\n" in buffer:
                    frame, buffer = buffer.split(b"\n\n", 1)
                    decoded = frame.decode("utf-8", errors="ignore")
                    for ln in decoded.splitlines():
                        if ln.startswith("data: "):
                            data_frame = ln[6:]
                            break
                    if data_frame is not None:
                        break
            except asyncio.TimeoutError:
                break

        assert data_frame is not None, f"未收到 SSE data 帧, buffer={buffer!r}"
        parsed = json.loads(data_frame)
        assert "msg_rate" in parsed
        assert "server_time" in parsed
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# ---- 404 / Method Not Allowed ----


@pytest.mark.asyncio
async def test_404_unknown_path(admin_env):
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET", "/api/v1/nonsense",
    )
    assert resp.status == 404


@pytest.mark.asyncio
async def test_method_not_allowed(admin_env):
    """DELETE 在 /api/v1/clients/{id} (只有 GET) 上应当 405."""
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "DELETE",
        "/api/v1/clients/deadbeef",
    )
    assert resp.status == 405


# ---- Performance 兜底: realtime 端点响应 < 50ms (含网络) ----


@pytest.mark.asyncio
async def test_realtime_endpoint_fast(admin_env):
    """realtime 端点延迟 < 50ms (本地回环, 远低于 spec 5ms+网络)."""
    t0 = time.time()
    resp = await _http_request(
        admin_env["host"], admin_env["port"], "GET", "/api/v1/metrics/realtime",
    )
    elapsed = (time.time() - t0) * 1000
    assert resp.status == 200
    # 宽松阈值: 含 asyncio + JSON 编码开销
    assert elapsed < 50, f"realtime 端点耗时 {elapsed:.2f}ms > 50ms"
