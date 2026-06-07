"""MetricsHTTPServer 单测。

覆盖:
- /api/v1/metrics/realtime 端点返回 200 + JSON
- /healthz 端点返回 200 + JSON
- 未知路径返回 404 + JSON
- Content-Type 头为 application/json
- snapshot_fn 为 None 时返回 error JSON
- HTTP 响应 content-length 与 body 一致
- JSON 字段能 roundtrip

MetricsHTTPServer 是独立 asyncio server, 启在随机空闲端口, 客户端用
asyncio.open_connection 直接发 HTTP, 不依赖外部测试 server。
"""

from __future__ import annotations

import asyncio
import json
import socket

import pytest

from pulsemq.monitoring.api import MetricsHTTPServer


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def _http_get(host: str, port: int, path: str) -> tuple[int, dict, str]:
    """发简单 HTTP GET, 返回 (status_code, headers, body)。"""
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(
            f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\nConnection: close\r\n\r\n".encode()
        )
        await writer.drain()
        # 读所有响应
        data = await asyncio.wait_for(reader.read(), timeout=5.0)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

    # 解析 HTTP/1.1 响应
    text = data.decode("utf-8", errors="ignore")
    head, _, body = text.partition("\r\n\r\n")
    status_line, *header_lines = head.split("\r\n")
    # "HTTP/1.1 200 OK"
    parts = status_line.split(" ", 2)
    status_code = int(parts[1])
    headers: dict[str, str] = {}
    for line in header_lines:
        if ":" in line:
            k, _, v = line.partition(":")
            headers[k.strip().lower()] = v.strip()
    return status_code, headers, body


# ---- 实时指标端点 ----


@pytest.mark.asyncio
async def test_realtime_endpoint_returns_200_and_json():
    """/api/v1/metrics/realtime 应返回 200 + JSON, 字段来自 snapshot_fn。"""
    snapshot = {
        "timestamp": 1234.5,
        "msg_rate": 1.0,
        "active_connections": 2,
    }
    port = _free_port()
    server = MetricsHTTPServer(
        bind=f"127.0.0.1:{port}",
        snapshot_fn=lambda: dict(snapshot),
    )
    await server.start()
    try:
        status, headers, body = await _http_get("127.0.0.1", port, "/api/v1/metrics/realtime")
        assert status == 200
        assert headers.get("content-type", "").startswith("application/json")
        parsed = json.loads(body)
        assert parsed["msg_rate"] == 1.0
        assert parsed["active_connections"] == 2
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_realtime_endpoint_includes_content_length():
    """HTTP 响应 Content-Length 头应与 body 字节长度一致。"""
    port = _free_port()
    server = MetricsHTTPServer(
        bind=f"127.0.0.1:{port}",
        snapshot_fn=lambda: {"k": "v"},
    )
    await server.start()
    try:
        status, headers, body = await _http_get(
            "127.0.0.1", port, "/api/v1/metrics/realtime"
        )
        assert status == 200
        assert int(headers["content-length"]) == len(body.encode("utf-8"))
    finally:
        await server.stop()


# ---- healthz ----


@pytest.mark.asyncio
async def test_healthz_returns_ok():
    """/healthz 应返回 200 + {"status": "ok"}。"""
    port = _free_port()
    server = MetricsHTTPServer(
        bind=f"127.0.0.1:{port}",
        snapshot_fn=lambda: {"ignored": True},
    )
    await server.start()
    try:
        status, headers, body = await _http_get("127.0.0.1", port, "/healthz")
        assert status == 200
        assert headers.get("content-type", "").startswith("application/json")
        assert json.loads(body) == {"status": "ok"}
    finally:
        await server.stop()


# ---- 未知路径 ----


@pytest.mark.asyncio
async def test_unknown_path_returns_404():
    """未知路径 → 404 + JSON。"""
    port = _free_port()
    server = MetricsHTTPServer(
        bind=f"127.0.0.1:{port}",
        snapshot_fn=lambda: {},
    )
    await server.start()
    try:
        status, headers, body = await _http_get("127.0.0.1", port, "/no/such/path")
        assert status == 404
        assert headers.get("content-type", "").startswith("application/json")
        assert json.loads(body) == {"error": "not found"}
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_root_path_returns_404():
    """/ (根) 不在白名单, 也应 404。"""
    port = _free_port()
    server = MetricsHTTPServer(
        bind=f"127.0.0.1:{port}",
        snapshot_fn=lambda: {},
    )
    await server.start()
    try:
        status, _, body = await _http_get("127.0.0.1", port, "/")
        assert status == 404
        assert json.loads(body) == {"error": "not found"}
    finally:
        await server.stop()


# ---- snapshot_fn 为 None ----


@pytest.mark.asyncio
async def test_realtime_without_snapshot_fn_returns_error():
    """snapshot_fn=None → 返回 error JSON (实现细节: {"error": "..."} 结构)。"""
    port = _free_port()
    server = MetricsHTTPServer(
        bind=f"127.0.0.1:{port}",
        snapshot_fn=None,
    )
    await server.start()
    try:
        status, _, body = await _http_get("127.0.0.1", port, "/api/v1/metrics/realtime")
        # 仍返回 200 (端点存在), 但 body 标记 metrics 不可用
        assert status == 200
        parsed = json.loads(body)
        assert "error" in parsed
    finally:
        await server.stop()


# ---- 多次请求 ----


@pytest.mark.asyncio
async def test_multiple_sequential_requests():
    """同一 server 可服务多个连续请求 (server 不自毁)。"""
    counter = {"n": 0}

    def _snapshot() -> dict:
        counter["n"] += 1
        return {"n": counter["n"]}

    port = _free_port()
    server = MetricsHTTPServer(
        bind=f"127.0.0.1:{port}",
        snapshot_fn=_snapshot,
    )
    await server.start()
    try:
        for _ in range(3):
            status, _, body = await _http_get(
                "127.0.0.1", port, "/api/v1/metrics/realtime"
            )
            assert status == 200
        assert counter["n"] == 3
    finally:
        await server.stop()


# ---- stop 幂等 ----


@pytest.mark.asyncio
async def test_stop_without_start_is_safe():
    """未 start 直接 stop: 不抛异常。"""
    server = MetricsHTTPServer(
        bind=f"127.0.0.1:{_free_port()}",
        snapshot_fn=lambda: {},
    )
    await server.stop()  # 不应抛
    assert server._server is None
