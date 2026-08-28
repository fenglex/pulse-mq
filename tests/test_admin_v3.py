"""Spec 3 Task 6: AdminServer 独立线程 + /clients /events 路由 + realtime 扩展。

- `test_admin_server_routes_directly`: 纯 AdminServer 单测（不经 Server）——Task 6 即 GREEN。
- 三个 Server 集成测试（依赖 Task 8 把 connection_stats/latency_stats 接线进 Server）——
  在 Task 8 之前会失败，标记为 `pytest.mark.skip(reason="Task 8 未接线")`。
  Task 8 接线后移除 skip 即可转 GREEN。
"""
from __future__ import annotations

import asyncio
import socket as _sock

from pulsemq.stats.connections import ConnectionStats
from pulsemq.stats.latency import LatencyStats


def _port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _get(port: int, path: str, token: str | None = None,
               timeout: float = 3.0) -> str:
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection("127.0.0.1", port), timeout=timeout
    )
    h = f"Authorization: Bearer {token}\r\n" if token else ""
    writer.write(
        f"GET {path} HTTP/1.1\r\nHost: x\r\n{h}Connection: close\r\n\r\n".encode()
    )
    await writer.drain()
    data = await asyncio.wait_for(reader.read(), timeout=timeout)
    writer.close()
    return data.decode(errors="replace")


# ---- Task 6 即可跑通的纯 AdminServer 单测 ----


async def test_admin_server_routes_directly():
    """直接构造 AdminServer（admin_thread=False，内联 loop）验证新路由 + realtime 字段。"""
    from pulsemq.admin.auth import TokenAuth
    from pulsemq.admin.server import AdminServer

    # registry_snapshot_fn 提供在线 client 数据（online_clients 的数据源）；
    # on_connect 仅追加生命周期事件，供 /api/v1/events 检验。
    registry = {
        "clients": [
            {
                "client_id": "c1", "username": "alice", "endpoint": "tcp://127.0.0.1:5555",
                "roles": ["sub"], "topics": ["t.*"], "connected_at": 1_700_000_000.0,
            }
        ]
    }
    cs = ConnectionStats(lambda: registry, ring_size=10)
    cs.on_connect("c1", "alice", "tcp://127.0.0.1:5555", "consumer")
    ls = LatencyStats(sample_rate=1.0)
    ls.record(100_000)  # 0.1ms

    adm = AdminServer(
        bind="127.0.0.1:0",  # 0 端口：OS 分配
        token_auth=TokenAuth("T"),
        connection_stats=cs,
        latency_stats=ls,
        admin_thread=False,
    )
    await adm.start()
    try:
        # 0 端口 → OS 实际分配端口
        port = adm._server.sockets[0].getsockname()[1]
        # /api/v1/clients：401 无 token / 200 带 token + 含 alice
        assert "401" in await _get(port, "/api/v1/clients")
        clients_resp = await _get(port, "/api/v1/clients", token="T")
        assert "200" in clients_resp
        assert "alice" in clients_resp
        # /api/v1/events：401 无 token / 200 带 token + 含 alice（事件消息）
        assert "401" in await _get(port, "/api/v1/events")
        events_resp = await _get(port, "/api/v1/events", token="T")
        assert "200" in events_resp
        assert "alice" in events_resp
        # realtime：含 latency_p50_ms + online_users
        rt = await _get(port, "/api/v1/stats/realtime", token="T")
        assert "200" in rt
        assert "latency_half" in rt
        assert "online_users" in rt
    finally:
        await adm.stop()


async def test_admin_thread_mode_serves_on_independent_loop():
    """admin_thread=True：start() 返回时端口已可访问，stop() 干净退出。"""
    from pulsemq.admin.auth import TokenAuth
    from pulsemq.admin.server import AdminServer

    adm = AdminServer(
        bind="127.0.0.1:0",
        token_auth=TokenAuth("T"),
        admin_thread=True,
    )
    await adm.start()
    try:
        port = adm._server.sockets[0].getsockname()[1]
        # healthz 公开
        assert "200" in await _get(port, "/healthz")
    finally:
        await adm.stop()
    # 停止后线程应已退出
    assert adm._thread is None or not adm._thread.is_alive()


async def test_admin_startup_logs_token_url_when_enabled():
    """token 启用时，启动日志应额外打一条带 token 的可点击 URL。

    回归：原启动日志只打 ``AdminServer 启动: http://0.0.0.0:9090``，token 启用
    时用户无法直接点进监控面板（需手动拼 ``?token=``）。
    """
    from io import StringIO
    from loguru import logger
    from pulsemq.admin.auth import TokenAuth
    from pulsemq.admin.server import AdminServer

    buf = StringIO()
    handle = logger.add(buf, level="INFO", format="{level}|{message}", catch=False)

    adm = AdminServer(
        bind="127.0.0.1:0",
        token_auth=TokenAuth("MySecretTok"),
        admin_thread=False,
    )
    try:
        await adm.start()
    finally:
        await adm.stop()
        logger.remove(handle)

    log_text = buf.getvalue()
    # 启动日志必须含一条带 ?token=MySecretTok 的 URL
    assert "?token=MySecretTok" in log_text, log_text


async def test_admin_startup_no_token_url_when_disabled():
    """token 禁用时（空串），启动日志不应出现 ?token=。"""
    from io import StringIO
    from loguru import logger
    from pulsemq.admin.auth import TokenAuth
    from pulsemq.admin.server import AdminServer

    buf = StringIO()
    handle = logger.add(buf, level="INFO", format="{level}|{message}", catch=False)

    adm = AdminServer(
        bind="127.0.0.1:0",
        token_auth=TokenAuth(""),  # 禁用
        admin_thread=False,
    )
    try:
        await adm.start()
    finally:
        await adm.stop()
        logger.remove(handle)

    log_text = buf.getvalue()
    assert "?token=" not in log_text, log_text


# ---- Server 集成测试：Task 8 接线后转 GREEN ----


async def test_admin_clients_and_events_routes_require_token():
    from pulsemq.server import Server

    dp, cp, ap = _port(), _port(), _port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"a": "b"},
        admin_token="T",
    )
    await srv.start()
    try:
        await asyncio.sleep(0.4)
        assert "401" in await _get(ap, "/api/v1/clients")
        assert "401" in await _get(ap, "/api/v1/events")
        assert "200" in await _get(ap, "/api/v1/clients", token="T")
        assert "200" in await _get(ap, "/api/v1/events", token="T")
    finally:
        await srv.stop()


async def test_realtime_has_latency_and_counters():
    from pulsemq.server import Server

    dp, cp, ap = _port(), _port(), _port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"a": "b"},
        admin_token="T",
    )
    await srv.start()
    try:
        await asyncio.sleep(0.3)
        resp = await _get(ap, "/api/v1/stats/realtime", token="T")
        assert "latency_half" in resp and "online_users" in resp
    finally:
        await srv.stop()


async def test_admin_runs_on_independent_thread():
    from pulsemq.client import ConsumerClient, ProducerClient
    from pulsemq.server import Server

    dp, cp, ap = _port(), _port(), _port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"p": "p", "c": "c"},
        admin_token="T",
    )
    await srv.start()
    try:
        cons = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "c", "c")
        prod = ProducerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "p", "p")
        await cons.start()
        await prod.start()
        got: list = []
        await cons.subscribe("t.*", lambda m: got.append(m.payload))
        await asyncio.sleep(0.3)

        async def _poll():
            for _ in range(5):
                await _get(ap, "/api/v1/stats/realtime", token="T", timeout=2.0)
                await asyncio.sleep(0.05)

        await asyncio.gather(_poll(), prod.publish("t.x", {"k": 1}))
        await asyncio.sleep(0.5)
        assert got == [{"k": 1}]
        await cons.stop()
        await prod.stop()
    finally:
        await srv.stop()
