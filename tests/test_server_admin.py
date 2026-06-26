import asyncio
import socket as _sock

from pulsemq.server import Server


def _free_port():
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _http_get(host, port, path, timeout=3.0):
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=timeout
    )
    writer.write(
        f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n".encode()
    )
    await writer.drain()
    data = await asyncio.wait_for(reader.read(), timeout=timeout)
    writer.close()
    return data.decode(errors="replace")


async def test_admin_healthz_served():
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"a": "b"},
    )
    await srv.start()
    try:
        await asyncio.sleep(0.3)  # admin server warmup
        resp = await _http_get("127.0.0.1", ap, "/healthz")
        assert "200" in resp
        assert '"status": "ok"' in resp or '"status":"ok"' in resp
    finally:
        await srv.stop()


async def test_admin_realtime_snapshot_includes_online_clients():
    """snapshot_fn 透传 online_clients/subscriptions（Spec 1 §9.2）。"""
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"a": "b"},
    )
    await srv.start()
    try:
        await asyncio.sleep(0.3)
        resp = await _http_get("127.0.0.1", ap, "/api/v1/stats/realtime")
        assert "200" in resp
        assert "online_clients" in resp
        assert "subscriptions" in resp
    finally:
        await srv.stop()
