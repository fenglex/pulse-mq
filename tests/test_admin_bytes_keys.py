"""Finding 1 回归：routing.snapshot() 字节键解码，admin /realtime JSON 可序列化。"""
from __future__ import annotations

import asyncio
import json
import socket as _sock

from pulsemq.client import ConsumerClient
from pulsemq.server import Server


def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _http_get(host: str, port: int, path: str, timeout: float = 3.0) -> str:
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


async def test_realtime_json_serializable_with_live_subscription():
    """订阅后 GET /api/v1/stats/realtime 必须返回可解析 JSON（含 subscriptions）。"""
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"c": "c"},
        admin_token="",  # Spec 2：禁用 token 校验，沿用 Spec 1 行为
    )
    await srv.start()
    try:
        await asyncio.sleep(0.2)
        c = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="c",
            password="c",
        )
        await c.start()
        await c.subscribe("market.*", lambda m: None)
        await asyncio.sleep(0.4)  # 让 SUBSCRIBE 帧写入 routing 表

        resp = await _http_get("127.0.0.1", ap, "/api/v1/stats/realtime")
        assert "200" in resp.split("\r\n")[0]
        # 取 body（最后一个空行后）
        body = resp.split("\r\n\r\n", 1)[1] if "\r\n\r\n" in resp else resp
        payload = json.loads(body)  # 不应抛 TypeError
        assert "subscriptions" in payload
        # 订阅键必须是 str（uuid-hex），不能是 bytes repr
        keys = list(payload["subscriptions"].keys())
        assert keys, "expected at least one subscription"
        for k in keys:
            assert isinstance(k, str)
            assert "b'" not in k
        await c.stop()
    finally:
        await srv.stop()
