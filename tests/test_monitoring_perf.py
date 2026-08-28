"""Spec 3 Task 8: Server 接线集成测试（延迟采样 + 连接/认证事件）。"""
from __future__ import annotations

import asyncio
import socket as _sock

from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.server import Server


def _port() -> int:
    s = _sock.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def test_latency_recorded_on_data_plane():
    dp, cp, ap = _port(), _port(), _port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"p": "p", "c": "c"},
        latency_sample_rate=1.0,
        admin_token="T",
    )
    await srv.start()
    try:
        prod = ProducerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "p", "p")
        cons = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "c", "c")
        await cons.start()
        await prod.start()
        await cons.subscribe("t.*", lambda m: None)
        await asyncio.sleep(0.3)
        for _ in range(20):
            await prod.publish("t.x", {"k": 1})
        await asyncio.sleep(0.5)
        snap = srv._lat_half.snapshot()
        assert "t.x" in snap  # topic 被记录
        assert snap["t.x"]["count"] > 0  # 采样次数 > 0
        assert snap["t.x"]["p50_ms"] >= 0.0  # 延迟被采
        await cons.stop()
        await prod.stop()
    finally:
        await srv.stop()


async def test_connection_events_emitted():
    dp, cp, ap = _port(), _port(), _port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"c": "c"},
        admin_token="T",
    )
    await srv.start()
    try:
        cons = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "c", "c")
        await cons.start()
        await asyncio.sleep(0.3)
        evts = srv._connections.recent_events(50)
        # 至少有 connect 事件（REGISTER）或 auth 事件
        assert any(e.type == "connect" for e in evts) or any(e.type == "auth" for e in evts)
        await cons.stop()
    finally:
        await srv.stop()
