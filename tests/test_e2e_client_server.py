# tests/test_e2e_client_server.py
"""End-to-end Client/Server 验证测试（Task 15）。

覆盖 Spec 1 §12 的两条消息流：
- 多 producer 扇入单 consumer（前缀订阅 ``market.*``）。
- 单用户单在线：同名 consumer 第二次 REGISTER 被拒，首个连接保持。

复用 ``tests/test_client_lifecycle.py`` 的端口/server-fixture 模式。
"""
from __future__ import annotations

import asyncio
import socket as _sock

import pytest

from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.errors import ClientStartupError
from pulsemq.server import Server


def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _start_server(creds: dict[str, str]) -> tuple[Server, int, int]:
    """启动 Server 并返回 (srv, data_port, control_port)。"""
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials=creds,
    )
    await srv.start()
    # 给 ZAP/ROUTER bind 一点时间稳定下来。
    await asyncio.sleep(0.2)
    return srv, dp, cp


async def test_multi_producer_single_consumer():
    """1 consumer 订阅 ``market.*``；2 producer 各发一条 → consumer 收到两条。"""
    srv, dp, cp = await _start_server({"c": "c", "p1": "p", "p2": "p"})
    try:
        c = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="c",
            password="c",
        )
        p1 = ProducerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="p1",
            password="p",
        )
        p2 = ProducerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="p2",
            password="p",
        )
        await c.start()
        await p1.start()
        await p2.start()
        got: list[str] = []
        await c.subscribe("market.*", lambda m: got.append(m.topic))
        # 让 SUBSCRIBE 控制帧被服务端处理并写入路由表。
        await asyncio.sleep(0.3)
        await p1.publish("market.stock.a", {"x": 1})
        await p2.publish("market.bond.b", {"x": 2})
        # 等数据帧被转发并由 recv_loop 投递到回调。
        await asyncio.sleep(0.5)
        assert sorted(got) == ["market.bond.b", "market.stock.a"]
        await c.stop()
        await p1.stop()
        await p2.stop()
    finally:
        await srv.stop()


async def test_single_user_single_online():
    """同名 consumer 第二次 REGISTER 被拒（ALREADY_ONLINE）；首个连接保持。"""
    srv, dp, cp = await _start_server({"alice": "s"})
    try:
        c1 = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="alice",
            password="s",
        )
        await c1.start()
        c2 = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="alice",
            password="s",
        )
        with pytest.raises(ClientStartupError):
            await c2.start()
        # c1 必须仍然在线：能正常订阅并停机。
        await c1.subscribe("t.*", lambda m: None)
        await c1.stop()
    finally:
        await srv.stop()
