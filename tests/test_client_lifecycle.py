# tests/test_client_lifecycle.py
"""Client 生命周期测试：publish/subscribe roundtrip、auth 失败、server 宕机。

Task 12 的 monitor-based startup 设计：
- handshake_ok → 认证通过，继续 REGISTER；
- auth_failed → AuthenticationError (exit 3)；
- 超时/其他 → ClientStartupError (exit 4)。
"""
from __future__ import annotations

import asyncio
import socket as _sock

import pytest

from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.errors import AuthenticationError, ClientStartupError
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


async def test_publish_subscribe_roundtrip():
    srv, dp, cp = await _start_server({"alice": "s", "bob": "p"})
    try:
        c = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="alice",
            password="s",
        )
        p = ProducerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="bob",
            password="p",
        )
        await c.start()
        await p.start()
        got: list = []
        await c.subscribe("market.stock.*", lambda m: got.append(m))
        # 让 SUBSCRIBE 控制帧被服务端处理并写入路由表。
        await asyncio.sleep(0.3)
        await p.publish("market.stock.600000", {"price": 12.3})
        # 等数据帧被转发并由 recv_loop 投递到回调。
        await asyncio.sleep(0.5)
        assert len(got) == 1, f"expected 1 msg, got {len(got)}"
        assert got[0].payload == {"price": 12.3}
        assert got[0].topic == "market.stock.600000"
        await c.stop()
        await p.stop()
    finally:
        await srv.stop()


async def test_auth_failure_exits():
    # 服务器在线，但密码错误 → AuthenticationError。
    srv, dp, cp = await _start_server({"alice": "s"})
    try:
        c = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="alice",
            password="WRONG",
        )
        with pytest.raises(AuthenticationError) as ei:
            await c.start()
        assert ei.value.reason == "invalid_password"
    finally:
        await srv.stop()


async def test_connect_failure_when_server_down():
    # 没有服务器在监听 → 握手不会发生 → 超时 → ClientStartupError。
    dp = _free_port()
    cp = _free_port()
    c = ConsumerClient(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        username="ghost",
        password="b",
    )
    with pytest.raises(ClientStartupError) as ei:
        await c.start()
    assert ei.value.reason == "CONNECT_FAILED"


async def test_producer_rejects_subscribe():
    """ProducerClient.subscribe 抛 NotImplementedError。"""
    p = ProducerClient(
        data_endpoint="tcp://127.0.0.1:1",
        control_endpoint="tcp://127.0.0.1:2",
        username="x",
        password="y",
    )
    with pytest.raises(NotImplementedError):
        await p.subscribe("t", lambda m: None)


async def test_consumer_rejects_publish():
    """ConsumerClient.publish 抛 NotImplementedError。"""
    c = ConsumerClient(
        data_endpoint="tcp://127.0.0.1:1",
        control_endpoint="tcp://127.0.0.1:2",
        username="x",
        password="y",
    )
    with pytest.raises(NotImplementedError):
        await c.publish("t", {"a": 1})
