"""PulseClient.subscribe 行为单测。

注: 大部分场景需要真实 server, 这里用 server_subprocess fixture。
"""
import asyncio
import pytest
from pulsemq.client.async_client import PulseClient


@pytest.mark.asyncio
async def test_subscribe_exact_topic(server_subprocess, port_pair):
    """精确 topic 订阅。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    received = []

    async def pub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            await asyncio.sleep(0.2)
            await c.publish("test.s.0", "a")
            await c.publish("test.s.1", "b")  # 不订阅

    async def sub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            async for msg in c.subscribe("test.s.0"):
                received.append(msg.payload)
                return

    await asyncio.wait_for(asyncio.gather(pub(), sub()), timeout=10)
    assert received == ["a"]


@pytest.mark.asyncio
async def test_subscribe_wildcard(server_subprocess, port_pair):
    """通配符订阅。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    received = []

    async def pub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            await asyncio.sleep(0.2)
            await c.publish("test.w.a.0", "x")
            await c.publish("test.w.b.0", "y")
            await c.publish("other.topic", "z")  # 不订阅

    async def sub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            async for msg in c.subscribe("test.w.>"):
                received.append(msg.payload)
                if len(received) >= 2:
                    return

    await asyncio.wait_for(asyncio.gather(pub(), sub()), timeout=10)
    assert sorted(received) == ["x", "y"]


@pytest.mark.asyncio
async def test_ping_returns_dict(server_subprocess, port_pair):
    """ping() 返回 dict 含 client_ts 与 server_ts。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
        result = await c.ping()
        assert isinstance(result, dict)
        assert "client_ts" in result
        assert "server_ts" in result


@pytest.mark.asyncio
async def test_query_system_status(server_subprocess, port_pair):
    """query() 返回 system_status dict。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
        result = await c.query({"action": "system_status"})
        assert isinstance(result, dict)
