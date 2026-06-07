"""engine + transport 集成测试。

启一个真实 server 子进程, 跑 client pub/sub, 验证 end-to-end 行为。
"""
from __future__ import annotations

import asyncio

import pytest

from pulsemq.client.async_client import PulseClient


@pytest.mark.asyncio
async def test_pubsub_roundtrip_str(server_subprocess, port_pair):
    """str 消息端到端。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    received = []

    async def publisher():
        async with PulseClient(
            address=address, xpub_address=xpub, auto_reconnect=False
        ) as c:
            await asyncio.sleep(0.2)  # 等 subscriber
            await c.publish("test.it.0", "hello-世界", compression="none")

    async def subscriber():
        async with PulseClient(
            address=address, xpub_address=xpub, auto_reconnect=False
        ) as c:
            async for msg in c.subscribe("test.it.>"):
                received.append(msg.payload)
                if len(received) >= 1:
                    return

    await asyncio.wait_for(
        asyncio.gather(publisher(), subscriber()), timeout=10
    )
    assert received == ["hello-世界"]


@pytest.mark.asyncio
async def test_pubsub_concurrent_publishers(server_subprocess, port_pair):
    """多个 publisher 并发发, 单 subscriber 收。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    received = []

    async def publisher(idx: int):
        async with PulseClient(
            address=address, xpub_address=xpub, auto_reconnect=False
        ) as c:
            await asyncio.sleep(0.3)  # 等 subscriber 完成 setsockopt + SUB 注册
            for j in range(5):
                await c.publish(f"test.con.{idx}", f"msg-{idx}-{j}")
                await asyncio.sleep(0.01)

    async def subscriber():
        async with PulseClient(
            address=address, xpub_address=xpub, auto_reconnect=False
        ) as c:
            async for msg in c.subscribe("test.con.>"):
                received.append(msg.payload)
                if len(received) >= 10:  # 2 pub × 5
                    return

    await asyncio.wait_for(
        asyncio.gather(publisher(0), publisher(1), subscriber()), timeout=15
    )
    assert len(received) == 10
