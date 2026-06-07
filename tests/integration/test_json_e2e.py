"""DataFrame JSON 端到端测试。

测:
- DataFrame 默认走 json (不传 format)
- DataFrame 显式指定 msgpack 仍可用 (向后兼容)
- DataFrame 显式指定 pyarrow 仍可用
"""
from __future__ import annotations

import asyncio
import sys

import pandas as pd
import pytest

# Windows 上 pyzmq 不兼容 ProactorEventLoop
if sys.platform == "win32":
    from pulsemq.event_loop import install_event_loop

    install_event_loop(use_uvloop=False)

from pulsemq.client.async_client import PulseClient


@pytest.mark.asyncio
async def test_dataframe_json_default(server_subprocess, port_pair):
    """DataFrame 默认走 json。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    received = []

    async def pub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            await asyncio.sleep(0.5)
            df = pd.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "charlie"]})
            await c.publish("test.json.df", df)  # 不指定 format, 默认 json

    async def sub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            async for msg in c.subscribe("test.json.>"):
                received.append(msg.payload)
                if len(received) >= 1:
                    return

    await asyncio.wait_for(asyncio.gather(pub(), sub()), timeout=15)
    assert received[0] == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 3, "name": "charlie"},
    ]


@pytest.mark.asyncio
async def test_dataframe_explicit_msgpack(server_subprocess, port_pair):
    """DataFrame 显式指定 msgpack 仍可用 (向后兼容)。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    received = []

    async def pub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            await asyncio.sleep(0.5)
            df = pd.DataFrame({"x": [42]})
            await c.publish("test.mp.df", df, format="msgpack")

    async def sub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            async for msg in c.subscribe("test.mp.>"):
                received.append(msg.payload)
                if len(received) >= 1:
                    return

    await asyncio.wait_for(asyncio.gather(pub(), sub()), timeout=15)
    assert received[0] == [{"x": 42}]


@pytest.mark.asyncio
async def test_dataframe_explicit_pyarrow(server_subprocess, port_pair):
    """DataFrame 显式指定 pyarrow 仍可用 (向后兼容)。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    received = []

    async def pub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            await asyncio.sleep(0.5)
            df = pd.DataFrame({"y": [1, 2, 3]})
            await c.publish("test.pa.df", df, format="pyarrow")

    async def sub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            async for msg in c.subscribe("test.pa.>"):
                received.append(msg.payload)
                if len(received) >= 1:
                    return

    await asyncio.wait_for(asyncio.gather(pub(), sub()), timeout=15)
    # pyarrow 反序列化返回 pa.Table, 还原为 DataFrame 后比对
    assert hasattr(received[0], "to_pandas")
    pd.testing.assert_frame_equal(
        received[0].to_pandas(), pd.DataFrame({"y": [1, 2, 3]})
    )


@pytest.mark.asyncio
async def test_dataframe_json_with_compression(server_subprocess, port_pair):
    """DataFrame + json + lz4 压缩端到端。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    received = []

    async def pub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            await asyncio.sleep(0.5)
            df = pd.DataFrame({"a": list(range(10)), "b": [f"x{i}" for i in range(10)]})
            await c.publish("test.json.cmp.row", df, compression="lz4")

    async def sub():
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            async for msg in c.subscribe("test.json.cmp.>"):
                received.append(msg.payload)
                if len(received) >= 1:
                    return

    await asyncio.wait_for(asyncio.gather(pub(), sub()), timeout=15)
    assert received[0] == [
        {"a": i, "b": f"x{i}"} for i in range(10)
    ]
