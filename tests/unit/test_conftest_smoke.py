"""验证 conftest 的 server_subprocess fixture 可用。"""

from __future__ import annotations

import pytest

from pulsemq.client.async_client import PulseClient


@pytest.mark.asyncio
async def test_server_subprocess_alive(server_subprocess, port_pair):
    """server_subprocess 启动后能正常接收连接。"""
    port, _ = port_pair
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    async with PulseClient(
        address=address, xpub_address=xpub, auto_reconnect=False
    ) as c:
        assert c._dealer is not None
        assert c._sub is not None
