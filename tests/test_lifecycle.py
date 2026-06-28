import asyncio
import socket as _sock

import pytest

from pulsemq import __version__
from pulsemq.lifecycle import run_server
from pulsemq.server import Server


def test_version_bumped():
    assert __version__ == "6.0.0"


def _fp():
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def test_run_server_handles_sigint(monkeypatch):
    # 不真发信号；直接测 run_server 在外部调用 server.stop() 后退出
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{_fp()}",
        control_endpoint=f"tcp://127.0.0.1:{_fp()}",
        admin_endpoint=f"127.0.0.1:{_fp()}",
        credentials={"a": "b"},
    )

    async def _stop_after(coro_srv):
        await asyncio.sleep(0.2)
        await coro_srv.stop()

    srv_task = asyncio.create_task(run_server(srv))
    asyncio.create_task(_stop_after(srv))
    await asyncio.wait_for(srv_task, timeout=3.0)  # 应正常返回
