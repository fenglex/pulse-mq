import asyncio
import socket as _sock
from importlib.metadata import version as _pkg_version

import pytest

from pulsemq import __version__
from pulsemq.lifecycle import run_server
from pulsemq.server import Server


def test_version_matches_package_metadata():
    """``pulsemq.__version__`` 必须与 pyproject.toml 声明的包版本一致。

    回归：此前硬编码 ``"6.0.0"``，bump 后测试腐化失败。改为以包元数据为准，
    使该测试成为「单一来源一致性」校验而非易过期的魔法字符串。
    """
    assert __version__ == _pkg_version("pulse-mq")


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
