# tests/test_zap_resilience.py
"""ZAP/PLAIN 认证韧性测试（Task 15 重写）。

服务器在线但客户端密码错误 → ZAP 拒绝握手 →
client.py 的 monitor-based 启动检测抛 ``AuthenticationError(reason="invalid_password")``。

这是 Task 9 删除的旧 ``test_zap_resilience.py``（导入旧 zmq_pub）的替代，
针对新 Client/Server 模型重写。复用 ``test_client_lifecycle.py`` 的端口/server 模式。
"""
from __future__ import annotations

import asyncio
import socket as _sock

import pytest

from pulsemq.client import ConsumerClient
from pulsemq.errors import AuthenticationError
from pulsemq.server import Server


def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _start_server(creds: dict[str, str]) -> tuple[Server, int, int]:
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials=creds,
    )
    await srv.start()
    await asyncio.sleep(0.2)
    return srv, dp, cp


async def test_auth_failure_on_wrong_password():
    """服务器凭据 ``alice:right``；client 用 ``WRONG`` → AuthenticationError。"""
    srv, dp, cp = await _start_server({"alice": "right"})
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
