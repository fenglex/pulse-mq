"""Finding 2：Server 加载 credentials_file TOML 白名单（Spec §1.3/§11.2）。"""
from __future__ import annotations

import asyncio
import socket as _sock

import pytest

from pulsemq.client import ConsumerClient
from pulsemq.config import ServerConfig
from pulsemq.errors import AuthenticationError, ClientStartupError
from pulsemq.server import Server


def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def test_credentials_file_loaded(tmp_path):
    """写入 [users] TOML，Server(credentials=None) 加载白名单。"""
    cred_path = tmp_path / "users.toml"
    cred_path.write_text(
        '[users]\nalice = "pw-alice"\nbob = "pw-bob"\n', encoding="utf-8"
    )
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    cfg = ServerConfig(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials_file=str(cred_path),
    )
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials=None,
        config=cfg,
    )
    await srv.start()
    try:
        await asyncio.sleep(0.2)
        # 文件中的用户能通过
        ok = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="alice",
            password="pw-alice",
        )
        await ok.start()
        await ok.stop()

        # 默认 admin:admin 不应通过（文件存在时不再回退默认）
        bad = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="admin",
            password="admin",
        )
        with pytest.raises((AuthenticationError, ClientStartupError)):
            await bad.start()
    finally:
        await srv.stop()


async def test_credentials_file_missing_falls_back_to_default(tmp_path):
    """文件缺失：回退 admin:admin 并告警。"""
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    cfg = ServerConfig(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials_file=str(tmp_path / "nope.toml"),
    )
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials=None,
        config=cfg,
    )
    await srv.start()
    try:
        await asyncio.sleep(0.2)
        c = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="admin",
            password="admin",
        )
        await c.start()
        await c.stop()
    finally:
        await srv.stop()
