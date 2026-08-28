"""Task 6：Server 凭据源走 CredentialStore+PlainAuth（默认生成 + SIGHUP reload）。"""
import asyncio
import socket as _sock

import pytest

from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.security import CredentialStore
from pulsemq.server import Server


def _free_port():
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


async def test_server_auth_via_bcrypt_credentialstore(tmp_path):
    f = str(tmp_path / "users.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    store.add_user("alice", "secret", roles=["subscriber"])
    store.add_user("bob", "pw", roles=["publisher"])
    store.save()
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}",
                 control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}",
                 credentials_file=f)
    await srv.start()
    try:
        c = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "alice", "secret")
        p = ProducerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "bob", "pw")
        await c.start(); await p.start()
        got = []
        await c.subscribe("t.*", lambda m: got.append(m.payload))
        await asyncio.sleep(0.3)
        await p.publish("t.x", {"k": 1})
        await asyncio.sleep(0.5)
        assert got == [{"k": 1}]
        await c.stop(); await p.stop()
    finally:
        await srv.stop()


async def test_server_auto_generates_default_admin(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("PULSEMQ_ADMIN_PASSWORD", raising=False)
    f = str(tmp_path / "auto.toml")
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}",
                 control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}",
                 credentials_file=f, allow_auto_generated=True)
    await srv.start()
    try:
        # 自动生成：文件已写回（哈希），日志含明文密码
        import os
        assert os.path.exists(f)
        captured = capsys.readouterr()
        # 自动生成的 admin 凭据能认证
        assert srv.generated_admin_password is not None
        c = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
                           "admin", srv.generated_admin_password)
        await c.start()
        await c.stop()
    finally:
        await srv.stop()


async def test_server_reload_credentials(tmp_path):
    f = str(tmp_path / "users.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    store.add_user("alice", "pw", roles=[])
    store.save()
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}",
                 control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}",
                 credentials_file=f)
    await srv.start()
    try:
        # CLI 风格：另一 store 加用户并 save
        cli = CredentialStore(f, allow_auto_generated=False)
        cli.load()
        cli.add_user("carol", "pw2", roles=[])
        cli.save()
        srv.reload_credentials()
        assert srv._auth.authenticate("carol", "pw2").success is True
    finally:
        await srv.stop()
