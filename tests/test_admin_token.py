import asyncio
import socket as _sock

import pytest

from pulsemq.admin.auth import TokenAuth


def test_tokenauth_disabled_when_none():
    ta = TokenAuth(None)
    assert ta.enabled is False
    assert ta.validate({}, {}) is True  # 禁用时一律放行


def test_tokenauth_query_and_header():
    ta = TokenAuth("s3cret")
    assert ta.enabled is True
    assert ta.validate({}, {"token": ["s3cret"]}) is True
    assert ta.validate({"authorization": "Bearer s3cret"}, {}) is True
    assert ta.validate({}, {}) is False
    assert ta.validate({"authorization": "Bearer wrong"}, {}) is False
    assert ta.validate({}, {"token": ["wrong"]}) is False


# ---- 集成测试（Task 8 解锁）：Server 端 admin token 解析 + AdminServer 强制 ----


def _port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _get(port: int, path: str, token: str | None = None, timeout: float = 3.0) -> str:
    """发一次 HTTP GET，返回响应文本（含状态行）。token 自动加到 ?token=。"""
    if token is not None:
        sep = "&" if "?" in path else "?"
        path = f"{path}{sep}token={token}"
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection("127.0.0.1", port), timeout=timeout
    )
    writer.write(
        f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n".encode()
    )
    await writer.drain()
    data = await asyncio.wait_for(reader.read(), timeout=timeout)
    writer.close()
    return data.decode(errors="replace")


async def test_admin_healthz_open_others_require_token():
    """显式 admin_token=TOK：/healthz 公开，其余路由需 token。"""
    from pulsemq.server import Server

    dp, cp, ap = _port(), _port(), _port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"a": "b"},
        admin_token="TOK",
    )
    await srv.start()
    try:
        await asyncio.sleep(0.3)  # admin server warmup
        # healthz 不需要 token
        assert "200" in await _get(ap, "/healthz")
        # /api/v1/stats/realtime 无 token → 401
        assert "401" in await _get(ap, "/api/v1/stats/realtime")
        # 带正确 token → 200
        assert "200" in await _get(ap, "/api/v1/stats/realtime", token="TOK")
        # 带错误 token → 401
        assert "401" in await _get(ap, "/api/v1/stats/realtime", token="wrong")
    finally:
        await srv.stop()


async def test_server_random_admin_token_written_to_file(tmp_path, monkeypatch):
    """admin_token=None 且无 config/env 时：随机生成并写入 admin_token_file（0600）。"""
    import os

    from pulsemq.server import Server

    monkeypatch.delenv("PULSEMQ_ADMIN_TOKEN", raising=False)
    tok_file = str(tmp_path / "admin.token")
    dp, cp, ap = _port(), _port(), _port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"a": "b"},
        admin_token_file=tok_file,
    )  # admin_token=None → 随机生成
    await srv.start()
    try:
        assert os.path.exists(tok_file)
        tok = open(tok_file).read().strip()
        # 随机 token 能用
        assert "200" in await _get(ap, "/api/v1/stats/realtime", token=tok)
        assert "401" in await _get(ap, "/api/v1/stats/realtime", token="wrong")
    finally:
        await srv.stop()


def test_token_file_mode_0600_posix(tmp_path, monkeypatch):
    """Spec §11.4：随机生成的 token 文件权限必须为 0600（POSIX）。"""
    import os
    import stat

    if os.name != "posix":
        pytest.skip("0600 is POSIX-only")

    from pulsemq.server import Server

    monkeypatch.delenv("PULSEMQ_ADMIN_TOKEN", raising=False)
    tok_file = str(tmp_path / "admin.token")
    dp, cp, ap = _port(), _port(), _port()
    # 仅构造 Server：__init__ 内 _resolve_admin_token 已写盘，无需 start()。
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"a": "b"},
        admin_token_file=tok_file,
    )
    assert os.path.exists(tok_file)
    mode = stat.S_IMODE(os.stat(tok_file).st_mode)
    assert mode == 0o600


def test_non_bcrypt_hash_algo_warns(tmp_path, capsys):
    """Fix 3：非 bcrypt 的 hash_algo 应告警（不抛异常）。"""
    from pulsemq.security import CredentialStore

    f = str(tmp_path / "u.toml")
    CredentialStore(f, allow_auto_generated=False, hash_algo="argon2")
    err = capsys.readouterr().err
    # loguru 默认写 stderr；非严格匹配，含关键词即可。
    assert "argon2" in err and "bcrypt" in err
