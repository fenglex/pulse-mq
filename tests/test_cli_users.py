import pytest
from pulsemq.cli.users import main
from pulsemq.security import CredentialStore


def test_cli_reload_no_pid_returns_nonzero(monkeypatch):
    monkeypatch.delenv("PULSEMQ_PID", raising=False)
    assert main(["reload"]) != 0  # 无 PID → 非零


def test_cli_reload_sends_sighup_posix(monkeypatch):
    import os
    import signal

    if os.name != "posix":
        pytest.skip("SIGHUP is POSIX-only")
    monkeypatch.setenv("PULSEMQ_PID", "4242")
    calls = []
    monkeypatch.setattr("pulsemq.cli.users.os.kill", lambda pid, sig: calls.append((pid, sig)))
    assert main(["reload"]) == 0
    assert calls == [(4242, signal.SIGHUP)]


def test_cli_add_list_disable_enable(tmp_path, capsys):
    f = str(tmp_path / "u.toml")
    assert main(["add", "alice", "--password", "pw", "--roles", "pub,sub",
                 "--file", f]) == 0
    assert main(["add", "bob", "--password", "pw2", "--file", f]) == 0
    # 重复 add 报错（退出码 6）
    assert main(["add", "alice", "--password", "x", "--file", f]) == 6
    out = capsys.readouterr().out
    assert main(["list", "--file", f]) == 0
    out = capsys.readouterr().out
    assert "alice" in out and "bob" in out
    assert main(["disable", "bob", "--file", f]) == 0
    s = CredentialStore(f, allow_auto_generated=False); s.load()
    assert s.verify("bob", "pw2").reason == "user_disabled"
    assert main(["enable", "bob", "--file", f]) == 0
    s = CredentialStore(f, allow_auto_generated=False); s.load()
    assert s.verify("bob", "pw2").success is True


def test_cli_passwd(tmp_path):
    f = str(tmp_path / "u.toml")
    main(["add", "alice", "--password", "old", "--file", f])
    assert main(["passwd", "alice", "--password", "new", "--file", f]) == 0
    s = CredentialStore(f, allow_auto_generated=False); s.load()
    assert s.verify("alice", "new").success is True
    assert s.verify("alice", "old").success is False
