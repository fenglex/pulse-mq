import pytest
from pulsemq.cli.users import main
from pulsemq.security import CredentialStore


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
