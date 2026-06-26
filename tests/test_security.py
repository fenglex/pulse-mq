import os
import secrets
import string
import pytest
from pulsemq.security import (CredentialStore, AuthResult, UserInfo,
                              generate_password)
from pulsemq.errors import ConfigurationError


def test_generate_password_complexity():
    pw = generate_password()
    assert len(pw) == 16
    assert any(c.isupper() for c in pw)
    assert any(c.islower() for c in pw)
    assert any(c.isdigit() for c in pw)
    assert any(c in string.punctuation for c in pw)
    # 随机性
    assert generate_password() != generate_password()


def test_hash_and_verify_roundtrip(tmp_path):
    f = str(tmp_path / "users.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    store.add_user("alice", "s3cret", roles=["subscriber"])
    assert store.verify("alice", "s3cret").success is True
    res = store.verify("alice", "wrong")
    assert res.success is False and res.reason == "invalid_password"
    res = store.verify("bob", "x")
    assert res.success is False and res.reason == "user_not_found"


def test_set_password_and_save_persists_hashed(tmp_path):
    f = str(tmp_path / "users.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    store.add_user("alice", "old", roles=[])
    store.set_password("alice", "new")
    store.save()
    raw = open(f, encoding="utf-8").read()
    assert "old" not in raw and "new" not in raw  # 明文不落盘
    assert "$2" in raw  # bcrypt 哈希特征
    # 重新加载仍能校验
    store2 = CredentialStore(f, allow_auto_generated=False)
    store2.load()
    assert store2.verify("alice", "new").success is True


def test_disable_yields_user_disabled(tmp_path):
    f = str(tmp_path / "users.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    store.add_user("alice", "pw", roles=[])
    store.set_enabled("alice", False)
    res = store.verify("alice", "pw")
    assert res.success is False and res.reason == "user_disabled"


def test_default_generation_three_priorities(tmp_path, monkeypatch):
    # 优先级 1：文件存在 → 加载，不生成
    f = str(tmp_path / "users.toml")
    s0 = CredentialStore(f, allow_auto_generated=False)
    s0.add_user("admin", "fixed", roles=["admin"])
    s0.save()
    s1 = CredentialStore(f, allow_auto_generated=True)
    pw = s1.load()
    assert pw is None  # 文件已存在，不输出明文
    assert s1.verify("admin", "fixed").success is True

    # 优先级 2：env PULSEMQ_ADMIN_PASSWORD
    f2 = str(tmp_path / "u2.toml")
    monkeypatch.setenv("PULSEMQ_ADMIN_PASSWORD", "envpw")
    s2 = CredentialStore(f2, allow_auto_generated=True)
    pw = s2.load()
    assert pw == "envpw"
    assert s2.verify("admin", "envpw").success is True

    # 优先级 3：随机生成
    f3 = str(tmp_path / "u3.toml")
    monkeypatch.delenv("PULSEMQ_ADMIN_PASSWORD", raising=False)
    s3 = CredentialStore(f3, allow_auto_generated=True)
    pw = s3.load()
    assert pw and len(pw) == 16
    assert s3.verify("admin", pw).success is True
    # 生成后写回文件，重启走优先级 1
    s4 = CredentialStore(f3, allow_auto_generated=True)
    assert s4.load() is None
    assert s4.verify("admin", pw).success is True


def test_allow_auto_false_no_file_raises(tmp_path):
    f = str(tmp_path / "none.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    with pytest.raises(ConfigurationError):
        store.load()


def test_reload_picks_up_changes(tmp_path):
    f = str(tmp_path / "users.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    store.add_user("alice", "pw", roles=[])
    store.save()
    # 另一个进程改文件
    other = CredentialStore(f, allow_auto_generated=False)
    other.load()
    other.add_user("bob", "pw2", roles=[])
    other.save()
    # 原 store reload 后能看到 bob
    store.reload()
    assert store.verify("bob", "pw2").success is True


def test_from_dict_in_memory():
    store = CredentialStore.from_dict({"alice": "pw"})
    assert store.verify("alice", "pw").success is True
    store.save()  # no-op，不抛
