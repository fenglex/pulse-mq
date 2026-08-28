import pytest
from pulsemq.auth import PlainAuth
from pulsemq.security import CredentialStore


def _store_with(tmp_path, **users):
    s = CredentialStore(str(tmp_path / "u.toml"), allow_auto_generated=False)
    for u, pw in users.items():
        s.add_user(u, pw, roles=[u])
    return s


def test_authenticate_success_and_reasons(tmp_path):
    auth = PlainAuth(_store_with(tmp_path, alice="pw"))
    r = auth.authenticate("alice", "pw")
    assert r.success is True and r.reason is None and "alice" in r.roles
    assert auth.authenticate("alice", "x").reason == "invalid_password"
    assert auth.authenticate("bob", "pw").reason == "user_not_found"


def test_verify_zap_compat(tmp_path):
    auth = PlainAuth(_store_with(tmp_path, alice="pw"))
    assert auth.verify("alice", "pw") == (True, None)
    assert auth.verify("alice", "x") == (False, "invalid_password")
    assert auth.verify("bob", "pw") == (False, "user_not_found")


def test_from_file_classmethod(tmp_path):
    f = str(tmp_path / "u.toml")
    s = CredentialStore(f, allow_auto_generated=False)
    s.add_user("alice", "pw", roles=[])
    s.save()
    auth = PlainAuth.from_file(f)
    assert auth.authenticate("alice", "pw").success is True
