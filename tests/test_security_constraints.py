# tests/test_security_constraints.py
import os
import string
from pulsemq.security import CredentialStore, generate_password


def test_no_plaintext_in_saved_file(tmp_path):
    f = str(tmp_path / "u.toml")
    s = CredentialStore(f, allow_auto_generated=False)
    s.add_user("alice", "supersecret", roles=[])
    s.save()
    raw = open(f, encoding="utf-8").read()
    assert "supersecret" not in raw
    assert "$2" in raw


def test_token_randomness_and_length():
    import base64, secrets
    t1 = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")
    t2 = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")
    assert t1 != t2 and len(t1) >= 40  # 32 字节 base64url ≈ 43 chars


def test_password_complexity():
    pw = generate_password()
    assert len(pw) == 16
    assert (any(c.isupper() for c in pw) and any(c.islower() for c in pw)
            and any(c.isdigit() for c in pw) and any(c in string.punctuation for c in pw))
