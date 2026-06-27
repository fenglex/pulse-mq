"""PLAIN 认证决策器。本项目唯一支持且强制启用。委托 CredentialStore。"""
from __future__ import annotations

from pulsemq.security import AuthResult, CredentialStore


class PlainAuth:
    def __init__(self, store: CredentialStore) -> None:
        self._store = store

    @classmethod
    def from_file(cls, credentials_file: str) -> "PlainAuth":
        store = CredentialStore(credentials_file)
        store.load()
        return cls(store)

    def authenticate(self, username: str, password: str) -> AuthResult:
        return self._store.verify(username, password)

    def verify(self, username: str, password: str) -> tuple[bool, str | None]:
        """ZAP handler 兼容接口（与 Spec 1 PlainAuthDict.verify 同签名）。"""
        r = self._store.verify(username, password)
        return r.success, r.reason
