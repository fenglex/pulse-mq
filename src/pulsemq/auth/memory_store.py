"""内存鉴权存储：identity → AuthUser 映射。

ZAP 认证通过后写入，消息处理时读取，断线时清理。
单线程 event loop 访问，无需锁。
"""

from __future__ import annotations

from pulsemq.models import AuthUser


class AuthMemoryStore:
    """内存鉴权缓存（identity → AuthUser），纯内存，不持久化。"""

    def __init__(self) -> None:
        # identity → AuthUser
        self._identity_user: dict[bytes, AuthUser] = {}
        # user_id → set of identity（支持多连接）
        self._user_identities: dict[int, set[bytes]] = {}

    def register(self, identity: bytes, user: AuthUser) -> None:
        """ZAP 认证通过后注册。"""
        self._identity_user[identity] = user
        if user.user_id not in self._user_identities:
            self._user_identities[user.user_id] = set()
        self._user_identities[user.user_id].add(identity)

    def unregister(self, identity: bytes) -> AuthUser | None:
        """断线时清理。"""
        user = self._identity_user.pop(identity, None)
        if user is not None:
            idents = self._user_identities.get(user.user_id)
            if idents is not None:
                idents.discard(identity)
        return user

    def get_user(self, identity: bytes) -> AuthUser | None:
        return self._identity_user.get(identity)

    def connection_count(self, user_id: int) -> int:
        """获取某用户的当前连接数。"""
        return len(self._user_identities.get(user_id, set()))

    def clear(self) -> None:
        """清空所有缓存。"""
        self._identity_user.clear()
        self._user_identities.clear()
