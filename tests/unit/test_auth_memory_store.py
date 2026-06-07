"""AuthMemoryStore 单测。

覆盖:
- register / get_user / unregister
- 多 identity 共享同一 user_id 的连接计数
- connection_count
- clear
- unregister 不存在的 identity / unregister 二次
- get_user 命中与未命中
"""

from __future__ import annotations

import pytest

from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.models import AuthUser


def _u(uid: int = 1, role: str = "user", api_key: str = "k1") -> AuthUser:
    return AuthUser(user_id=uid, role=role, groups=[], api_key=api_key, namespace="ns")


# ---- 基础 ----


def test_register_and_get_user():
    """register 后能 get_user 拿到原对象。"""
    s = AuthMemoryStore()
    user = _u()
    s.register(b"ident-1", user)

    got = s.get_user(b"ident-1")
    assert got is user
    assert got.user_id == 1
    assert got.role == "user"
    assert got.api_key == "k1"


def test_get_user_missing_returns_none():
    """未注册 identity → get_user 返回 None。"""
    s = AuthMemoryStore()
    assert s.get_user(b"nope") is None


def test_register_overwrites_same_identity():
    """相同 identity 二次 register, 后者覆盖前者。
    注: 实现把 _user_identities 集合 add 一次，不会因覆盖而重复。"""
    s = AuthMemoryStore()
    s.register(b"id", _u(uid=1))
    s.register(b"id", _u(uid=2))

    assert s.get_user(b"id").user_id == 2
    # user 1 集合可能残留, 但 connection_count(1) 应是 0（被 discard）
    # 这里检查覆盖行为正确：当前 identity 绑定到 user 2
    assert s.connection_count(2) == 1


def test_register_unregister_and_get_returns_none():
    """unregister 后 get_user 返回 None, 且返回原 user。"""
    s = AuthMemoryStore()
    user = _u(uid=7)
    s.register(b"x", user)
    removed = s.unregister(b"x")

    assert removed is user
    assert s.get_user(b"x") is None


def test_unregister_missing_returns_none():
    """unregister 不存在的 identity → None, 不抛异常。"""
    s = AuthMemoryStore()
    assert s.unregister(b"ghost") is None


def test_unregister_same_identity_twice_is_idempotent():
    """同一 identity 二次 unregister 第二次返回 None, 不抛异常。"""
    s = AuthMemoryStore()
    s.register(b"x", _u(uid=1))
    first = s.unregister(b"x")
    second = s.unregister(b"x")

    assert first is not None
    assert second is None


# ---- connection_count ----


def test_connection_count_no_user():
    """未注册 user_id → connection_count 为 0。"""
    s = AuthMemoryStore()
    assert s.connection_count(999) == 0


def test_connection_count_single():
    s = AuthMemoryStore()
    s.register(b"a", _u(uid=1))
    assert s.connection_count(1) == 1


def test_connection_count_multiple_identities_same_user():
    """同一 user 多个 identity → connection_count 反映数量。"""
    s = AuthMemoryStore()
    s.register(b"a", _u(uid=1))
    s.register(b"b", _u(uid=1))
    s.register(b"c", _u(uid=1))
    assert s.connection_count(1) == 3


def test_connection_count_different_users_isolated():
    """不同 user_id 计数互相独立。"""
    s = AuthMemoryStore()
    s.register(b"a", _u(uid=1))
    s.register(b"b", _u(uid=1))
    s.register(b"c", _u(uid=2))
    assert s.connection_count(1) == 2
    assert s.connection_count(2) == 1
    assert s.connection_count(3) == 0


def test_connection_count_decrements_on_unregister():
    """unregister 一个 identity 后 connection_count 减 1。"""
    s = AuthMemoryStore()
    s.register(b"a", _u(uid=1))
    s.register(b"b", _u(uid=1))
    s.unregister(b"a")
    assert s.connection_count(1) == 1
    s.unregister(b"b")
    assert s.connection_count(1) == 0


def test_connection_count_not_incremented_by_re_register():
    """同一 identity 重复 register 不应增加 connection_count。"""
    s = AuthMemoryStore()
    s.register(b"a", _u(uid=1))
    s.register(b"a", _u(uid=1))
    assert s.connection_count(1) == 1


# ---- clear ----


def test_clear_empties_everything():
    """clear 后所有 identity 和 user_id 计数归零。"""
    s = AuthMemoryStore()
    s.register(b"a", _u(uid=1))
    s.register(b"b", _u(uid=1))
    s.register(b"c", _u(uid=2))

    s.clear()

    assert s.get_user(b"a") is None
    assert s.get_user(b"b") is None
    assert s.get_user(b"c") is None
    assert s.connection_count(1) == 0
    assert s.connection_count(2) == 0


def test_clear_on_empty_is_noop():
    """空 store clear 不抛异常。"""
    s = AuthMemoryStore()
    s.clear()
    assert s.connection_count(1) == 0
    assert s.get_user(b"x") is None


# ---- 安全性 / 边界 ----


def test_unregister_does_not_affect_other_user_id_set():
    """unregister 某 user 的 identity, 不应污染其他 user_id 集合。"""
    s = AuthMemoryStore()
    s.register(b"a", _u(uid=1))
    s.register(b"b", _u(uid=2))
    s.unregister(b"a")
    assert s.connection_count(1) == 0
    assert s.connection_count(2) == 1
    assert s.get_user(b"b") is not None


def test_register_after_clear_works():
    """clear 之后能继续 register 并正确工作。"""
    s = AuthMemoryStore()
    s.register(b"a", _u(uid=1))
    s.clear()
    s.register(b"b", _u(uid=1))
    assert s.get_user(b"b") is not None
    assert s.connection_count(1) == 1
