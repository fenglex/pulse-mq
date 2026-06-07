"""ZAP handler 单测。

覆盖:
- 合法 PLAIN 请求 → 200 + 写入 memory_store
- 未知 api_key → 400
- 空 api_key → 400
- 禁用账户 → 400
- 连接数超限 → 400
- user_lookup_fn 抛异常 → 400
- admin 与 user 角色都能注册
- 多连接场景（同一 user 多个 identity）
- client_key 参数（NULL 机制）应被忽略
"""

from __future__ import annotations

import asyncio

import pytest

from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.auth.zap_handler import PulseMQZAPHandler, ZapResponse
from pulsemq.models import AuthUser
from pulsemq.storage.interfaces import User


# ---- helpers ----


def _make_user(
    user_id: int = 1,
    username: str = "alice",
    api_key: str = "key-alice",
    role: str = "user",
    namespace: str = "team-a",
    disabled: bool = False,
    max_connections: int = 10,
) -> User:
    return User(
        id=user_id,
        username=username,
        api_key=api_key,
        role=role,
        namespace=namespace,
        disabled=disabled,
        max_connections=max_connections,
    )


def _build_handler(store, lookup_fn) -> PulseMQZAPHandler:
    """把任意 lookup 包成 async lookup, 让 handler 内部 asyncio.run 能工作。

    关键: 必须用 await, 否则同步 callable 返回 User, async callable 返回 coroutine,
    不 await 会让 outer coroutine 提前完成并把内层 coroutine 当作返回值。
    """
    async def _async_lookup(key):
        result = lookup_fn(key)
        if asyncio.iscoroutine(result):
            result = await result
        return result
    return PulseMQZAPHandler(auth_store=store, user_lookup_fn=_async_lookup)


# ---- 正常路径 ----


@pytest.mark.asyncio
async def test_zap_handler_accepts_valid_plain():
    """合法 PLAIN 请求应被接受（200）并写入 memory_store。"""
    store = AuthMemoryStore()
    user = _make_user()
    handler = _build_handler(store, lambda key: user)

    resp = handler.handle_zap_request(
        domain="global",
        address="127.0.0.1:5555",
        mechanism="PLAIN",
        username="key-alice",
        password="ignored",
    )

    assert isinstance(resp, ZapResponse)
    assert resp.status_code == "200"
    assert resp.user_id == "1"
    # 内存中能找到该 connection
    identity = b"127.0.0.1:5555"
    cached = store.get_user(identity)
    assert cached is not None
    assert cached.user_id == 1
    assert cached.role == "user"
    assert cached.api_key == "key-alice"
    assert cached.namespace == "team-a"


@pytest.mark.asyncio
async def test_zap_handler_admin_role():
    """admin 角色能正常注册。"""
    store = AuthMemoryStore()
    user = _make_user(user_id=42, role="admin", namespace="")
    handler = _build_handler(store, lambda key: user)

    resp = handler.handle_zap_request(
        domain="global",
        address="tcp://10.0.0.1:5555",
        mechanism="PLAIN",
        username="key-alice",
        password="x",
    )
    assert resp.status_code == "200"
    assert store.get_user(b"tcp://10.0.0.1:5555").is_admin is True


@pytest.mark.asyncio
async def test_zap_handler_supports_multiple_connections_same_user():
    """同一用户多个连接 → 全部注册, connection_count 累加。"""
    store = AuthMemoryStore()
    user = _make_user(max_connections=5)
    handler = _build_handler(store, lambda key: user)

    for i in range(3):
        r = handler.handle_zap_request(
            domain="global",
            address=f"addr-{i}",
            mechanism="PLAIN",
            username="key-alice",
            password="",
        )
        assert r.status_code == "200"
    assert store.connection_count(1) == 3
    assert store.get_user(b"addr-0") is not None
    assert store.get_user(b"addr-2") is not None


@pytest.mark.asyncio
async def test_zap_handler_empty_address_falls_back_to_unknown():
    """空 address 时 identity 用 b"unknown", 不崩溃。"""
    store = AuthMemoryStore()
    user = _make_user()
    handler = _build_handler(store, lambda key: user)

    resp = handler.handle_zap_request(
        domain="global",
        address="",
        mechanism="PLAIN",
        username="key-alice",
        password="",
    )
    assert resp.status_code == "200"
    assert store.get_user(b"unknown") is not None


# ---- 拒绝路径 ----


@pytest.mark.asyncio
async def test_zap_handler_rejects_empty_api_key():
    """空 api_key → 400。"""
    store = AuthMemoryStore()
    handler = _build_handler(store, lambda key: _make_user())

    resp = handler.handle_zap_request(
        domain="global",
        address="x",
        mechanism="PLAIN",
        username="",
        password="",
    )
    assert resp.status_code == "400"
    assert "Empty" in resp.status_text or "empty" in resp.status_text.lower()
    # 内存中无记录
    assert store.get_user(b"x") is None


@pytest.mark.asyncio
async def test_zap_handler_rejects_unknown_api_key():
    """lookup_fn 返回 None → 400。"""
    store = AuthMemoryStore()
    handler = _build_handler(store, lambda key: None)

    resp = handler.handle_zap_request(
        domain="global",
        address="x",
        mechanism="PLAIN",
        username="nope",
        password="",
    )
    assert resp.status_code == "400"
    assert store.get_user(b"x") is None


@pytest.mark.asyncio
async def test_zap_handler_rejects_disabled_user():
    """disabled=True → 400。"""
    store = AuthMemoryStore()
    user = _make_user(disabled=True)
    handler = _build_handler(store, lambda key: user)

    resp = handler.handle_zap_request(
        domain="global",
        address="x",
        mechanism="PLAIN",
        username="key-alice",
        password="",
    )
    assert resp.status_code == "400"
    assert "disabled" in resp.status_text.lower()
    assert store.get_user(b"x") is None


@pytest.mark.asyncio
async def test_zap_handler_rejects_when_too_many_connections():
    """连接数 ≥ max_connections → 400。"""
    store = AuthMemoryStore()
    user = _make_user(max_connections=2)
    handler = _build_handler(store, lambda key: user)

    # 先注册 2 个连接
    for i in range(2):
        r = handler.handle_zap_request(
            domain="global",
            address=f"a-{i}",
            mechanism="PLAIN",
            username="key-alice",
            password="",
        )
        assert r.status_code == "200"

    # 第 3 个应被拒
    r = handler.handle_zap_request(
        domain="global",
        address="a-2",
        mechanism="PLAIN",
        username="key-alice",
        password="",
    )
    assert r.status_code == "400"
    assert "connection" in r.status_text.lower()
    # 内存中应只有 2 个
    assert store.connection_count(1) == 2


@pytest.mark.asyncio
async def test_zap_handler_returns_400_on_lookup_exception():
    """lookup_fn 抛异常 → 400 + 内存中无脏数据。"""
    store = AuthMemoryStore()

    async def broken_lookup(key):
        raise RuntimeError("DB down")

    handler = _build_handler(store, broken_lookup)

    resp = handler.handle_zap_request(
        domain="global",
        address="addr",
        mechanism="PLAIN",
        username="k",
        password="",
    )
    assert resp.status_code == "400"
    assert store.get_user(b"addr") is None


# ---- 鲁棒性 ----


@pytest.mark.asyncio
async def test_zap_handler_ignores_password_field():
    """PLAIN 模式密码字段被忽略, 只看 username(api_key)。"""
    store = AuthMemoryStore()
    user = _make_user()
    handler = _build_handler(store, lambda key: user)

    # 任意密码都应通过
    for pw in ["", "wrong", "any-garbage"]:
        r = handler.handle_zap_request(
            domain="global",
            address="same",
            mechanism="PLAIN",
            username="key-alice",
            password=pw,
        )
        assert r.status_code == "200"


@pytest.mark.asyncio
async def test_zap_handler_unsupported_mechanism_still_proceeds():
    """非 PLAIN 机制当前实现不拦截, 仍按 username 查。
    这是已知行为: 任何机制都视作 api_key。"""
    store = AuthMemoryStore()
    user = _make_user()
    handler = _build_handler(store, lambda key: user)

    resp = handler.handle_zap_request(
        domain="global",
        address="x",
        mechanism="CURVE",  # 非 PLAIN, 仍应按 api_key 查
        username="key-alice",
        password="ignored",
    )
    assert resp.status_code == "200"


@pytest.mark.asyncio
async def test_zap_handler_with_client_key_does_not_break():
    """client_key 参数被接受（NULL 机制场景）, 不应影响 PLAIN 流程。"""
    store = AuthMemoryStore()
    user = _make_user()
    handler = _build_handler(store, lambda key: user)

    resp = handler.handle_zap_request(
        domain="global",
        address="x",
        mechanism="PLAIN",
        username="key-alice",
        password="",
        client_key=b"some-public-key",
    )
    assert resp.status_code == "200"
