"""ZAP handler 安全 fuzz 测试。

目标: 各种异常/恶意输入都不能让 handler 崩溃或返回 200。
所有路径必须返回 400 ("xxx") 或 200。
"""
from __future__ import annotations

import asyncio

import pytest

from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.auth.zap_handler import PulseMQZAPHandler, ZapResponse
from pulsemq.storage.interfaces import User


def _user(uid: int = 1, api_key: str = "good-key", **kw) -> User:
    defaults = dict(
        id=uid,
        username="u",
        api_key=api_key,
        role="user",
        namespace="ns",
        disabled=False,
        max_connections=10,
    )
    defaults.update(kw)
    return User(**defaults)


def _build(lookup_fn) -> PulseMQZAPHandler:
    """把任意 lookup 包成 async, 让 handler 内部 asyncio.run 能工作。

    必须用 await, 否则 async callable 返回的 coroutine 会被当成 outer 协程的返回值。
    """
    async def _async_lookup(key):
        result = lookup_fn(key)
        if asyncio.iscoroutine(result):
            result = await result
        return result
    return PulseMQZAPHandler(auth_store=AuthMemoryStore(), user_lookup_fn=_async_lookup)


# ---- Fuzz: handle_zap_request 各种异常输入 ----


@pytest.mark.parametrize(
    "kwargs",
    [
        # 1. 全空 username/password/address
        {"domain": "", "address": "", "mechanism": "", "username": "", "password": ""},
        # 2. None 值混入
        {"domain": "global", "address": None, "mechanism": "PLAIN", "username": "k", "password": ""},
        # 3. 整数代替字符串
        {"domain": "global", "address": "x", "mechanism": "PLAIN", "username": 12345, "password": ""},
        # 4. 极长 username (1KB)
        {"domain": "global", "address": "x", "mechanism": "PLAIN", "username": "A" * 1024, "password": ""},
        # 5. 含 NUL / 不可见字符
        {"domain": "global", "address": "x", "mechanism": "PLAIN", "username": "k\x00\x00\x00", "password": "\x00"},
        # 6. 超大 address
        {"domain": "global", "address": "a" * 1024, "mechanism": "PLAIN", "username": "k", "password": ""},
        # 7. 未知机制 (handler 当前不区分, 仍走查 user 路径)
        {"domain": "global", "address": "x", "mechanism": "GSSAPI", "username": "k", "password": ""},
        # 8. 异常 domain
        {"domain": "global\x00\x00", "address": "x", "mechanism": "PLAIN", "username": "k", "password": ""},
        # 9. 不可打印字符 password
        {"domain": "global", "address": "x", "mechanism": "PLAIN", "username": "k", "password": "\xff\xfe\xfd"},
    ],
)
def test_zap_handler_robust_to_weird_inputs(kwargs):
    """任何异常输入不应让 handler 崩溃, 返回的 status_code 必须是 "200" 或 "400"。"""
    handler = _build(lambda k: None)  # 全部不命中 → 期望 400
    try:
        resp = handler.handle_zap_request(**kwargs)
    except Exception as e:
        pytest.fail(f"handler crashed: {e!r}")
    assert isinstance(resp, ZapResponse)
    assert resp.status_code in ("200", "400")
    # lookup_fn 返回 None, 全部应被拒
    assert resp.status_code == "400"


# ---- Fuzz: lookup_fn 抛各种异常 ----


@pytest.mark.parametrize(
    "exc",
    [
        RuntimeError("DB down"),
        ValueError("bad"),
        TypeError("type"),
        KeyError("k"),
        OSError("io"),
        MemoryError(),
        ZeroDivisionError(),
    ],
)
def test_zap_handler_swallows_lookup_exception(exc):
    """lookup_fn 抛任何异常, handler 仍返回 400 而不崩溃。"""
    async def bad_lookup(key):
        raise exc

    handler = _build(bad_lookup)
    try:
        resp = handler.handle_zap_request(
            domain="global",
            address="x",
            mechanism="PLAIN",
            username="good-key",
            password="",
        )
    except Exception as e:
        pytest.fail(f"handler leaked exception: {e!r}")
    assert resp.status_code == "400"


# ---- Fuzz: client_key 各种形态 ----


@pytest.mark.parametrize(
    "client_key",
    [
        None,
        b"",
        b"x",
        b"x" * 1024,  # 1KB 足够
        b"\x00",
        b"\xff" * 1000,
    ],
)
def test_zap_handler_handles_arbitrary_client_key(client_key):
    """client_key 任意形态不应影响 PLAIN 流程。"""
    handler = _build(lambda k: _user(api_key="good-key"))
    resp = handler.handle_zap_request(
        domain="global",
        address="x",
        mechanism="PLAIN",
        username="good-key",
        password="",
        client_key=client_key,
    )
    assert resp.status_code == "200"


# ---- 边界: disabled / max_connections 边界 ----


def test_zap_handler_disabled_user_returns_400():
    user = _user(disabled=True)
    handler = _build(lambda k: user)
    resp = handler.handle_zap_request(
        domain="global",
        address="x",
        mechanism="PLAIN",
        username="good-key",
        password="",
    )
    assert resp.status_code == "400"
    assert "disabled" in resp.status_text.lower()


def test_zap_handler_max_connections_zero_blocks_first():
    """max_connections=0 → 第一次连接就被拒 (现有连接数 0 >= 0)。"""
    user = _user(max_connections=0)
    handler = _build(lambda k: user)
    resp = handler.handle_zap_request(
        domain="global",
        address="x",
        mechanism="PLAIN",
        username="good-key",
        password="",
    )
    # connection_count(uid)=0 >= max_connections=0, 应被拒
    assert resp.status_code == "400"
    assert "connection" in resp.status_text.lower()


def test_zap_handler_repeated_requests_use_fresh_executor_state():
    """多次连续调用, handler 不应泄漏状态 (端口/线程泄漏)。"""
    handler = _build(lambda k: None)
    for i in range(50):
        r = handler.handle_zap_request(
            domain="global",
            address=f"a-{i}",
            mechanism="PLAIN",
            username="",
            password="",
        )
        assert r.status_code == "400"
