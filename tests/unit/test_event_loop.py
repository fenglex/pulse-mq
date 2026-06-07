"""event_loop 单测。

覆盖:
- Windows 平台强制 SelectorEventLoopPolicy
- use_uvloop=False 时不安装 uvloop
- 重复调用幂等
- get_event_loop_info 返回合理字典
"""

from __future__ import annotations

import asyncio
import sys

import pytest

from pulsemq.event_loop import (
    get_event_loop_info,
    install_event_loop,
)


# ---- Windows 路径 ----


@pytest.mark.skipif(sys.platform != "win32", reason="仅 Windows 验证")
def test_install_windows_uses_selector_loop():
    """Windows 强制安装 WindowsSelectorEventLoopPolicy（pyzmq 必需）。"""
    install_event_loop(use_uvloop=False)
    policy = asyncio.get_event_loop_policy()
    assert isinstance(policy, asyncio.WindowsSelectorEventLoopPolicy)


@pytest.mark.skipif(sys.platform != "win32", reason="仅 Windows 验证")
def test_install_windows_returns_asyncio():
    """Windows 即便 use_uvloop=True 也返回 asyncio。"""
    result = install_event_loop(use_uvloop=True)
    assert result == "asyncio"


# ---- 非 Windows 路径 ----


@pytest.mark.skipif(sys.platform == "win32", reason="非 Windows 平台")
def test_install_non_windows_uvloop_disabled():
    """非 Windows + use_uvloop=False → 返回 asyncio。"""
    result = install_event_loop(use_uvloop=False)
    assert result == "asyncio"


@pytest.mark.skipif(sys.platform == "win32", reason="非 Windows 平台")
def test_install_non_windows_uvloop_enabled():
    """非 Windows + use_uvloop=True → 若 uvloop 可用则安装并返回 uvloop，否则降级 asyncio。"""
    try:
        import uvloop  # noqa: F401
        uvloop_available = True
    except ImportError:
        uvloop_available = False

    result = install_event_loop(use_uvloop=True)
    if uvloop_available:
        assert result == "uvloop"
    else:
        assert result == "asyncio"


# ---- 幂等性 ----


def test_install_idempotent_no_error():
    """重复调用 install_event_loop 不抛异常。

    注意：每次调用都会 set_event_loop_policy 一次，这是可接受的（覆盖即可）。
    """
    install_event_loop(use_uvloop=False)
    install_event_loop(use_uvloop=False)
    install_event_loop(use_uvloop=False)


@pytest.mark.skipif(sys.platform != "win32", reason="仅 Windows 可安全测策略")
def test_install_idempotent_keeps_policy():
    """Windows 上重复安装保持 SelectorEventLoopPolicy。"""
    install_event_loop(use_uvloop=False)
    install_event_loop(use_uvloop=False)
    policy = asyncio.get_event_loop_policy()
    assert isinstance(policy, asyncio.WindowsSelectorEventLoopPolicy)


# ---- get_event_loop_info ----


def test_event_loop_info_contains_platform():
    """返回值至少包含 platform 字段。"""
    info = get_event_loop_info()
    assert "platform" in info
    assert info["platform"] == sys.platform


def test_event_loop_info_contains_uvloop_status():
    """返回值包含 uvloop 可用性。"""
    info = get_event_loop_info()
    assert "uvloop_available" in info
    assert isinstance(info["uvloop_available"], bool)


def test_event_loop_info_uvloop_version_if_available():
    """若 uvloop 可用，应报告其版本。"""
    info = get_event_loop_info()
    if info["uvloop_available"]:
        assert "uvloop_version" in info
        assert info["uvloop_version"]
    else:
        assert "uvloop_version" not in info


def test_event_loop_info_loop_type():
    """返回值包含 loop_type（在 loop 创建后）。"""
    # 显式创建 loop 让 asyncio.get_event_loop() 不会抛 RuntimeError
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        info = get_event_loop_info()
        assert "loop_type" in info
        assert info["loop_type"] != "none"
    finally:
        loop.close()
        asyncio.set_event_loop(None)
