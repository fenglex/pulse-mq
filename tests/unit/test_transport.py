"""transport/zmq_transport.py 单元测试。

ZmqTransport 几乎所有方法都依赖真实 zmq socket，单元测试覆盖受限。
重点覆盖：未启动时的 RuntimeError 路径（纯逻辑分支）。
"""

from __future__ import annotations

import pytest

from pulsemq.config import ServerConfig
from pulsemq.transport.zmq_transport import ZmqTransport


def _make_transport() -> ZmqTransport:
    """构造一个未启动的 ZmqTransport 实例。"""
    return ZmqTransport(ServerConfig())


@pytest.mark.asyncio
async def test_recv_uninitialized_raises():
    """未 start() 时 recv() 必须抛 RuntimeError，不能静默返回 None。"""
    t = _make_transport()
    with pytest.raises(RuntimeError, match="Transport 未启动"):
        await t.recv()


@pytest.mark.asyncio
async def test_send_uninitialized_raises():
    """未 start() 时 send() 必须抛 RuntimeError。"""
    t = _make_transport()
    with pytest.raises(RuntimeError, match="Transport 未启动"):
        await t.send(b"identity", [b"a", b"b"])


@pytest.mark.asyncio
async def test_broadcast_uninitialized_raises():
    """未 start() 时 broadcast() 必须抛 RuntimeError。"""
    t = _make_transport()
    with pytest.raises(RuntimeError, match="Transport 未启动"):
        await t.broadcast([b"topic", b"meta", b"rc", b"payload"])


@pytest.mark.asyncio
async def test_stop_when_uninitialized_is_safe():
    """未启动状态下 stop() 不应抛错。"""
    t = _make_transport()
    # 不应抛错
    await t.stop()


@pytest.mark.asyncio
async def test_stop_can_be_called_multiple_times():
    """重复 stop() 是幂等的。"""
    t = _make_transport()
    await t.stop()
    # 第二次不应抛错
    await t.stop()


def test_constructor_initializes_sockets_to_none():
    """构造时所有 socket 引用必须是 None。"""
    t = _make_transport()
    assert t._router is None
    assert t._xpub is None
    assert t._ctx is None


def test_constructor_stores_config():
    """构造时 config 必须被保留（用于后续 start 读取配置）。"""
    cfg = ServerConfig()
    cfg.bind = "tcp://*:9999"
    cfg.xpub_bind = "tcp://*:9998"
    t = ZmqTransport(cfg)
    assert t._config is cfg
    assert t._config.bind == "tcp://*:9999"
    assert t._config.xpub_bind == "tcp://*:9998"
