"""transport/zmq_transport.py 单元测试。

v1 zmq_io_threading: recv/send/broadcast 都是 queue 操作,
不依赖 socket 状态, 改为覆盖 queue/thread 相关行为。
"""

from __future__ import annotations

import pytest

from pulsemq.config import ServerConfig
from pulsemq.transport.zmq_transport import ZmqTransport


def _make_transport() -> ZmqTransport:
    """构造一个未启动的 ZmqTransport 实例。"""
    return ZmqTransport(ServerConfig())


@pytest.mark.asyncio
async def test_recv_uninitialized_blocks_until_stop():
    """v1: 未 start() 时 recv() 阻塞在 queue.get(), 直到 stop() 放哨兵 None 后返回 None."""
    t = _make_transport()
    import asyncio
    # 后台启 stop, 短延迟后应该放哨兵让 recv 返回 None
    async def stop_soon():
        await asyncio.sleep(0.1)
        await t.stop()
    asyncio.create_task(stop_soon())
    result = await asyncio.wait_for(t.recv(), timeout=2.0)
    assert result is None  # 哨兵


@pytest.mark.asyncio
async def test_send_uninitialized_is_safe():
    """v1: send/broadcast 只是 put 到 thread-safe queue, 不需 socket 状态."""
    t = _make_transport()
    # 不应抛错
    await t.send(b"identity", [b"a", b"b"])
    await t.broadcast([b"topic", b"meta", b"rc", b"payload"])


@pytest.mark.asyncio
async def test_broadcast_uninitialized_is_safe():
    """同 test_send_uninitialized_is_safe, broadcast 也安全。"""
    t = _make_transport()
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


def test_constructor_initializes_queues_and_stop_event():
    """构造时 thread-safe queue 与 stop_event 必须就绪。"""
    t = _make_transport()
    assert t._recv_queue is not None
    assert t._broadcast_queue is not None
    assert t._send_queue is not None
    assert t._stop_event is not None
    assert not t._stop_event.is_set()


def test_constructor_stores_config():
    """构造时 config 必须被保留（用于后续 start 读取配置）。"""
    cfg = ServerConfig()
    cfg.bind = "tcp://*:9999"
    cfg.xpub_bind = "tcp://*:9998"
    t = ZmqTransport(cfg)
    assert t._config is cfg
    assert t._config.bind == "tcp://*:9999"
    assert t._config.xpub_bind == "tcp://*:9998"
