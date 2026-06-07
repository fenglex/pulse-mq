"""engine/engine.py 单元测试。

覆盖:
- _is_pub_frames 静态方法（plan 要求: 不依赖真实 socket）
- _drain_buffers 优先级消费
- metrics 字段
- EngineMetrics dataclass
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from pulsemq.config import ServerConfig
from pulsemq.engine.engine import Engine, EngineMetrics
from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.router import MessageRouter
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType
from pulsemq.transport.zmq_transport import ZmqTransport


# ---- EngineMetrics ----


def test_engine_metrics_defaults():
    m = EngineMetrics()
    assert m.pending_tasks == 0
    assert m.concurrency_usage == 0.0
    assert m.backpressure_events == 0
    assert m.total_messages == 0
    assert m.total_errors == 0


# ---- _is_pub_frames 静态方法 ----


def _pub_frames_6(topic: str = "a.b.c", record_count: int = 1) -> list[bytes]:
    """构造 6 帧 PUB 消息。"""
    inner = FrameCodec.encode(MsgType.PUB, topic, record_count, b"p", "msgpack", "none")
    return [b"sender", b""] + inner


def _pub_frames_5(topic: str = "a.b.c", record_count: int = 1) -> list[bytes]:
    inner = FrameCodec.encode(MsgType.PUB, topic, record_count, b"p", "msgpack", "none")
    return [b"sender"] + inner


def test_is_pub_frames_true_6():
    frames = _pub_frames_6()
    assert Engine._is_pub_frames(frames) is True


def test_is_pub_frames_true_5():
    frames = _pub_frames_5()
    assert Engine._is_pub_frames(frames) is True


def test_is_pub_frames_false_for_ping():
    """PING 帧不应被识别为 PUB。"""
    inner = FrameCodec.encode(MsgType.PING, "", 0, b"", "msgpack", "none")
    frames = [b"sender", b""] + inner
    assert Engine._is_pub_frames(frames) is False


def test_is_pub_frames_false_for_sub():
    inner = FrameCodec.encode(MsgType.SUB, "a.b.c", 0, b"", "msgpack", "none")
    frames = [b"sender", b""] + inner
    assert Engine._is_pub_frames(frames) is False


def test_is_pub_frames_false_for_short_frames():
    assert Engine._is_pub_frames([b"only_one"]) is False
    assert Engine._is_pub_frames([]) is False


def test_is_pub_frames_false_for_short_meta():
    """meta 帧长度 < 1 时, 不能访问 meta[0]。"""
    frames = [b"sender", b"", b""]  # 3 帧, meta 帧为空
    assert Engine._is_pub_frames(frames) is False


# ---- _drain_buffers 优先级消费 ----


def _make_engine() -> Engine:
    """构造一个不绑定 socket 的 Engine 实例, 用 stub transport。"""
    cfg = ServerConfig()
    cfg.max_concurrency = 4
    transport = ZmqTransport(cfg)  # 未 start, 仅占位
    router = MessageRouter()
    handlers = MessageHandlers(
        router=router,
        send_fn=lambda *a, **kw: asyncio.sleep(0),
        broadcast_fn=lambda *a, **kw: asyncio.sleep(0),
    )
    return Engine(transport=transport, handlers=handlers, config=cfg)


# ---- _drain_buffers 优先级消费 ----


@pytest.mark.asyncio
async def test_drain_buffers_ctrl_before_data():
    """控制消息应在数据消息之前被消费。"""
    e = _make_engine()
    # 灌入数据消息
    e._dual_buffer.enqueue(_pub_frames_6("a.b.c"))
    e._dual_buffer.enqueue(_pub_frames_6("x.y.z"))
    # 灌入控制消息 (PING)
    inner_ping = FrameCodec.encode(MsgType.PING, "", 0, b"", "msgpack", "none")
    e._dual_buffer.enqueue([b"c1", b""] + inner_ping)
    inner_sub = FrameCodec.encode(MsgType.SUB, "topic", 0, b"", "msgpack", "none")
    e._dual_buffer.enqueue([b"c2", b""] + inner_sub)

    # _drain_buffers 调用 _process_single, 但我们没真实 handlers 配置
    # 用 monkeypatch _process_single 记录顺序
    order = []
    orig = e._process_single
    async def fake_process(frames):
        order.append(frames)
    e._process_single = fake_process  # type: ignore

    consumed = await e._drain_buffers()
    assert consumed == 4
    # 前两个是控制, 后两个是数据
    assert Engine._is_pub_frames(order[2]) or Engine._is_pub_frames(order[3])
    # 控制 (PING, SUB) 应在前
    assert order[0][3 if len(order[0]) == 6 else 2][0] == MsgType.PING
    assert order[1][3 if len(order[1]) == 6 else 2][0] == MsgType.SUB


# ---- metrics 字段 ----


def test_metrics_property_returns_dataclass():
    e = _make_engine()
    m = e.metrics
    assert isinstance(m, EngineMetrics)
    assert m.pending_tasks == 0


def test_metrics_concurrency_usage_zero_when_max_zero():
    """max_concurrency=0 时, concurrency_usage 应该是 0 (不抛 ZeroDivisionError)。"""
    cfg = ServerConfig()
    cfg.max_concurrency = 0
    transport = ZmqTransport(cfg)
    router = MessageRouter()
    handlers = MessageHandlers(
        router=router,
        send_fn=lambda *a, **kw: asyncio.sleep(0),
        broadcast_fn=lambda *a, **kw: asyncio.sleep(0),
    )
    e = Engine(transport=transport, handlers=handlers, config=cfg)
    m = e.metrics
    assert m.concurrency_usage == 0.0


# ---- _broadcast_queue 优雅关闭 ----


@pytest.mark.asyncio
async def test_stop_clears_broadcast_queue():
    """stop() 后 _broadcast_queue 应为 None。"""
    e = _make_engine()
    # 模拟 run 启动过
    e._running = True
    e._broadcast_queue = asyncio.Queue()
    e._broadcast_task = None
    await e.stop()
    assert e._broadcast_queue is None
    assert e._running is False


@pytest.mark.asyncio
async def test_stop_cancels_broadcast_task():
    """stop() 必须取消 broadcast_task, 最多 2 秒等待。"""
    e = _make_engine()
    e._running = True
    e._broadcast_queue = asyncio.Queue(maxsize=0)

    async def never_end():
        await asyncio.sleep(100)

    e._broadcast_task = asyncio.create_task(never_end())
    await e.stop()
    assert e._broadcast_task is None
    assert e._broadcast_queue is None


@pytest.mark.asyncio
async def test_stop_idempotent():
    """重复 stop() 不应抛错。"""
    e = _make_engine()
    e._running = True
    e._broadcast_queue = asyncio.Queue()
    await e.stop()
    await e.stop()  # 第二次


# ---- pub_fast_path 标志 ----


def test_pub_fast_path_depends_on_auth():
    """auth_enabled=False 时启用 fast path。"""
    cfg = ServerConfig()
    cfg.auth_enabled = False
    transport = ZmqTransport(cfg)
    handlers = MessageHandlers(router=MessageRouter(), send_fn=lambda *a, **kw: None,
                               broadcast_fn=lambda *a, **kw: None)
    e = Engine(transport=transport, handlers=handlers, config=cfg)
    assert e._pub_fast_path is True


def test_pub_fast_path_disabled_when_auth():
    cfg = ServerConfig()
    cfg.auth_enabled = True
    transport = ZmqTransport(cfg)
    handlers = MessageHandlers(router=MessageRouter(), send_fn=lambda *a, **kw: None,
                               broadcast_fn=lambda *a, **kw: None)
    e = Engine(transport=transport, handlers=handlers, config=cfg)
    assert e._pub_fast_path is False
