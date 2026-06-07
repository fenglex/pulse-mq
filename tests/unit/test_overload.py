"""engine/overload.py 单元测试。

覆盖:
- DualBuffer 限流/反压触发条件
- control buffer 行为 (满了丢最旧)
- data buffer 行为 (满了丢最新)
- drain 优先级 (ctrl 先于 data)
- 统计字段
"""

from __future__ import annotations

import pytest

from pulsemq.engine.overload import DualBuffer, OverloadStats
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType


def _pub_frames(topic: str = "a.b.c") -> list[bytes]:
    """构造 PUB 6 帧 (数据消息)。"""
    inner = FrameCodec.encode(MsgType.PUB, topic, 1, b"p", "msgpack", "none")
    return [b"sender", b""] + inner


def _ping_frames() -> list[bytes]:
    """构造 PING 6 帧 (控制消息)。"""
    inner = FrameCodec.encode(MsgType.PING, "", 0, b"", "msgpack", "none")
    return [b"sender", b""] + inner


def _sub_frames(topic: str = "a.b.c") -> list[bytes]:
    inner = FrameCodec.encode(MsgType.SUB, topic, 0, b"", "msgpack", "none")
    return [b"sender", b""] + inner


def _unsub_frames(topic: str = "a.b.c") -> list[bytes]:
    inner = FrameCodec.encode(MsgType.UNSUB, topic, 0, b"", "msgpack", "none")
    return [b"sender", b""] + inner


def _query_frames() -> list[bytes]:
    inner = FrameCodec.encode(MsgType.QUERY, "", 0, b"", "msgpack", "none")
    return [b"sender", b""] + inner


# ---- enqueue 分流 ----


def test_enqueue_routes_pub_to_data():
    b = DualBuffer(data_buffer_size=10, ctrl_buffer_size=10)
    b.enqueue(_pub_frames("a.b.c"))
    assert len(b.data_buffer) == 1
    assert len(b.ctrl_buffer) == 0


def test_enqueue_routes_ping_to_ctrl():
    b = DualBuffer(data_buffer_size=10, ctrl_buffer_size=10)
    b.enqueue(_ping_frames())
    assert len(b.ctrl_buffer) == 1
    assert len(b.data_buffer) == 0


def test_enqueue_routes_sub_unsub_query_to_ctrl():
    """SUB/UNSUB/QUERY 都是控制消息。"""
    b = DualBuffer(data_buffer_size=10, ctrl_buffer_size=10)
    b.enqueue(_sub_frames())
    b.enqueue(_unsub_frames())
    b.enqueue(_query_frames())
    assert len(b.ctrl_buffer) == 3
    assert len(b.data_buffer) == 0


# ---- data buffer 满了丢最新 ----


def test_data_buffer_drops_newest_when_full():
    """data buffer 满了: 新的消息直接丢弃, 不插入。"""
    b = DualBuffer(data_buffer_size=3, ctrl_buffer_size=10)
    b.enqueue(_pub_frames("t1"))
    b.enqueue(_pub_frames("t2"))
    b.enqueue(_pub_frames("t3"))
    assert len(b.data_buffer) == 3
    # 第 4 条: 丢弃
    b.enqueue(_pub_frames("t4"))
    assert len(b.data_buffer) == 3
    # dropped_total 应为 1
    assert b.dropped_total == 1


def test_data_buffer_drops_increments_counter():
    b = DualBuffer(data_buffer_size=2, ctrl_buffer_size=10)
    for i in range(5):
        b.enqueue(_pub_frames(f"t{i}"))
    assert b.dropped_total == 3


# ---- ctrl buffer 满了丢最旧 ----


def test_ctrl_buffer_drops_oldest_when_full():
    """ctrl buffer 满了: 丢弃最旧, 保留最新。"""
    b = DualBuffer(data_buffer_size=10, ctrl_buffer_size=2)
    b.enqueue(_ping_frames())  # 第 1 条
    b.enqueue(_ping_frames())  # 第 2 条
    b.enqueue(_ping_frames())  # 第 3 条: 挤掉第 1 条
    assert len(b.ctrl_buffer) == 2
    # dropped_total: ctrl buffer 不计入 dropped_total (只看 data)
    # 通过 drain 验证保留的是后两条
    msgs = b.drain_ctrl()
    assert len(msgs) == 2


# ---- drain 优先级 ----


def test_drain_ctrl_before_data():
    """先 drain 控制, 再 drain 数据。"""
    b = DualBuffer(data_buffer_size=10, ctrl_buffer_size=10)
    b.enqueue(_pub_frames("data1"))
    b.enqueue(_pub_frames("data2"))
    b.enqueue(_ping_frames())
    b.enqueue(_sub_frames())
    # 验证 drain 顺序: 双缓冲在 engine 中调用 _drain_buffers 先 ctrl 后 data
    # 我们直接验证两个 drain 各自返回正确内容
    ctrl_msgs = b.drain_ctrl()
    data_msgs = b.drain_data()
    assert len(ctrl_msgs) == 2
    assert len(data_msgs) == 2
    # 全部清空
    assert len(b.ctrl_buffer) == 0
    assert len(b.data_buffer) == 0


def test_drain_ctrl_limit():
    b = DualBuffer(data_buffer_size=10, ctrl_buffer_size=10)
    for _ in range(5):
        b.enqueue(_ping_frames())
    msgs = b.drain_ctrl(limit=2)
    assert len(msgs) == 2
    assert len(b.ctrl_buffer) == 3


def test_drain_data_limit():
    b = DualBuffer(data_buffer_size=10, ctrl_buffer_size=10)
    for _ in range(5):
        b.enqueue(_pub_frames())
    msgs = b.drain_data(limit=3)
    assert len(msgs) == 3
    assert len(b.data_buffer) == 2


def test_drain_empty_buffers():
    b = DualBuffer()
    assert b.drain_ctrl() == []
    assert b.drain_data() == []


# ---- 5 帧格式 (DEALER) ----


def test_enqueue_5_frames_pub():
    """5 帧格式的 PUB 也能正确分流。"""
    b = DualBuffer()
    inner = FrameCodec.encode(MsgType.PUB, "a.b.c", 1, b"p", "msgpack", "none")
    frames_5 = [b"sender"] + inner
    b.enqueue(frames_5)
    assert len(b.data_buffer) == 1


def test_enqueue_5_frames_ping():
    b = DualBuffer()
    inner = FrameCodec.encode(MsgType.PING, "", 0, b"", "msgpack", "none")
    frames_5 = [b"sender"] + inner
    b.enqueue(frames_5)
    assert len(b.ctrl_buffer) == 1


# ---- 边界 ----


def test_enqueue_short_frames_ignored():
    """帧数过少 (无 meta) 时, enqueue 不抛错, 直接忽略。"""
    b = DualBuffer()
    b.enqueue([b"only_one"])  # 1 帧
    b.enqueue([b"a", b"b"])  # 2 帧
    assert len(b.data_buffer) == 0
    assert len(b.ctrl_buffer) == 0


def test_enqueue_empty_frames_ignored():
    b = DualBuffer()
    b.enqueue([])
    assert len(b.data_buffer) == 0
    assert len(b.ctrl_buffer) == 0


# ---- 统计 ----


def test_stats_after_enqueue():
    b = DualBuffer(data_buffer_size=10, ctrl_buffer_size=10)
    b.enqueue(_pub_frames())
    b.enqueue(_pub_frames())
    b.enqueue(_ping_frames())
    s = b.stats
    assert s.data_buffer_usage == 0.2  # 2/10
    assert s.ctrl_buffer_usage == 0.1  # 1/10


def test_stats_dropped():
    b = DualBuffer(data_buffer_size=2, ctrl_buffer_size=10)
    for _ in range(5):
        b.enqueue(_pub_frames())
    s = b.stats
    assert s.dropped_total == 3


def test_stats_zero_usage_when_empty():
    b = DualBuffer()
    s = b.stats
    assert s.data_buffer_usage == 0.0
    assert s.ctrl_buffer_usage == 0.0
    assert s.dropped_total == 0


def test_overload_stats_dataclass():
    s = OverloadStats()
    assert s.dropped_total == 0
    assert s.data_buffer_usage == 0.0
    assert s.ctrl_buffer_usage == 0.0
