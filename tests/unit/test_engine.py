"""Engine + 对象池 + 过载保护 测试。"""

import asyncio
import pytest
from pulsemq.engine.engine import Engine, EngineMetrics
from pulsemq.engine.overload import DualBuffer
from pulsemq.engine.pool import MessageContextPool, MessagePool
from pulsemq.protocol.msg_type import MsgType


class TestMessageContextPool:
    def test_acquire_and_release(self):
        pool = MessageContextPool(size=10)
        ctx = pool.acquire()
        assert isinstance(ctx, dict)
        assert pool.available == 9

    def test_release_clears(self):
        pool = MessageContextPool(size=10)
        ctx = pool.acquire()
        ctx["key"] = "value"
        pool.release(ctx)
        assert ctx == {}
        assert pool.available == 10

    def test_exhaust_fallback(self):
        pool = MessageContextPool(size=2)
        c1 = pool.acquire()
        c2 = pool.acquire()
        c3 = pool.acquire()  # 回退
        assert isinstance(c3, dict)
        assert pool.available == 0

    def test_reuse(self):
        pool = MessageContextPool(size=5)
        ctx = pool.acquire()
        ctx["x"] = 1
        pool.release(ctx)
        ctx2 = pool.acquire()
        assert ctx2 == {}
        assert ctx2 is ctx  # 复用同一对象


class TestMessagePool:
    def test_acquire_and_release(self):
        pool = MessagePool(size=10)
        msg = pool.acquire()
        assert isinstance(msg, list)
        assert pool.available == 9

    def test_release_clears(self):
        pool = MessagePool(size=10)
        msg = pool.acquire()
        msg.extend([b"id", b"topic"])
        pool.release(msg)
        assert msg == []
        assert pool.available == 10


class TestDualBuffer:
    def test_data_enqueue(self):
        buf = DualBuffer(data_buffer_size=5, ctrl_buffer_size=3)
        # PUB 消息 = 6帧，meta 在 frames[3]
        meta = bytes([MsgType.PUB, 0x20])
        frames = [b"id", b"", b"topic", meta, b"\x00\x00\x00\x01", b"payload"]
        buf.enqueue(frames)
        assert len(buf.data_buffer) == 1
        assert len(buf.ctrl_buffer) == 0

    def test_ctrl_enqueue(self):
        buf = DualBuffer(data_buffer_size=5, ctrl_buffer_size=3)
        # SUB 消息
        meta = bytes([MsgType.SUB, 0x20])
        frames = [b"id", b"", b"topic", meta, b"\x00\x00\x00\x00", b""]
        buf.enqueue(frames)
        assert len(buf.ctrl_buffer) == 1
        assert len(buf.data_buffer) == 0

    def test_ctrl_overflow_discards_oldest(self):
        buf = DualBuffer(data_buffer_size=5, ctrl_buffer_size=2)
        for i in range(3):
            meta = bytes([MsgType.PING, 0x00])
            frames = [f"id{i}".encode(), b"", b"", meta, b"\x00\x00\x00\x00", b""]
            buf.enqueue(frames)
        assert len(buf.ctrl_buffer) == 2
        # 第一个被淘汰
        drained = buf.drain_ctrl()
        assert len(drained) == 2

    def test_data_overflow_drops_new(self):
        buf = DualBuffer(data_buffer_size=2, ctrl_buffer_size=3)
        for i in range(4):
            meta = bytes([MsgType.PUB, 0x20])
            frames = [f"id{i}".encode(), b"", b"t", meta, b"\x00\x00\x00\x01", b"p"]
            buf.enqueue(frames)
        assert len(buf.data_buffer) == 2
        assert buf.dropped_total == 2

    def test_drain_ctrl_first(self):
        buf = DualBuffer(data_buffer_size=5, ctrl_buffer_size=3)
        # 混合入队
        meta_pub = bytes([MsgType.PUB, 0x20])
        meta_sub = bytes([MsgType.SUB, 0x20])
        buf.enqueue([b"id", b"", b"t", meta_pub, b"\x00\x00\x00\x01", b"p"])  # data
        buf.enqueue([b"id", b"", b"t", meta_sub, b"\x00\x00\x00\x00", b""])  # ctrl

        ctrl = buf.drain_ctrl()
        data = buf.drain_data()
        assert len(ctrl) == 1
        assert len(data) == 1

    def test_stats(self):
        buf = DualBuffer(data_buffer_size=10, ctrl_buffer_size=5)
        meta_pub = bytes([MsgType.PUB, 0x20])
        for _ in range(3):
            buf.enqueue([b"id", b"", b"t", meta_pub, b"\x00\x00\x00\x01", b"p"])
        stats = buf.stats
        assert stats.data_buffer_usage == 0.3


class TestEngineMetrics:
    def test_initial_values(self):
        m = EngineMetrics()
        assert m.effective_batch_size == 1
        assert m.pending_tasks == 0
        assert m.backpressure_events == 0
