"""engine/pool.py 单元测试。

覆盖:
- MessageContextPool 借还 + 容量限制
- PipelineContextPool 借还 + 容量限制
- MessagePool 借还 + 容量限制
"""

from __future__ import annotations

import pytest

from pulsemq.engine.pool import (
    MessageContextPool,
    MessagePool,
    PipelineContextPool,
)


# ---- MessageContextPool ----


def test_message_context_pool_initial_full():
    p = MessageContextPool(size=4)
    assert p.available == 4


def test_message_context_pool_acquire_decrements():
    p = MessageContextPool(size=4)
    a = p.acquire()
    assert p.available == 3
    b = p.acquire()
    assert p.available == 2


def test_message_context_pool_acquire_returns_dict():
    p = MessageContextPool(size=4)
    ctx = p.acquire()
    assert isinstance(ctx, dict)


def test_message_context_pool_release_increments():
    p = MessageContextPool(size=4)
    a = p.acquire()
    p.release(a)
    assert p.available == 4


def test_message_context_pool_release_clears():
    """release 必须清空 dict, 避免脏数据残留。"""
    p = MessageContextPool(size=4)
    a = p.acquire()
    a["dirty"] = "data"
    p.release(a)
    assert "dirty" not in a


def test_message_context_pool_acquire_exhausted_fallback():
    """池耗尽时, acquire 仍返回新 dict (回退到普通分配)。"""
    p = MessageContextPool(size=2)
    a = p.acquire()
    b = p.acquire()
    c = p.acquire()  # 池外回退
    assert isinstance(c, dict)
    assert p.available == 0


def test_message_context_pool_release_beyond_capacity_noop():
    """释放超过池容量时不抛错。"""
    p = MessageContextPool(size=2)
    p.release({})  # 满池时再 release, _available < len, 但 {} 仍放回
    # 不应抛
    assert p.available >= 1


# ---- PipelineContextPool ----


def test_pipeline_context_pool_acquire_returns_context():
    p = PipelineContextPool(size=4)
    ctx = p.acquire(identity=b"c1", msg_type=1, topic="t", meta=b"m", payload=b"p", record_count=0)
    assert ctx.identity == b"c1"
    assert ctx.msg_type == 1
    assert ctx.topic == "t"
    assert ctx.meta == b"m"
    assert ctx.payload == b"p"
    assert ctx.user is None  # 重置


def test_pipeline_context_pool_release_resets_fields():
    p = PipelineContextPool(size=2)
    ctx = p.acquire(identity=b"c1", msg_type=1, topic="t", meta=b"m", payload=b"p", record_count=0)
    ctx.user = "user_obj"
    p.release(ctx)
    assert ctx.identity == b""
    assert ctx.topic == ""
    assert ctx.meta == b""
    assert ctx.payload == b""
    assert ctx.user is None


def test_pipeline_context_pool_reuse_object():
    """acquire 应复用池中对象 (同一对象引用, 而非新建)。"""
    p = PipelineContextPool(size=2)
    ctx1 = p.acquire(identity=b"c1", msg_type=1, topic="t", meta=b"m", payload=b"p", record_count=0)
    p.release(ctx1)
    ctx2 = p.acquire(identity=b"c2", msg_type=2, topic="t2", meta=b"m2", payload=b"p2", record_count=0)
    # 同一对象 (slots=True, 但引用应相同)
    assert ctx1 is ctx2


def test_pipeline_context_pool_exhausted_fallback():
    p = PipelineContextPool(size=2)
    a = p.acquire(identity=b"c1", msg_type=1, topic="t", meta=b"m", payload=b"p", record_count=0)
    b = p.acquire(identity=b"c2", msg_type=1, topic="t", meta=b"m", payload=b"p", record_count=0)
    c = p.acquire(identity=b"c3", msg_type=1, topic="t", meta=b"m", payload=b"p", record_count=0)
    assert c.identity == b"c3"
    assert p.available == 0


def test_pipeline_context_pool_available():
    p = PipelineContextPool(size=10)
    assert p.available == 10
    p.acquire(identity=b"c1", msg_type=1, topic="t", meta=b"m", payload=b"p", record_count=0)
    assert p.available == 9


# ---- MessagePool ----


def test_message_pool_initial_full():
    p = MessagePool(size=4)
    assert p.available == 4


def test_message_pool_acquire_returns_list():
    p = MessagePool(size=4)
    msg = p.acquire()
    assert isinstance(msg, list)
    assert p.available == 3


def test_message_pool_release_clears():
    p = MessagePool(size=4)
    msg = p.acquire()
    msg.extend([b"a", b"b", b"c"])
    p.release(msg)
    assert msg == []


def test_message_pool_exhausted_fallback():
    p = MessagePool(size=2)
    a = p.acquire()
    b = p.acquire()
    c = p.acquire()
    assert isinstance(c, list)
    assert p.available == 0


def test_message_pool_reuse_object():
    p = MessagePool(size=2)
    a = p.acquire()
    p.release(a)
    b = p.acquire()
    assert a is b
