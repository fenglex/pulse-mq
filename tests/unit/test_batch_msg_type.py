"""BATCH msg_type 单测。

覆盖:
- MsgType.BATCH 值正确
- BATCH 不与现有 msg_type 冲突
- from_byte 接受 BATCH
- encode_batch_payload / decode_batch_payload 16 组合 roundtrip
- _handle_batch 正确拆 N 条并调用 broadcast
"""

from __future__ import annotations

import struct

import pytest

from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.pipeline import PipelineContext
from pulsemq.engine.router import MessageRouter
from pulsemq.protocol.flags import FrameFlags
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType
from pulsemq.serialization.registry import (
    CompressionRegistry,
    SerializationRegistry,
)


# ---- MsgType ----


def test_batch_msg_type_value():
    """BATCH = 0x0C。"""
    assert MsgType.BATCH == 0x0C


def test_msg_types_distinct_includes_batch():
    """BATCH 加入后所有 msg_type 仍唯一。"""
    values = [
        getattr(MsgType, name) for name in dir(MsgType)
        if not name.startswith("_") and isinstance(getattr(MsgType, name), int)
    ]
    assert len(values) == len(set(values)), f"重复 msg_type: {values}"


def test_from_byte_accepts_batch():
    """from_byte(0x0C) 返回 0x0C。"""
    assert MsgType.from_byte(MsgType.BATCH) == MsgType.BATCH


def test_batch_not_in_control_types():
    """BATCH 不在控制消息集合中（要走 data_buffer）。"""
    assert MsgType.is_control(MsgType.BATCH) is False


# ---- encode_batch_payload / decode_batch_payload roundtrip ----


def _available_ser_combos():
    sers = ["str", "msgpack", "bytes", "pyarrow"]
    comps = ["none", "snappy", "lz4", "zstd"]
    return [
        (s, c) for s in sers for c in comps
        if SerializationRegistry.has(s) and CompressionRegistry.has(c)
    ]


@pytest.mark.parametrize("ser_fmt,comp", [
    (s, c) for s, c in _available_ser_combos()
])
def test_encode_decode_batch_payload_roundtrip(ser_fmt, comp):
    """encode → decode roundtrip。"""
    payloads = [b"hello", b"world", b"foo", b""]
    encoded = FrameCodec.encode_batch_payload(payloads, comp=comp)
    decoded = FrameCodec.decode_batch_payload(encoded, comp=comp)
    assert decoded == payloads


def test_batch_payload_empty_list_roundtrip():
    """空 list 也应正确 roundtrip。"""
    encoded = FrameCodec.encode_batch_payload([], comp="none")
    decoded = FrameCodec.decode_batch_payload(encoded, comp="none")
    assert decoded == []


def test_batch_payload_preserves_binary_data():
    """二进制 bytes 应当原样保留。"""
    payloads = [b"\x00\x01\x02", b"\xff\xfe\xfd", b"\x80\x90\xa0"]
    encoded = FrameCodec.encode_batch_payload(payloads, comp="none")
    decoded = FrameCodec.decode_batch_payload(encoded, comp="none")
    assert decoded == payloads


# ---- _handle_batch 行为 ----


class _CapturingHandlers(MessageHandlers):
    """拦截 broadcast 调用的 MessageHandlers 子类。"""

    def __init__(self, router):
        super().__init__(
            router=router,
            send_fn=lambda i, f: None,
            broadcast_fn=lambda f: self._captured.append(f),
            pipeline=None,
            default_ser="msgpack",
            default_comp="none",
        )
        self._captured: list[list[bytes]] = []


def _make_batch_ctx(payload: bytes, topic: str = "t.batch",
                    comp: str = "none") -> PipelineContext:
    """构造 BATCH 帧的 PipelineContext。"""
    flags = FrameFlags(ser_fmt="msgpack", comp=comp, has_topic=True)
    meta = bytes([MsgType.BATCH, flags.encode()])
    return PipelineContext(
        identity=b"client-1",
        msg_type=MsgType.BATCH,
        topic=topic,
        meta=meta,
        payload=payload,
        record_count=4,
    )


@pytest.mark.asyncio
async def test_handle_batch_splits_into_n_pubs():
    """_handle_batch 应当把 BATCH 拆成 N 条 PUB broadcast。"""
    router = MessageRouter()
    router.subscribe(b"client-1", "t.batch")
    handlers = _CapturingHandlers(router)

    payloads = [b"a", b"b", b"c"]
    encoded = FrameCodec.encode_batch_payload(payloads, comp="none")
    ctx = _make_batch_ctx(encoded)

    await handlers._handle_batch(ctx)

    # 应当 broadcast 3 条
    assert len(handlers._captured) == 3
    # 每条 4 帧
    for frames in handlers._captured:
        assert len(frames) == 4
        assert frames[0] == b"t.batch"
        # meta: msg_type=BROADCAST(0x0A), flags 沿用 BATCH 的 flags 字节
        assert frames[1][0] == MsgType.BROADCAST
        # record_count=1
        assert struct.unpack(">I", frames[2])[0] == 1
    # payload 内容
    payload_list = [f[3] for f in handlers._captured]
    assert payload_list == payloads


@pytest.mark.asyncio
async def test_handle_batch_empty_payload_noop():
    """空 list 不应 broadcast。"""
    router = MessageRouter()
    router.subscribe(b"client-1", "t.batch")
    handlers = _CapturingHandlers(router)

    encoded = FrameCodec.encode_batch_payload([], comp="none")
    ctx = _make_batch_ctx(encoded)

    await handlers._handle_batch(ctx)

    assert handlers._captured == []


@pytest.mark.asyncio
async def test_handle_batch_no_subscribers_no_broadcast():
    """无订阅者时不应 broadcast。"""
    router = MessageRouter()
    handlers = _CapturingHandlers(router)

    payloads = [b"a", b"b"]
    encoded = FrameCodec.encode_batch_payload(payloads, comp="none")
    ctx = _make_batch_ctx(encoded)

    await handlers._handle_batch(ctx)

    assert handlers._captured == []


@pytest.mark.asyncio
async def test_handle_batch_preserves_topic():
    """BATCH 的 topic 应被所有子 PUB 沿用。"""
    router = MessageRouter()
    router.subscribe(b"client-1", "team-a.mkt.sh.600000")
    handlers = _CapturingHandlers(router)

    payloads = [b"\x01", b"\x02"]
    encoded = FrameCodec.encode_batch_payload(payloads, comp="none")
    ctx = _make_batch_ctx(encoded, topic="team-a.mkt.sh.600000")

    await handlers._handle_batch(ctx)

    assert len(handlers._captured) == 2
    for frames in handlers._captured:
        assert frames[0] == b"team-a.mkt.sh.600000"


@pytest.mark.asyncio
async def test_handle_batch_uses_flags_byte_for_broadcast_meta():
    """broadcast meta 应使用 BATCH 帧的 flags 字节（保持 ser/comp 信息）。"""
    router = MessageRouter()
    router.subscribe(b"client-1", "t.f")
    handlers = _CapturingHandlers(router)

    # flags: msgpack + snappy + has_topic
    flags = FrameFlags(ser_fmt="msgpack", comp="snappy", has_topic=True)
    meta = bytes([MsgType.BATCH, flags.encode()])
    # 编码 1 个 payload（空 list 会让 snappy 解压失败）
    encoded = FrameCodec.encode_batch_payload([b"x"], comp="snappy")
    ctx = PipelineContext(
        identity=b"c",
        msg_type=MsgType.BATCH,
        topic="t.f",
        meta=meta,
        payload=encoded,
        record_count=1,
    )
    await handlers._handle_batch(ctx)
    # 1 条 broadcast
    assert len(handlers._captured) == 1
    # broadcast meta[0] 应是 BROADCAST, meta[1] 沿用 BATCH 的 flags
    assert handlers._captured[0][1][0] == MsgType.BROADCAST
    assert handlers._captured[0][1][1] == flags.encode()
