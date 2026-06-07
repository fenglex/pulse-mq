"""engine/handlers.py 单元测试。

覆盖:
- _build_broadcast_meta 边界（plan 要求）
- _get_topic_bytes 缓存
- dispatch -> handler 路由（用 stub transport）
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from pulsemq.engine.handlers import MessageHandlers
from pulsemq.engine.router import MessageRouter
from pulsemq.protocol.flags import FrameFlags
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType


# ---- _build_broadcast_meta 边界 ----


def test_build_broadcast_meta_preserves_flags():
    h = MessageHandlers.__new__(MessageHandlers)
    wire_meta = bytes([0x04, 0b0010_0100])
    out = h._build_broadcast_meta(wire_meta)
    assert out[1] == 0b0010_0100


def test_build_broadcast_meta_short_wire_meta():
    h = MessageHandlers.__new__(MessageHandlers)
    out = h._build_broadcast_meta(b"\x04")
    assert len(out) == 2
    assert out[1] == 0


def test_build_broadcast_meta_empty_wire_meta():
    h = MessageHandlers.__new__(MessageHandlers)
    out = h._build_broadcast_meta(b"")
    assert len(out) == 2
    assert out[1] == 0


def test_build_broadcast_meta_replaces_msg_type_with_broadcast():
    """原 wire_meta 的 msg_type 字节应被替换为 BROADCAST。"""
    h = MessageHandlers.__new__(MessageHandlers)
    wire_meta = bytes([MsgType.PUB, 0b0010_0100])  # msg_type=PUB
    out = h._build_broadcast_meta(wire_meta)
    assert out[0] == MsgType.BROADCAST


def test_build_broadcast_meta_preserves_ser_and_comp():
    """flags 字节中 ser_fmt/comp/has_topic 位都应保留。"""
    h = MessageHandlers.__new__(MessageHandlers)
    # ser=msgpack(000), comp=zstd(11), has_topic=True
    flags = FrameFlags(ser_fmt="msgpack", comp="zstd", has_topic=True)
    wire_meta = bytes([MsgType.PUB, flags.encode()])
    out = h._build_broadcast_meta(wire_meta)
    assert out[0] == MsgType.BROADCAST
    decoded = FrameFlags.decode(out[1])
    assert decoded.ser_fmt == "msgpack"
    assert decoded.comp == "zstd"
    assert decoded.has_topic is True


# ---- _get_topic_bytes 缓存 ----


def test_get_topic_bytes_caches():
    h = MessageHandlers.__new__(MessageHandlers)
    h._topic_bytes_cache = {}
    a = h._get_topic_bytes("a.b.c")
    b = h._get_topic_bytes("a.b.c")
    assert a is b  # 同一对象 (缓存命中)
    assert a == b"a.b.c"


def test_get_topic_bytes_distinct_topics():
    h = MessageHandlers.__new__(MessageHandlers)
    h._topic_bytes_cache = {}
    a = h._get_topic_bytes("a.b.c")
    b = h._get_topic_bytes("x.y.z")
    assert a == b"a.b.c"
    assert b == b"x.y.z"


# ---- handler 路由 ----


class _StubTransport:
    """记录 send/broadcast 调用, 不实际发包。"""

    def __init__(self):
        self.sent: list[tuple[bytes, list[bytes]]] = []
        self.broadcasts: list[list[bytes]] = []

    async def send(self, identity, frames):
        self.sent.append((identity, frames))

    async def broadcast(self, frames):
        self.broadcasts.append(frames)


def _make_handlers():
    router = MessageRouter()
    transport = _StubTransport()
    h = MessageHandlers(
        router=router,
        send_fn=transport.send,
        broadcast_fn=transport.broadcast,
    )
    return h, router, transport


def _encode_frames(identity: bytes, msg_type: int, topic: str,
                   record_count: int, payload: bytes,
                   ser: str = "msgpack", comp: str = "none") -> list[bytes]:
    """构造服务端 6 帧格式。"""
    inner = FrameCodec.encode(msg_type, topic, record_count, payload, ser, comp)
    return [identity, b""] + inner


@pytest.mark.asyncio
async def test_dispatch_ping_sends_pong():
    """PING 收到后必须回 PONG 帧。"""
    h, router, transport = _make_handlers()
    payload = FrameCodec.encode_payload({"client_ts": 123.0}, "msgpack", "none")
    frames = _encode_frames(b"c1", MsgType.PING, "", 0, payload)
    await h.dispatch(frames)
    assert len(transport.sent) == 1
    identity, sent = transport.sent[0]
    assert identity == b"c1"
    # 第 2 字节是 msg_type
    assert sent[1][0] == MsgType.PONG


@pytest.mark.asyncio
async def test_dispatch_sub_registers_topic():
    """SUB 注册 topic 到 router 并返回 expanded_topics。"""
    h, router, transport = _make_handlers()
    payload = FrameCodec.encode_payload({}, "msgpack", "none")
    frames = _encode_frames(b"c1", MsgType.SUB, "team-a.mkt.sh.600000", 0, payload)
    await h.dispatch(frames)
    assert router.get_topic("team-a.mkt.sh.600000") is not None
    assert b"c1" in router.get_subscribers("team-a.mkt.sh.600000")
    # 回复 SUB 确认
    assert len(transport.sent) == 1
    assert transport.sent[0][1][1][0] == MsgType.SUB


@pytest.mark.asyncio
async def test_dispatch_unsub_removes_subscriber():
    h, router, transport = _make_handlers()
    payload = FrameCodec.encode_payload({}, "msgpack", "none")
    # 先订阅
    await h.dispatch(_encode_frames(b"c1", MsgType.SUB, "a.b.c", 0, payload))
    # 再取消
    await h.dispatch(_encode_frames(b"c1", MsgType.UNSUB, "a.b.c", 0, payload))
    assert router.get_subscribers("a.b.c") == set()
    assert len(transport.sent) == 2


@pytest.mark.asyncio
async def test_dispatch_query_system_status_returns_status():
    """QUERY system_status 必须返回 QUERY 帧 + status 字段。"""
    h, router, transport = _make_handlers()
    payload = FrameCodec.encode_payload({"action": "system_status"}, "msgpack", "none")
    frames = _encode_frames(b"c1", MsgType.QUERY, "", 0, payload)
    await h.dispatch(frames)
    assert len(transport.sent) == 1
    identity, sent = transport.sent[0]
    assert identity == b"c1"
    assert sent[1][0] == MsgType.QUERY
    # 解码 payload
    decoded = FrameCodec.decode_payload(sent[3], "msgpack", "none")
    assert decoded["status"] == "ok"


@pytest.mark.asyncio
async def test_dispatch_query_unknown_action_returns_error():
    """未知 action 必须返回 ERROR 帧, code=3004。"""
    h, router, transport = _make_handlers()
    payload = FrameCodec.encode_payload({"action": "bogus_action"}, "msgpack", "none")
    frames = _encode_frames(b"c1", MsgType.QUERY, "", 0, payload)
    await h.dispatch(frames)
    assert len(transport.sent) == 1
    assert transport.sent[0][1][1][0] == MsgType.ERROR
    decoded = FrameCodec.decode_payload(transport.sent[0][1][3], "msgpack", "none")
    assert decoded["code"] == 3004


@pytest.mark.asyncio
async def test_dispatch_pub_fast_path_broadcasts():
    """dispatch_pub_fast 在有订阅者时必须广播。"""
    h, router, transport = _make_handlers()
    router.register_topic("a.b.c")
    router.subscribe(b"c1", "a.b.c")
    # 构造 6 帧 PUB
    inner = FrameCodec.encode(MsgType.PUB, "a.b.c", 1, b"payload", "msgpack", "none")
    frames = [b"sender", b""] + inner
    await h.dispatch_pub_fast(frames)
    assert len(transport.broadcasts) == 1
    broadcast = transport.broadcasts[0]
    assert broadcast[0] == b"a.b.c"
    assert broadcast[1][0] == MsgType.BROADCAST  # msg_type 改为 BROADCAST
    assert broadcast[3] == b"payload"


@pytest.mark.asyncio
async def test_dispatch_pub_fast_path_no_subscribers_no_broadcast():
    """无订阅者时 dispatch_pub_fast 不应广播。"""
    h, router, transport = _make_handlers()
    router.register_topic("a.b.c")
    # 无订阅
    inner = FrameCodec.encode(MsgType.PUB, "a.b.c", 1, b"payload", "msgpack", "none")
    frames = [b"sender", b""] + inner
    await h.dispatch_pub_fast(frames)
    assert transport.broadcasts == []


@pytest.mark.asyncio
async def test_dispatch_pub_fast_path_5_frames():
    """5 帧 (DEALER 直连) 格式的 PUB 也能正确处理。"""
    h, router, transport = _make_handlers()
    router.register_topic("a.b.c")
    router.subscribe(b"c1", "a.b.c")
    # 5 帧: [identity, topic, meta, rc, payload]
    inner = FrameCodec.encode(MsgType.PUB, "a.b.c", 1, b"payload", "msgpack", "none")
    frames = [b"sender"] + inner
    await h.dispatch_pub_fast(frames)
    assert len(transport.broadcasts) == 1


@pytest.mark.asyncio
async def test_dispatch_pub_fast_preserves_per_message_ser_comp():
    """dispatch_pub_fast 应保留原始 PUB 消息的 ser/comp（不覆盖为 default）。"""
    h, router, transport = _make_handlers()
    router.register_topic("a.b.c")
    router.subscribe(b"c1", "a.b.c")
    inner = FrameCodec.encode(MsgType.PUB, "a.b.c", 1, b"payload", "str", "zstd")
    frames = [b"sender", b""] + inner
    await h.dispatch_pub_fast(frames)
    broadcast = transport.broadcasts[0]
    flags = FrameFlags.decode(broadcast[1][1])
    assert flags.ser_fmt == "str"
    assert flags.comp == "zstd"


@pytest.mark.asyncio
async def test_dispatch_with_broadcast_queue_uses_queue():
    """set_broadcast_queue 后, dispatch_pub_fast 优先入队。"""
    h, router, transport = _make_handlers()
    router.register_topic("a.b.c")
    router.subscribe(b"c1", "a.b.c")
    q: asyncio.Queue = asyncio.Queue()
    h.set_broadcast_queue(q)
    inner = FrameCodec.encode(MsgType.PUB, "a.b.c", 1, b"payload", "msgpack", "none")
    frames = [b"sender", b""] + inner
    await h.dispatch_pub_fast(frames)
    assert transport.broadcasts == []
    assert not q.empty()
    queued = q.get_nowait()
    assert queued[0] == b"a.b.c"
    assert queued[1][0] == MsgType.BROADCAST


@pytest.mark.asyncio
async def test_dispatch_invalid_frames_does_not_propagate():
    """dispatch 收到错误帧数时必须 catch 异常, 不向外传播。"""
    h, router, transport = _make_handlers()
    # 只有 2 帧, 故意非法
    frames = [b"c1", b""]
    # 不应抛错 (handlers 内 decode_server 抛 ValueError, dispatch 内层 _process_single 已被吞)
    # 但 _dispatch_internal 走 _handle_pub path 会先 register_topic
    # 直接调 dispatch (用 fast path 之外会进入 _decode_server, 我们只测 fast path 之外也会 catch)
    # 这里仅验证不抛: try/except 包裹在 _process_single 中
    # 模拟 Engine._process_single 的行为
    try:
        await h.dispatch(frames)
    except Exception as e:
        # 不允许异常逃逸
        pytest.fail(f"dispatch 异常逃逸: {e}")


@pytest.mark.asyncio
async def test_dispatch_history_replay_empty_buffer():
    """HISTORY_REPLAY 在 buffer 为空时回 status=ok, count=0。"""
    h, router, transport = _make_handlers()
    router.register_topic("a.b.c")
    payload = FrameCodec.encode_payload({"from_seq": 0, "limit": 100}, "msgpack", "none")
    frames = _encode_frames(b"c1", MsgType.HISTORY_REPLAY, "a.b.c", 0, payload)
    await h.dispatch(frames)
    # 至少一个 done 帧
    assert len(transport.sent) >= 1
    # 最后一帧是 done 帧
    last_sent = transport.sent[-1][1]
    assert last_sent[1][0] == MsgType.HISTORY_REPLAY
    decoded = FrameCodec.decode_payload(last_sent[3], "msgpack", "none")
    assert decoded["status"] == "ok"
    assert decoded["count"] == 0


@pytest.mark.asyncio
async def test_dispatch_history_replay_with_buffered_messages():
    """HISTORY_REPLAY 在 buffer 有消息时回放每条 BROADCAST + done 帧。"""
    h, router, transport = _make_handlers()
    router.buffer_enabled = True
    router.register_topic("a.b.c")
    router.append_message("a.b.c", bytes([MsgType.PUB, 0]), 1, b"p1")
    router.append_message("a.b.c", bytes([MsgType.PUB, 0]), 1, b"p2")
    payload = FrameCodec.encode_payload({"from_seq": 0, "limit": 100}, "msgpack", "none")
    frames = _encode_frames(b"c1", MsgType.HISTORY_REPLAY, "a.b.c", 0, payload)
    await h.dispatch(frames)
    # 应有 2 条 BROADCAST + 1 条 HISTORY_REPLAY done
    broadcast_count = sum(1 for _, s in transport.sent if s[1][0] == MsgType.BROADCAST)
    done_count = sum(1 for _, s in transport.sent if s[1][0] == MsgType.HISTORY_REPLAY)
    assert broadcast_count == 2
    assert done_count == 1
