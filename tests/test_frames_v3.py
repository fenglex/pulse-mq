"""PulseMQ v3 单 bytes 帧格式测试（9.2.5 起唯一版本，无兼容层）。

关键约束：
- 定长头 27B：seq（8B，偏移 7）恒在帧内，ack_token（4B，偏移 27）按 flags
  bit6 可选，topic_len 1B（上限 255 字节）。
- rewrite_seq 固定偏移改写，payload 字节不变；CRC 帧重算 CRC。
- 结构开销有界：最坏 27+4+255+4 = 290B（不含 payload）。
"""

import struct

import pytest

from pulsemq.errors import FrameError
from pulsemq.protocol import frames
from pulsemq.protocol.frames import (
    MAGIC,
    VERSION,
    PulseMessage,
    decode,
    decode_control,
    encode,
    encode_control,
    rewrite_seq,
)
from pulsemq.protocol.msg_type import MsgType


def test_magic_and_version():
    assert MAGIC == b"PM"
    assert VERSION == 0x03


def test_header_layout():
    """定长头 27B：偏移 6=topic_len，7..15=seq（占位 0），15..23=ts，23..27=rc。"""
    f = encode("ab", {"x": 1})
    assert f[:2] == MAGIC
    assert f[2] == VERSION
    magic, ver, msg_type, flags, dtype, tlen, seq, ts, rc = \
        struct.unpack(">2sBBBBBQQI", f[:27])
    assert tlen == 2 and seq == 0 and ts > 0 and rc == 1
    assert f[27:29] == b"ab"  # 无 ack_token 时 topic 紧跟定长头


def test_encode_decode_roundtrip_dict():
    data = {"price": 12.3, "sym": "600000"}
    raw = encode("market.stock", data, serializer="msgpack")
    msg = decode(raw)
    assert isinstance(msg, PulseMessage)
    assert msg.topic == "market.stock"
    assert msg.payload == data
    assert msg.msg_type == MsgType.DATA
    assert msg.record_count == 1


def test_encode_infers_record_count_from_dataframe():
    """DataFrame 行数应自动推断为 record_count，而非恒为 1。"""
    import pandas as pd
    df = pd.DataFrame([{"i": i} for i in range(50)])
    raw = encode("batch.topic", df, serializer="msgpack")
    msg = decode(raw)
    assert msg.record_count == 50, f"DataFrame 行数=50，record_count 应为 50，实际={msg.record_count}"


def test_encode_does_not_infer_for_scalar():
    """单条 dict 应保持 record_count=1（list 以外不做推断）。"""
    raw = encode("t", {"x": 1}, serializer="msgpack")
    assert decode(raw).record_count == 1


def test_encode_explicit_record_count_overrides_inference():
    """显式传 record_count 应覆盖自动推断。"""
    import pandas as pd
    df = pd.DataFrame([{"i": i} for i in range(10)])
    raw = encode("t", df, serializer="msgpack", record_count=3)
    assert decode(raw).record_count == 3


def decode_header_seq(frame: bytes) -> int:
    return struct.unpack(">Q", frame[7:15])[0]


def test_seq_placeholder_and_rewrite():
    """encode 时 seq 占位 0；rewrite_seq 固定偏移改写，payload 字节不变。"""
    f = encode("mkt.tick", {"px": 10.5})
    assert decode_header_seq(f) == 0
    f2 = rewrite_seq(f, 42)
    assert decode_header_seq(f2) == 42
    assert frames.decode_header(f2).raw_payload == frames.decode_header(f).raw_payload
    assert decode(f2).payload == {"px": 10.5}
    # 长帧同样只动 7..15
    big = encode("mkt.tick", {"vals": list(range(1000))})
    big2 = rewrite_seq(big, 2**63)
    assert decode_header_seq(big2) == 2**63
    assert decode(big2).payload == decode(big).payload


def test_rewrite_seq_with_crc():
    """CRC 帧 rewrite 后 CRC 重算，decode 校验通过、payload 完整。"""
    f = encode("t", {"x": 1}, crc=True)
    f2 = rewrite_seq(f, 7)
    assert decode(f2).payload == {"x": 1}
    assert decode_header_seq(f2) == 7


def test_ack_token_roundtrip():
    """ack_token 帧：flags bit6 置位，4B 编号解出，payload 完整。"""
    f = encode("t.ack", {"b": [1, 2, 3]}, ack_token=0xDEADBEEF)
    # ack_token 位于偏移 27（定长头与 topic 之间）
    assert struct.unpack(">I", f[27:31])[0] == 0xDEADBEEF
    assert f[31:33] == b"t."  # topic 顺延到偏移 31
    hdr = frames.decode_header(f)
    assert hdr.ack_token == 0xDEADBEEF
    assert hdr.seq == 0
    assert decode(f).payload == {"b": [1, 2, 3]}
    # rewrite_seq 与 ack_token 共存（服务端改写上行确认帧的 seq）
    f2 = rewrite_seq(f, 9)
    hdr2 = frames.decode_header(f2)
    assert hdr2.seq == 9 and hdr2.ack_token == 0xDEADBEEF
    assert decode(f2).payload == {"b": [1, 2, 3]}


def test_plain_frame_has_no_ack_token():
    """普通帧 flags bit6=0，decode_header.ack_token 为 None。"""
    hdr = frames.decode_header(encode("t", {"a": 1}))
    assert hdr.ack_token is None


@pytest.mark.parametrize("bad", [-1, 2**32, 2**40])
def test_ack_token_out_of_range(bad):
    with pytest.raises(FrameError):
        encode("t", {"a": 1}, ack_token=bad)


def test_ack_token_zero_and_max_valid():
    for tok in (0, 0xFFFFFFFF):
        f = encode("t", {"a": 1}, ack_token=tok)
        assert frames.decode_header(f).ack_token == tok


def test_topic_255_ok_and_256_rejected():
    """topic 上限 255 字节（1B topic_len）。"""
    encode("t" * 255, {"a": 1})  # 不 raise
    with pytest.raises(FrameError):
        encode("t" * 256, {"a": 1})


def test_structural_overhead_bounded():
    """结构开销有界：极端帧（最长 topic + ack_token + CRC）≤ 290B 结构段。"""
    f = encode("t" * 255, b"", data_type=frames.DataType.BYTES,
               serializer="bytes", crc=True, ack_token=0xFFFFFFFF)
    assert len(f) <= 27 + 4 + 255 + 4


def test_frame_size_guard(monkeypatch):
    """单帧超过 MAX_FRAME_BYTES 时 encode 拒绝（防误用保护）。"""
    monkeypatch.setattr(frames, "MAX_FRAME_BYTES", 100)
    with pytest.raises(FrameError):
        encode("t", {"x": "y" * 200})


def test_decode_bad_magic():
    bad = b"XX" + b"\x00" * 30
    with pytest.raises(FrameError):
        decode(bad)


def test_decode_bad_version():
    bad = MAGIC + b"\x01" + b"\x00" * 30
    with pytest.raises(FrameError):
        decode(bad)


def test_decode_truncated_topic():
    """topic_len 声明越界 → 显式拒绝（不再静默截断）。"""
    f = bytearray(encode("topic", {"x": 1}))
    f[6] = 200  # topic_len 改成 200，但帧内 topic 只有 5 字节
    with pytest.raises(FrameError):
        frames.decode_header(bytes(f))


def test_crc_roundtrip():
    data = {"x": 1}
    raw = encode("t", data, crc=True)
    msg = decode(raw)
    assert msg.payload == data


def test_crc_corruption_detected():
    raw = bytearray(encode("t", {"x": 1}, crc=True))
    raw[-1] ^= 0xFF  # 破坏 CRC
    with pytest.raises(FrameError):
        decode(bytes(raw))


def test_control_roundtrip():
    raw = encode_control("SUBSCRIBE", {"client_id": "c1", "topic": "a.*"})
    msg = decode_control(raw)
    assert msg.cmd == "SUBSCRIBE"
    assert msg.payload["topic"] == "a.*"


def test_control_frame_is_v3_with_seq_field():
    """控制帧同为 v3（seq 占位 0），msg_type=CONTROL。"""
    f = encode_control("HEARTBEAT", {"client_id": "c"})
    assert f[2] == VERSION
    hdr = frames.decode_header(f)
    assert hdr.msg_type == MsgType.CONTROL
    assert hdr.seq == 0


def test_timestamp_ns_present():
    raw = encode("t", {"x": 1}, ts_ns=1700000000_000000000)
    msg = decode(raw)
    assert msg.timestamp_ns == 1700000000_000000000


def test_record_count_field():
    raw = encode("t", {"x": 1}, record_count=42)
    msg = decode(raw)
    assert msg.record_count == 42


def test_dataframe_roundtrip_after_rewrite():
    """DataFrame 大 payload rewrite_seq 后反序列化仍还原为 DataFrame。"""
    pd = pytest.importorskip("pandas")
    df = pd.DataFrame({"px": [1.0, 2.0], "qty": [10, 20]})
    f = rewrite_seq(encode("mkt.bar", df), 99)
    out = decode(f)
    assert isinstance(out.payload, pd.DataFrame)
    assert len(out.payload) == 2
    assert out.seq == 99


@pytest.mark.parametrize("bad", [
    [{"i": 1}],   # list（即便 list[dict] 也禁止直发）
    42,           # int
    3.14,         # float
    None,         # None
    {1, 2},       # set
])
def test_encode_rejects_non_whitelist_type(bad):
    """非白名单类型应在 encode 时被拒绝。

    白名单仅允许 DataFrame/dict/str/bytes（PubData 语义）。显式传 data_type
    的路径（如 encode_control）不经过自动推断分支，不受影响。
    """
    with pytest.raises(TypeError):
        encode("t", bad)


def test_encode_accepts_whitelist_types():
    """4 种白名单类型应正常编码，不触发 UNKNOWN 校验。"""
    import pandas as pd
    for data in [pd.DataFrame({"a": [1]}), {"k": 1}, "hello", b"bytes"]:
        encode("t", data)  # 不 raise 即通过
