"""PulseMQ v2 单 bytes 帧格式测试。"""

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
)
from pulsemq.protocol.msg_type import MsgType


def test_magic_and_version():
    assert MAGIC == b"PM"
    assert VERSION == 0x01


def test_encode_decode_roundtrip_dict():
    data = {"price": 12.3, "sym": "600000"}
    raw = encode("market.stock", data, serializer="msgpack")
    assert raw[:2] == MAGIC
    assert raw[2] == VERSION
    assert raw[3] == MsgType.DATA
    msg = decode(raw)
    assert isinstance(msg, PulseMessage)
    assert msg.topic == "market.stock"
    assert msg.payload == data
    assert msg.msg_type == MsgType.DATA
    assert msg.record_count == 1


def test_encode_auto_infers_record_count_from_list():
    """list 类型应自动推断 record_count = len(list)，而非恒为 1。

    回归 Bug：record_count 默认 1，且 ``_infer_record_count`` 虽在 docstring
    中提及但从未实现。导致发送 100 条记录的 list 和发送 1 条 dict 的
    record_count 相同，Web UI 消息量显示与实际不符。
    """
    data = [{"i": i} for i in range(50)]
    raw = encode("batch.topic", data, serializer="msgpack")
    msg = decode(raw)
    assert msg.record_count == 50, f"list 长度=50，record_count 应为 50，实际={msg.record_count}"


def test_encode_does_not_infer_for_scalar():
    """单条 dict 应保持 record_count=1（list 以外不做推断）。"""
    raw = encode("t", {"x": 1}, serializer="msgpack")
    assert decode(raw).record_count == 1


def test_encode_explicit_record_count_overrides_inference():
    """显式传 record_count 应覆盖自动推断。"""
    data = [{"i": i} for i in range(10)]
    raw = encode("t", data, serializer="msgpack", record_count=3)
    assert decode(raw).record_count == 3


def test_decode_bad_magic():
    bad = b"XX" + b"\x00" * 20
    with pytest.raises(FrameError):
        decode(bad)


def test_decode_bad_version():
    bad = MAGIC + b"\x09" + b"\x00" * 20
    with pytest.raises(FrameError):
        decode(bad)


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


def test_timestamp_ns_present():
    raw = encode("t", {"x": 1}, ts_ns=1700000000_000000000)
    msg = decode(raw)
    assert msg.timestamp_ns == 1700000000_000000000


def test_record_count_field():
    raw = encode("t", {"x": 1}, record_count=42)
    msg = decode(raw)
    assert msg.record_count == 42
