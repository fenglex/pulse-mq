"""协议层单元测试：帧编解码、flags、序列化、压缩。"""

from __future__ import annotations

import pytest

from pulsemq.protocol.flags import decode_flags, encode_flags
from pulsemq.protocol.frames import PulseMessage, decode, encode
from pulsemq.protocol.msg_type import MsgType


class TestMsgType:
    def test_constants(self):
        assert MsgType.DATA == 0x01
        assert MsgType.PING == 0x02


class TestFlags:
    @pytest.mark.parametrize("ser,comp", [
        ("msgpack", "none"),
        ("json", "snappy"),
        ("str", "lz4"),
        ("bytes", "zstd"),
        ("pyarrow", "none"),
    ])
    def test_roundtrip(self, ser: str, comp: str) -> None:
        byte_val = encode_flags(ser, comp)
        result_ser, result_comp = decode_flags(byte_val)
        assert result_ser == ser
        assert result_comp == comp


class TestFrameCodec:
    def test_encode_decode_dict(self) -> None:
        frames = encode("test", {"key": "value"}, serializer="msgpack")
        assert len(frames) == 4
        msg = decode(frames)
        assert msg.topic == "test"
        assert msg.payload == {"key": "value"}
        assert msg.record_count == 1
        assert msg.serializer == "msgpack"
        assert msg.compression == "none"
        assert msg.timestamp_ns > 0

    def test_encode_decode_string(self) -> None:
        frames = encode("topic", "hello world", serializer="str")
        msg = decode(frames)
        assert msg.payload == "hello world"
        assert msg.serializer == "str"

    def test_encode_decode_bytes(self) -> None:
        frames = encode("topic", b"\x01\x02\x03", serializer="bytes")
        msg = decode(frames)
        assert msg.payload == b"\x01\x02\x03"

    def test_encode_decode_list(self) -> None:
        data = ["a", "b", "c"]
        frames = encode("topic", data, serializer="msgpack", record_count=3)
        msg = decode(frames)
        assert msg.payload == ["a", "b", "c"]
        assert msg.record_count == 3

    def test_encode_decode_with_compression(self) -> None:
        data = {"msg": "x" * 1000}
        for comp in ("snappy", "lz4", "zstd"):
            frames = encode("topic", data, serializer="msgpack", compression=comp)
            msg = decode(frames)
            assert msg.payload == data
            assert msg.compression == comp

    def test_encode_decode_json(self) -> None:
        data = {"key": "value", "num": 42}
        frames = encode("topic", data, serializer="json")
        msg = decode(frames)
        assert msg.payload == data
        assert msg.serializer == "json"

    def test_invalid_frame_count(self) -> None:
        with pytest.raises(ValueError, match="帧数不正确"):
            decode([b"topic", b"\x01\x00\x01", b"payload"])

    def test_record_count_field(self) -> None:
        for rc in (1, 10, 100, 255):
            frames = encode("topic", {"x": 1}, record_count=rc)
            msg = decode(frames)
            assert msg.record_count == rc
