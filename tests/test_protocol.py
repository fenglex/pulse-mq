"""协议层单元测试：帧编解码、flags、序列化、压缩。"""

from __future__ import annotations

import pytest

from pulsemq.protocol.flags import decode_flags, encode_flags
from pulsemq.protocol.frames import PulseMessage, decode, encode
from pulsemq.protocol.msg_type import DataType, MsgType


class TestMsgType:
    def test_constants(self):
        from pulsemq.protocol.msg_type import MsgType
        assert MsgType.DATA == 0x01
        assert MsgType.CONTROL == 0x02
        assert MsgType.HEARTBEAT == 0x03
        assert MsgType.ADMIN == 0x04


class TestDataType:
    """v3 新增：data_type 字段编解码。"""

    def test_constants(self):
        assert DataType.UNKNOWN == 0x00
        assert DataType.DATAFRAME == 0x02
        assert DataType.BYTES == 0x04

    def test_data_type_stored_in_meta(self):
        """encode 应把 data_type 写入 meta[2]，decode 应读出。"""
        frames = encode("t", {"x": 1}, serializer="msgpack",
                        data_type=DataType.DICT)
        meta = frames[1]
        assert len(meta) == 7, f"meta 帧应为 7 字节，实际 {len(meta)}"
        assert meta[0] == MsgType.DATA
        assert meta[2] == DataType.DICT
        msg = decode(frames)
        assert msg.data_type == DataType.DICT

    def test_data_type_default_unknown(self):
        """未传 data_type 时默认 UNKNOWN，decode 读出 UNKNOWN。"""
        frames = encode("t", {"x": 1}, serializer="msgpack")
        msg = decode(frames)
        assert msg.data_type == DataType.UNKNOWN

    @pytest.mark.parametrize("data_type", [
        DataType.DICT, DataType.DATAFRAME, DataType.STR, DataType.BYTES,
    ])
    def test_data_type_roundtrip(self, data_type):
        """各种 data_type 值都能无损往返。"""
        frames = encode("t", {"x": 1}, serializer="msgpack",
                        data_type=data_type)
        msg = decode(frames)
        assert msg.data_type == data_type


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

    def test_crc_bit(self):
        from pulsemq.protocol.flags import encode_flags, decode_flags, has_crc
        base = encode_flags("msgpack", "none")
        assert has_crc(base) is False
        with_crc = encode_flags("msgpack", "none", crc=True)
        assert has_crc(with_crc) is True
        # crc 位不影响 ser/comp 解码
        assert decode_flags(with_crc) == ("msgpack", "none")


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


class TestHeartbeat:
    """心跳帧编解码。

    回归：v3 meta 帧统一为 7 字节（Byte 2 = data_type），
    但 encode_heartbeat 漏了 data_type 字节，编码出 6 字节 meta，
    导致 decode 时 struct.unpack 读 meta[3:7]（仅 3 字节）报
    "Input data was truncated"，心跳功能端到端不可用。
    """

    def test_heartbeat_meta_is_7_bytes(self) -> None:
        """心跳帧 meta 必须与 DATA 帧一致为 7 字节。"""
        from pulsemq.protocol.frames import encode_heartbeat
        frames = encode_heartbeat()
        meta = frames[1]
        assert len(meta) == 7, f"心跳 meta 应为 7 字节，实际 {len(meta)}"
        assert meta[0] == MsgType.PING

    def test_heartbeat_is_pure_control_frame(self) -> None:
        """心跳帧 topic 为 __pulse_hb__，空 payload，record_count=0。"""
        from pulsemq.protocol.frames import encode_heartbeat
        frames = encode_heartbeat()
        assert frames[0] == b"__pulse_hb__"
        assert frames[3] == b""
