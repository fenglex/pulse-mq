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
    """data_type 字段编解码（v2 单 bytes 帧格式）。"""

    def test_constants(self):
        assert DataType.UNKNOWN == 0x00
        assert DataType.DATAFRAME == 0x02
        assert DataType.BYTES == 0x04

    def test_data_type_stored_in_frame(self):
        """encode 应把 data_type 写入帧 byte 5，decode 应读出。

        布局: magic(2) ver(1) msg_type(1) flags(1) data_type(1) ...
        """
        raw = encode("t", {"x": 1}, serializer="msgpack", data_type=DataType.DICT)
        assert raw[3] == MsgType.DATA
        assert raw[5] == DataType.DICT
        msg = decode(raw)
        assert msg.data_type == DataType.DICT

    def test_data_type_default_unknown(self):
        """未传 data_type 时默认 UNKNOWN，decode 读出 UNKNOWN。"""
        raw = encode("t", {"x": 1}, serializer="msgpack")
        msg = decode(raw)
        assert msg.data_type == DataType.UNKNOWN

    @pytest.mark.parametrize("data_type", [
        DataType.DICT, DataType.DATAFRAME, DataType.STR, DataType.BYTES,
    ])
    def test_data_type_roundtrip(self, data_type):
        """各种 data_type 值都能无损往返。"""
        raw = encode("t", {"x": 1}, serializer="msgpack", data_type=data_type)
        msg = decode(raw)
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
