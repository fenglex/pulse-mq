"""FrameCodec 帧编解码 单测。"""
from __future__ import annotations

import os
import struct

import pytest

from pulsemq.protocol.flags import FrameFlags
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType


def test_encode_4_frames():
    """encode 返回 4 帧: [topic, meta(2B), rc(4B), payload]。"""
    frames = FrameCodec.encode(MsgType.PUB, "test.t", 5, b"hello")
    assert len(frames) == 4
    assert frames[0] == b"test.t"
    assert len(frames[1]) == 2  # meta
    assert len(frames[2]) == 4  # rc
    assert frames[3] == b"hello"


def test_meta_byte_layout():
    """meta[0]=msg_type, meta[1]=flags_byte。"""
    f = FrameFlags(ser_fmt="str", comp="none", has_topic=True)
    frames = FrameCodec.encode(MsgType.PUB, "t", 1, b"", "str", "none")
    msg_type, flags_byte = frames[1][0], frames[1][1]
    assert msg_type == MsgType.PUB
    assert flags_byte == f.encode()


def test_record_count_big_endian():
    """rc 是大端 4 字节 uint32。"""
    frames = FrameCodec.encode(MsgType.PUB, "t", 0x01020304, b"")
    assert frames[2] == b"\x01\x02\x03\x04"


def test_decode_server_5_frames():
    """decode_server 处理 5 帧 (无 delimiter)。"""
    frames = FrameCodec.encode(MsgType.PUB, "t", 1, b"hello", "str", "none")
    server_frames = [b"identity-uuid", *frames]  # 5 帧
    decoded = FrameCodec.decode_server(server_frames)
    assert decoded.identity == b"identity-uuid"
    assert decoded.topic == "t"
    assert decoded.msg_type == MsgType.PUB
    assert decoded.ser_fmt == "str"
    assert decoded.comp == "none"
    assert decoded.record_count == 1
    assert decoded.payload == b"hello"


def test_decode_server_6_frames_with_delimiter():
    """decode_server 处理 6 帧 (含 delimiter)。"""
    frames = FrameCodec.encode(MsgType.PUB, "t", 1, b"hello", "str", "none")
    server_frames = [b"identity-uuid", b"", *frames]  # 6 帧
    decoded = FrameCodec.decode_server(server_frames)
    assert decoded.identity == b"identity-uuid"
    assert decoded.topic == "t"
    assert decoded.ser_fmt == "str"


def test_decode_server_wrong_frame_count_raises():
    """帧数不在 5-6 抛 ValueError。"""
    with pytest.raises(ValueError, match="帧数不正确"):
        FrameCodec.decode_server([b"x", b"y", b"z"])  # 3 帧


def test_encode_decode_payload_roundtrip_all_ser_comp():
    """16 组合 payload roundtrip。"""
    test_obj = {"k": "v-中文-🚀", "n": 42, "b": os.urandom(8)}
    sers = ["msgpack", "bytes", "str", "pyarrow"]
    comps = ["none", "snappy", "lz4", "zstd"]
    for ser in sers:
        for comp in comps:
            if ser == "str":
                obj = "test string"
            elif ser == "bytes":
                obj = b"test bytes"
            else:
                # msgpack / pyarrow 需要可序列化对象
                obj = test_obj if ser == "msgpack" else None
                if ser == "pyarrow":
                    import pandas as pd
                    obj = pd.DataFrame({"a": [1, 2]})
            enc = FrameCodec.encode_payload(obj, ser, comp)
            dec = FrameCodec.decode_payload(enc, ser, comp)
            if ser == "pyarrow":
                import pandas as pd
                pd.testing.assert_frame_equal(dec.to_pandas(), obj)
            else:
                assert dec == obj, f"roundtrip failed: {ser}/{comp}"
