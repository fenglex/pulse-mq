"""Cython 优化路径测试。

1. 验证 Cython 扩展可加载 (或 fallback 到纯 Python)
2. 验证 Cython 输出与纯 Python ``to_dict + msgspec`` 完全一致
3. 测多列/混合类型/空 DF/单行/大批
4. 验证 FrameCodec.encode_payload 集成 (DataFrame + msgpack 走 Cython 路径)
"""
from __future__ import annotations

import msgspec
import pandas as pd
import pytest

from pulsemq.protocol.frames import FrameCodec
from pulsemq.serialization._df_msgpack_loader import (
    encode_dataframe_to_msgpack,
    is_using_cython,
)
from pulsemq.serialization._df_msgpack_py import (
    encode_dataframe_to_msgpack as py_encode,
)


def test_loader_works():
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    result = encode_dataframe_to_msgpack(df)
    assert isinstance(result, bytes)
    assert len(result) > 0


def test_output_matches_pure_python():
    """Cython 输出与纯 Python to_dict+msgpack 输出一致."""
    df = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["alice", "bob", "charlie"],
            "price": [1.5, 2.5, 3.5],
            "flag": [True, False, True],
            "data": [b"\x00\x01", b"\x02\x03", b"\x04\x05"],
        }
    )
    cython_out = encode_dataframe_to_msgpack(df)
    py_out = py_encode(df)
    assert cython_out == py_out


def test_empty_dataframe():
    df = pd.DataFrame({"a": [], "b": []})
    result = encode_dataframe_to_msgpack(df)
    expected = msgspec.msgpack.encode([])
    assert result == expected


def test_single_row():
    df = pd.DataFrame({"x": [42]})
    result = encode_dataframe_to_msgpack(df)
    decoded = msgspec.msgpack.decode(result)
    assert decoded == [{"x": 42}]


def test_large_dataframe():
    df = pd.DataFrame(
        {
            "i": list(range(10000)),
            "s": [f"row{i}" for i in range(10000)],
        }
    )
    result = encode_dataframe_to_msgpack(df)
    decoded = msgspec.msgpack.decode(result)
    assert len(decoded) == 10000
    assert decoded[0] == {"i": 0, "s": "row0"}
    assert decoded[9999] == {"i": 9999, "s": "row9999"}


def test_mixed_types():
    df = pd.DataFrame(
        {
            "int": [1, 2],
            "float": [1.5, 2.5],
            "str": ["a", "b"],
            "bool": [True, False],
            "bytes": [b"x", b"y"],
            "none": [None, None],
        }
    )
    result = encode_dataframe_to_msgpack(df)
    decoded = msgspec.msgpack.decode(result)
    assert decoded[0]["none"] is None
    assert decoded[0]["int"] == 1
    assert decoded[0]["float"] == 1.5
    assert decoded[0]["str"] == "a"
    assert decoded[0]["bool"] is True
    assert decoded[0]["bytes"] == b"x"


def test_use_bin_type_false():
    """use_bin_type=False 时 str → raw, 不影响最终结果兼容性 (raw 在 unpackb raw=False 时仍解码为 str)."""
    df = pd.DataFrame({"s": ["hello", "world"]})
    result = encode_dataframe_to_msgpack(df, use_bin_type=False)
    decoded = msgspec.msgpack.decode(result)
    assert decoded == [{"s": "hello"}, {"s": "world"}]


def test_int32_column():
    """numpy int32 元素经 .item() 转 Python int."""
    df = pd.DataFrame({"x": pd.array([1, 2, 3], dtype="int32")})
    result = encode_dataframe_to_msgpack(df)
    decoded = msgspec.msgpack.decode(result)
    assert decoded == [{"x": 1}, {"x": 2}, {"x": 3}]


def test_float32_column():
    df = pd.DataFrame({"x": pd.array([1.5, 2.5], dtype="float32")})
    result = encode_dataframe_to_msgpack(df)
    decoded = msgspec.msgpack.decode(result)
    assert decoded == [{"x": 1.5}, {"x": 2.5}]


def test_frame_codec_integration():
    """FrameCodec.encode_payload(DataFrame, msgpack, none) 走 Cython 路径."""
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    payload = FrameCodec.encode_payload(df, "msgpack", "none")
    expected = encode_dataframe_to_msgpack(df)
    assert payload == expected
    # 解码回来正确
    decoded = FrameCodec.decode_payload(payload, "msgpack", "none")
    assert decoded == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]


def test_frame_codec_dataframe_with_compression():
    """DataFrame + msgpack + snappy 压缩路径走 Cython 序列化 + 压缩."""
    df = pd.DataFrame({"x": list(range(100))})
    payload = FrameCodec.encode_payload(df, "msgpack", "snappy")
    # 解码回来正确
    decoded = FrameCodec.decode_payload(payload, "msgpack", "snappy")
    assert decoded[0] == {"x": 0}
    assert decoded[99] == {"x": 99}


def test_frame_codec_non_dataframe_msgpack():
    """非 DataFrame + msgpack 仍走原 serializer (兼容性)."""
    data = [{"x": 1}, {"x": 2}]
    payload = FrameCodec.encode_payload(data, "msgpack", "none")
    decoded = FrameCodec.decode_payload(payload, "msgpack", "none")
    assert decoded == data


def test_loader_status():
    """is_using_cython 返回 bool."""
    assert isinstance(is_using_cython(), bool)
