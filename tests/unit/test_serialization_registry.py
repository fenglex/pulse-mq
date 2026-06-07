"""SerializationRegistry 与 CompressionRegistry 单测。"""
from __future__ import annotations

import os

import pytest

from pulsemq.serialization.registry import (
    SerializationRegistry,
    CompressionRegistry,
    StringSerializer,
    MsgpackSerializer,
    BytesSerializer,
    PyArrowSerializer,
    NoneCompressor,
    SnappyCompressor,
    Lz4Compressor,
    ZstdCompressor,
)


# ---- 序列化器 ----

def test_string_serializer_roundtrip():
    s = StringSerializer()
    assert s.deserialize(s.serialize("hello-世界")) == "hello-世界"


def test_string_serializer_rejects_int():
    """StringSerializer.serialize 只接受 str 或 bytes, 其它类型抛 TypeError。"""
    s = StringSerializer()
    with pytest.raises(TypeError):
        s.serialize(42)


def test_bytes_serializer_roundtrip():
    s = BytesSerializer()
    data = os.urandom(64)
    assert s.deserialize(s.serialize(data)) == data


def test_bytes_serializer_rejects_str():
    """BytesSerializer.serialize 只接受 bytes, 字符串抛 TypeError。"""
    s = BytesSerializer()
    with pytest.raises(TypeError):
        s.serialize("not bytes")


def test_msgpack_serializer_roundtrip():
    s = MsgpackSerializer()
    obj = {"a": [1, 2, 3], "b": "中文"}
    decoded = s.deserialize(s.serialize(obj))
    assert decoded == obj


def test_pyarrow_serializer_dataframe():
    """PyArrowSerializer 支持 pd.DataFrame 输入, 返 pa.Table。"""
    import pandas as pd
    s = PyArrowSerializer()
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    enc = s.serialize(df)
    dec = s.deserialize(enc)
    pd.testing.assert_frame_equal(dec.to_pandas(), df)


def test_pyarrow_serializer_dict_single_row():
    """PyArrowSerializer 支持 dict (单条) → 1 行 pa.Table。"""
    import pandas as pd
    s = PyArrowSerializer()
    obj = {"a": 1, "b": "x"}
    enc = s.serialize(obj)
    dec = s.deserialize(enc)
    assert isinstance(dec.to_pandas(), pd.DataFrame)
    assert dec.to_pandas().iloc[0].to_dict() == obj


# ---- 压缩器 ----

def test_no_compressor_roundtrip():
    c = NoneCompressor()
    data = b"hello"
    assert c.decompress(c.compress(data)) == data


def test_no_compressor_passthrough():
    """NoneCompressor 不改变字节。"""
    c = NoneCompressor()
    data = b"abc"
    assert c.compress(data) == data


def test_snappy_compressor_roundtrip():
    c = SnappyCompressor()
    data = b"a" * 1024
    assert c.decompress(c.compress(data)) == data


def test_lz4_compressor_roundtrip():
    c = Lz4Compressor()
    data = b"b" * 1024
    assert c.decompress(c.compress(data)) == data


def test_zstd_compressor_roundtrip():
    c = ZstdCompressor()
    data = b"c" * 1024
    assert c.decompress(c.compress(data)) == data


def test_zstd_actually_compresses():
    """高度可压缩的数据应能压缩到比原数据小。"""
    c = ZstdCompressor()
    data = b"x" * 10000
    compressed = c.compress(data)
    assert len(compressed) < len(data)


# ---- Registry 查找 ----

def test_registry_lookup_all_ser():
    """4 个内置序列化器全部能通过 name 查到 (str/bytes/msgpack/pyarrow)。
    注意: registry 中没有 'protobuf', 因为当前实现未注册 ProtobufSerializer。
    """
    for name in ("str", "bytes", "msgpack", "pyarrow"):
        s = SerializationRegistry.get(name)
        assert s is not None, f"未注册的 serializer: {name}"


def test_registry_lookup_all_comp():
    """4 个压缩器全部能通过 name 查到。"""
    for name in ("none", "snappy", "lz4", "zstd"):
        c = CompressionRegistry.get(name)
        assert c is not None, f"未注册的 compressor: {name}"


def test_registry_unknown_ser_raises():
    """未注册的 serializer 应抛 KeyError。"""
    with pytest.raises(KeyError, match="未注册的序列化格式"):
        SerializationRegistry.get("nonexistent")


def test_registry_unknown_comp_raises():
    """未注册的 compressor 应抛 KeyError。"""
    with pytest.raises(KeyError, match="未注册的压缩算法"):
        CompressionRegistry.get("nonexistent")


def test_registry_ser_none_is_bytes_alias():
    """'none' 与 'bytes' 是等价别名, 序列化同一 bytes 产出相同结果。"""
    a = SerializationRegistry.get("none")
    b = SerializationRegistry.get("bytes")
    data = b"abc"
    assert a.serialize(data) == b.serialize(data) == data
    assert a.deserialize(data) == b.deserialize(data) == data


# ---- 16 组合 roundtrip ----

@pytest.mark.parametrize("ser", ["str", "bytes", "msgpack", "pyarrow"])
@pytest.mark.parametrize("comp", ["none", "snappy", "lz4", "zstd"])
def test_full_roundtrip(ser, comp):
    """FrameCodec.encode_payload + decode_payload 16 组合。"""
    from pulsemq.protocol.frames import FrameCodec
    if ser == "str":
        obj = "test"
    elif ser == "bytes":
        obj = b"test"
    elif ser == "msgpack":
        obj = {"k": "v"}
    elif ser == "pyarrow":
        import pandas as pd
        obj = pd.DataFrame({"a": [1]})
    enc = FrameCodec.encode_payload(obj, ser, comp)
    dec = FrameCodec.decode_payload(enc, ser, comp)
    if ser == "pyarrow":
        import pandas as pd
        pd.testing.assert_frame_equal(dec.to_pandas(), obj)
    else:
        assert dec == obj, f"roundtrip failed: {ser}/{comp}"
