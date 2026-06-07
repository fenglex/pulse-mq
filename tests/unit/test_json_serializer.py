"""JsonSerializer 单测。

测: 注册表查找, 5 种类型 roundtrip, 空对象, 特殊字符, 嵌套结构。
"""
from __future__ import annotations

import pandas as pd
import pytest

from pulsemq.serialization.registry import (
    JsonSerializer,
    SerializationRegistry,
)


def test_json_serializer_registered():
    """json 序列化器已注册到 SerializationRegistry。"""
    s = SerializationRegistry.get("json")
    assert s is not None
    assert isinstance(s, JsonSerializer)


def test_json_dict_roundtrip():
    s = JsonSerializer()
    obj = {"a": [1, 2, 3], "b": "中文-🚀", "c": None, "d": True}
    enc = s.serialize(obj)
    assert isinstance(enc, bytes)
    dec = s.deserialize(enc)
    assert dec == obj


def test_json_list_roundtrip():
    s = JsonSerializer()
    obj = [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    dec = s.deserialize(s.serialize(obj))
    assert dec == obj


def test_json_dataframe():
    """DataFrame 转 list[dict] → JSON → 反序列化结果一致。"""
    s = JsonSerializer()
    df = pd.DataFrame({"id": [1, 2], "name": ["alice", "bob"], "price": [1.5, 2.5]})
    enc = s.serialize(df.to_dict(orient="records"))
    dec = s.deserialize(enc)
    assert dec == df.to_dict(orient="records")


def test_json_special_chars():
    s = JsonSerializer()
    obj = {"emoji": "🚀", "chinese": "中文", "quote": "'\"\\", "tab": "\t"}
    dec = s.deserialize(s.serialize(obj))
    assert dec == obj


def test_json_nested():
    s = JsonSerializer()
    obj = {"level1": {"level2": {"level3": [1, {"a": "b"}]}}}
    dec = s.deserialize(s.serialize(obj))
    assert dec == obj


def test_json_empty():
    s = JsonSerializer()
    assert s.deserialize(s.serialize({})) == {}
    assert s.deserialize(s.serialize([])) == []


def test_json_numbers():
    """整数、浮点、负数、零、极大值 roundtrip。"""
    s = JsonSerializer()
    obj = {"i": 0, "neg": -42, "big": 10**18, "pi": 3.14159, "neg_float": -0.001}
    dec = s.deserialize(s.serialize(obj))
    assert dec == obj


def test_json_unicode():
    """unicode 多字符 + emoji 混合 roundtrip。"""
    s = JsonSerializer()
    obj = {"jp": "こんにちは", "ar": "مرحبا", "emoji": "🎉🚀🌟"}
    dec = s.deserialize(s.serialize(obj))
    assert dec == obj


def test_json_via_framecodec():
    """FrameCodec.encode_payload / decode_payload 走 json 完整路径。"""
    from pulsemq.protocol.frames import FrameCodec

    obj = {"k": "v", "n": [1, 2, 3]}
    for comp in ("none", "snappy", "lz4", "zstd"):
        enc = FrameCodec.encode_payload(obj, "json", comp)
        dec = FrameCodec.decode_payload(enc, "json", comp)
        assert dec == obj, f"roundtrip failed: json/{comp}"
