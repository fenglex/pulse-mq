"""数据类型白名单与序列化器强绑定的专项测试。

覆盖：
1. 4 种白名单类型的 record_count 推断正确性
2. 白名单外类型全部抛 TypeError
3. 数据类型 ↔ 序列化器强绑定（str→str, bytes→bytes 等）
4. bytes × json 报错
5. 合法组合的端到端编解码往返
"""

from __future__ import annotations

import pandas as pd
import pyarrow as pa
import pytest

from pulsemq.publisher import PulsePublisher
from pulsemq.protocol import frames as f
from pulsemq.protocol.serialization import get as get_serializer


# ---------------------------------------------------------------------------
# record_count 推断：4 种白名单
# ---------------------------------------------------------------------------


class TestRecordCountWhitelist:
    """白名单类型的 record_count 推断。"""

    @pytest.mark.parametrize("data,expected", [
        (pd.DataFrame({"a": [1, 2, 3]}), 3),  # DataFrame → 行数
        ({"a": 1}, 1),                        # dict → 1
        ("hello", 1),                         # str → 1
        (b"\x00\x01", 1),                     # bytes → 1
    ])
    def test_whitelist_record_count(self, data, expected):
        assert PulsePublisher._infer_record_count(data) == expected

    def test_empty_list_rejected(self):
        """空 list 不被支持。"""
        with pytest.raises(TypeError, match="不支持的返回类型: list"):
            PulsePublisher._infer_record_count([])


# ---------------------------------------------------------------------------
# 白名单外类型：全部 TypeError
# ---------------------------------------------------------------------------


class TestNonWhitelistRejected:
    """白名单外类型必须抛 TypeError。"""

    @pytest.mark.parametrize("data,type_name", [
        (42, "int"),
        (3.14, "float"),
        (True, "bool"),
        (None, "NoneType"),
        (pa.Table.from_pandas(pd.DataFrame({"a": [1]})), "Table"),
        ([pd.DataFrame({"a": [1]})], "list[DataFrame]"),
        ([{"a": 1}, {"a": 2}], "list[dict]"),
        (["x", "y", "z"], "list[str]"),
        ([1, 2, 3], "list[int]"),
        ([b"x", b"y"], "list[bytes]"),
        ([{"a": 1}, "hello"], "list[混合]"),       # 混合元素
        ([pd.DataFrame({"a": [1]}), "x"], "list[混合]"),  # DataFrame + str 混合
        ({1, 2, 3}, "set"),
        ((1, 2), "tuple"),
    ])
    def test_non_whitelist_raises(self, data, type_name):
        with pytest.raises(TypeError):
            PulsePublisher._infer_record_count(data)

    def test_mixed_list_rejected(self):
        """list 元素类型不一致（混合）必须报错。"""
        with pytest.raises(TypeError, match="不支持的返回类型: list"):
            PulsePublisher._infer_record_count([{"a": 1}, "hello", {"b": 2}])


# ---------------------------------------------------------------------------
# 序列化器强绑定（方案 A）
# ---------------------------------------------------------------------------


class TestSerializerBinding:
    """数据类型 ↔ 序列化器强绑定校验。"""

    @pytest.mark.parametrize("data,ser", [
        # str → 只允许 str 序列化器
        ("hello", "str"),
        # bytes → 只允许 bytes 序列化器
        (b"\x00\x01", "bytes"),
        # 结构化数据 → msgpack/json/pyarrow
        ({"a": 1}, "msgpack"),
        ({"a": 1}, "json"),
        ({"a": 1}, "pyarrow"),
        (pd.DataFrame({"a": [1]}), "msgpack"),
        (pd.DataFrame({"a": [1]}), "pyarrow"),
    ])
    def test_valid_binding_passes(self, data, ser):
        """合法的类型↔序列化器组合不应抛错。"""
        PulsePublisher._validate_serializer(data, ser)  # 不抛即通过

    @pytest.mark.parametrize("data,ser", [
        # str 不允许 msgpack/json/pyarrow/bytes
        ("hello", "msgpack"),
        ("hello", "json"),
        ("hello", "pyarrow"),
        ("hello", "bytes"),
        # bytes 不允许 msgpack/json/pyarrow/str
        (b"\x00", "msgpack"),
        (b"\x00", "json"),
        (b"\x00", "pyarrow"),
        (b"\x00", "str"),
        # 结构化数据不允许 str/bytes
        ({"a": 1}, "str"),
        ({"a": 1}, "bytes"),
        (pd.DataFrame({"a": [1]}), "str"),
        (pd.DataFrame({"a": [1]}), "bytes"),
        # list 不再支持任何序列化器
        ([{"a": 1}], "msgpack"),
        (["a"], "json"),
        ([pd.DataFrame({"a": [1]})], "pyarrow"),
    ])
    def test_invalid_binding_raises(self, data, ser):
        """非法的类型↔序列化器组合必须抛 TypeError。"""
        with pytest.raises(TypeError):
            PulsePublisher._validate_serializer(data, ser)


# ---------------------------------------------------------------------------
# bytes × json 序列化器报错
# ---------------------------------------------------------------------------


class TestBytesJsonRejected:
    """bytes × json 组合在序列化层就报错（语义不一致保护）。"""

    def test_json_rejects_bytes(self):
        with pytest.raises(TypeError, match="json.*不支持 bytes"):
            get_serializer("json").serialize(b"\x00\x01")

    def test_json_accepts_str(self):
        """json 仍接受 str（str×json 虽然在 publisher 层被拦，但序列化器本身能处理）。"""
        result = get_serializer("json").serialize("hello")
        assert b"hello" in result


# ---------------------------------------------------------------------------
# 端到端往返：合法组合编解码一致
# ---------------------------------------------------------------------------


class TestRoundtrip:
    """合法的类型↔序列化器组合的端到端编解码往返。"""

    @pytest.mark.parametrize("data,ser", [
        (pd.DataFrame({"a": [1, 2, 3]}), "pyarrow"),
        (pd.DataFrame({"a": [1, 2, 3]}), "msgpack"),
        ({"a": 1}, "msgpack"),
        ("hello", "str"),
        (b"\x00\x01", "bytes"),
    ])
    def test_roundtrip(self, data, ser):
        """完整流程：_infer_record_count + _validate_serializer + encode + decode。"""
        rc = PulsePublisher._infer_record_count(data)
        PulsePublisher._validate_serializer(data, ser)
        payload = PulsePublisher._prepare_payload(data)
        frames = f.encode(topic="t", data=payload, serializer=ser, compression="none", record_count=rc)
        msg = f.decode(frames)
        assert msg.record_count == rc
        assert msg.serializer == ser
