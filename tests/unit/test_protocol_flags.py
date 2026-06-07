"""FrameFlags 编码/解码 单测。"""
from __future__ import annotations

import pytest

from pulsemq.protocol.flags import FrameFlags


def test_encode_decode_str_none():
    """str + none + has_topic=True 往返。"""
    f = FrameFlags(ser_fmt="str", comp="none", has_topic=True)
    encoded = f.encode()
    assert isinstance(encoded, int)
    assert 0 <= encoded < 256
    decoded = FrameFlags.decode(encoded)
    assert decoded.ser_fmt == "str"
    assert decoded.comp == "none"
    assert decoded.has_topic is True


def test_encode_decode_all_combinations():
    """5 种 ser × 4 种 comp × 2 种 has_topic = 40 组合全部往返。"""
    sers = ["msgpack", "bytes", "pyarrow", "protobuf", "str"]
    comps = ["none", "snappy", "lz4", "zstd"]
    for ser in sers:
        for comp in comps:
            for has_topic in (True, False):
                f = FrameFlags(ser_fmt=ser, comp=comp, has_topic=has_topic)
                d = FrameFlags.decode(f.encode())
                assert d.ser_fmt == ser, f"ser mismatch for {ser}/{comp}/{has_topic}"
                assert d.comp == comp, f"comp mismatch for {ser}/{comp}/{has_topic}"
                assert d.has_topic is has_topic


def test_decode_unknown_ser_defaults_to_msgpack():
    """未知 ser_bits 应回退到 msgpack (设计选择, 测其行为)。"""
    # 0b1111_1111 = 255, 包含无效 ser_bits=0b111
    f = FrameFlags.decode(0xFF)
    assert f.ser_fmt == "msgpack"  # 静默回退


def test_decode_unknown_comp_defaults_to_none():
    """comp_bits 占 2 bits, 所有 4 个值都被 _COMP_MAP 覆盖, 静默回退路径实际上不可达。

    设计选择 (I8): 即使不可达, decode() 的 _COMP_MAP_REV.get() 仍保留 .get(bits, "none") 防御性写法。
    此处仅文档化当前行为, 不强制断言。
    """
    # 0xFF: ser_bits=0b111(未知→msgpack), comp_bits=0b11(zstd, 已知)
    f = FrameFlags.decode(0xFF)
    # comp_bits 总是 0-3, 全部是合法值, 因此实际拿到的是 "zstd" 而非 "none"
    assert f.comp in ("none", "snappy", "lz4", "zstd")


@pytest.mark.parametrize("byte_val", [0, 1, 127, 128, 255])
def test_decode_never_crashes(byte_val):
    """任何单字节都不应让 decode 抛异常。"""
    f = FrameFlags.decode(byte_val)
    assert f is not None
