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


def test_decode_comp_bits_full_coverage():
    """comp_bits 4 个值 (0-3) 必须正确解码到 none/snappy/lz4/zstd。

    通过构造让 comp_bits 分别为 0b00/0b01/0b10/0b11 的字节验证映射:
    - comp_bits 占据 bit[3:4], 因此目标字节 = comp_bits << 3
    - 0x00 << 3 = 0x00, 0x01 << 3 = 0x08, 0x02 << 3 = 0x10, 0x03 << 3 = 0x18
    """
    # comp_bits=0b11, ser_bits=0b111(已知未知→msgpack), 整字节 = 0xFF
    assert FrameFlags.decode(0xFF).comp == "zstd"
    # comp_bits=0b00, 其余位为 0 → 0x00
    assert FrameFlags.decode(0x00).comp == "none"
    # comp_bits=0b00, 高位清零 → 0xE0 (has_topic 置位但 comp 高位不受影响)
    # 0xE0 = 0b11100000, >> 3 = 0b11100, & 0b11 = 0b00 → "none"
    assert FrameFlags.decode(0xE0).comp == "none"
    # comp_bits=0b01 → 整字节至少 0x08
    # 0x08 = 0b00001000, >> 3 = 0b00001, & 0b11 = 0b01 → "snappy"
    assert FrameFlags.decode(0x08).comp == "snappy"
    # comp_bits=0b10 → 整字节至少 0x10
    # 0x10 = 0b00010000, >> 3 = 0b00010, & 0b11 = 0b10 → "lz4"
    assert FrameFlags.decode(0x10).comp == "lz4"


@pytest.mark.parametrize("byte_val", [0, 1, 127, 128, 255])
def test_decode_never_crashes(byte_val):
    """任何单字节都不应让 decode 抛异常。"""
    f = FrameFlags.decode(byte_val)
    assert f is not None
