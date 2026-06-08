"""Frame meta 帧 Byte 1 flags bitfield 编解码。

bit[0:2] = 序列化格式 (000=msgpack, 001=bytes, 010=pyarrow, 100=str, 101=json)
bit[3:4] = 压缩算法   (00=none, 01=snappy, 10=lz4, 11=zstd)
bit[5:7] = reserved
"""

from __future__ import annotations

# 序列化格式名 → bit[0:2] 编码
_SER_MAP: dict[str, int] = {
    "msgpack": 0b000,
    "bytes": 0b001,
    "pyarrow": 0b010,
    "str": 0b100,
    "json": 0b101,
}
_SER_MAP_REV: dict[int, str] = {v: k for k, v in _SER_MAP.items()}

# 压缩算法名 → bit[3:4] 编码
_COMP_MAP: dict[str, int] = {
    "none": 0b00,
    "snappy": 0b01,
    "lz4": 0b10,
    "zstd": 0b11,
}
_COMP_MAP_REV: dict[int, str] = {v: k for k, v in _COMP_MAP.items()}


def encode_flags(ser_fmt: str, comp: str) -> int:
    """编码序列化+压缩标志为单字节。"""
    ser_bits = _SER_MAP.get(ser_fmt, 0b000)
    comp_bits = _COMP_MAP.get(comp, 0b00)
    return ser_bits | (comp_bits << 3)


def decode_flags(byte_val: int) -> tuple[str, str]:
    """解码单字节 → (ser_fmt, comp)。"""
    ser_bits = byte_val & 0b0000_0111
    comp_bits = (byte_val >> 3) & 0b0000_0011
    return (
        _SER_MAP_REV.get(ser_bits, "msgpack"),
        _COMP_MAP_REV.get(comp_bits, "none"),
    )
