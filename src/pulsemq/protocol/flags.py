"""Frame 3 Byte 1 flags bitfield 编解码。

bit[0:2] = 序列化格式 (000=msgpack, 001=bytes, 010=pyarrow, 011=protobuf)
bit[3:4] = 压缩算法   (00=none, 01=snappy, 10=lz4, 11=zstd)
bit[5]   = has_topic  (0=无topic, 1=有topic)
bit[6:7] = reserved
"""

from __future__ import annotations

from dataclasses import dataclass

# 序列化格式名 → bit[0:2] 编码
_SER_MAP: dict[str, int] = {
    "msgpack": 0b000,
    "bytes": 0b001,
    "pyarrow": 0b010,
    "protobuf": 0b011,
    "str": 0b100,
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


@dataclass
class FrameFlags:
    """Frame 3 Byte 1 的 flags 解析结果。"""

    ser_fmt: str        # 序列化格式名
    comp: str           # 压缩算法名
    has_topic: bool     # 是否有 topic

    def encode(self) -> int:
        """编码为单字节整数。"""
        ser_bits = _SER_MAP.get(self.ser_fmt, 0b000)
        comp_bits = _COMP_MAP.get(self.comp, 0b00)
        topic_bit = 0b0010_0000 if self.has_topic else 0
        return ser_bits | (comp_bits << 3) | topic_bit

    @classmethod
    def decode(cls, byte_val: int) -> FrameFlags:
        """从单字节解码。"""
        ser_bits = byte_val & 0b0000_0111
        comp_bits = (byte_val >> 3) & 0b0000_0011
        has_topic = bool(byte_val & 0b0010_0000)
        return cls(
            ser_fmt=_SER_MAP_REV.get(ser_bits, "msgpack"),
            comp=_COMP_MAP_REV.get(comp_bits, "none"),
            has_topic=has_topic,
        )
