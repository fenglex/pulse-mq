"""4 帧格式编解码。

Frame 1: topic (UTF-8 bytes)
Frame 2: meta (5 bytes)
  Byte 0: msg_type (0x01=DATA, 0x02=PING)
  Byte 1: flags (ser_fmt + comp 编码)
  Byte 2-3: record_count (big-endian uint16, 0-65535)
  Byte 4: reserved
Frame 3: timestamp (8 bytes, big-endian int64, 纳秒)
Frame 4: payload (序列化+压缩后的 bytes)
"""

from __future__ import annotations

import struct
import time
from dataclasses import dataclass
from typing import Any

from pulsemq.protocol import compression as comp_mod
from pulsemq.protocol import serialization as ser_mod
from pulsemq.protocol.flags import decode_flags, encode_flags
from pulsemq.protocol.msg_type import MsgType

# timestamp 编码：8 字节 big-endian int64
_TS_STRUCT = struct.Struct(">q")
# record_count 编码：2 字节 big-endian uint16
_RC_STRUCT = struct.Struct(">H")


@dataclass
class PulseMessage:
    """解码后的消息。"""

    topic: str
    payload: Any              # 解码后数据
    raw_payload: bytes        # 原始字节
    record_count: int         # 本帧记录数
    timestamp_ns: int         # 纳秒时间戳
    serializer: str           # 序列化格式名
    compression: str          # 压缩格式名


def encode(
    topic: str,
    data: Any,
    serializer: str = "msgpack",
    compression: str = "none",
    record_count: int = 1,
) -> list[bytes]:
    """编码数据为 4 帧。

    Returns:
        [topic_bytes, meta(5B), timestamp(8B), payload]
    """
    # 序列化 + 压缩
    serializer_obj = ser_mod.get(serializer)
    encoded = serializer_obj.serialize(data)
    compressor = comp_mod.get(compression)
    payload = compressor.compress(encoded)

    # meta 5 字节
    flags_byte = encode_flags(serializer, compression)
    rc_bytes = _RC_STRUCT.pack(record_count & 0xFFFF)
    meta = bytes([MsgType.DATA, flags_byte]) + rc_bytes + b'\x00'

    # 纳秒时间戳
    timestamp_ns = time.time_ns()
    ts_bytes = _TS_STRUCT.pack(timestamp_ns)

    return [topic.encode("utf-8"), meta, ts_bytes, payload]


def decode(frames: list[bytes]) -> PulseMessage:
    """解码 4 帧为 PulseMessage。"""
    if len(frames) != 4:
        raise ValueError(f"帧数不正确：期望 4 帧，收到 {len(frames)} 帧")

    topic = frames[0].decode("utf-8")
    meta = frames[1]
    timestamp_ns = _TS_STRUCT.unpack(frames[2])[0]
    raw_payload = frames[3]

    msg_type = meta[0]
    flags_byte = meta[1]
    record_count = _RC_STRUCT.unpack(meta[2:4])[0]

    ser_fmt, comp_name = decode_flags(flags_byte)

    # 解压 + 反序列化
    compressor = comp_mod.get(comp_name)
    decompressed = compressor.decompress(raw_payload)
    serializer = ser_mod.get(ser_fmt)
    payload = serializer.deserialize(decompressed)

    return PulseMessage(
        topic=topic,
        payload=payload,
        raw_payload=raw_payload,
        record_count=record_count,
        timestamp_ns=timestamp_ns,
        serializer=ser_fmt,
        compression=comp_name,
    )


def encode_payload(obj: Any, serializer: str = "msgpack", compression: str = "none") -> bytes:
    """序列化 + 压缩。"""
    serializer_obj = ser_mod.get(serializer)
    encoded = serializer_obj.serialize(obj)
    compressor = comp_mod.get(compression)
    return compressor.compress(encoded)


def decode_payload(data: bytes, serializer: str = "msgpack", compression: str = "none") -> Any:
    """解压 + 反序列化。"""
    compressor = comp_mod.get(compression)
    serializer_obj = ser_mod.get(serializer)
    return serializer_obj.deserialize(compressor.decompress(data))
