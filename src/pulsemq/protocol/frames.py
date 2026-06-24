"""4 帧格式编解码（v3）。

Frame 1: topic (UTF-8 bytes)
Frame 2: meta (7 bytes)
  Byte 0: msg_type (0x01=DATA, 0x02=PING)
  Byte 1: flags (ser_fmt + comp 编码)
  Byte 2: data_type (原始数据类型标记，v3 新增)
  Byte 3-6: record_count (big-endian uint32, 0-4294967295)
Frame 3: timestamp (8 bytes, big-endian int64, 纳秒)
Frame 4: payload (序列化+压缩后的 bytes)

v3 Breaking Change：meta 帧从 6 字节扩展到 7 字节，新增 Byte 2 = data_type，
让 sub 端能还原 pub 端的原始 Python 类型（如 DataFrame）。
"""

from __future__ import annotations

import struct
import time
from dataclasses import dataclass
from typing import Any

from pulsemq.protocol import compression as comp_mod
from pulsemq.protocol import serialization as ser_mod
from pulsemq.protocol.flags import decode_flags, encode_flags
from pulsemq.protocol.msg_type import DataType, MsgType

# timestamp 编码：8 字节 big-endian int64
_TS_STRUCT = struct.Struct(">q")
# record_count 编码：4 字节 big-endian uint32
_RC_STRUCT = struct.Struct(">I")


@dataclass
class PulseMessage:
    """解码后的消息。"""

    topic: str
    payload: Any              # 解码后数据（已按 data_type 还原为原始类型）
    raw_payload: bytes        # 原始字节
    record_count: int         # 本帧记录数
    timestamp_ns: int         # 纳秒时间戳
    serializer: str           # 序列化格式名
    compression: str          # 压缩格式名
    data_type: int = DataType.UNKNOWN  # 原始数据类型标记（v3 新增）


def encode(
    topic: str,
    data: Any,
    serializer: str = "msgpack",
    compression: str = "none",
    record_count: int = 1,
    data_type: int = DataType.UNKNOWN,
) -> list[bytes]:
    """编码数据为 4 帧。

    Args:
        record_count: 本帧记录数，最大 1,000,000。
        data_type: 原始数据类型标记（DataType 常量），sub 端据此还原原始类型。

    Returns:
        [topic_bytes, meta(7B), timestamp(8B), payload]

    Raises:
        ValueError: record_count > 1,000,000。
    """
    if record_count > 1_000_000:
        raise ValueError(f"单批次最大 1,000,000 条记录，收到 {record_count:,}")
    # 序列化 + 压缩
    serializer_obj = ser_mod.get(serializer)
    encoded = serializer_obj.serialize(data)
    compressor = comp_mod.get(compression)
    payload = compressor.compress(encoded)

    # meta 7 字节（v3：新增 Byte 2 = data_type）
    flags_byte = encode_flags(serializer, compression)
    rc_bytes = _RC_STRUCT.pack(record_count & 0xFFFFFFFF)
    meta = bytes([MsgType.DATA, flags_byte, data_type & 0xFF]) + rc_bytes

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
    data_type = meta[2] if len(meta) >= 3 else DataType.UNKNOWN
    record_count = _RC_STRUCT.unpack(meta[3:7])[0] if len(meta) >= 7 else 0

    ser_fmt, comp_name = decode_flags(flags_byte)

    # 解压 + 反序列化
    compressor = comp_mod.get(comp_name)
    decompressed = compressor.decompress(raw_payload)
    serializer = ser_mod.get(ser_fmt)
    payload = serializer.deserialize(decompressed)

    # 按 data_type 还原原始 Python 类型（v3 新增）
    payload = _restore_type(payload, data_type)

    return PulseMessage(
        topic=topic,
        payload=payload,
        raw_payload=raw_payload,
        record_count=record_count,
        timestamp_ns=timestamp_ns,
        serializer=ser_fmt,
        compression=comp_name,
        data_type=data_type,
    )


def _restore_type(payload: Any, data_type: int) -> Any:
    """按 meta 记录的原始类型标记，把反序列化结果还原为原始 Python 类型。

    解决两类类型变形：
    - pub 端 _prepare_payload 把 DataFrame 转 list[dict]（msgpack/json 路径）
    - pyarrow 反序列化统一返回 pa.Table（pyarrow 路径）

    据此标记把 list[dict] / pa.Table 还原为 DataFrame / dict。
    STR / BYTES / UNKNOWN：原样返回（本身不变形）。
    """
    # 无需还原的类型（序列化器天然保真）
    if data_type in (DataType.STR, DataType.BYTES, DataType.UNKNOWN):
        return payload

    try:
        import pandas as pd
    except ImportError:
        pd = None  # type: ignore[assignment]

    # 把 payload 规整为 list[dict]（无论来自 msgpack/json 还是 pyarrow）
    rows: list | None = None
    if hasattr(payload, "to_pylist") and not isinstance(payload, list):
        # pa.Table → list[dict]
        rows = payload.to_pylist()
    elif isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = [payload]

    if data_type == DataType.DATAFRAME:
        if pd is not None and rows is not None:
            return pd.DataFrame(rows)
        return payload  # pandas 缺失：退回原样

    if data_type == DataType.DICT:
        # pyarrow 单 dict 路径：Table(1行) → dict
        if rows is not None:
            return rows[0] if rows else {}
        return payload

    return payload


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


def encode_heartbeat() -> list[bytes]:
    """编码心跳帧（PING 类型，空载荷）。

    4 帧格式：
      Frame 1: topic = b"__pulse_hb__"
      Frame 2: meta (7B) = [PING, flags(msgpack|none), data_type=UNKNOWN, record_count=0]
      Frame 3: timestamp (8B) = 当前纳秒
      Frame 4: payload = b""（空）

    meta 帧布局与 DATA 帧一致（v3 统一 7 字节，Byte 2 = data_type），
    保证 decode() 读取 meta[2]/meta[3:7] 时不会越界。
    """
    flags_byte = encode_flags("msgpack", "none")
    rc_bytes = _RC_STRUCT.pack(0)
    meta = bytes([MsgType.PING, flags_byte, DataType.UNKNOWN]) + rc_bytes
    ts_bytes = _TS_STRUCT.pack(time.time_ns())
    return [b"__pulse_hb__", meta, ts_bytes, b""]
