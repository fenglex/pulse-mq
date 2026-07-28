"""PulseMQ v2 帧格式（单 bytes 帧）。

布局: magic(2) ver(1) msg_type(1) flags(1) data_type(1) topic_len(2 BE)
      topic(N) ts(8 BE int64 ns) record_count(4 BE uint32) payload(变长) CRC32?(4)
"""

from __future__ import annotations

import struct
import time
import zlib
from dataclasses import dataclass
from typing import Any

from pulsemq.errors import FrameError, SerializationError
from pulsemq.protocol import compression, serialization
from pulsemq.protocol.flags import decode_flags, encode_flags, has_crc
from pulsemq.protocol.msg_type import DataType, MsgType

MAGIC = b"PM"
VERSION = 0x01

# 头定长部分（不含 topic 变长）：
#   magic(2)+ver(1)+msg_type(1)+flags(1)+data_type(1)+topic_len(2) = 8B
#   +ts(8)+rc(4) = 12B  → 共 20B
_HEAD_BEFORE_TOPIC = struct.Struct(">2sBBBBH")  # magic ver msg_type flags data_type topic_len
_HEAD_AFTER_TOPIC = struct.Struct(">qI")         # ts rc


@dataclass
class PulseMessage:
    """解码后的完整消息（含 payload）。"""

    topic: str
    payload: Any
    raw_payload: bytes
    record_count: int
    timestamp_ns: int
    serializer: str
    compression: str
    data_type: int = DataType.UNKNOWN
    msg_type: int = MsgType.DATA


@dataclass
class FrameHeader:
    """仅帧头部字段，不解压/不反序列化 payload。供服务端路由使用。"""
    topic: str
    record_count: int
    timestamp_ns: int
    msg_type: int
    raw_payload: bytes


def _encode_payload(obj: Any, serializer: str, compression_fmt: str) -> tuple[bytes, str]:
    """序列化 + 压缩 payload。返回 (压缩后 bytes, 实际使用的压缩算法)。

    compression_fmt="auto" 时根据序列化后 raw bytes 长度自动选择：
    <256B 用 none（压缩开销 > 节省），>=256B 用 lz4。
    """
    try:
        ser = serialization.get(serializer)
    except KeyError as e:
        raise SerializationError(f"未注册的序列化器: {e}") from e
    raw = ser.serialize(obj)
    if compression_fmt == "auto":
        compression_fmt = "none" if len(raw) < 256 else "lz4"
    try:
        comp = compression.get(compression_fmt)
    except KeyError as e:
        raise SerializationError(f"未注册的压缩算法: {e}") from e
    return comp.compress(raw), compression_fmt


def _decode_payload(raw: bytes, serializer: str, compression_fmt: str) -> Any:
    ser = serialization.get(serializer)
    comp = compression.get(compression_fmt)
    return ser.deserialize(comp.decompress(raw))


def _restore_type(data: Any, data_type: int, serializer: str) -> Any:
    """根据 data_type 还原原始 Python 类型（DataFrame / dict / str / bytes）。

    pyarrow 序列化器总是返回 ``pa.Table``；msgpack/json 下 DataFrame 被转为
    list[dict]。此函数负责将这两种情况还原为调用方期待的原始类型。
    """
    from pulsemq.protocol.msg_type import DataType

    if data_type == DataType.DATAFRAME:
        if serializer == "pyarrow":
            import pyarrow as pa
            return data.to_pandas() if isinstance(data, pa.Table) else data
        # msgpack / json: list[dict] → DataFrame
        import pandas as _pd
        if isinstance(data, list):
            return _pd.DataFrame(data)
        return data

    if data_type == DataType.DICT:
        if serializer == "pyarrow":
            import pyarrow as pa
            if hasattr(data, "to_pylist"):
                lst = data.to_pylist()
                return lst[0] if lst else data
        return data

    if data_type == DataType.STR:
        if isinstance(data, bytes):
            return data.decode("utf-8")
        return data

    if data_type == DataType.BYTES:
        if isinstance(data, str):
            return data.encode("utf-8")
        return data

    return data


def _infer_record_count(data: Any) -> int:
    """从数据对象自动推断记录数。

    - ``list`` → ``len(data)``
    - ``pandas.DataFrame`` → ``len(data)``（pandas 可用时）
    - 标量/dict/其他 → ``1``
    """
    if isinstance(data, list):
        return max(1, len(data))
    try:
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            return max(1, len(data))
    except ImportError:
        pass
    return 1


# —— 数据类型 × 序列化器兼容规则 ——
# 值: (允许的序列化器集合, 默认序列化器)
_SERIALIZER_RULES: dict[int, tuple[set[str], str]] = {
    DataType.DICT:     ({"msgpack", "json"},        "msgpack"),
    DataType.DATAFRAME:({"msgpack", "json", "pyarrow"}, "pyarrow"),
    DataType.STR:      ({"str"},                    "str"),
    DataType.BYTES:    ({"bytes"},                  "bytes"),
}


def _infer_data_type(data: Any) -> int:
    """根据 Python 类型推断 DataType 标记。"""
    try:
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            return DataType.DATAFRAME
    except ImportError:
        pass
    if isinstance(data, dict):
        return DataType.DICT
    if isinstance(data, str):
        return DataType.STR
    if isinstance(data, bytes):
        return DataType.BYTES
    return DataType.UNKNOWN


def encode(
    topic: str,
    data: Any,
    *,
    msg_type: int = MsgType.DATA,
    serializer: str | None = None,
    compression: str = "none",
    record_count: int | None = None,
    data_type: int | None = None,
    crc: bool = False,
    ts_ns: int | None = None,
) -> bytes:
    """编码数据为单 bytes 帧。

    Args:
        topic: 主题（UTF-8，最长 65535 字节）。
        data: 待编码对象。
        msg_type: 帧类型（MsgType 常量）。
        serializer: 序列化格式名。None 时根据 data_type 自动选择默认值。
        compression: 压缩格式名。
        record_count: 本帧记录数。None 时自动推断（list 取 len，Df 取行数，
            标量/dict 取 1）；显式传值则覆盖推断。最大 1,000,000。
        data_type: 原始数据类型标记（DataType 常量）。None 时自动推断。
        crc: 是否追加 CRC32 校验。
        ts_ns: 纳秒时间戳；None 表示取当前 time.time_ns()。

    Returns:
        编码后的 bytes 帧。

    Raises:
        FrameError: record_count 超限或 topic 过长。
        TypeError: serializer 与 data_type 不兼容。
        SerializationError: 未注册的序列化/压缩格式。
    """
    # ---- 1. 推断 data_type ----
    if data_type is None:
        data_type = _infer_data_type(data)

    # ---- 2. 校验 + 选择默认序列化器 ----
    if data_type in _SERIALIZER_RULES:
        allowed, default = _SERIALIZER_RULES[data_type]
        if serializer is None:
            serializer = default
        elif serializer not in allowed:
            raise TypeError(
                f"数据类型 {data_type} 不支持 serializer={serializer!r}，"
                f"可选: {sorted(allowed)}"
            )

    # 兜底：serializer 仍为 None 时用 "msgpack"（对 UNKNOWN 等不在规则内的类型）
    if serializer is None:
        serializer = "msgpack"

    # ---- 3. DataFrame + msgpack/json → 转 list[dict] 预处理 ----
    if data_type == DataType.DATAFRAME and serializer in ("msgpack", "json"):
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            data = data.to_dict(orient="records")

    # ---- 4. 常规编码 ----
    if record_count is None:
        record_count = _infer_record_count(data)
    if record_count > 1_000_000:
        raise FrameError(f"record_count 超限: {record_count}")
    ts = ts_ns if ts_ns is not None else time.time_ns()
    topic_bytes = topic.encode("utf-8")
    if len(topic_bytes) > 65535:
        raise FrameError("topic 过长")
    payload, compression = _encode_payload(data, serializer, compression)
    flags = encode_flags(serializer, compression, crc=crc)
    head = _HEAD_BEFORE_TOPIC.pack(MAGIC, VERSION, msg_type, flags, data_type,
                                   len(topic_bytes))
    tail = _HEAD_AFTER_TOPIC.pack(ts, record_count)
    body = head + topic_bytes + tail + payload
    if crc:
        body += struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)
    return body


def decode(frame: bytes) -> PulseMessage:
    """解码单 bytes 帧为 PulseMessage。

    Raises:
        FrameError: 帧过短、魔数不匹配、版本不支持、CRC 校验失败。
    """
    if len(frame) < _HEAD_BEFORE_TOPIC.size + _HEAD_AFTER_TOPIC.size:
        raise FrameError("帧过短")
    magic, ver, msg_type, flags, data_type, topic_len = _HEAD_BEFORE_TOPIC.unpack_from(frame, 0)
    if magic != MAGIC:
        raise FrameError("魔数不匹配")
    if ver != VERSION:
        raise FrameError(f"版本不支持: {ver}")
    off = _HEAD_BEFORE_TOPIC.size
    topic = frame[off:off + topic_len].decode("utf-8")
    off += topic_len
    ts, record_count = _HEAD_AFTER_TOPIC.unpack_from(frame, off)
    off += _HEAD_AFTER_TOPIC.size
    crc_on = has_crc(flags)
    if crc_on:
        if len(frame) - off < 4:
            raise FrameError("CRC 缺失")
        body, crc_val = frame[:-4], struct.unpack(">I", frame[-4:])[0]
        if (zlib.crc32(body) & 0xFFFFFFFF) != crc_val:
            raise FrameError("CRC 校验失败")
        payload = frame[off:-4]
    else:
        payload = frame[off:]
    serializer, compression_fmt = decode_flags(flags)
    data = _decode_payload(payload, serializer, compression_fmt)
    data = _restore_type(data, data_type, serializer)
    return PulseMessage(
        topic=topic,
        payload=data,
        raw_payload=payload,
        record_count=record_count,
        timestamp_ns=ts,
        serializer=serializer,
        compression=compression_fmt,
        data_type=data_type,
        msg_type=msg_type,
    )


def decode_header(frame: bytes) -> FrameHeader:
    """仅提取帧头部字段，不解压/不反序列化 payload。

    服务端 ``_data_loop`` 使用此函数获取 topic/record_count/timestamp_ns
    用于路由匹配与统计，避免 msgspec 反序列化开销（占完整 decode ~80% 时间）。
    """
    if len(frame) < _HEAD_BEFORE_TOPIC.size + _HEAD_AFTER_TOPIC.size:
        raise FrameError("帧过短")
    magic, ver, msg_type, flags, data_type, topic_len = _HEAD_BEFORE_TOPIC.unpack_from(frame, 0)
    if magic != MAGIC:
        raise FrameError("魔数不匹配")
    if ver != VERSION:
        raise FrameError(f"版本不支持: {ver}")
    off = _HEAD_BEFORE_TOPIC.size
    topic = frame[off:off + topic_len].decode("utf-8")
    off += topic_len
    ts, record_count = _HEAD_AFTER_TOPIC.unpack_from(frame, off)
    off += _HEAD_AFTER_TOPIC.size
    crc_on = has_crc(flags)
    raw_payload = frame[off:] if not crc_on else frame[off:-4]
    return FrameHeader(topic, record_count, ts, msg_type, raw_payload)


def encode_control(
    cmd: str,
    payload: dict | None = None,
    serializer: str = "msgpack",
) -> bytes:
    """编码控制帧（msg_type=CONTROL，cmd 作为 topic）。"""
    return encode(
        cmd,
        payload or {},
        msg_type=MsgType.CONTROL,
        serializer=serializer,
        compression="none",
        record_count=1,
        data_type=DataType.UNKNOWN,
    )


def decode_control(frame: bytes) -> "ControlMessage":  # noqa: F821
    """解码控制帧为 ControlMessage。

    Raises:
        FrameError: 非 CONTROL 帧。
    """
    from pulsemq.control import ControlMessage  # 函数内导入，打破循环

    msg = decode(frame)
    if msg.msg_type != MsgType.CONTROL:
        raise FrameError("非 CONTROL 帧")
    return ControlMessage(
        cmd=msg.topic,
        payload=msg.payload if isinstance(msg.payload, dict) else {},
    )


__all__ = [
    "PulseMessage",
    "FrameHeader",
    "MAGIC",
    "VERSION",
    "encode",
    "decode",
    "decode_header",
    "encode_control",
    "decode_control",
]
