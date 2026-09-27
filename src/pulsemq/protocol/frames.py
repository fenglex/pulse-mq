"""PulseMQ v3 帧格式（单 bytes 帧，唯一版本，不考虑向后兼容）。

布局（9.2.5 起，ver=0x03）:
    magic(2) ver(1) msg_type(1) flags(1) data_type(1) topic_len(1)
    seq(8 BE uint64) ts(8 BE int64 ns) record_count(4 BE uint32)
    [ack_token(4 BE uint32, flags bit6 置位时存在)]
    topic(N, N=topic_len, 最长 255 字节)
    payload(变长，帧边界 = ZMQ 消息边界，头部无 payload 长度字段)
    CRC32?(4)

- seq：服务端 per-topic 单调序号。生产者编码时占位 0，服务端按固定偏移 7
  改写后广播；所有 v3 帧都携带 seq，消费端据此做缺口检测（无条件）。
- ack_token：确认发布的回执编号（4B uint32 per-publisher 计数器，flags
  bit6 置位）。服务端据此回 PUBLISH_ACK 控制帧（回执也是消息，走数据面）。
相对 v1/v2 的变化：扩展帧并入头部（删除 ext 段与 v1/v2 双版本分流）、
topic 长度 2B→1B（上限 255 字节）、头部长度 20B→27B、结构开销有界
（最坏 27+4+255+4 = 290B，v1/v2 因 topic_len 2B 最坏 ~64KB）。
"""

from __future__ import annotations

import struct
import time
import zlib
from dataclasses import dataclass
from typing import Any

from pulsemq.errors import FrameError, SerializationError
from pulsemq.protocol import compression, serialization
from pulsemq.protocol.flags import decode_flags, encode_flags, has_ack, has_crc
from pulsemq.protocol.msg_type import DataType, MsgType

# 模块级缓存 pandas 引用，避免每次 encode 都做 import 查找（热路径优化）
try:
    import pandas as _pd
except ImportError:
    _pd = None

MAGIC = b"PM"
VERSION = 0x03

# v3 定长头（27B）：magic(2)+ver(1)+msg_type(1)+flags(1)+data_type(1)
#                   +topic_len(1)+seq(8)+ts(8)+record_count(4)
_HEAD_V3 = struct.Struct(">2sBBBBBQQI")
_SEQ_V3 = struct.Struct(">Q")   # seq 独立 pack（服务端固定偏移改写用）
_SEQ_OFFSET = 7                 # seq 字段在帧内的起始偏移
_ACK_V3 = struct.Struct(">I")   # 可选 4B 确认发布回执编号（flags bit6）
_CRC_V3 = struct.Struct(">I")

# 单帧字节上限保护：头部无 payload 长度字段，帧边界由 ZMQ 决定；
# 该上限只防误用（如把 GB 级 DataFrame 一帧发出）。默认配置下真正的
# 瓶颈在消费端共享内存环（默认 64MB，超限帧被静默丢弃）与服务端
# 订阅者缓冲（默认 10MB，慢消费者下单帧超限即被淘汰）。
MAX_FRAME_BYTES = 256 * 1024 * 1024

# topic bytes → str 缓存：同一 topic 反复 decode_header 时避免重复 UTF-8 decode。
# 有界缓存：超过上限后不再缓存新 topic（仍正确，只是退化为每次 decode）。
_TOPIC_INTERN: dict[bytes, str] = {}
_TOPIC_INTERN_MAX = 10000


def _intern_topic(topic_bytes: bytes) -> str:
    """缓存 topic bytes → str，消除每消息的 UTF-8 分配。"""
    topic = _TOPIC_INTERN.get(topic_bytes)
    if topic is None:
        topic = topic_bytes.decode("utf-8")
        if len(_TOPIC_INTERN) < _TOPIC_INTERN_MAX:
            _TOPIC_INTERN[topic_bytes] = topic
    return topic


@dataclass(slots=True)
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
    seq: int = 0


@dataclass(slots=True)
class FrameHeader:
    """仅帧头部字段，不解压/不反序列化 payload。供服务端路由使用。

    seq 为服务端分配的 per-topic 序号（上行帧编码时占位 0）；
    ack_token 仅确认发布上行帧携带（flags bit6），其余帧为 None。
    """
    topic: str
    record_count: int
    timestamp_ns: int
    msg_type: int
    raw_payload: bytes
    seq: int = 0
    ack_token: int | None = None


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
        if _pd is not None and isinstance(data, list):
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
    if _pd is not None and isinstance(data, _pd.DataFrame):
        return max(1, len(data))
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
    if _pd is not None and isinstance(data, _pd.DataFrame):
        return DataType.DATAFRAME
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
    ack_token: int | None = None,
) -> bytes:
    """编码数据为单 bytes 帧。

    Args:
        topic: 主题（UTF-8，最长 255 字节）。
        data: 待编码对象。
        msg_type: 帧类型（MsgType 常量）。
        serializer: 序列化格式名。None 时根据 data_type 自动选择默认值。
        compression: 压缩格式名。
        record_count: 本帧记录数。None 时自动推断（list 取 len，Df 取行数，
            标量/dict 取 1）；显式传值则覆盖推断。最大 1,000,000。
        data_type: 原始数据类型标记（DataType 常量）。None 时自动推断。
        crc: 是否追加 CRC32 校验。
        ts_ns: 纳秒时间戳；None 表示取当前 time.time_ns()。
        ack_token: 确认发布回执编号（0 ~ 2^32-1）。非 None 时 flags bit6
            置位，头部追加 4B 回执编号；seq 占位 0，由服务端赋值后回
            PUBLISH_ACK。

    Returns:
        编码后的 bytes 帧。

    Raises:
        FrameError: record_count 超限、topic 过长（>255 字节）、ack_token
            越界或单帧超过 MAX_FRAME_BYTES。
        TypeError: serializer 与 data_type 不兼容。
        SerializationError: 未注册的序列化/压缩格式。
    """
    # ---- 1. 推断 data_type ----
    if data_type is None:
        data_type = _infer_data_type(data)
        # 自动推断得到 UNKNOWN：非白名单类型（list/int/None/set 等），拒绝。
        # 显式传 data_type（如 encode_control 传 UNKNOWN）不经过此分支，不受影响。
        if data_type == DataType.UNKNOWN:
            raise TypeError(
                f"发送数据必须是 DataFrame/dict/bytes/str 之一，"
                f"收到 {type(data).__name__}"
            )

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
        if _pd is not None and isinstance(data, _pd.DataFrame):
            data = data.to_dict(orient="records")

    # ---- 4. 常规编码 ----
    if record_count is None:
        record_count = _infer_record_count(data)
    if record_count > 1_000_000:
        raise FrameError(f"record_count 超限: {record_count}")
    ts = ts_ns if ts_ns is not None else time.time_ns()
    topic_bytes = topic.encode("utf-8")
    if len(topic_bytes) > 255:
        raise FrameError(f"topic 过长（>255 字节）: {len(topic_bytes)}")
    payload, compression_fmt = _encode_payload(data, serializer, compression)
    flags = encode_flags(serializer, compression_fmt, crc=crc,
                         ack_token=ack_token is not None)
    head = _HEAD_V3.pack(MAGIC, VERSION, msg_type, flags, data_type,
                         len(topic_bytes), 0, ts, record_count)
    if ack_token is not None:
        if not 0 <= ack_token <= 0xFFFFFFFF:
            raise FrameError(f"ack_token 越界（0 ~ 2^32-1）: {ack_token}")
        body = head + _ACK_V3.pack(ack_token) + topic_bytes + payload
    else:
        body = head + topic_bytes + payload
    if crc:
        body += _CRC_V3.pack(zlib.crc32(body) & 0xFFFFFFFF)
    if len(body) > MAX_FRAME_BYTES:
        raise FrameError(
            f"单帧超限（{len(body)} > {MAX_FRAME_BYTES} 字节）；"
            "请拆分批次或减小 payload")
    return body


def rewrite_seq(frame_bytes: bytes, seq: int) -> bytes:
    """改写 v3 帧头 seq 字段（固定偏移 7..15，定长头内，无需重组帧）。

    服务端为每个接受的数据帧分配 per-topic seq 后调用，同一输出帧广播给
    全部订阅者。CRC 帧重算 CRC。
    """
    end = _SEQ_OFFSET + 8
    out = frame_bytes[:_SEQ_OFFSET] + _SEQ_V3.pack(seq) + frame_bytes[end:]
    if has_crc(frame_bytes[4]):
        body = out[:-4]
        return body + _CRC_V3.pack(zlib.crc32(body) & 0xFFFFFFFF)
    return out


def decode(frame: bytes) -> PulseMessage:
    """解码单 bytes 帧为 PulseMessage。

    Raises:
        FrameError: 帧过短、魔数不匹配、版本不支持、CRC 校验失败。
    """
    if len(frame) < _HEAD_V3.size:
        raise FrameError("帧过短")
    magic, ver, msg_type, flags, data_type, topic_len, seq, ts, record_count = \
        _HEAD_V3.unpack_from(frame, 0)
    if magic != MAGIC:
        raise FrameError("魔数不匹配")
    if ver != VERSION:
        raise FrameError(f"版本不支持: {ver}")
    off = _HEAD_V3.size
    if has_ack(flags):
        if len(frame) - off < 4:
            raise FrameError("ack_token 缺失")
        off += 4
    if len(frame) - off < topic_len:
        raise FrameError("topic 越界")
    topic = frame[off:off + topic_len].decode("utf-8")
    off += topic_len
    crc_on = has_crc(flags)
    if crc_on:
        if len(frame) - off < 4:
            raise FrameError("CRC 缺失")
        body, crc_val = frame[:-4], _CRC_V3.unpack_from(frame, len(frame) - 4)[0]
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
        seq=seq,
    )


def decode_header(frame: bytes) -> FrameHeader:
    """仅提取帧头部字段，不解压/不反序列化 payload。

    服务端 ``_data_loop`` 与客户端主进程接收循环使用此函数获取
    topic/record_count/timestamp_ns/seq/ack_token，避免 msgpack
    反序列化开销（占完整 decode ~80% 时间）。
    """
    if len(frame) < _HEAD_V3.size:
        raise FrameError("帧过短")
    magic, ver, msg_type, flags, data_type, topic_len, seq, ts, record_count = \
        _HEAD_V3.unpack_from(frame, 0)
    if magic != MAGIC:
        raise FrameError("魔数不匹配")
    if ver != VERSION:
        raise FrameError(f"版本不支持: {ver}")
    off = _HEAD_V3.size
    ack_token: int | None = None
    if has_ack(flags):
        if len(frame) - off < 4:
            raise FrameError("ack_token 缺失")
        (ack_token,) = _ACK_V3.unpack_from(frame, off)
        off += 4
    if len(frame) - off < topic_len:
        raise FrameError("topic 越界")
    topic = _intern_topic(frame[off:off + topic_len])
    off += topic_len
    crc_on = has_crc(flags)
    raw_payload = frame[off:] if not crc_on else frame[off:-4]
    return FrameHeader(topic, record_count, ts, msg_type, raw_payload,
                       seq=seq, ack_token=ack_token)


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
    "MAX_FRAME_BYTES",
    "encode",
    "decode",
    "decode_header",
    "encode_control",
    "decode_control",
    "rewrite_seq",
]
