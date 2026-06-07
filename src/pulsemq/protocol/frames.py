"""固定 6 帧格式的编解码。

客户端发送 4 帧: [topic][meta(2B)][record_count(4B)][payload]
ZMQ 自动附加:   [identity][delimiter] + 客户端 4 帧 = 服务端收到 6 帧

服务端广播 4 帧: [topic][meta(2B)][record_count(4B)][payload]
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

from pulsemq.protocol.flags import FrameFlags
from pulsemq.protocol.msg_type import MsgType
from pulsemq.serialization.registry import SerializationRegistry, CompressionRegistry

# record_count 编码格式：4 字节 big-endian uint32
_RECORD_COUNT_STRUCT = struct.Struct(">I")


def _is_dataframe(obj) -> bool:
    """检查 obj 是否是 pandas DataFrame (避免对非 df 误判)."""
    cls = type(obj)
    if cls.__name__ == "DataFrame" and (
        cls.__module__ == "pandas" or cls.__module__.startswith("pandas.")
    ):
        return True
    return False


@dataclass
class DecodedFrame:
    """解码后的帧数据。"""

    identity: bytes       # ZMQ identity
    topic: str            # Frame 2
    msg_type: int         # Frame 3 Byte 0
    flags: FrameFlags     # Frame 3 Byte 1 解析结果
    record_count: int     # Frame 4
    payload: bytes        # Frame 5
    has_topic: bool       # topic 是否非空
    ser_fmt: str          # 序列化格式名
    comp: str             # 压缩算法名


class FrameCodec:
    """帧编解码器。"""

    @staticmethod
    def encode(
        msg_type: int,
        topic: str,
        record_count: int,
        payload: bytes,
        ser_fmt: str = "msgpack",
        comp: str = "none",
    ) -> list[bytes]:
        """编码为 4 帧（客户端发送或服务端广播）。

        Returns:
            [topic_bytes, meta_bytes(2B), record_count_bytes(4B), payload_bytes]
        """
        has_topic = bool(topic)
        flags = FrameFlags(ser_fmt=ser_fmt, comp=comp, has_topic=has_topic)
        meta = bytes([msg_type, flags.encode()])
        rc_bytes = _RECORD_COUNT_STRUCT.pack(record_count)
        return [topic.encode("utf-8"), meta, rc_bytes, payload]

    @staticmethod
    def decode_server(frames: list[bytes]) -> DecodedFrame:
        """解码服务端 ROUTER 收到的帧。

        支持两种情况:
            5 帧: [identity, topic, meta(2B), record_count(4B), payload]
                  DEALER → ROUTER（无 delimiter）
            6 帧: [identity, delimiter, topic, meta(2B), record_count(4B), payload]
                  ROUTER 路由信封（含 delimiter）

        Raises:
            ValueError: 帧数不在 5-6 范围内。
        """
        if len(frames) == 6:
            # 含 delimiter
            identity = frames[0]
            # frames[1] = delimiter（空帧，跳过）
            topic = frames[2].decode("utf-8")
            meta = frames[3]
            record_count_raw = frames[4]
            payload = frames[5]
        elif len(frames) == 5:
            # 无 delimiter（DEALER 直连 ROUTER）
            identity = frames[0]
            topic = frames[1].decode("utf-8")
            meta = frames[2]
            record_count_raw = frames[3]
            payload = frames[4]
        else:
            raise ValueError(
                f"帧数不正确：期望 5-6 帧，收到 {len(frames)} 帧"
            )

        msg_type = meta[0]
        flags = FrameFlags.decode(meta[1])
        record_count = _RECORD_COUNT_STRUCT.unpack(record_count_raw)[0]

        return DecodedFrame(
            identity=identity,
            topic=topic,
            msg_type=msg_type,
            flags=flags,
            record_count=record_count,
            payload=payload,
            has_topic=flags.has_topic,
            ser_fmt=flags.ser_fmt,
            comp=flags.comp,
        )

    @staticmethod
    def encode_payload(obj, ser_fmt: str = "msgpack", comp: str = "none") -> bytes:
        """序列化 + 压缩 payload。

        DataFrame + msgpack 走 Cython 加速路径 (loader 自动 fallback 到纯 Python).
        """
        if ser_fmt == "msgpack" and _is_dataframe(obj):
            from pulsemq.serialization._df_msgpack_loader import (
                encode_dataframe_to_msgpack,
            )
            encoded = encode_dataframe_to_msgpack(obj, use_bin_type=True)
        else:
            serializer = SerializationRegistry.get(ser_fmt)
            encoded = serializer.serialize(obj)
        compressor = CompressionRegistry.get(comp)
        return compressor.compress(encoded)

    @staticmethod
    def decode_payload(data: bytes, ser_fmt: str = "msgpack", comp: str = "none"):
        """解压 + 反序列化 payload。"""
        compressor = CompressionRegistry.get(comp)
        serializer = SerializationRegistry.get(ser_fmt)
        return serializer.deserialize(compressor.decompress(data))

    @staticmethod
    def encode_batch_payload(payloads, comp: str = "none") -> bytes:
        """批量编码：msgpack 编码 list[(ser_fmt, payload)] 后压缩。

        BATCH 协议的 payload 部分：msgpack(list[N (ser_fmt, payload_bytes)])，再压缩。
        每条 PUB 在 client 端按各自 ser_fmt 序列化为 bytes, 这里把 ser_fmt 也编码进 batch,
        避免 server 端无法反序列化的歧义 (BATCH 帧外层 flags 只能表示 1 种 ser_fmt).

        Args:
            payloads: list[(ser_fmt, payload_bytes), ...], 每条 PUB 预序列化的结果.
        """
        import msgspec
        compressor = CompressionRegistry.get(comp)
        # msgpack 不能直接序列化 str/binary 的混合 tuple, 但 (str, bytes) 可以
        wrapped = [(sf, p) for sf, p in payloads]
        encoded_list = msgspec.msgpack.encode(wrapped)
        return compressor.compress(encoded_list)

    @staticmethod
    def decode_batch_payload(data: bytes, comp: str = "none") -> list:
        """批量解码：先解压，再 msgpack 解码 list[(ser_fmt, payload)]。

        Returns:
            list of (ser_fmt, payload_bytes) tuples.
        """
        import msgspec
        compressor = CompressionRegistry.get(comp)
        decompressed = compressor.decompress(data)
        raw = msgspec.msgpack.decode(decompressed)
        # raw 是 list[tuple[str, bytes]]
        return [(item[0], item[1]) for item in raw]
