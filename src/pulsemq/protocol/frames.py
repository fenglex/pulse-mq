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
        """解码服务端 ROUTER 收到的 6 帧。

        Args:
            frames: [identity, delimiter, topic, meta(2B), record_count(4B), payload]

        Raises:
            ValueError: 帧数不等于 6。
        """
        if len(frames) != 6:
            raise ValueError(
                f"帧数不正确：期望 6 帧，收到 {len(frames)} 帧"
            )

        identity = frames[0]
        # frames[1] = delimiter（空帧，跳过）
        topic = frames[2].decode("utf-8")
        meta = frames[3]
        msg_type = meta[0]
        flags = FrameFlags.decode(meta[1])
        record_count = _RECORD_COUNT_STRUCT.unpack(frames[4])[0]
        payload = frames[5]

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
        """序列化 + 压缩 payload。"""
        serializer = SerializationRegistry.get(ser_fmt)
        compressor = CompressionRegistry.get(comp)
        return compressor.compress(serializer.serialize(obj))

    @staticmethod
    def decode_payload(data: bytes, ser_fmt: str = "msgpack", comp: str = "none"):
        """解压 + 反序列化 payload。"""
        compressor = CompressionRegistry.get(comp)
        serializer = SerializationRegistry.get(ser_fmt)
        return serializer.deserialize(compressor.decompress(data))
