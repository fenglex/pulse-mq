"""raw 纯字节透传序列化器。"""

from __future__ import annotations

from typing import Any

from pulsemq.serialization.registry import Serializer


class RawSerializer(Serializer):
    """不做任何序列化，直接透传 bytes。"""

    def serialize(self, obj: Any) -> bytes:
        if not isinstance(obj, bytes):
            raise TypeError(f"raw 序列化只接受 bytes，收到 {type(obj).__name__}")
        return obj

    def deserialize(self, data: bytes) -> bytes:
        return data
