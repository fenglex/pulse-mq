"""msgpack 序列化器。"""

from __future__ import annotations

from typing import Any

import msgpack

from pulsemq.serialization.registry import Serializer


class MsgpackSerializer(Serializer):
    """msgpack 二进制 JSON 序列化。"""

    def serialize(self, obj: Any) -> bytes:
        return msgpack.packb(obj, use_bin_type=True)

    def deserialize(self, data: bytes) -> Any:
        return msgpack.unpackb(data, raw=False)
