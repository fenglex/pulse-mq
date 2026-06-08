"""序列化注册表 + 内置实现。

支持: str, bytes, msgpack, json, pyarrow
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from io import BytesIO
from typing import Any


# ---------------------------------------------------------------------------
# 抽象接口
# ---------------------------------------------------------------------------


class Serializer(ABC):
    """序列化器抽象接口。"""

    @abstractmethod
    def serialize(self, obj: Any) -> bytes: ...

    @abstractmethod
    def deserialize(self, data: bytes) -> Any: ...


# ---------------------------------------------------------------------------
# 序列化器实现
# ---------------------------------------------------------------------------


class StringSerializer(Serializer):
    """字符串序列化：str ↔ UTF-8 bytes。"""

    def serialize(self, obj: Any) -> bytes:
        if isinstance(obj, str):
            return obj.encode("utf-8")
        if isinstance(obj, bytes):
            return obj
        raise TypeError(f"str 序列化只接受 str 或 bytes，收到 {type(obj).__name__}")

    def deserialize(self, data: bytes) -> str:
        return data.decode("utf-8")


class MsgpackSerializer(Serializer):
    """msgpack 二进制序列化（msgspec 后端）。"""

    def serialize(self, obj: Any) -> bytes:
        import msgspec

        return msgspec.msgpack.encode(obj)

    def deserialize(self, data: bytes) -> Any:
        import msgspec

        return msgspec.msgpack.decode(data)


class JsonSerializer(Serializer):
    """JSON 文本序列化 (msgspec.json)。"""

    def serialize(self, obj: Any) -> bytes:
        import msgspec

        return msgspec.json.encode(obj)

    def deserialize(self, data: bytes) -> Any:
        import msgspec

        return msgspec.json.decode(data)


class PyArrowSerializer(Serializer):
    """PyArrow IPC 流式序列化。

    支持 pa.Table / pd.DataFrame / dict / list[dict]。
    """

    def serialize(self, obj: Any) -> bytes:
        import pyarrow as pa

        if isinstance(obj, pa.Table):
            table = obj
        else:
            import pandas as pd

            if isinstance(obj, pd.DataFrame):
                table = pa.Table.from_pandas(obj, preserve_index=False)
            elif isinstance(obj, list) and obj and isinstance(obj[0], dict):
                df = pd.DataFrame(obj)
                table = pa.Table.from_pandas(df, preserve_index=False)
            elif isinstance(obj, dict):
                df = pd.DataFrame([obj])
                table = pa.Table.from_pandas(df, preserve_index=False)
            else:
                import msgspec

                return msgspec.msgpack.encode(obj)

        sink = BytesIO()
        writer = pa.ipc.new_stream(sink, table.schema)
        writer.write_table(table)
        writer.close()
        return sink.getvalue()

    def deserialize(self, data: bytes) -> Any:
        import pyarrow as pa

        reader = pa.ipc.open_stream(BytesIO(data))
        return reader.read_all()


class BytesSerializer(Serializer):
    """纯字节透传。"""

    def serialize(self, obj: Any) -> bytes:
        if not isinstance(obj, bytes):
            raise TypeError(f"bytes 序列化只接受 bytes，收到 {type(obj).__name__}")
        return obj

    def deserialize(self, data: bytes) -> bytes:
        return data


# ---------------------------------------------------------------------------
# 注册表
# ---------------------------------------------------------------------------

_REGISTRY: dict[str, Serializer] = {}


def register(name: str, serializer: Serializer) -> None:
    _REGISTRY[name] = serializer


def get(name: str) -> Serializer:
    if name not in _REGISTRY:
        raise KeyError(f"未注册的序列化格式: {name}")
    return _REGISTRY[name]


def available() -> list[str]:
    return list(_REGISTRY.keys())


# ---------------------------------------------------------------------------
# 自动注册内置实现
# ---------------------------------------------------------------------------


def _init_builtins() -> None:
    register("str", StringSerializer())
    register("msgpack", MsgpackSerializer())
    register("json", JsonSerializer())
    register("bytes", BytesSerializer())
    try:
        register("pyarrow", PyArrowSerializer())
    except ImportError:
        pass


_init_builtins()
