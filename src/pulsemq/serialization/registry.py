"""序列化与压缩：注册表 + 内置实现。

序列化器：StringSerializer、MsgpackSerializer、PyArrowSerializer、BytesSerializer
压缩器：NoneCompressor、SnappyCompressor、Lz4Compressor、ZstdCompressor
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


class Compressor(ABC):
    """压缩器抽象接口。"""

    @abstractmethod
    def compress(self, data: bytes) -> bytes: ...

    @abstractmethod
    def decompress(self, data: bytes) -> bytes: ...


# ---------------------------------------------------------------------------
# 序列化器实现
# ---------------------------------------------------------------------------


class StringSerializer(Serializer):
    """字符串序列化：str ↔ UTF-8 bytes 互转。

    默认序列化格式，适用于文本消息、JSON 字符串等场景。
    """

    def serialize(self, obj: Any) -> bytes:
        if isinstance(obj, str):
            return obj.encode("utf-8")
        if isinstance(obj, bytes):
            return obj
        raise TypeError(f"str 序列化只接受 str 或 bytes，收到 {type(obj).__name__}")

    def deserialize(self, data: bytes) -> str:
        return data.decode("utf-8")


class MsgpackSerializer(Serializer):
    """msgpack 二进制序列化（msgspec 后端，Rust 实现）。

    不再为 DataFrame 做特化（to_dict 由调用方在 publish 前预转换），
    所有 obj 走同一 msgspec 路径，简洁且零依赖编译。
    """

    def serialize(self, obj: Any) -> bytes:
        import msgspec
        return msgspec.msgpack.encode(obj)

    def deserialize(self, data: bytes) -> Any:
        import msgspec
        return msgspec.msgpack.decode(data)


class PyArrowSerializer(Serializer):
    """PyArrow IPC 流式序列化。

    支持输入类型:
      - pa.Table / pd.DataFrame → 序列化为 Arrow IPC stream
      - dict (单条) → 自动转为 1 行 pa.Table 再序列化
    """

    def serialize(self, obj: Any) -> bytes:
        import pyarrow as pa

        if isinstance(obj, pa.Table):
            table = obj
        else:
            import pandas as pd

            if isinstance(obj, pd.DataFrame):
                table = pa.Table.from_pandas(obj, preserve_index=False)
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
        table = reader.read_all()
        return table


class BytesSerializer(Serializer):
    """纯字节透传，不做任何序列化。仅接受 bytes 类型。"""

    def serialize(self, obj: Any) -> bytes:
        if not isinstance(obj, bytes):
            raise TypeError(f"bytes 序列化只接受 bytes，收到 {type(obj).__name__}")
        return obj

    def deserialize(self, data: bytes) -> bytes:
        return data


# ---------------------------------------------------------------------------
# 压缩器实现
# ---------------------------------------------------------------------------


class NoneCompressor(Compressor):
    """不压缩，直接透传。"""

    def compress(self, data: bytes) -> bytes:
        return data

    def decompress(self, data: bytes) -> bytes:
        return data


class SnappyCompressor(Compressor):
    """Google Snappy 极速压缩。"""

    def __init__(self):
        import snappy
        self._snappy = snappy

    def compress(self, data: bytes) -> bytes:
        return self._snappy.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return self._snappy.decompress(data)


class Lz4Compressor(Compressor):
    """LZ4 极速压缩/解压。"""

    def __init__(self):
        import lz4.frame
        self._lz4_frame = lz4.frame

    def compress(self, data: bytes) -> bytes:
        return self._lz4_frame.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return self._lz4_frame.decompress(data)


class ZstdCompressor(Compressor):
    """Facebook Zstandard 高压缩比。"""

    def __init__(self):
        import zstandard as zstd
        self._zstd = zstd

    def compress(self, data: bytes) -> bytes:
        return self._zstd.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return self._zstd.decompress(data)


# ---------------------------------------------------------------------------
# 注册表
# ---------------------------------------------------------------------------


class SerializationRegistry:
    """序列化格式注册表（全局单例）。"""

    _serializers: dict[str, Serializer] = {}

    @classmethod
    def register(cls, name: str, serializer: Serializer) -> None:
        cls._serializers[name] = serializer

    @classmethod
    def get(cls, name: str) -> Serializer:
        if name not in cls._serializers:
            raise KeyError(f"未注册的序列化格式: {name}")
        return cls._serializers[name]

    @classmethod
    def list(cls) -> list[str]:
        return list(cls._serializers.keys())

    @classmethod
    def has(cls, name: str) -> bool:
        return name in cls._serializers


class CompressionRegistry:
    """压缩算法注册表（全局单例）。"""

    _compressors: dict[str, Compressor] = {}

    @classmethod
    def register(cls, name: str, compressor: Compressor) -> None:
        cls._compressors[name] = compressor

    @classmethod
    def get(cls, name: str) -> Compressor:
        if name not in cls._compressors:
            raise KeyError(f"未注册的压缩算法: {name}")
        return cls._compressors[name]

    @classmethod
    def list(cls) -> list[str]:
        return list(cls._compressors.keys())

    @classmethod
    def has(cls, name: str) -> bool:
        return name in cls._compressors


# ---------------------------------------------------------------------------
# 自动注册内置实现
# ---------------------------------------------------------------------------


def _init_builtins() -> None:
    """注册内置序列化器和压缩器。"""
    SerializationRegistry.register("str", StringSerializer())
    SerializationRegistry.register("msgpack", MsgpackSerializer())
    SerializationRegistry.register("bytes", BytesSerializer())
    SerializationRegistry.register("none", BytesSerializer())  # none 别名，等价 bytes

    try:
        SerializationRegistry.register("pyarrow", PyArrowSerializer())
    except ImportError:
        pass  # pyarrow 未安装，跳过

    CompressionRegistry.register("none", NoneCompressor())
    CompressionRegistry.register("snappy", SnappyCompressor())
    CompressionRegistry.register("lz4", Lz4Compressor())
    CompressionRegistry.register("zstd", ZstdCompressor())


# 模块加载时自动注册
_init_builtins()
