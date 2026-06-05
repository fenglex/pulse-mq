"""序列化和压缩的注册表模式。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


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


def _init_builtins() -> None:
    """注册内置序列化器和压缩器。"""
    from pulsemq.serialization.msgpack_ser import MsgpackSerializer
    from pulsemq.serialization.raw_ser import RawSerializer

    SerializationRegistry.register("msgpack", MsgpackSerializer())
    SerializationRegistry.register("raw", RawSerializer())

    from pulsemq.serialization.compressors import (
        NoneCompressor,
        SnappyCompressor,
        Lz4Compressor,
        ZstdCompressor,
    )

    CompressionRegistry.register("none", NoneCompressor())
    CompressionRegistry.register("snappy", SnappyCompressor())
    CompressionRegistry.register("lz4", Lz4Compressor())
    CompressionRegistry.register("zstd", ZstdCompressor())


# 模块加载时自动注册
_init_builtins()
