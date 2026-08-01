"""压缩注册表 + 内置实现。

支持: none, snappy, lz4, zstd
"""

from __future__ import annotations

from abc import ABC, abstractmethod


# ---------------------------------------------------------------------------
# 抽象接口
# ---------------------------------------------------------------------------


class Compressor(ABC):
    """压缩器抽象接口。"""

    @abstractmethod
    def compress(self, data: bytes) -> bytes: ...

    @abstractmethod
    def decompress(self, data: bytes) -> bytes: ...


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

    def __init__(self) -> None:
        import snappy

        self._snappy = snappy

    def compress(self, data: bytes) -> bytes:
        return self._snappy.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return self._snappy.decompress(data)


class Lz4Compressor(Compressor):
    """LZ4 极速压缩/解压。"""

    def __init__(self) -> None:
        import lz4.frame

        self._lz4 = lz4.frame

    def compress(self, data: bytes) -> bytes:
        return self._lz4.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return self._lz4.decompress(data)


class ZstdCompressor(Compressor):
    """Zstandard 高压缩比。每线程独立 context（ZstdCompressor 非线程安全）。"""

    def __init__(self) -> None:
        import zstandard as zstd
        import threading

        self._zstd = zstd
        self._local = threading.local()

    def _cctx(self):
        cctx = getattr(self._local, "cctx", None)
        if cctx is None:
            cctx = self._zstd.ZstdCompressor()
            self._local.cctx = cctx
        return cctx

    def _dctx(self):
        dctx = getattr(self._local, "dctx", None)
        if dctx is None:
            dctx = self._zstd.ZstdDecompressor()
            self._local.dctx = dctx
        return dctx

    def compress(self, data: bytes) -> bytes:
        return self._cctx().compress(data)

    def decompress(self, data: bytes) -> bytes:
        return self._dctx().decompress(data)


# ---------------------------------------------------------------------------
# 注册表
# ---------------------------------------------------------------------------

_REGISTRY: dict[str, Compressor] = {}


def register(name: str, compressor: Compressor) -> None:
    _REGISTRY[name] = compressor


def get(name: str) -> Compressor:
    if name not in _REGISTRY:
        raise KeyError(f"未注册的压缩算法: {name}")
    return _REGISTRY[name]


def available() -> list[str]:
    return list(_REGISTRY.keys())


# ---------------------------------------------------------------------------
# 自动注册内置实现
# ---------------------------------------------------------------------------


def _init_builtins() -> None:
    register("none", NoneCompressor())
    register("snappy", SnappyCompressor())
    register("lz4", Lz4Compressor())
    register("zstd", ZstdCompressor())


_init_builtins()
