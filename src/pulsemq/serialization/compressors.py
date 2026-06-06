"""内置压缩算法实现。"""

from __future__ import annotations

from pulsemq.serialization.registry import Compressor


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
