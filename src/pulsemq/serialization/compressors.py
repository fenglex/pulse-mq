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

    def compress(self, data: bytes) -> bytes:
        import snappy
        return snappy.compress(data)

    def decompress(self, data: bytes) -> bytes:
        import snappy
        return snappy.decompress(data)


class Lz4Compressor(Compressor):
    """LZ4 极速压缩/解压。"""

    def compress(self, data: bytes) -> bytes:
        import lz4.frame
        return lz4.frame.compress(data)

    def decompress(self, data: bytes) -> bytes:
        import lz4.frame
        return lz4.frame.decompress(data)


class ZstdCompressor(Compressor):
    """Facebook Zstandard 高压缩比。"""

    def compress(self, data: bytes) -> bytes:
        import zstandard as zstd
        return zstd.compress(data)

    def decompress(self, data: bytes) -> bytes:
        import zstandard as zstd
        return zstd.decompress(data)
