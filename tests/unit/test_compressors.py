import pytest
from pulsemq.serialization.registry import CompressionRegistry


class TestCompressors:
    @pytest.mark.parametrize("name", ["none", "snappy", "lz4", "zstd"])
    def test_roundtrip(self, name):
        comp = CompressionRegistry.get(name)
        data = b"hello world " * 100
        compressed = comp.compress(data)
        decompressed = comp.decompress(compressed)
        assert decompressed == data

    def test_none_is_passthrough(self):
        comp = CompressionRegistry.get("none")
        data = b"test"
        assert comp.compress(data) is data
        assert comp.decompress(data) is data

    @pytest.mark.parametrize("name", ["snappy", "lz4", "zstd"])
    def test_compressed_smaller(self, name):
        comp = CompressionRegistry.get(name)
        data = b"hello world " * 1000
        compressed = comp.compress(data)
        assert len(compressed) < len(data)
