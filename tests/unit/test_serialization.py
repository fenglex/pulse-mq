import pytest
from pulsemq.serialization.registry import (
    SerializationRegistry,
    CompressionRegistry,
    Serializer,
    Compressor,
)


class DummySerializer(Serializer):
    def serialize(self, obj: bytes) -> bytes:
        return obj

    def deserialize(self, data: bytes) -> bytes:
        return data


class DummyCompressor(Compressor):
    def compress(self, data: bytes) -> bytes:
        return data

    def decompress(self, data: bytes) -> bytes:
        return data


class TestSerializationRegistry:
    def test_builtin_msgpack(self):
        ser = SerializationRegistry.get("msgpack")
        data = {"price": 15.8, "volume": 1000}
        encoded = ser.serialize(data)
        decoded = ser.deserialize(encoded)
        assert decoded == data

    def test_builtin_raw(self):
        ser = SerializationRegistry.get("raw")
        data = b"hello world"
        encoded = ser.serialize(data)
        assert encoded is data
        assert ser.deserialize(encoded) == data

    def test_register_custom(self):
        SerializationRegistry.register("test_ser", DummySerializer())
        assert SerializationRegistry.has("test_ser")
        assert SerializationRegistry.get("test_ser") is not None

    def test_list(self):
        names = SerializationRegistry.list()
        assert "msgpack" in names
        assert "raw" in names

    def test_get_nonexistent_raises(self):
        with pytest.raises(KeyError):
            SerializationRegistry.get("nonexistent")


class TestCompressionRegistry:
    def test_builtin_none(self):
        comp = CompressionRegistry.get("none")
        data = b"hello"
        assert comp.compress(data) == data
        assert comp.decompress(data) == data

    def test_register_custom(self):
        CompressionRegistry.register("test_comp", DummyCompressor())
        assert CompressionRegistry.has("test_comp")

    def test_list(self):
        names = CompressionRegistry.list()
        assert "none" in names
