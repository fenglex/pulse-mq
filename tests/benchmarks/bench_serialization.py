"""序列化 + 压缩性能基准测试。

测试维度:
- 序列化格式: msgpack, raw
- 压缩格式: none, snappy, lz4, zstd
- 数据集: 单条行情 / 批量行情
"""

import pytest

from pulsemq.serialization.registry import SerializationRegistry, CompressionRegistry
from pulsemq.protocol.frames import FrameCodec
from tests.benchmarks.data_generators import get_preset_single, get_preset_batch
from tests.benchmarks.conftest import BenchResult


# ---- 数据集 fixtures ----

@pytest.fixture(scope="module")
def single_snapshot():
    return get_preset_single()


@pytest.fixture(scope="module")
def batch_100():
    return get_preset_batch(100)


@pytest.fixture(scope="module")
def batch_1000():
    return get_preset_batch(1000)


# ---- 序列化吞吐 ----

class TestSerializationThroughput:
    """纯序列化吞吐测试（不含压缩）。"""

    ITERATIONS = 10_000

    def test_msgpack_serialize_throughput(self, single_snapshot):
        ser = SerializationRegistry.get("msgpack")
        data = single_snapshot

        with BenchResult("msgpack serialize") as br:
            for _ in range(self.ITERATIONS):
                ser.serialize(data)
            br.set_ops(self.ITERATIONS)
        print(br.report())
        assert br.ops_per_sec > 0

    def test_msgpack_deserialize_throughput(self, single_snapshot):
        ser = SerializationRegistry.get("msgpack")
        encoded = ser.serialize(single_snapshot)

        with BenchResult("msgpack deserialize") as br:
            for _ in range(self.ITERATIONS):
                ser.deserialize(encoded)
            br.set_ops(self.ITERATIONS)
        print(br.report())
        assert br.ops_per_sec > 0

    def test_raw_serialize_throughput(self):
        ser = SerializationRegistry.get("raw")
        data = b"x" * 256

        with BenchResult("raw serialize") as br:
            for _ in range(self.ITERATIONS):
                ser.serialize(data)
            br.set_ops(self.ITERATIONS)
        print(br.report())
        assert br.ops_per_sec > 0


# ---- 压缩吞吐 ----

class TestCompressionThroughput:
    """各压缩算法吞吐测试。"""

    ITERATIONS = 5_000

    @pytest.mark.parametrize("comp_name", ["none", "snappy", "lz4", "zstd"])
    def test_compress_throughput(self, comp_name, single_snapshot):
        comp = CompressionRegistry.get(comp_name)
        raw_bytes = SerializationRegistry.get("msgpack").serialize(single_snapshot)

        with BenchResult(f"{comp_name} compress") as br:
            for _ in range(self.ITERATIONS):
                comp.compress(raw_bytes)
            br.set_ops(self.ITERATIONS)
        print(br.report())

    @pytest.mark.parametrize("comp_name", ["none", "snappy", "lz4", "zstd"])
    def test_decompress_throughput(self, comp_name, single_snapshot):
        comp = CompressionRegistry.get(comp_name)
        raw_bytes = SerializationRegistry.get("msgpack").serialize(single_snapshot)
        compressed = comp.compress(raw_bytes)

        with BenchResult(f"{comp_name} decompress") as br:
            for _ in range(self.ITERATIONS):
                comp.decompress(compressed)
            br.set_ops(self.ITERATIONS)
        print(br.report())


# ---- 序列化+压缩组合端到端 ----

class TestSerCompCombo:
    """序列化+压缩完整管线吞吐。"""

    ITERATIONS = 5_000

    @pytest.mark.parametrize("ser,comp", [
        ("msgpack", "none"),
        ("msgpack", "snappy"),
        ("msgpack", "lz4"),
        ("msgpack", "zstd"),
        ("raw", "none"),
    ])
    def test_encode_payload_throughput(self, ser, comp, single_snapshot):
        data = single_snapshot if ser != "raw" else b"binary" * 50
        with BenchResult(f"{ser}+{comp} encode") as br:
            for _ in range(self.ITERATIONS):
                FrameCodec.encode_payload(data, ser, comp)
            br.set_ops(self.ITERATIONS)
        print(br.report())

    @pytest.mark.parametrize("ser,comp", [
        ("msgpack", "none"),
        ("msgpack", "snappy"),
        ("msgpack", "lz4"),
        ("msgpack", "zstd"),
    ])
    def test_decode_payload_throughput(self, ser, comp, single_snapshot):
        encoded = FrameCodec.encode_payload(single_snapshot, ser, comp)
        with BenchResult(f"{ser}+{comp} decode") as br:
            for _ in range(self.ITERATIONS):
                FrameCodec.decode_payload(encoded, ser, comp)
            br.set_ops(self.ITERATIONS)
        print(br.report())


# ---- 压缩比 ----

class TestCompressionRatio:
    """各压缩算法压缩比测试。"""

    @pytest.mark.parametrize("comp_name", ["none", "snappy", "lz4", "zstd"])
    def test_compression_ratio_single(self, comp_name, single_snapshot):
        raw = SerializationRegistry.get("msgpack").serialize(single_snapshot)
        comp = CompressionRegistry.get(comp_name)
        compressed = comp.compress(raw)
        ratio = len(compressed) / len(raw)
        print(f"  {comp_name}: {len(raw)}B → {len(compressed)}B (ratio={ratio:.2f})")

    @pytest.mark.parametrize("comp_name", ["none", "snappy", "lz4", "zstd"])
    def test_compression_ratio_batch(self, comp_name, batch_1000):
        """批量数据压缩比（更贴近真实场景）。"""
        raw = SerializationRegistry.get("msgpack").serialize(batch_1000)
        comp = CompressionRegistry.get(comp_name)
        compressed = comp.compress(raw)
        ratio = len(compressed) / len(raw)
        print(f"  {comp_name} (1000条): {len(raw)}B → {len(compressed)}B (ratio={ratio:.2f})")


# ---- 帧编解码吞吐 ----

class TestFrameCodecThroughput:
    """完整帧编解码吞吐。"""

    ITERATIONS = 5_000

    @pytest.mark.parametrize("comp", ["none", "snappy", "lz4", "zstd"])
    def test_full_encode_decode_roundtrip(self, comp, single_snapshot):
        with BenchResult(f"frame roundtrip msgpack+{comp}") as br:
            for _ in range(self.ITERATIONS):
                payload = FrameCodec.encode_payload(single_snapshot, "msgpack", comp)
                frames = FrameCodec.encode(
                    0x02, "bench.topic", 1, payload, "msgpack", comp
                )
                server_frames = [b"id", b""] + frames
                decoded = FrameCodec.decode_server(server_frames)
                FrameCodec.decode_payload(decoded.payload, decoded.ser_fmt, decoded.comp)
            br.set_ops(self.ITERATIONS)
        print(br.report())
