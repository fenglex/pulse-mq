import pytest
from pulsemq.protocol.flags import FrameFlags


class TestFrameFlags:
    def test_encode_default(self):
        flags = FrameFlags(ser_fmt="msgpack", comp="none", has_topic=False)
        byte_val = flags.encode()
        assert byte_val == 0b0000_0000

    def test_encode_has_topic(self):
        flags = FrameFlags(ser_fmt="msgpack", comp="none", has_topic=True)
        byte_val = flags.encode()
        assert byte_val == 0b0010_0000

    def test_encode_raw(self):
        flags = FrameFlags(ser_fmt="raw", comp="none", has_topic=False)
        byte_val = flags.encode()
        assert byte_val == 0b0000_0001

    def test_encode_pyarrow(self):
        flags = FrameFlags(ser_fmt="pyarrow", comp="none", has_topic=False)
        byte_val = flags.encode()
        assert byte_val == 0b0000_0010

    def test_encode_snappy(self):
        flags = FrameFlags(ser_fmt="msgpack", comp="snappy", has_topic=False)
        byte_val = flags.encode()
        assert byte_val == 0b0000_1000

    def test_encode_lz4(self):
        flags = FrameFlags(ser_fmt="msgpack", comp="lz4", has_topic=False)
        byte_val = flags.encode()
        assert byte_val == 0b0001_0000

    def test_encode_zstd(self):
        flags = FrameFlags(ser_fmt="msgpack", comp="zstd", has_topic=False)
        byte_val = flags.encode()
        assert byte_val == 0b0001_1000

    def test_decode_roundtrip(self):
        original = FrameFlags(ser_fmt="pyarrow", comp="zstd", has_topic=True)
        byte_val = original.encode()
        decoded = FrameFlags.decode(byte_val)
        assert decoded.ser_fmt == "pyarrow"
        assert decoded.comp == "zstd"
        assert decoded.has_topic is True

    @pytest.mark.parametrize("ser,comp", [
        ("msgpack", "none"),
        ("raw", "none"),
        ("pyarrow", "snappy"),
        ("msgpack", "lz4"),
        ("msgpack", "zstd"),
    ])
    def test_roundtrip_all(self, ser, comp):
        for has_topic in (True, False):
            original = FrameFlags(ser_fmt=ser, comp=comp, has_topic=has_topic)
            decoded = FrameFlags.decode(original.encode())
            assert decoded.ser_fmt == ser
            assert decoded.comp == comp
            assert decoded.has_topic == has_topic
