import struct
import pytest
from pulsemq.protocol.frames import FrameCodec, DecodedFrame
from pulsemq.protocol.msg_type import MsgType


class TestFrameCodec:
    def test_encode_pub_message(self):
        """PUB 消息编码为 4 帧。"""
        payload = FrameCodec.encode_payload(
            {"price": 15.8}, ser_fmt="msgpack", comp="none"
        )
        frames = FrameCodec.encode(
            msg_type=MsgType.PUB,
            topic="team-a.mkt.sh.600000",
            record_count=1,
            payload=payload,
            ser_fmt="msgpack",
            comp="none",
        )
        assert len(frames) == 4
        assert frames[0] == b"team-a.mkt.sh.600000"
        # meta: msg_type=0x02, flags=has_topic=1 | ser=msgpack(000) | comp=none(00)
        assert frames[1] == bytes([0x02, 0b0010_0000])
        assert struct.unpack(">I", frames[2])[0] == 1

    def test_encode_ping_message(self):
        """PING 无 topic。"""
        payload = FrameCodec.encode_payload(
            {"client_ts": 1717516800.123}, ser_fmt="msgpack", comp="none"
        )
        frames = FrameCodec.encode(
            msg_type=MsgType.PING,
            topic="",
            record_count=0,
            payload=payload,
            ser_fmt="msgpack",
            comp="none",
        )
        assert len(frames) == 4
        assert frames[0] == b""
        assert frames[1][0] == MsgType.PING

    def test_decode_server_received(self):
        """服务端 ROUTER 收到 6 帧解码。"""
        payload = FrameCodec.encode_payload(
            {"price": 15.8}, ser_fmt="msgpack", comp="none"
        )
        client_frames = FrameCodec.encode(
            msg_type=MsgType.PUB,
            topic="team-a.mkt.sh.600000",
            record_count=1,
            payload=payload,
            ser_fmt="msgpack",
            comp="none",
        )
        # ZMQ 自动附加 identity + delimiter
        server_frames = [b"identity_abc", b""] + list(client_frames)

        decoded = FrameCodec.decode_server(server_frames)
        assert decoded.identity == b"identity_abc"
        assert decoded.topic == "team-a.mkt.sh.600000"
        assert decoded.msg_type == MsgType.PUB
        assert decoded.record_count == 1
        assert decoded.ser_fmt == "msgpack"
        assert decoded.comp == "none"
        assert decoded.has_topic is True

    def test_decode_and_decode_payload(self):
        """完整编解码 + payload 反序列化往返。"""
        original_data = {"price": 15.8, "volume": 1000}
        payload = FrameCodec.encode_payload(original_data, "msgpack", "none")
        frames = FrameCodec.encode(
            msg_type=MsgType.PUB,
            topic="test.topic",
            record_count=1,
            payload=payload,
            ser_fmt="msgpack",
            comp="none",
        )
        server_frames = [b"id", b""] + list(frames)
        decoded = FrameCodec.decode_server(server_frames)
        result = FrameCodec.decode_payload(decoded.payload, decoded.ser_fmt, decoded.comp)
        assert result == original_data

    def test_decode_5_frames_no_delimiter(self):
        """DEALER→ROUTER 无 delimiter，5 帧解码。"""
        payload = FrameCodec.encode_payload(
            {"price": 15.8}, ser_fmt="msgpack", comp="none"
        )
        client_frames = FrameCodec.encode(
            msg_type=MsgType.PUB,
            topic="team-a.mkt.sh.600000",
            record_count=1,
            payload=payload,
        )
        # DEALER→ROUTER 只有 5 帧（无 delimiter）
        server_frames = [b"identity_abc"] + list(client_frames)

        decoded = FrameCodec.decode_server(server_frames)
        assert decoded.identity == b"identity_abc"
        assert decoded.topic == "team-a.mkt.sh.600000"
        assert decoded.msg_type == MsgType.PUB
        assert decoded.record_count == 1

    def test_decode_invalid_frame_count(self):
        """帧数不对时抛出异常。"""
        with pytest.raises(ValueError, match="帧数"):
            FrameCodec.decode_server([b"id", b"", b"topic"])

    def test_encode_for_broadcast(self):
        """XPUB 广播 4 帧。"""
        payload = FrameCodec.encode_payload(
            {"price": 15.8}, ser_fmt="msgpack", comp="none"
        )
        frames = FrameCodec.encode(
            msg_type=MsgType.BROADCAST,
            topic="team-a.mkt.sh.600000",
            record_count=1,
            payload=payload,
            ser_fmt="msgpack",
            comp="none",
        )
        assert len(frames) == 4
        assert frames[0] == b"team-a.mkt.sh.600000"
        assert frames[1][0] == MsgType.BROADCAST

    @pytest.mark.parametrize("ser,comp", [
        ("msgpack", "none"),
        ("msgpack", "snappy"),
        ("raw", "none"),
    ])
    def test_full_roundtrip(self, ser, comp):
        data = b"binary data" if ser == "raw" else {"key": "value", "num": 42}
        payload = FrameCodec.encode_payload(data, ser, comp)
        frames = FrameCodec.encode(MsgType.PUB, "test.topic", 1, payload, ser, comp)
        server_frames = [b"id", b""] + list(frames)
        decoded = FrameCodec.decode_server(server_frames)
        result = FrameCodec.decode_payload(decoded.payload, decoded.ser_fmt, decoded.comp)
        assert result == data
