from pulsemq.protocol.msg_type import MsgType


class TestMsgType:
    def test_values(self):
        assert MsgType.AUTH == 0x01
        assert MsgType.PUB == 0x02
        assert MsgType.SUB == 0x03
        assert MsgType.UNSUB == 0x04
        assert MsgType.QUERY == 0x05
        assert MsgType.PING == 0x06
        assert MsgType.PONG == 0x07
        assert MsgType.STATUS == 0x08
        assert MsgType.ERROR == 0x09
        assert MsgType.BROADCAST == 0x0A
        assert MsgType.HISTORY_REPLAY == 0x0B

    def test_is_control(self):
        assert MsgType.is_control(MsgType.AUTH) is True
        assert MsgType.is_control(MsgType.SUB) is True
        assert MsgType.is_control(MsgType.UNSUB) is True
        assert MsgType.is_control(MsgType.QUERY) is True
        assert MsgType.is_control(MsgType.PING) is True
        assert MsgType.is_control(MsgType.PUB) is False
        assert MsgType.is_control(MsgType.BROADCAST) is False

    def test_from_byte(self):
        assert MsgType.from_byte(0x02) == MsgType.PUB
        assert MsgType.from_byte(0xFF) is None
