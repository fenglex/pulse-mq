"""PulseClient SDK 测试。"""

import pytest
from pulsemq.client.async_client import (
    PulseClient,
    PulseError,
    ConnectionError,
    AuthError,
    PermissionError,
    ServerError,
    PulseMessage,
)


class TestErrorTypes:
    def test_server_error(self):
        err = ServerError(2001, "Permission denied")
        assert err.code == 2001
        assert "2001" in str(err)
        assert "Permission denied" in str(err)

    def test_error_hierarchy(self):
        assert issubclass(ConnectionError, PulseError)
        assert issubclass(AuthError, PulseError)
        assert issubclass(PermissionError, PulseError)
        assert issubclass(ServerError, PulseError)


class TestPulseMessage:
    def test_create(self):
        msg = PulseMessage(
            topic="team-a.mkt.sh.600000",
            msg_type=0x0A,
            payload={"price": 15.8},
            raw_payload=b"\x93...",
            meta_flags=0x20,
            timestamp=1717516800.0,
        )
        assert msg.topic == "team-a.mkt.sh.600000"
        assert msg.payload == {"price": 15.8}
        assert msg.msg_type == 0x0A


class TestPulseClientInit:
    def test_default_config(self):
        client = PulseClient("tcp://localhost:5555")
        assert client._address == "tcp://localhost:5555"
        assert client._serializer == "msgpack"
        assert client._compressor == "none"
        assert client._auto_reconnect is True
        assert client._identity is not None

    def test_custom_config(self):
        client = PulseClient(
            "tcp://localhost:5555",
            api_key="pulse_sk_test",
            serializer="raw",
            compressor="snappy",
            recv_timeout=10.0,
        )
        assert client._api_key == "pulse_sk_test"
        assert client._serializer == "raw"
        assert client._compressor == "snappy"
        assert client._recv_timeout == 10.0
