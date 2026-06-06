"""PulseClient SDK 测试。"""

import pytest
from pulsemq.client.async_client import (
    PulseClient,
    PulseError,
    PulseConnectionError,
    PulseAuthError,
    PulsePermissionError,
    PulseServerError,
    PulseMessage,
)


class TestErrorTypes:
    def test_server_error(self):
        err = PulseServerError(2001, "Permission denied")
        assert err.code == 2001
        assert "2001" in str(err)
        assert "Permission denied" in str(err)

    def test_error_hierarchy(self):
        assert issubclass(PulseConnectionError, PulseError)
        assert issubclass(PulseAuthError, PulseError)
        assert issubclass(PulsePermissionError, PulseError)
        assert issubclass(PulseServerError, PulseError)


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
        assert client._auto_reconnect is True
        assert client._identity is not None

    def test_custom_config(self):
        client = PulseClient(
            "tcp://localhost:5555",
            api_key="pulse_sk_test",
            recv_timeout=10.0,
        )
        assert client._api_key == "pulse_sk_test"
        assert client._recv_timeout == 10.0


class TestInferRecordCount:
    def test_dict_returns_1(self):
        assert PulseClient._infer_record_count({"price": 15.8}) == 1

    def test_bytes_returns_1(self):
        assert PulseClient._infer_record_count(b"hello") == 1

    def test_str_returns_1(self):
        assert PulseClient._infer_record_count("hello") == 1

    def test_list_dict_returns_1(self):
        assert PulseClient._infer_record_count([{"a": 1}, {"b": 2}]) == 1

    def test_dataframe_returns_row_count(self):
        import pandas as pd
        df = pd.DataFrame({"price": [1.0, 2.0, 3.0]})
        assert PulseClient._infer_record_count(df) == 3

    def test_empty_dataframe_returns_0(self):
        import pandas as pd
        df = pd.DataFrame({"price": []})
        assert PulseClient._infer_record_count(df) == 0


class TestPrepareData:
    def test_str_with_none_format_encodes_to_bytes(self):
        result = PulseClient._prepare_data("hello", "none")
        assert result == b"hello"

    def test_bytes_with_none_format_passthrough(self):
        result = PulseClient._prepare_data(b"\x01\x02", "none")
        assert result == b"\x01\x02"

    def test_dict_with_none_format_raises_type_error(self):
        with pytest.raises(TypeError, match="format='none'"):
            PulseClient._prepare_data({"key": 1}, "none")

    def test_list_with_none_format_raises_type_error(self):
        with pytest.raises(TypeError, match="format='none'"):
            PulseClient._prepare_data([{"a": 1}], "none")

    def test_dict_with_msgpack_passthrough(self):
        result = PulseClient._prepare_data({"key": 1}, "msgpack")
        assert result == {"key": 1}

    def test_bytes_with_msgpack_passthrough(self):
        result = PulseClient._prepare_data(b"\x01\x02", "msgpack")
        assert result == b"\x01\x02"
