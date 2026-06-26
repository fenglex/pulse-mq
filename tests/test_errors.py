# tests/test_errors.py
import pytest
from pulsemq.errors import (
    PulseMQError, TransportError, ConnectionError, AuthenticationError,
    ClientStartupError, FrameError, SerializationError, ConfigurationError,
    ResourceExhaustedError, exit_code_for,
)


def test_base_exit_code():
    assert PulseMQError.exit_code == 1
    assert PulseMQError("x").exit_code == 1


@pytest.mark.parametrize("exc_cls,code", [
    (TransportError, 2),
    (ConnectionError, 2),
    (AuthenticationError, 3),
    (ClientStartupError, 4),
    (FrameError, 5),
    (SerializationError, 5),
    (ConfigurationError, 6),
    (ResourceExhaustedError, 7),
])
def test_exit_codes(exc_cls, code):
    assert exc_cls.exit_code == code


def test_authentication_error_reason():
    err = AuthenticationError("bad", reason="invalid_password")
    assert err.reason == "invalid_password"
    assert exit_code_for(err) == 3


def test_client_startup_error_fields():
    err = ClientStartupError("nope", reason="CONNECT_FAILED",
                             address="tcp://1.2.3.4:5555", username="alice")
    assert err.reason == "CONNECT_FAILED"
    assert err.address == "tcp://1.2.3.4:5555"
    assert err.username == "alice"
    assert exit_code_for(err) == 4


def test_exit_code_for_unknown_exception():
    assert exit_code_for(ValueError("x")) == 1
