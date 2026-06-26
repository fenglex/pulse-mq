# tests/test_logging_setup.py
import io
from pulsemq.logging_setup import setup_logging, logger, log_event


def test_setup_logging_text(capfd):
    setup_logging(level="INFO", json=False)
    logger.info("hello")
    out = capfd.readouterr().err
    assert "hello" in out
    assert "INFO" in out


def test_log_event_emits(capfd):
    setup_logging(level="INFO", json=False)
    log_event("INFO", "CLIENT", username="alice", action="online")
    out = capfd.readouterr().err
    assert "alice" in out
    assert "CLIENT" in out
