import os
import pytest
from pulsemq.config import BrokerConfig, load_config


class TestBrokerConfig:
    def test_default_values(self):
        cfg = BrokerConfig()
        assert cfg.bind == "tcp://*:5555"
        assert cfg.xpub_bind == "tcp://*:5556"
        assert cfg.max_concurrency == 100
        assert cfg.max_batch_size == 64
        assert cfg.drain_timeout_ms == 1
        assert cfg.use_uvloop is True
        assert cfg.object_pool_size == 4096
        assert cfg.zmq_rcvhwm == 10000
        assert cfg.zmq_sndhwm == 10000
        assert cfg.zmq_heartbeat_ivl == 2000
        assert cfg.zmq_heartbeat_timeout == 5000
        assert cfg.zmq_heartbeat_ttl == 8000
        assert cfg.data_buffer_size == 9000
        assert cfg.ctrl_buffer_size == 1000
        assert cfg.backpressure_threshold == 0.8
        assert cfg.default_serializer == "msgpack"
        assert cfg.default_compressor == "none"
        assert cfg.auth_enabled is True
        assert cfg.default_admin_key == "pulse_sk_admin_default"

    def test_from_env_override(self, monkeypatch):
        monkeypatch.setenv("PULSEMQ_BIND", "tcp://*:6666")
        monkeypatch.setenv("PULSEMQ_CONCURRENCY", "200")
        cfg = load_config()
        assert cfg.bind == "tcp://*:6666"
        assert cfg.max_concurrency == 200

    def test_env_priority_over_default(self, monkeypatch):
        monkeypatch.setenv("PULSEMQ_BATCH_SIZE", "32")
        cfg = load_config()
        assert cfg.max_batch_size == 32
        # 其他保持默认
        assert cfg.max_concurrency == 100
