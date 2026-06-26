import os
from pulsemq.config import ServerConfig, ClientConfig, load_server_config, load_client_config
from pulsemq.errors import ConfigurationError


def test_server_config_defaults():
    cfg = ServerConfig()
    assert cfg.data_endpoint == "tcp://0.0.0.0:5555"
    assert cfg.control_endpoint == "tcp://0.0.0.0:5556"
    assert cfg.admin_endpoint == "0.0.0.0:9090"
    assert cfg.heartbeat_timeout == 6.0
    assert cfg.stats_db == "sqlite://./pulsemq_stats.sqlite"


def test_client_config_defaults():
    cfg = ClientConfig()
    assert cfg.data_endpoint == "tcp://localhost:5555"
    assert cfg.control_endpoint == "tcp://localhost:5556"
    assert cfg.heartbeat_interval == 1.0
    assert cfg.reconnect_initial_delay == 1.0
    assert cfg.reconnect_max_delay == 30.0
    assert cfg.reconnect_backoff_multiplier == 2.0
    assert cfg.client_id  # 自动生成非空


def test_load_server_config_from_toml(tmp_path):
    p = tmp_path / "s.toml"
    p.write_text(
        '[server]\ndata_endpoint = "tcp://0.0.0.0:6000"\n'
        '[auth]\ntype = "plain"\n', encoding="utf-8")
    cfg = load_server_config(str(p))
    assert cfg.data_endpoint == "tcp://0.0.0.0:6000"


def test_env_override(monkeypatch):
    monkeypatch.setenv("PULSEMQ_DATA_ENDPOINT", "tcp://1.2.3.4:7000")
    cfg = load_server_config(None)
    assert cfg.data_endpoint == "tcp://1.2.3.4:7000"


def test_invalid_auth_type_rejected(tmp_path):
    p = tmp_path / "bad.toml"
    p.write_text('[auth]\ntype = "curve"\n', encoding="utf-8")
    with __import__("pytest").raises(ConfigurationError):
        load_server_config(str(p))
