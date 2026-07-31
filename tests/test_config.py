import os
from pulsemq.config import ServerConfig, ClientConfig, load_server_config, load_client_config
from pulsemq.errors import ConfigurationError


def test_server_config_defaults():
    cfg = ServerConfig()
    assert cfg.data_endpoint == "tcp://0.0.0.0:5555"
    assert cfg.control_endpoint == "tcp://0.0.0.0:5556"
    assert cfg.admin_endpoint == "0.0.0.0:9090"
    assert cfg.heartbeat_timeout == 6.0
    assert cfg.stats_db == "sqlite://./data/pulsemq_stats.sqlite"


def test_client_config_defaults():
    cfg = ClientConfig()
    assert cfg.data_endpoint == "tcp://localhost:5555"
    assert cfg.control_endpoint == "tcp://localhost:5556"
    assert cfg.heartbeat_interval == 1.0
    assert cfg.reconnect_initial_delay == 1.0
    assert cfg.reconnect_max_delay == 30.0
    assert cfg.reconnect_backoff_multiplier == 2.0
    assert cfg.client_id  # 自动生成非空


def test_env_overrides(monkeypatch):
    """ServerConfig 常用字段支持环境变量覆盖（C2）。"""
    monkeypatch.setenv("PULSEMQ_HEARTBEAT_TIMEOUT", "10.0")
    monkeypatch.setenv("PULSEMQ_LATENCY_SAMPLE_RATE", "0.05")
    monkeypatch.setenv("PULSEMQ_RETENTION_DAYS", "14")
    monkeypatch.setenv("PULSEMQ_BCRYPT_COST", "14")
    cfg = load_server_config(None)
    assert cfg.heartbeat_timeout == 10.0
    assert cfg.latency_sample_rate == 0.05
    assert cfg.retention_days == 14
    assert cfg.bcrypt_cost == 14


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


def test_server_config_security_defaults():
    cfg = ServerConfig()
    assert cfg.allow_auto_generated_credentials is True
    assert cfg.password_hash_algo == "bcrypt"
    assert cfg.bcrypt_cost == 12
    assert cfg.admin_token == ""
    assert cfg.admin_token_file == "./data/pulsemq_admin.token"


def test_load_auth_block_from_toml(tmp_path):
    p = tmp_path / "s.toml"
    p.write_text(
        '[auth]\ntype = "plain"\nallow_auto_generated_credentials = false\n'
        'bcrypt_cost = 10\n[monitoring]\nadmin_token = "tok123"\n',
        encoding="utf-8")
    cfg = load_server_config(str(p))
    assert cfg.allow_auto_generated_credentials is False
    assert cfg.bcrypt_cost == 10
    assert cfg.admin_token == "tok123"


def test_env_admin_token_and_password(monkeypatch):
    monkeypatch.setenv("PULSEMQ_ADMIN_TOKEN", "envtok")
    monkeypatch.setenv("PULSEMQ_ADMIN_PASSWORD", "envpw")
    cfg = load_server_config(None)
    assert cfg.admin_token == "envtok"
    # PULSEMQ_ADMIN_PASSWORD 不进 config（仅 security 模块读），这里只验证不报错


def test_monitoring_defaults():
    cfg = ServerConfig()
    assert cfg.sse_interval == 1.0
    assert cfg.latency_sample_rate == 0.01
    assert cfg.event_ring_size == 200
    assert cfg.stats_archive_batch_size == 50
    assert cfg.admin_thread is True
    assert cfg.ui_enabled is True
    assert cfg.retention_days == 7


def test_load_monitoring_block(tmp_path):
    p = tmp_path / "s.toml"
    p.write_text('[monitoring]\nlatency_sample_rate = 0.5\nevent_ring_size = 50\n'
                 'admin_thread = false\n', encoding="utf-8")
    cfg = load_server_config(str(p))
    assert cfg.latency_sample_rate == 0.5
    assert cfg.event_ring_size == 50
    assert cfg.admin_thread is False
