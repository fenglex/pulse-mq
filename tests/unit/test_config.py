"""ServerConfig 单测。

覆盖:
- 默认值合理性
- 环境变量覆盖
- 整数/布尔/浮点类型转换
- config_dict 覆盖（递归 + 扁平）
- 未知键/未知 section 静默忽略
"""

from __future__ import annotations

import pytest

from pulsemq.config import ServerConfig, load_config


# ---- 默认值 ----


def test_config_defaults_basic():
    """默认值非空、合理。"""
    c = ServerConfig()
    assert c.bind  # 非空
    assert c.xpub_bind
    assert c.transport == "zmq"
    assert c.db_url
    assert c.stats_db_url
    assert c.default_serializer == "msgpack"
    assert c.default_compressor == "none"


def test_config_defaults_numeric():
    """数值默认值合理。"""
    c = ServerConfig()
    assert c.max_concurrency > 0
    assert c.stats_retention_days > 0
    assert c.object_pool_size > 0
    assert c.zmq_rcvhwm > 0
    assert c.zmq_sndhwm > 0
    assert 0.0 < c.backpressure_threshold < 1.0


def test_config_auth_default_is_true():
    """auth_enabled 默认是 True（生产安全默认值）。"""
    c = ServerConfig()
    assert c.auth_enabled is True


def test_config_metrics_default():
    """metrics 默认开启。"""
    c = ServerConfig()
    assert c.metrics_enabled is True
    assert c.metrics_bind


# ---- 环境变量覆盖 ----


def test_config_env_override_string(monkeypatch):
    """字符串环境变量可覆盖。"""
    monkeypatch.setenv("PULSEMQ_BIND", "tcp://*:9999")
    monkeypatch.setenv("PULSEMQ_XPUB_BIND", "tcp://*:9998")
    c = load_config()
    assert c.bind == "tcp://*:9999"
    assert c.xpub_bind == "tcp://*:9998"


def test_config_env_override_int(monkeypatch):
    """整数环境变量正确转换。"""
    monkeypatch.setenv("PULSEMQ_CONCURRENCY", "200")
    monkeypatch.setenv("PULSEMQ_STATS_RETENTION", "30")
    c = load_config()
    assert c.max_concurrency == 200
    assert c.stats_retention_days == 30


def test_config_env_override_bool(monkeypatch):
    """布尔环境变量正确解析 true/1/yes（大小写不敏感）。"""
    monkeypatch.setenv("PULSEMQ_USE_UVLOOP", "false")
    monkeypatch.setenv("PULSEMQ_AUTH_ENABLED", "0")
    c = load_config()
    assert c.use_uvloop is False
    assert c.auth_enabled is False

    monkeypatch.setenv("PULSEMQ_USE_UVLOOP", "True")
    monkeypatch.setenv("PULSEMQ_AUTH_ENABLED", "YES")
    c = load_config()
    assert c.use_uvloop is True
    assert c.auth_enabled is True


def test_config_env_override_float(monkeypatch):
    """浮点环境变量正确转换。"""
    monkeypatch.setenv("PULSEMQ_BP_THRESHOLD", "0.95")
    c = load_config()
    assert c.backpressure_threshold == 0.95


def test_config_env_override_unset_keeps_default(monkeypatch):
    """未设置的环境变量保持默认值。"""
    monkeypatch.delenv("PULSEMQ_BIND", raising=False)
    c = load_config()
    assert c.bind == ServerConfig().bind


# ---- config_dict 覆盖 ----


def test_config_dict_override_flat():
    """扁平字典覆盖顶层字段。"""
    c = load_config({"bind": "tcp://*:7777"})
    assert c.bind == "tcp://*:7777"


def test_config_dict_override_section():
    """嵌套 section 字典可设置字段。"""
    c = load_config(
        {
            "server": {
                "bind": "tcp://*:8888",
                "max_concurrency": 50,
            }
        }
    )
    assert c.bind == "tcp://*:8888"
    assert c.max_concurrency == 50


def test_config_dict_ignores_unknown_keys():
    """未知键静默忽略（不抛异常）。"""
    c = load_config({"unknown_key_xxx": "value", "server": {"another_unknown": 1}})
    # 默认值未被破坏
    assert c.bind == ServerConfig().bind


def test_config_dict_env_overrides_default_and_dict_overrides_env(monkeypatch):
    """config_dict 优先级 > 环境变量 > 默认值。"""
    monkeypatch.setenv("PULSEMQ_BIND", "tcp://*:9999")
    c = load_config({"bind": "tcp://*:8888"})
    # config_dict 应当覆盖环境变量
    assert c.bind == "tcp://*:8888"


# ---- 不可变 dataclass 性质 ----


def test_config_is_dataclass():
    """ServerConfig 是 dataclass，可比较。"""
    c1 = ServerConfig()
    c2 = ServerConfig()
    assert c1 == c2
    c3 = ServerConfig(bind="tcp://*:9999")
    assert c1 != c3
