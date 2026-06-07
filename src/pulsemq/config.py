"""配置加载：环境变量 > 配置文件 > 默认值"""

from __future__ import annotations

import os
from dataclasses import dataclass

# 环境变量名 → (config_field, 类型转换函数)
_ENV_MAP: dict[str, tuple[str, type]] = {
    "PULSEMQ_TRANSPORT": ("transport", str),
    "PULSEMQ_BIND": ("bind", str),
    "PULSEMQ_XPUB_BIND": ("xpub_bind", str),
    "PULSEMQ_DB_URL": ("db_url", str),
    "PULSEMQ_STATS_DB_URL": ("stats_db_url", str),
    "PULSEMQ_STATS_RETENTION": ("stats_retention_days", int),
    "PULSEMQ_CONCURRENCY": ("max_concurrency", int),
    "PULSEMQ_BATCH_SIZE": ("max_batch_size", int),
    "PULSEMQ_DRAIN_TIMEOUT": ("drain_timeout_ms", int),
    "PULSEMQ_USE_UVLOOP": ("use_uvloop", lambda v: v.lower() in ("true", "1", "yes")),
    "PULSEMQ_POOL_SIZE": ("object_pool_size", int),
    "PULSEMQ_ZMQ_RCVHWM": ("zmq_rcvhwm", int),
    "PULSEMQ_ZMQ_SNDHWM": ("zmq_sndhwm", int),
    "PULSEMQ_ZMQ_XPUB_NODROP": ("zmq_xpub_nodrop", lambda v: v.lower() in ("true", "1", "yes")),
    "PULSEMQ_HEARTBEAT_IVL": ("zmq_heartbeat_ivl", int),
    "PULSEMQ_HEARTBEAT_TIMEOUT": ("zmq_heartbeat_timeout", int),
    "PULSEMQ_HEARTBEAT_TTL": ("zmq_heartbeat_ttl", int),
    "PULSEMQ_DATA_BUFFER": ("data_buffer_size", int),
    "PULSEMQ_CTRL_BUFFER": ("ctrl_buffer_size", int),
    "PULSEMQ_BP_THRESHOLD": ("backpressure_threshold", float),
    "PULSEMQ_SERIALIZER": ("default_serializer", str),
    "PULSEMQ_COMPRESSOR": ("default_compressor", str),
    "PULSEMQ_AUTH_ENABLED": ("auth_enabled", lambda v: v.lower() in ("true", "1", "yes")),
    "PULSEMQ_ADMIN_KEY": ("default_admin_key", str),
    "PULSEMQ_ADMIN_BIND": ("admin_bind", str),
    "PULSEMQ_ADMIN_ENABLED": ("admin_enabled", lambda v: v.lower() in ("true", "1", "yes")),
}


@dataclass
class ServerConfig:
    """服务端全部配置项，全部有合理默认值。"""

    # 传输层
    transport: str = "zmq"
    bind: str = "tcp://*:5555"
    xpub_bind: str = "tcp://*:5556"

    # 存储层
    db_url: str = "sqlite://./pulse_mq.db"
    stats_db_url: str = "sqlite://./stats.sqlite"
    stats_retention_days: int = 7

    # 引擎层
    max_concurrency: int = 100
    max_batch_size: int = 64
    drain_timeout_ms: int = 1
    use_uvloop: bool = True
    object_pool_size: int = 4096

    # ZMQ socket
    zmq_rcvhwm: int = 10000
    zmq_sndhwm: int = 10000
    zmq_xpub_nodrop: bool = False  # True = pub 阻塞, False = 丢消息
    zmq_heartbeat_ivl: int = 2000
    zmq_heartbeat_timeout: int = 5000
    zmq_heartbeat_ttl: int = 8000

    # 过载保护
    data_buffer_size: int = 9000
    ctrl_buffer_size: int = 1000
    backpressure_threshold: float = 0.8

    # 序列化/压缩
    default_serializer: str = "msgpack"
    default_compressor: str = "none"

    # 认证
    auth_enabled: bool = True
    default_admin_key: str = "pulse_sk_admin_default"

    # 监控
    metrics_enabled: bool = True
    metrics_bind: str = "0.0.0.0:9091"     # 旧 MetricsHTTPServer (AdminServer 含更全的功能)

    # Phase 8: 后台管理 (AdminServer + Web UI)
    admin_enabled: bool = True
    admin_bind: str = "0.0.0.0:9090"


def load_config(config_dict: dict | None = None) -> ServerConfig:
    """加载配置：环境变量覆盖默认值，config_dict 覆盖环境变量。

    Args:
        config_dict: 从 TOML 配置文件解析的字典（Phase 1 暂不实现文件解析）。
    """
    cfg = ServerConfig()

    # 环境变量覆盖默认值
    for env_key, (field_name, type_fn) in _ENV_MAP.items():
        value = os.environ.get(env_key)
        if value is not None:
            setattr(cfg, field_name, type_fn(value))

    # config_dict 覆盖（预留）
    if config_dict:
        _apply_dict(cfg, config_dict)

    return cfg


def _apply_dict(cfg: ServerConfig, d: dict) -> None:
    """递归应用字典到配置对象。"""
    for section_key, section_val in d.items():
        if isinstance(section_val, dict):
            for k, v in section_val.items():
                if hasattr(cfg, k):
                    setattr(cfg, k, type(getattr(cfg, k))(v))
        elif hasattr(cfg, section_key):
            setattr(cfg, section_key, type(getattr(cfg, section_key))(section_val))
