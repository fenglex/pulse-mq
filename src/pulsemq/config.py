"""配置加载：TOML + 环境变量，全默认值。零配置可启动。"""
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass, field
from pathlib import Path

try:
    import tomllib  # py311+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

from pulsemq.errors import ConfigurationError


@dataclass
class ServerConfig:
    data_endpoint: str = "tcp://0.0.0.0:5555"
    control_endpoint: str = "tcp://0.0.0.0:5556"
    admin_endpoint: str = "0.0.0.0:9090"
    credentials_file: str = "./data/pulsemq_users.toml"
    heartbeat_timeout: float = 6.0
    stats_db: str = "sqlite://./data/pulsemq_stats.sqlite"
    stats_retention_minutes: int = 480
    allow_auto_generated_credentials: bool = True
    password_hash_algo: str = "bcrypt"
    bcrypt_cost: int = 12
    admin_token: str = ""
    admin_token_file: str = "./data/pulsemq_admin.token"
    sse_interval: float = 1.0
    latency_sample_rate: float = 0.01
    event_ring_size: int = 200
    stats_archive_batch_size: int = 50
    admin_thread: bool = True
    ui_enabled: bool = True
    retention_days: int = 7
    sndhwm: int = 1000   # ZMQ 发送高水位（帧数），大 payload 可调低控制内存
    rcvhwm: int = 1000   # ZMQ 接收高水位（帧数）

    def __post_init__(self) -> None:
        """确保 data/ 目录存在，日志/SQLite/凭据/token 等运行时文件统一存放。"""
        Path("data").mkdir(parents=True, exist_ok=True)


@dataclass
class ClientConfig:
    data_endpoint: str = "tcp://localhost:5555"
    control_endpoint: str = "tcp://localhost:5556"
    username: str = ""
    password: str = ""
    client_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    heartbeat_interval: float = 1.0
    reconnect_initial_delay: float = 1.0
    reconnect_max_delay: float = 30.0
    reconnect_backoff_multiplier: float = 2.0


def _read_toml(path: str | None) -> dict:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    with p.open("rb") as f:
        return tomllib.load(f)


def _env(name: str, default: str | None = None) -> str | None:
    return os.environ.get(name, default)


def load_server_config(path: str | None = None) -> ServerConfig:
    data = _read_toml(path)
    s = data.get("server", {})
    a = data.get("auth", {})
    m = data.get("monitoring", {})
    if a.get("type", "plain") not in ("plain", "PLAIN"):
        raise ConfigurationError(f"auth.type 仅支持 plain，拒绝 {a.get('type')!r}")
    cfg = ServerConfig(
        data_endpoint=s.get("data_endpoint", ServerConfig.data_endpoint),
        control_endpoint=s.get("control_endpoint", ServerConfig.control_endpoint),
        admin_endpoint=s.get("admin_endpoint", ServerConfig.admin_endpoint),
        credentials_file=a.get("credentials_file", ServerConfig.credentials_file),
        heartbeat_timeout=float(s.get("heartbeat_timeout", ServerConfig.heartbeat_timeout)),
        stats_db=s.get("stats_db", ServerConfig.stats_db),
        stats_retention_minutes=int(s.get("stats_retention_minutes",
                                          ServerConfig.stats_retention_minutes)),
        allow_auto_generated_credentials=bool(
            a.get("allow_auto_generated_credentials",
                  ServerConfig.allow_auto_generated_credentials)),
        password_hash_algo=a.get("password_hash_algo",
                                 ServerConfig.password_hash_algo),
        bcrypt_cost=int(a.get("bcrypt_cost", ServerConfig.bcrypt_cost)),
        admin_token=m.get("admin_token", ServerConfig.admin_token),
        admin_token_file=m.get("admin_token_file",
                               ServerConfig.admin_token_file),
        sse_interval=float(m.get("sse_interval", ServerConfig.sse_interval)),
        latency_sample_rate=float(m.get("latency_sample_rate",
                                        ServerConfig.latency_sample_rate)),
        event_ring_size=int(m.get("event_ring_size",
                                  ServerConfig.event_ring_size)),
        stats_archive_batch_size=int(m.get("stats_archive_batch_size",
                                           ServerConfig.stats_archive_batch_size)),
        admin_thread=bool(m.get("admin_thread", ServerConfig.admin_thread)),
        ui_enabled=bool(m.get("ui_enabled", ServerConfig.ui_enabled)),
        retention_days=int(m.get("retention_days", ServerConfig.retention_days)),
        sndhwm=int(s.get("sndhwm", ServerConfig.sndhwm)),
        rcvhwm=int(s.get("rcvhwm", ServerConfig.rcvhwm)),
    )
    # 环境变量覆盖
    if (v := _env("PULSEMQ_DATA_ENDPOINT")):
        cfg.data_endpoint = v
    if (v := _env("PULSEMQ_CONTROL_ENDPOINT")):
        cfg.control_endpoint = v
    if (v := _env("PULSEMQ_ADMIN_BIND")):
        cfg.admin_endpoint = v
    if (v := _env("PULSEMQ_CREDENTIALS_FILE")):
        cfg.credentials_file = v
    if (v := _env("PULSEMQ_ADMIN_TOKEN")):
        cfg.admin_token = v
    if (v := _env("PULSEMQ_SNDHWM")):
        cfg.sndhwm = int(v)
    if (v := _env("PULSEMQ_RCVHWM")):
        cfg.rcvhwm = int(v)
    return cfg


def load_client_config(path: str | None = None) -> ClientConfig:
    data = _read_toml(path)
    c = data.get("client", {})
    cfg = ClientConfig(
        data_endpoint=c.get("data_endpoint", ClientConfig.data_endpoint),
        control_endpoint=c.get("control_endpoint", ClientConfig.control_endpoint),
        username=c.get("username", ""),
        password=c.get("password", ""),
        heartbeat_interval=float(c.get("heartbeat_interval", ClientConfig.heartbeat_interval)),
        reconnect_initial_delay=float(c.get("reconnect_initial_delay",
                                            ClientConfig.reconnect_initial_delay)),
        reconnect_max_delay=float(c.get("reconnect_max_delay",
                                        ClientConfig.reconnect_max_delay)),
        reconnect_backoff_multiplier=float(c.get("reconnect_backoff_multiplier",
                                                 ClientConfig.reconnect_backoff_multiplier)),
    )
    if (v := _env("PULSEMQ_DATA_ENDPOINT")):
        cfg.data_endpoint = v
    if (v := _env("PULSEMQ_CONTROL_ENDPOINT")):
        cfg.control_endpoint = v
    if (v := _env("PULSEMQ_USERNAME")):
        cfg.username = v
    if (v := _env("PULSEMQ_PASSWORD")):
        cfg.password = v
    return cfg
