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
    credentials_file: str = "./pulsemq_users.toml"
    heartbeat_timeout: float = 6.0
    stats_db: str = "sqlite://./pulsemq_stats.sqlite"
    stats_retention_minutes: int = 480


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
