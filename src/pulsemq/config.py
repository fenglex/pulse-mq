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
    sndhwm: int = 10000   # ZMQ 发送高水位（帧数），大 payload 可调低控制内存
    rcvhwm: int = 10000   # ZMQ 接收高水位（帧数）
    # 订阅者缓冲（慢消费者策略 drop_old/conflate）全局上限；客户端只能请求更小值。
    # max_age_s=0 表示不限存活时间。条数/字节/时间三上限，任一触发即开始淘汰。
    # 默认 50 万帧 / 64MB（9.2.8 从 10 万条/10MB 调大，匹配批量行情负载下
    # 的积压容量需求；需要控制服务端内存时经 TOML [server] 或环境变量调低，
    # 客户端请求不能超过此上限）。
    buffer_max_messages: int = 500_000
    buffer_max_bytes: int = 64 * 1024 * 1024   # 64MB
    buffer_max_age_s: float = 0.0
    # 告警规则阈值（9.2.8）：无 webhook 时降级为日志事件。
    # 各阈值语义见 pulsemq.alerts.AlertManager；设 0 关闭对应规则
    # （buffer_age_s=0 关闭积压告警，kick 规则随 heartbeat_kick 布尔）。
    alert_webhook: str = ""
    alert_cooldown_s: float = 60.0
    alert_drop_per_min: int = 1000
    alert_gap_per_min: int = 1000
    alert_buffer_age_s: float = 5.0
    alert_starved_per_s: float = 100.0
    alert_loop_stall_ms: float = 1000.0

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
    sndhwm: int = 10000
    rcvhwm: int = 10000
    # 9.2.4：消费端唯一模式为多进程池（workers=1 即主进程+1 worker），
    # 由 ConsumerClient(workers=..., key=...) 直接配置。


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
        buffer_max_messages=int(s.get("buffer_max_messages",
                                      ServerConfig.buffer_max_messages)),
        buffer_max_bytes=int(s.get("buffer_max_bytes",
                                   ServerConfig.buffer_max_bytes)),
        buffer_max_age_s=float(s.get("buffer_max_age_s",
                                     ServerConfig.buffer_max_age_s)),
        alert_webhook=m.get("alert_webhook", ServerConfig.alert_webhook),
        alert_cooldown_s=float(m.get("alert_cooldown_s",
                                     ServerConfig.alert_cooldown_s)),
        alert_drop_per_min=int(m.get("alert_drop_per_min",
                                     ServerConfig.alert_drop_per_min)),
        alert_gap_per_min=int(m.get("alert_gap_per_min",
                                    ServerConfig.alert_gap_per_min)),
        alert_buffer_age_s=float(m.get("alert_buffer_age_s",
                                       ServerConfig.alert_buffer_age_s)),
        alert_starved_per_s=float(m.get("alert_starved_per_s",
                                        ServerConfig.alert_starved_per_s)),
        alert_loop_stall_ms=float(m.get("alert_loop_stall_ms",
                                        ServerConfig.alert_loop_stall_ms)),
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
    if (v := _env("PULSEMQ_HEARTBEAT_TIMEOUT")):
        cfg.heartbeat_timeout = float(v)
    if (v := _env("PULSEMQ_LATENCY_SAMPLE_RATE")):
        cfg.latency_sample_rate = float(v)
    if (v := _env("PULSEMQ_RETENTION_DAYS")):
        cfg.retention_days = int(v)
    if (v := _env("PULSEMQ_BCRYPT_COST")):
        cfg.bcrypt_cost = int(v)
    if (v := _env("PULSEMQ_SSE_INTERVAL")):
        cfg.sse_interval = float(v)
    if (v := _env("PULSEMQ_STATS_RETENTION_MINUTES")):
        cfg.stats_retention_minutes = int(v)
    if (v := _env("PULSEMQ_BUFFER_MAX_MESSAGES")):
        cfg.buffer_max_messages = int(v)
    if (v := _env("PULSEMQ_BUFFER_MAX_BYTES")):
        cfg.buffer_max_bytes = int(v)
    if (v := _env("PULSEMQ_BUFFER_MAX_AGE_S")):
        cfg.buffer_max_age_s = float(v)
    if (v := _env("PULSEMQ_ALERT_WEBHOOK")):
        cfg.alert_webhook = v
    if (v := _env("PULSEMQ_ALERT_COOLDOWN_S")):
        cfg.alert_cooldown_s = float(v)
    if (v := _env("PULSEMQ_ALERT_DROP_PER_MIN")):
        cfg.alert_drop_per_min = int(v)
    if (v := _env("PULSEMQ_ALERT_GAP_PER_MIN")):
        cfg.alert_gap_per_min = int(v)
    if (v := _env("PULSEMQ_ALERT_BUFFER_AGE_S")):
        cfg.alert_buffer_age_s = float(v)
    if (v := _env("PULSEMQ_ALERT_STARVED_PER_S")):
        cfg.alert_starved_per_s = float(v)
    if (v := _env("PULSEMQ_ALERT_LOOP_STALL_MS")):
        cfg.alert_loop_stall_ms = float(v)
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
