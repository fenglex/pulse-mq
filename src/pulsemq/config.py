"""配置加载：环境变量 > 默认值。"""

from __future__ import annotations

import os
from dataclasses import dataclass


# 环境变量名 → (config_field, 类型转换函数)
_ENV_MAP: dict[str, tuple[str, type]] = {
    "PULSEMQ_BIND": ("bind", str),
    "PULSEMQ_ADMIN_BIND": ("admin_bind", str),
    "PULSEMQ_STATS_DB": ("stats_db", str),
    "PULSEMQ_API_KEYS": ("api_keys_str", str),
}


@dataclass
class PublisherConfig:
    """Publisher 配置，全部有合理默认值。"""

    # ZMQ PUB 绑定地址
    bind: str = "tcp://*:5555"

    # Admin 后台绑定地址
    admin_bind: str = "0.0.0.0:9090"

    # 统计 SQLite 路径
    stats_db: str = "sqlite://./stats.sqlite"

    # 统计内存窗口（分钟）
    stats_retention_minutes: int = 480  # 8 小时

    # API Keys 字符串（user1:pass1,user2:pass2），空=关闭认证
    api_keys_str: str = ""

    @property
    def api_keys(self) -> dict[str, str]:
        """解析 api_keys_str 为 {username: password} 字典。"""
        if not self.api_keys_str:
            return {}
        result: dict[str, str] = {}
        for pair in self.api_keys_str.split(","):
            pair = pair.strip()
            if ":" in pair:
                k, v = pair.split(":", 1)
                result[k.strip()] = v.strip()
        return result


def load_config() -> PublisherConfig:
    """加载配置：环境变量覆盖默认值。"""
    cfg = PublisherConfig()
    for env_key, (field_name, type_fn) in _ENV_MAP.items():
        value = os.environ.get(env_key)
        if value is not None:
            setattr(cfg, field_name, type_fn(value))
    return cfg
