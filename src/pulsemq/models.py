"""核心领域模型：纯数据结构，无业务逻辑。"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class AuthUser:
    """已认证用户信息，存储在内存中。"""

    user_id: int
    role: str                     # "admin" | "user"
    groups: list[str]             # 权限组名称列表
    api_key: str                  # 原始 api_key
    namespace: str = ""           # home_namespace

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


@dataclass
class TopicInfo:
    """Topic 注册信息。"""

    full_name: str                # "team-a.mkt.sh.600000"
    namespace: str                # "team-a"（第一段）
    topic_path: str               # "mkt.sh.600000"（除第一段外）
    is_wildcard: bool = False
    subscriber_count: int = 0
    created_at: float = field(default_factory=time.time)

    @classmethod
    def from_name(cls, full_name: str) -> TopicInfo:
        """从完整 topic 名创建 TopicInfo。"""
        parts = full_name.split(".")
        # 检测通配符
        is_wildcard = "*" in parts or ">" in parts
        return cls(
            full_name=full_name,
            namespace=parts[0] if parts else "",
            topic_path=".".join(parts[1:]) if len(parts) > 1 else "",
            is_wildcard=is_wildcard,
        )


@dataclass(slots=True)
class BufferedMessage:
    """消息缓冲区中的单条消息。"""

    topic: str
    seq: int                      # topic 内单调递增消息序号
    record_count: int             # 本条消息携带的数据行数（≥1）
    meta: bytes                   # Frame 3 的 2 字节
    payload: bytes                # Frame 5 的序列化+压缩后的 payload
    timestamp: float              # Broker 接收时间


@dataclass
class ExpandedPermissions:
    """用户展开后的权限列表（从权限组合并而来）。"""

    pub: list[str] = field(default_factory=list)
    sub: list[str] = field(default_factory=list)
    query: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict[str, list[str]]) -> ExpandedPermissions:
        return cls(
            pub=d.get("pub", []),
            sub=d.get("sub", []),
            query=d.get("query", []),
        )
