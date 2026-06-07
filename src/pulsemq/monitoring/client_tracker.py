"""ClientTracker: 在线客户端追踪。

不记录 payload, 仅追踪 identity / 连接时间 / 心跳 / 订阅 / msg 速率。
- 60s 无心跳视为离线 (list_online 过滤)
- msg_in / msg_out 计数按 1 分钟窗口 EWMA 平滑
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field

from pulsemq.monitoring.realtime import EWMA


@dataclass
class ClientInfo:
    """单个客户端的状态。"""

    identity: bytes
    user_id: int | None
    connected_at: float
    last_heartbeat: float
    subscribed_topics: set[str] = field(default_factory=set)
    msg_in_count: int = 0       # 收到的 PUB (server 广播给此 client 的)
    msg_out_count: int = 0      # 发出的 PUB (此 client publish 的)
    # EWMA 速率: 1min 窗口 (alpha=0.3 适配 ~10s 衰减)
    msg_in_rate_1min: EWMA = field(default_factory=lambda: EWMA(alpha=0.3))
    msg_out_rate_1min: EWMA = field(default_factory=lambda: EWMA(alpha=0.3))


# 60s 内有心跳视为在线
HEARTBEAT_TIMEOUT_SECONDS = 60.0


class ClientTracker:
    """在线客户端追踪器。"""

    def __init__(self, heartbeat_timeout: float = HEARTBEAT_TIMEOUT_SECONDS):
        self._clients: dict[bytes, ClientInfo] = {}
        self._heartbeat_timeout = heartbeat_timeout

    # ---- 生命周期 ----

    def on_connect(self, identity: bytes, user_id: int | None) -> None:
        """新连接建立: 注册客户端。"""
        now = time.time()
        # 若已存在, 视为重连: 不重置 subscribed_topics (router 已清),
        # 这里以新 connected_at 覆盖
        self._clients[identity] = ClientInfo(
            identity=identity,
            user_id=user_id,
            connected_at=now,
            last_heartbeat=now,
        )

    def on_disconnect(self, identity: bytes) -> None:
        """连接断开: 移除客户端。"""
        self._clients.pop(identity, None)

    def on_heartbeat(self, identity: bytes) -> None:
        """心跳: 更新 last_heartbeat。"""
        info = self._clients.get(identity)
        if info is not None:
            info.last_heartbeat = time.time()

    # ---- 订阅 ----

    def on_sub(self, identity: bytes, topic: str) -> None:
        """订阅: 加入 subscribed_topics。"""
        info = self._clients.get(identity)
        if info is not None:
            info.subscribed_topics.add(topic)

    def on_unsub(self, identity: bytes, topic: str) -> None:
        """取消订阅: 从 subscribed_topics 移除。"""
        info = self._clients.get(identity)
        if info is not None:
            info.subscribed_topics.discard(topic)

    # ---- 流量 ----

    def on_pub(self, identity: bytes, payload_size: int) -> None:
        """客户端发出一条 PUB。

        Args:
            identity: 发布者 identity
            payload_size: 负载字节数 (不存, 仅作未来扩展)
        """
        info = self._clients.get(identity)
        if info is not None:
            info.msg_out_count += 1
            info.msg_out_rate_1min.update(1)

    def on_deliver(self, identity: bytes, payload_size: int) -> None:
        """server 向此 client 广播了一条 PUB (delivery count)。

        payload_size 暂不存储, 保留接口对称。
        """
        info = self._clients.get(identity)
        if info is not None:
            info.msg_in_count += 1
            info.msg_in_rate_1min.update(1)

    # ---- 查询 ----

    def get(self, identity: bytes) -> ClientInfo | None:
        """按 identity 获取客户端信息 (不论是否在线)。"""
        return self._clients.get(identity)

    def list_online(self) -> list[ClientInfo]:
        """列出在线客户端 (60s 内有心跳)。"""
        now = time.time()
        cutoff = now - self._heartbeat_timeout
        return [c for c in self._clients.values() if c.last_heartbeat >= cutoff]

    def list_all(self) -> list[ClientInfo]:
        """列出所有已知客户端 (含已断开但未移除的; 实际上 on_disconnect 已清, 实际等于 list_online)。"""
        return list(self._clients.values())

    # ---- AdminServer 快照 ----

    def snapshot(self) -> dict:
        """给 AdminServer 的全量快照。

        Returns:
            {
              "online_count": int,
              "clients": [{"identity": "hex", "user_id": int|None, ...}, ...]
            }
        """
        online = self.list_online()
        return {
            "online_count": len(online),
            "clients": [
                {
                    "identity": c.identity.hex() if isinstance(c.identity, (bytes, bytearray)) else str(c.identity),
                    "user_id": c.user_id,
                    "connected_at": c.connected_at,
                    "last_heartbeat": c.last_heartbeat,
                    "subscribed_topics": sorted(c.subscribed_topics),
                    "msg_in_count": c.msg_in_count,
                    "msg_out_count": c.msg_out_count,
                    "msg_in_rate_1min": round(c.msg_in_rate_1min.value, 2),
                    "msg_out_rate_1min": round(c.msg_out_rate_1min.value, 2),
                }
                for c in online
            ],
        }
