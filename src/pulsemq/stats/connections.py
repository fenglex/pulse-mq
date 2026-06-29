"""在线 Client 与生命周期事件统计。事件环有界，on_* 无锁（单写者+GIL）。"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Callable


@dataclass
class ClientSnapshot:
    client_id: str
    username: str
    role: str            # producer / consumer / both
    endpoint: str
    topics: list[str]
    connected_at: float
    duration_seconds: float


@dataclass
class LifecycleEvent:
    ts: float
    level: str           # INFO / WARNING / ERROR
    type: str            # AUTH / CLIENT
    message: str


def _role_of(roles: list[str]) -> str:
    has_pub = any("pub" in r for r in roles)
    has_sub = any("sub" in r for r in roles)
    if has_pub and has_sub:
        return "both"
    if has_pub:
        return "producer"
    if has_sub:
        return "consumer"
    return "consumer"


class ConnectionStats:
    def __init__(self, registry_snapshot_fn: Callable[[], dict],
                 ring_size: int = 200) -> None:
        self._reg_snap = registry_snapshot_fn
        self._events: deque[LifecycleEvent] = deque(maxlen=ring_size)

    # ---- 事件埋点（Server 数据线程调用）----
    # type 词表：auth / connect / disconnect / subscribe / unsubscribe（统一小写，
    # 与前端 web_ui 的 tCls 颜色分类 ['connect','disconnect','subscribe',
    # 'unsubscribe','auth'] 对齐，否则事件会掉进默认灰色样式）。
    def on_connect(self, client_id: str, username: str, endpoint: str, role: str) -> None:
        self._events.append(LifecycleEvent(
            ts=time.time(), level="INFO", type="connect",
            message=f"{username} 上线 role={role} endpoint={endpoint}"))

    def on_disconnect(self, client_id: str, reason: str) -> None:
        self._events.append(LifecycleEvent(
            ts=time.time(), level="INFO", type="disconnect",
            message=f"{client_id} 离线 reason={reason}"))

    def on_subscribe(self, client_id: str, username: str, pattern: str) -> None:
        self._events.append(LifecycleEvent(
            ts=time.time(), level="INFO", type="subscribe",
            message=f"{username} 订阅 {pattern}"))

    def on_unsubscribe(self, client_id: str, username: str, pattern: str) -> None:
        self._events.append(LifecycleEvent(
            ts=time.time(), level="INFO", type="unsubscribe",
            message=f"{username} 取消订阅 {pattern}"))

    def on_auth(self, username: str, endpoint: str, success: bool,
                reason: str | None) -> None:
        level = "INFO" if success else "WARNING"
        msg = (f"{username} 认证成功" if success
               else f"{username} 认证失败: {reason or 'unknown'}")
        self._events.append(LifecycleEvent(ts=time.time(), level=level, type="auth", message=msg))

    # ---- 读取（admin 线程调用，只读快照）----
    def online_clients(self) -> list[ClientSnapshot]:
        data = self._reg_snap() or {}
        now = time.time()
        out: list[ClientSnapshot] = []
        for c in data.get("clients", []):
            connected_at = float(c.get("connected_at", 0.0))
            out.append(ClientSnapshot(
                client_id=str(c.get("client_id", "")),
                username=str(c.get("username", "")),
                role=_role_of(list(c.get("roles", []))),
                endpoint=str(c.get("endpoint", "")),
                topics=list(c.get("topics", [])),
                connected_at=connected_at,
                duration_seconds=max(0.0, now - connected_at) if connected_at else 0.0,
            ))
        return out

    def recent_events(self, limit: int = 50) -> list[LifecycleEvent]:
        if limit <= 0:
            return []
        items = list(self._events)
        return items[-limit:]

    def counters(self) -> dict:
        clients = self.online_clients()
        producers = sum(1 for c in clients if c.role in ("producer", "both"))
        consumers = sum(1 for c in clients if c.role in ("consumer", "both"))
        total_subs = sum(len(c.topics) for c in clients)
        return {
            "online_users": len(clients),
            "online_producers": producers,
            "online_consumers": consumers,
            "total_subscriptions": total_subs,
        }
