"""心跳质量监控（9.2.8）：客户端心跳到达间隔 + 服务端踢线计数。

背景：心跳超时踢线（默认 6s）是消费端"消失"的主要路径，而踢线的近因
通常是心跳协程被饿死（事件循环拥塞/机器过载）而非连接真的断了。这里
按客户端跟踪心跳到达间隔（保留最近 60 次），在踢线发生前给出"心跳迟到"
的前置信号：interval_max_s 接近 heartbeat_timeout 即危险。
"""
from __future__ import annotations

import threading
import time
from collections import deque


class HeartbeatMonitor:
    """per-client 心跳间隔统计 + 累计踢线次数。"""

    def __init__(self, keep: int = 60) -> None:
        self._keep = keep
        self._lock = threading.Lock()
        self._last_ts: dict[str, float] = {}
        self._intervals: dict[str, deque[float]] = {}
        self._kicks: list[dict] = []   # 最近踢线记录（容量 20）
        self.kick_total = 0

    # ---- 控制面线程 ----

    def record(self, client_id: str) -> None:
        now = time.monotonic()
        with self._lock:
            last = self._last_ts.get(client_id)
            self._last_ts[client_id] = now
            if last is not None:
                dq = self._intervals.get(client_id)
                if dq is None:
                    dq = deque(maxlen=self._keep)
                    self._intervals[client_id] = dq
                dq.append(now - last)

    def kick(self, client_id: str, username: str = "") -> None:
        with self._lock:
            self.kick_total += 1
            self._kicks.append({
                "client_id": client_id, "username": username,
                "ts": time.time(),
            })
            if len(self._kicks) > 20:
                del self._kicks[:-20]

    def remove(self, client_id: str) -> None:
        with self._lock:
            self._last_ts.pop(client_id, None)
            self._intervals.pop(client_id, None)

    # ---- admin 线程 ----

    def snapshot(self) -> dict:
        with self._lock:
            clients = {}
            for cid, dq in self._intervals.items():
                if not dq:
                    continue
                n = len(dq)
                clients[cid] = {
                    "interval_avg_s": round(sum(dq) / n, 3),
                    "interval_max_s": round(max(dq), 3),
                    "samples": n,
                }
            return {
                "clients": clients,
                "kick_total": self.kick_total,
                "recent_kicks": [dict(k) for k in self._kicks],
            }
