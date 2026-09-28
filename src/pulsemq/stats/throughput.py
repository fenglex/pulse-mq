"""ClientProcStats：每客户端处理速率 / 延迟聚合（心跳 proc 字段上报）。

数据流：client HEARTBEAT 携带 ``proc={topic: {count, rate, recv_avg_ns,
proc_avg_ns}}`` → ``record()`` 存每客户端最新快照（带时间戳）→ admin API
``client_entry()/clients()``（明细）与 ``topics()``（按 topic 跨客户端聚合）。

超时清理：DISCONNECT / 心跳扫描时显式 ``remove()``；异常路径兜底
``sweep_stale()`` 丢弃超过 ``stale_seconds`` 未更新的条目。
"""
from __future__ import annotations

import threading
import time


class ClientProcStats:
    """每客户端处理指标（最新一次心跳快照），线程安全。"""

    def __init__(self, stale_seconds: float = 15.0) -> None:
        self._stale = stale_seconds
        self._lock = threading.Lock()
        # client_id -> {"username": str, "ts": float, "topics": {topic: metrics}}
        self._by_client: dict[str, dict] = {}

    def record(self, client_id: str, username: str, proc: dict) -> None:
        if not proc:
            return
        with self._lock:
            prev = self._by_client.get(client_id)
            topics = dict(proc)
            if prev is not None:
                # 字段级 latch：count/rate 是窗口量、每跳覆盖；延迟均值是
                # 观测量，recv 与 proc 常落在不同心跳窗口（worker ≥1s 批量
                # flush）——保留最后一次非空值，快照才始终完整（9.2.7）。
                for t, m in topics.items():
                    pm = prev["topics"].get(t)
                    if pm is None:
                        continue
                    for f in ("recv_avg_ns", "proc_avg_ns"):
                        if f not in m and f in pm:
                            m[f] = pm[f]
            self._by_client[client_id] = {
                "username": username,
                "ts": time.time(),
                "topics": topics,
            }

    def remove(self, client_id: str) -> None:
        with self._lock:
            self._by_client.pop(client_id, None)

    def sweep_stale(self) -> None:
        """丢弃超过 stale_seconds 未更新的条目（心跳扫描循环低频调用）。"""
        cutoff = time.time() - self._stale
        with self._lock:
            stale = [cid for cid, e in self._by_client.items() if e["ts"] < cutoff]
            for cid in stale:
                del self._by_client[cid]

    def client_entry(self, client_id: str) -> dict | None:
        """单客户端最新处理指标（含汇总速率/延迟），无数据返回 None。"""
        with self._lock:
            e = self._by_client.get(client_id)
            if e is None:
                return None
            entry = dict(e)
            entry["topics"] = dict(e["topics"])
            return entry

    def clients(self) -> list[dict]:
        """全部客户端处理明细（admin 用）。"""
        with self._lock:
            return [
                {"client_id": cid, "username": e["username"], "ts": e["ts"],
                 "topics": dict(e["topics"])}
                for cid, e in self._by_client.items()
            ]

    def topics(self) -> dict[str, dict]:
        """按 topic 跨客户端聚合：总处理速率 + 按 count 加权的平均延迟。

        返回 {topic: {rate_per_sec, recv_avg_ms?, proc_avg_ms?}}。
        """
        agg: dict[str, dict] = {}
        now = time.time()
        with self._lock:
            entries = [(e["ts"], e["topics"]) for e in self._by_client.values()]
        for ts, topics in entries:
            if now - ts > self._stale:
                continue
            for topic, m in topics.items():
                a = agg.setdefault(topic, {"rate": 0.0, "recv_sum": 0,
                                           "recv_w": 0, "proc_sum": 0, "proc_w": 0})
                a["rate"] += float(m.get("rate", 0.0))
                w = max(int(m.get("count", 0) or 0), 1)
                recv_ns = m.get("recv_avg_ns")
                if recv_ns is not None:
                    a["recv_sum"] += int(recv_ns) * w
                    a["recv_w"] += w
                proc_ns = m.get("proc_avg_ns")
                if proc_ns is not None:
                    a["proc_sum"] += int(proc_ns) * w
                    a["proc_w"] += w
        out: dict[str, dict] = {}
        for topic, a in agg.items():
            entry: dict = {"rate_per_sec": round(a["rate"], 2)}
            if a["recv_w"]:
                entry["recv_avg_ms"] = round(a["recv_sum"] / a["recv_w"] / 1e6, 3)
            if a["proc_w"]:
                entry["proc_avg_ms"] = round(a["proc_sum"] / a["proc_w"] / 1e6, 3)
            out[topic] = entry
        return out

    @staticmethod
    def summarize(entry: dict) -> dict:
        """单客户端条目 → 汇总（总速率 + 加权平均延迟），供 /api/v1/clients。"""
        total_rate = 0.0
        recv_sum = recv_w = proc_sum = proc_w = 0
        count_total = 0
        for m in entry.get("topics", {}).values():
            total_rate += float(m.get("rate", 0.0))
            count_total += int(m.get("count", 0) or 0)
            w = max(int(m.get("count", 0) or 0), 1)
            recv_ns = m.get("recv_avg_ns")
            if recv_ns is not None:
                recv_sum += int(recv_ns) * w
                recv_w += w
            proc_ns = m.get("proc_avg_ns")
            if proc_ns is not None:
                proc_sum += int(proc_ns) * w
                proc_w += w
        out: dict = {
            "rate_per_sec": round(total_rate, 2),
            "count": count_total,
            "ts": entry.get("ts"),
        }
        if recv_w:
            out["recv_avg_ms"] = round(recv_sum / recv_w / 1e6, 3)
        if proc_w:
            out["proc_avg_ms"] = round(proc_sum / proc_w / 1e6, 3)
        return out
