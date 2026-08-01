"""消费端消息丢弃统计：按 topic 聚合（来自心跳），分钟桶 + 1 小时窗口。

线程模型：控制面协程（心跳处理）写 record()，分钟滚动协程写 roll_minute()，
admin 线程读 snapshot()。用 threading.Lock 保护。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass


@dataclass
class MinuteDrop:
    """一个 topic 一分钟的丢弃快照。"""
    timestamp: int       # 整分钟秒
    drop_count: int


class DropStats:
    """按 topic 聚合消费端丢弃量（来自消费者心跳）。

    分钟桶 + 1 小时滚动窗口，提供三级粒度：
    - drops_current：当前分钟进行中的丢弃量
    - drops_last_min：上一完整分钟的丢弃量
    - drops_1h_total：近 1 小时累计丢弃量
    """

    def __init__(self, retention_minutes: int = 60) -> None:
        self._retention = retention_minutes
        self._current: dict[str, int] = {}
        self._history: dict[str, deque[MinuteDrop]] = {}
        self._lock = threading.Lock()

    def record(self, topic: str, count: int) -> None:
        """累加 topic 的丢弃量（来自消费者心跳）。"""
        if count <= 0:
            return
        with self._lock:
            self._current[topic] = self._current.get(topic, 0) + count

    def roll_minute(self) -> None:
        """整分钟归档：当前累积 → 历史窗口。"""
        ts = int(time.time()) // 60 * 60
        with self._lock:
            for topic, count in self._current.items():
                if count > 0:
                    dq = self._history.get(topic)
                    if dq is None:
                        dq = deque(maxlen=self._retention)
                        self._history[topic] = dq
                    dq.append(MinuteDrop(timestamp=ts, drop_count=count))
            self._current.clear()
            # 清理空 topic（deque maxlen 已淘汰过期数据）
            empty = [t for t, q in self._history.items() if len(q) == 0]
            for t in empty:
                del self._history[t]

    def snapshot(self) -> dict[str, dict]:
        """各 topic 丢弃快照（给 Admin API / SSE）。"""
        with self._lock:
            result: dict[str, dict] = {}
            all_topics = set(self._current) | set(self._history)
            for topic in all_topics:
                cur = self._current.get(topic, 0)
                hist = self._history.get(topic)
                last_min = hist[-1].drop_count if hist else 0
                total_1h = sum(m.drop_count for m in hist) if hist else 0
                result[topic] = {
                    "drops_current": cur,
                    "drops_last_min": last_min,
                    "drops_1h_total": total_1h + cur,
                }
            return result
