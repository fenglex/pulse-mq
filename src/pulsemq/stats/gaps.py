"""消费端缺口统计（9.2.8 从 Server 内联 dict 升级）。

职责：累计缺口（对账用）+ 分钟桶（历史曲线用）+ 归档行（SQLite 持久化）。
线程模型与 DropStats 相同：控制面协程写，分钟滚动协程 roll，admin 读。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass


@dataclass
class MinuteGap:
    timestamp: int      # 整分钟秒
    gap_count: int


class GapStats:
    """per-topic 累计缺失帧数（心跳 gaps 字段聚合）+ 分钟历史。"""

    def __init__(self, retention_minutes: int = 60) -> None:
        self._retention = retention_minutes
        self._cum: dict[str, int] = {}
        self._current: dict[str, int] = {}
        self._history: dict[str, deque[MinuteGap]] = {}
        self._lock = threading.Lock()

    def record(self, topic: str, count: int) -> None:
        if count <= 0:
            return
        with self._lock:
            self._cum[topic] = self._cum.get(topic, 0) + count
            self._current[topic] = self._current.get(topic, 0) + count

    def roll_minute(self) -> list[tuple[str, int, int]]:
        """整分钟归档：返回 [(topic, timestamp, gap_count)] 归档行。"""
        ts = int(time.time()) // 60 * 60
        rows: list[tuple[str, int, int]] = []
        with self._lock:
            for topic, count in self._current.items():
                if count > 0:
                    rows.append((topic, ts, count))
                    dq = self._history.get(topic)
                    if dq is None:
                        dq = deque(maxlen=self._retention)
                        self._history[topic] = dq
                    dq.append(MinuteGap(timestamp=ts, gap_count=count))
            self._current.clear()
            empty = [t for t, q in self._history.items() if len(q) == 0]
            for t in empty:
                del self._history[t]
        return rows

    def snapshot(self) -> dict[str, int]:
        """per-topic 累计缺失（与旧 Server 内联 dict 同形状）。"""
        with self._lock:
            return dict(self._cum)

    def history(self, minutes: int) -> list[dict]:
        """内存分钟历史（不合并 SQLite，合并由 admin 层做）。"""
        with self._lock:
            out: dict[int, dict] = {}
            for topic, dq in self._history.items():
                for m in dq:
                    slot = out.setdefault(m.timestamp, {"timestamp": m.timestamp, "topics": {}})
                    slot["topics"][topic] = m.gap_count
            cur_ts = int(time.time()) // 60 * 60
            if self._current:
                slot = out.setdefault(cur_ts, {"timestamp": cur_ts, "topics": {}})
                for topic, count in self._current.items():
                    slot["topics"][topic] = count
            cutoff = int(time.time()) - minutes * 60
            return [out[k] for k in sorted(out) if k >= cutoff]

    def remove_topic(self, topic: str) -> None:
        """topic 下线清理（保留累计值用于对账，仅清窗口）。"""
        with self._lock:
            self._current.pop(topic, None)
