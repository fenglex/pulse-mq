"""TrafficStats: 分钟聚合 + 内存 8 小时窗口。

内存中维护每个 topic 的分钟级时序数据，自动淘汰过期分钟。
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field


@dataclass
class MinuteSlot:
    """一个 topic 一分钟的统计快照。"""

    timestamp: int           # 整分钟秒
    msg_count: int = 0       # 消息条数（帧数）
    record_count: int = 0    # 记录条数（含批量拆分）
    bytes_total: int = 0     # payload 总字节数


class TrafficStats:
    """分钟粒度流量统计，内存 8 小时窗口。

    线程安全：单写者（publisher 主线程）+ 多读者（admin HTTP）。
    使用 GIL 保证安全，无需加锁。
    """

    def __init__(self, retention_minutes: int = 480) -> None:
        self._retention = retention_minutes
        # {topic: deque[MinuteSlot]}
        self._slots: dict[str, deque[MinuteSlot]] = {}
        # 当前分钟累积器: {topic: MinuteSlot}
        self._current: dict[str, MinuteSlot] = {}
        self._current_minute: int = self._minute_now()

    def record(self, topic: str, record_count: int, payload_size: int) -> None:
        """记录一条消息（同步，无锁）。"""
        self._ensure_current(topic)
        cur = self._current[topic]
        cur.msg_count += 1
        cur.record_count += record_count
        cur.bytes_total += payload_size

    def roll_minute(self) -> dict[str, MinuteSlot]:
        """整分钟时调用：归档当前累积器 → 滚动窗口淘汰过期数据。

        Returns:
            刚归档的分钟数据（用于 SQLite 落库）。
        """
        now_minute = self._minute_now()
        if now_minute == self._current_minute:
            return {}  # 同一分钟内不重复归档

        archived: dict[str, MinuteSlot] = {}

        for topic, slot in self._current.items():
            if slot.msg_count > 0:
                archived[topic] = MinuteSlot(
                    timestamp=slot.timestamp,
                    msg_count=slot.msg_count,
                    record_count=slot.record_count,
                    bytes_total=slot.bytes_total,
                )
                # 加入滚动窗口
                if topic not in self._slots:
                    self._slots[topic] = deque(maxlen=self._retention)
                self._slots[topic].append(archived[topic])

        # 切换到新分钟
        self._current_minute = now_minute
        self._current.clear()

        # 淘汰过期数据（deque maxlen 已自动处理，这里清理空 topic）
        empty_topics = [t for t, q in self._slots.items() if len(q) == 0]
        for t in empty_topics:
            del self._slots[t]

        return archived

    def get_history(self, topic: str, minutes: int = 60) -> list[dict]:
        """获取 topic 最近 N 分钟流量数据（给 Admin 曲线用）。"""
        slots = self._slots.get(topic, deque())
        history = list(slots)[-minutes:]
        return [
            {
                "timestamp": s.timestamp,
                "msg_count": s.msg_count,
                "record_count": s.record_count,
                "bytes_total": s.bytes_total,
                "msg_rate": round(s.msg_count / 60.0, 2),
            }
            for s in history
        ]

    def snapshot(self) -> dict[str, dict]:
        """所有 topic 实时快照（给 Admin 卡片指标用）。"""
        result: dict[str, dict] = {}
        for topic, cur in self._current.items():
            result[topic] = {
                "msg_count": cur.msg_count,
                "record_count": cur.record_count,
                "bytes_total": cur.bytes_total,
            }
        return result

    def all_topics_snapshot(self) -> dict[str, dict]:
        """所有 topic 完整快照（含历史信息）。"""
        result: dict[str, dict] = {}
        all_topics = set(self._current.keys()) | set(self._slots.keys())
        now_ts = time.time()
        elapsed = max(now_ts - self._current_minute, 1.0)

        for topic in all_topics:
            cur = self._current.get(topic)
            slots = self._slots.get(topic, deque())
            cur_msg = cur.msg_count if cur else 0
            cur_rec = cur.record_count if cur else 0
            cur_bytes = cur.bytes_total if cur else 0

            # 近 60 秒滚动均值：当前分钟 + 上一分钟按比例补齐
            prev = slots[-1] if slots else None
            prev_msg = prev.msg_count if prev else 0
            prev_rec = prev.record_count if prev else 0
            prev_bytes = prev.bytes_total if prev else 0

            # elapsed 秒用当前分钟累积，剩余 (60-elapsed) 秒用上一分钟按比例估算
            remaining = max(60.0 - elapsed, 0.0)
            window_msg = cur_msg + prev_msg * (remaining / 60.0)
            window_rec = cur_rec + prev_rec * (remaining / 60.0)
            window_bytes = cur_bytes + prev_bytes * (remaining / 60.0)

            result[topic] = {
                "msg_count_current": cur_msg,
                "record_count_current": cur_rec,
                "bytes_total_current": cur_bytes,
                "msg_rate_1min": round(window_msg / 60.0, 2),
                "record_rate_1min": round(window_rec / 60.0, 2),
                "bytes_rate_1min": round(window_bytes / 60.0, 2),
                "history_minutes": len(slots),
            }
        return result

    def _ensure_current(self, topic: str) -> None:
        """确保当前分钟累积器存在。"""
        now_minute = self._minute_now()
        if now_minute != self._current_minute:
            # 分钟切换时自动归档（兜底，正常由 roll_minute 触发）
            self.roll_minute()
        if topic not in self._current:
            self._current[topic] = MinuteSlot(timestamp=self._current_minute)

    @staticmethod
    def _minute_now() -> int:
        """当前整分钟秒。"""
        return int(time.time()) // 60 * 60
