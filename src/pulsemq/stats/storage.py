"""StatsStorage: SQLite 分钟统计持久化。

落库策略：roll_minute() 之后异步写入，不阻塞主流程。
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

from pulsemq.stats.traffic import MinuteSlot

logger = logging.getLogger(__name__)


class StatsStorage:
    """分钟统计 SQLite 持久化。"""

    def __init__(self, db_path: str = "./stats.sqlite") -> None:
        # 解析 sqlite:// 前缀
        if db_path.startswith("sqlite://"):
            db_path = db_path[len("sqlite://"):]
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> None:
        """建立 SQLite 连接并创建表。"""
        self._conn = sqlite3.connect(self._db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS minute_stats (
                topic TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                msg_count INTEGER DEFAULT 0,
                record_count INTEGER DEFAULT 0,
                bytes_total INTEGER DEFAULT 0,
                PRIMARY KEY (topic, timestamp)
            )
        """)
        self._conn.commit()
        logger.info("StatsStorage 连接: %s", self._db_path)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def save_minute(self, topic: str, slot: MinuteSlot) -> None:
        """同步写入一条分钟记录。"""
        if self._conn is None:
            return
        try:
            self._conn.execute(
                """INSERT OR REPLACE INTO minute_stats
                   (topic, timestamp, msg_count, record_count, bytes_total)
                   VALUES (?, ?, ?, ?, ?)""",
                (topic, slot.timestamp, slot.msg_count, slot.record_count, slot.bytes_total),
            )
            self._conn.commit()
        except Exception:
            logger.debug("save_minute 失败", exc_info=True)

    def save_minutes_batch(self, data: dict[str, MinuteSlot]) -> None:
        """批量写入多条分钟记录。"""
        if self._conn is None or not data:
            return
        try:
            for topic, slot in data.items():
                self._conn.execute(
                    """INSERT OR REPLACE INTO minute_stats
                       (topic, timestamp, msg_count, record_count, bytes_total)
                       VALUES (?, ?, ?, ?, ?)""",
                    (topic, slot.timestamp, slot.msg_count, slot.record_count, slot.bytes_total),
                )
            self._conn.commit()
        except Exception:
            logger.debug("save_minutes_batch 失败", exc_info=True)

    def load_history(self, topic: str, since_ts: int) -> list[dict]:
        """加载历史数据（进程重启后恢复图表用）。"""
        if self._conn is None:
            return []
        try:
            cursor = self._conn.execute(
                """SELECT timestamp, msg_count, record_count, bytes_total
                   FROM minute_stats
                   WHERE topic = ? AND timestamp >= ?
                   ORDER BY timestamp""",
                (topic, since_ts),
            )
            return [
                {
                    "timestamp": row[0],
                    "msg_count": row[1],
                    "record_count": row[2],
                    "bytes_total": row[3],
                    "msg_rate": round(row[1] / 60.0, 2),
                }
                for row in cursor.fetchall()
            ]
        except Exception:
            logger.debug("load_history 失败", exc_info=True)
            return []

    def cleanup(self, retention_days: int = 7) -> int:
        """清理过期数据，返回删除行数。"""
        if self._conn is None:
            return 0
        cutoff = int(time.time()) - retention_days * 86400
        try:
            cursor = self._conn.execute(
                "DELETE FROM minute_stats WHERE timestamp < ?", (cutoff,)
            )
            self._conn.commit()
            return cursor.rowcount
        except Exception:
            logger.debug("cleanup 失败", exc_info=True)
            return 0
