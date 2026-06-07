"""SQLite 持久化的 topic 统计仓库 (7 天 TTL)。

每分钟从 TopicMetricsRegistry 拉快照写入 topic_stats 表,
后台清理任务 (engine.start 时启动) 每 5 分钟跑一次 cleanup_expired().
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from functools import partial

from pulsemq.storage.database import run_sync_locked

logger = logging.getLogger(__name__)


# topic_stats 表 DDL
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS topic_stats (
    stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    topic TEXT NOT NULL,
    minute_ts INTEGER NOT NULL,
    msg_count INTEGER NOT NULL,
    latency_p50_ms REAL,
    latency_p99_ms REAL,
    latency_max_ms REAL,
    peak_in_flight INTEGER,
    UNIQUE(topic, minute_ts)
);
CREATE INDEX IF NOT EXISTS idx_topic_stats_minute_ts ON topic_stats(minute_ts);
"""


class SQLiteStatsRepo:
    """topic_stats 仓库 (7 天 TTL)。"""

    def __init__(self, db_path: str, retention_days: int = 7):
        """初始化仓库 (创建表 + 打开连接)。

        Args:
            db_path: SQLite 文件路径
            retention_days: 数据保留天数 (默认 7)
        """
        self._db_path = db_path
        self._retention_days = retention_days
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA_SQL)
        self._conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ---- 写 ----

    async def upsert_minute(
        self,
        topic: str,
        minute_ts: int,
        msg_count: int,
        p50: float,
        p99: float,
        max_lat: float,
        peak_in_flight: int,
    ) -> None:
        """upsert 一行 (topic, minute_ts) → 聚合指标。

        minute_ts 应该是整分钟时间戳 (秒), 由调用方对齐。
        UNIQUE(topic, minute_ts) 冲突时覆盖。
        """
        def _do():
            self._conn.execute(
                """INSERT INTO topic_stats
                       (topic, minute_ts, msg_count, latency_p50_ms,
                        latency_p99_ms, latency_max_ms, peak_in_flight)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(topic, minute_ts) DO UPDATE SET
                       msg_count = excluded.msg_count,
                       latency_p50_ms = excluded.latency_p50_ms,
                       latency_p99_ms = excluded.latency_p99_ms,
                       latency_max_ms = excluded.latency_max_ms,
                       peak_in_flight = excluded.peak_in_flight
                """,
                (topic, minute_ts, msg_count, p50, p99, max_lat, peak_in_flight),
            )
            self._conn.commit()
        await run_sync_locked(_do)

    # ---- 读 ----

    async def get_topic_history(
        self, topic: str, since_ts: int
    ) -> list[dict]:
        """读取 topic 在 since_ts 之后的所有分钟记录, 按 minute_ts 升序。

        Args:
            topic: topic 名
            since_ts: 起始时间戳 (秒, 含)

        Returns:
            [{"topic", "minute_ts", "msg_count", "latency_p50_ms", ...}, ...]
        """
        def _do():
            rows = self._conn.execute(
                """SELECT topic, minute_ts, msg_count,
                          latency_p50_ms, latency_p99_ms, latency_max_ms,
                          peak_in_flight
                   FROM topic_stats
                   WHERE topic = ? AND minute_ts >= ?
                   ORDER BY minute_ts""",
                (topic, since_ts),
            ).fetchall()
            return [dict(r) for r in rows]
        return await run_sync_locked(_do)

    async def list_all_topics(self) -> list[str]:
        """列出 topic_stats 中出现过的所有 topic。"""
        def _do():
            rows = self._conn.execute(
                "SELECT DISTINCT topic FROM topic_stats ORDER BY topic"
            ).fetchall()
            return [r["topic"] for r in rows]
        return await run_sync_locked(_do)

    # ---- 清理 ----

    async def cleanup_expired(self) -> int:
        """删除 retention_days 之前的数据, 返回删除行数。"""
        cutoff = int(time.time()) - self._retention_days * 86400

        def _do():
            cur = self._conn.execute(
                "DELETE FROM topic_stats WHERE minute_ts < ?", (cutoff,)
            )
            self._conn.commit()
            return cur.rowcount
        return await run_sync_locked(_do)

    # ---- 后台清理协程 ----

    async def start_cleanup_task(self, interval_seconds: float = 300.0) -> asyncio.Task:
        """启动后台清理协程, 每 interval_seconds 秒跑一次 cleanup_expired()。

        Returns:
            asyncio.Task (caller 持有引用, stop 时 cancel)
        """
        async def _loop():
            while True:
                try:
                    n = await self.cleanup_expired()
                    if n > 0:
                        logger.info(
                            "topic_stats 清理: 删除 %d 条超过 %d 天的数据",
                            n, self._retention_days,
                        )
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.debug("topic_stats 清理异常: %s", e)
                await asyncio.sleep(interval_seconds)
        return asyncio.create_task(_loop())


# ---- 便捷构造: 与其他 storage 模块一致 ----


def init_stats_db(db_path: str) -> SQLiteStatsRepo:
    """初始化并返回 SQLiteStatsRepo。

    Args:
        db_path: SQLite 文件路径

    Returns:
        SQLiteStatsRepo 实例
    """
    return SQLiteStatsRepo(db_path=db_path)


# 兼容旧 run_sync 接口 (用法: await run_sync(func, *args))
_run_sync = run_sync_locked
