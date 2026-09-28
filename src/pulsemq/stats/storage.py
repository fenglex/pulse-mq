"""StatsStorage: SQLite 分钟统计持久化。

落库策略：roll_minute() 之后异步写入，不阻塞主流程。
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from loguru import logger

from pulsemq.stats.traffic import MinuteSlot


class StatsStorage:
    """分钟统计 SQLite 持久化。

    线程模型：``AdminServer`` 默认在独立线程（``admin_thread=True``）调用
    ``load_history``（读），而写（``save_minutes_batch``）在主线程的
    ``AsyncArchiveWriter`` consumer 任务中执行。SQLite 连接默认
    ``check_same_thread=True``，跨线程访问会抛 ``ProgrammingError``。
    解决：连接用 ``check_same_thread=False`` 打开 + ``threading.Lock`` 串行化
    所有连接操作。锁只在「主线程归档任务」与「admin 线程」之间共享，zmq
    数据接收循环（``_data_loop``）从不触碰 SQLite，因此 DB 读写不会阻塞 zmq。
    """

    def __init__(self, db_path: str = "./stats.sqlite") -> None:
        # 解析 sqlite:// 前缀
        if db_path.startswith("sqlite://"):
            db_path = db_path[len("sqlite://"):]
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None
        # 保护 _conn 的所有读写（跨线程：主线程写 + admin 线程读）。
        self._lock = threading.Lock()

    def connect(self) -> None:
        """建立 SQLite 连接并创建表。"""
        # check_same_thread=False：允许 admin 线程读此连接；线程安全由 _lock 保证。
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        with self._lock:
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
            # 9.2.8：丢弃/缺口分钟归档（对账与历史曲线，跨进程重启可回溯）
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS minute_drops (
                    topic TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    drop_count INTEGER DEFAULT 0,
                    PRIMARY KEY (topic, timestamp)
                )
            """)
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS minute_gaps (
                    topic TEXT NOT NULL,
                    timestamp INTEGER NOT NULL,
                    gap_count INTEGER DEFAULT 0,
                    PRIMARY KEY (topic, timestamp)
                )
            """)
            self._conn.commit()
        logger.info("StatsStorage 连接: {}", self._db_path)

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def save_minutes_batch(self, data: dict[str, MinuteSlot]) -> None:
        """批量写入多条分钟记录。"""
        if self._conn is None or not data:
            return
        try:
            with self._lock:
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
        """加载历史数据（进程重启后恢复图表用）。

        可被 admin 独立线程调用；与写路径共用同一连接，由 ``_lock`` 串行化。
        """
        if self._conn is None:
            return []
        try:
            with self._lock:
                cursor = self._conn.execute(
                    """SELECT timestamp, msg_count, record_count, bytes_total
                       FROM minute_stats
                       WHERE topic = ? AND timestamp >= ?
                       ORDER BY timestamp""",
                    (topic, since_ts),
                )
                rows = cursor.fetchall()
            return [
                {
                    "timestamp": row[0],
                    "msg_count": row[1],
                    "record_count": row[2],
                    "bytes_total": row[3],
                    "msg_rate": round(row[1] / 60.0, 2),
                }
                for row in rows
            ]
        except Exception:
            logger.debug("load_history 失败", exc_info=True)
            return []

    def save_drop_minutes(self, rows: list[tuple[str, int, int]]) -> None:
        """写入丢弃分钟归档行 [(topic, timestamp, drop_count)]（9.2.8）。"""
        if self._conn is None or not rows:
            return
        try:
            with self._lock:
                self._conn.executemany(
                    """INSERT OR REPLACE INTO minute_drops
                       (topic, timestamp, drop_count) VALUES (?, ?, ?)""",
                    rows,
                )
                self._conn.commit()
        except Exception:
            logger.debug("save_drop_minutes 失败", exc_info=True)

    def save_gap_minutes(self, rows: list[tuple[str, int, int]]) -> None:
        """写入缺口分钟归档行 [(topic, timestamp, gap_count)]（9.2.8）。"""
        if self._conn is None or not rows:
            return
        try:
            with self._lock:
                self._conn.executemany(
                    """INSERT OR REPLACE INTO minute_gaps
                       (topic, timestamp, gap_count) VALUES (?, ?, ?)""",
                    rows,
                )
                self._conn.commit()
        except Exception:
            logger.debug("save_gap_minutes 失败", exc_info=True)

    def _load_minute_events(self, table: str, count_col: str, topic: str,
                            since_ts: int) -> list[dict]:
        """通用：读 minute_drops / minute_gaps 的 (timestamp, count)。"""
        if self._conn is None:
            return []
        try:
            with self._lock:
                cursor = self._conn.execute(
                    f"SELECT timestamp, {count_col} FROM {table} "
                    "WHERE topic = ? AND timestamp >= ? ORDER BY timestamp",
                    (topic, since_ts),
                )
                rows = cursor.fetchall()
            return [{"timestamp": r[0], "count": r[1]} for r in rows]
        except Exception:
            logger.debug(f"load {table} 失败", exc_info=True)
            return []

    def load_drop_history(self, topic: str, since_ts: int) -> list[dict]:
        return self._load_minute_events("minute_drops", "drop_count",
                                        topic, since_ts)

    def load_gap_history(self, topic: str, since_ts: int) -> list[dict]:
        return self._load_minute_events("minute_gaps", "gap_count",
                                        topic, since_ts)

    def cleanup(self, retention_days: int = 7) -> int:
        """清理过期数据，返回删除行数。"""
        if self._conn is None:
            return 0
        cutoff = int(time.time()) - retention_days * 86400
        try:
            with self._lock:
                total = 0
                for table in ("minute_stats", "minute_drops", "minute_gaps"):
                    cursor = self._conn.execute(
                        f"DELETE FROM {table} WHERE timestamp < ?", (cutoff,)
                    )
                    total += cursor.rowcount
                self._conn.commit()
                return total
        except Exception:
            logger.debug("cleanup 失败", exc_info=True)
            return 0


class AsyncArchiveWriter:
    """分钟归档异步批量写：enqueue 进 queue，consumer 任务批量落库。

    SQLite 写仅在 consumer 任务，数据接收循环不阻塞。
    队列项为 (kind, payload)：kind="minutes" → dict[str, MinuteSlot]（流量），
    "drops"/"gaps" → [(topic, ts, count)]。公开 enqueue() 保持旧的
    dict 签名（自动打 "minutes" 标签），drops/gaps 走 enqueue_rows()。
    """

    def __init__(self, storage: "StatsStorage", batch_size: int = 50) -> None:
        self._storage = storage
        self._batch_size = batch_size
        self._queue: asyncio.Queue | None = None
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._queue = asyncio.Queue()
        self._task = asyncio.create_task(self._consume())

    async def enqueue(self, archived: dict) -> None:
        if self._queue is None:
            return
        await self._queue.put(("minutes", archived))

    async def enqueue_rows(self, kind: str, rows: list) -> None:
        """kind ∈ {"drops", "gaps"}；rows 为 [(topic, ts, count)]。"""
        if self._queue is None or not rows:
            return
        await self._queue.put((kind, rows))

    async def _consume(self) -> None:
        assert self._queue is not None
        while True:
            minutes: dict = {}
            drops: list = []
            gaps: list = []
            try:
                # 阻塞取第一个，再批量取最多 batch_size-1 个
                kind, payload = await self._queue.get()
                if kind == "minutes":
                    minutes.update(payload)
                elif kind == "drops":
                    drops.extend(payload)
                elif kind == "gaps":
                    gaps.extend(payload)
                for _ in range(self._batch_size - 1):
                    try:
                        kind, payload = self._queue.get_nowait()
                        if kind == "minutes":
                            minutes.update(payload)
                        elif kind == "drops":
                            drops.extend(payload)
                        elif kind == "gaps":
                            gaps.extend(payload)
                    except asyncio.QueueEmpty:
                        break
                if minutes:
                    self._storage.save_minutes_batch(minutes)
                if drops:
                    self._storage.save_drop_minutes(drops)
                if gaps:
                    self._storage.save_gap_minutes(gaps)
            except asyncio.CancelledError:
                # 收到停止信号；剩余项由 stop() 统一 drain
                raise
            except Exception:
                pass  # 单批失败不杀 consumer

    def _drain(self) -> None:
        if self._queue is None:
            return
        minutes: dict = {}
        drops: list = []
        gaps: list = []
        while True:
            try:
                kind, payload = self._queue.get_nowait()
                if kind == "minutes":
                    minutes.update(payload)
                elif kind == "drops":
                    drops.extend(payload)
                elif kind == "gaps":
                    gaps.extend(payload)
            except asyncio.QueueEmpty:
                break
        if minutes:
            try:
                self._storage.save_minutes_batch(minutes)
            except Exception:
                pass
        if drops:
            try:
                self._storage.save_drop_minutes(drops)
            except Exception:
                pass
        if gaps:
            try:
                self._storage.save_gap_minutes(gaps)
            except Exception:
                pass

    async def stop(self) -> None:
        # 1. 取消 consumer 任务（当前正在处理的批会被放弃，但项仍在队列中）
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        # 2. 在主上下文 drain 剩余项（可靠：不依赖被取消任务内执行）
        self._drain()
        self._task = None
        self._queue = None
