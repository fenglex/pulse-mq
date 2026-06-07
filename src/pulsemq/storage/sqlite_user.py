"""SQLite UserRepository 实现。"""

from __future__ import annotations

import sqlite3
import time

from pulsemq.storage.database import run_sync_locked
from pulsemq.storage.interfaces import User, UserRepository


class SqliteUserRepo(UserRepository):
    """基于 sqlite3 的 UserRepository（同步 IO 通过 run_in_executor 执行）。"""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    async def get_by_id(self, user_id: int) -> User | None:
        row = await run_sync_locked(
            lambda: self._conn.execute(
                "SELECT * FROM users WHERE id = ?", (user_id,)
            ).fetchone()
        )
        return self._row_to_user(row) if row else None

    async def get_by_api_key(self, api_key: str) -> User | None:
        row = await run_sync_locked(
            lambda: self._conn.execute(
                "SELECT * FROM users WHERE api_key = ?", (api_key,)
            ).fetchone()
        )
        return self._row_to_user(row) if row else None

    async def create(self, user: User) -> User:
        now = time.time()

        def _do():
            cursor = self._conn.execute(
                """INSERT INTO users (username, api_key, role, namespace, disabled, max_connections,
                                      batch_size, batch_interval_ms, batch_max_wait_ms,
                                      created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (user.username, user.api_key, user.role, user.namespace,
                 int(user.disabled), user.max_connections,
                 user.batch_size, user.batch_interval_ms, user.batch_max_wait_ms,
                 now, now),
            )
            self._conn.commit()
            return cursor.lastrowid

        user.id = await run_sync_locked(_do)
        user.created_at = now
        user.updated_at = now
        return user

    async def update(self, user: User) -> User:
        now = time.time()

        def _do():
            self._conn.execute(
                """UPDATE users SET username=?, api_key=?, role=?, namespace=?,
                   disabled=?, max_connections=?,
                   batch_size=?, batch_interval_ms=?, batch_max_wait_ms=?,
                   updated_at=?
                   WHERE id=?""",
                (user.username, user.api_key, user.role, user.namespace,
                 int(user.disabled), user.max_connections,
                 user.batch_size, user.batch_interval_ms, user.batch_max_wait_ms,
                 now, user.id),
            )
            self._conn.commit()

        await run_sync_locked(_do)
        user.updated_at = now
        return user

    async def delete(self, user_id: int) -> None:
        def _do():
            self._conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
            self._conn.commit()
        await run_sync_locked(_do)

    async def list_all(self) -> list[User]:
        rows = await run_sync_locked(
            lambda: self._conn.execute("SELECT * FROM users ORDER BY id").fetchall()
        )
        return [self._row_to_user(r) for r in rows]

    @staticmethod
    def _row_to_user(row: sqlite3.Row) -> User:
        # 兼容老库（Phase 7 之前没有 batch_* 字段）
        def _col(name, default):
            try:
                return row[name]
            except (IndexError, KeyError):
                return default
        return User(
            id=row["id"],
            username=row["username"],
            api_key=row["api_key"],
            role=row["role"],
            namespace=row["namespace"],
            disabled=bool(row["disabled"]),
            max_connections=row["max_connections"],
            batch_size=_col("batch_size", 100),
            batch_interval_ms=_col("batch_interval_ms", 50),
            batch_max_wait_ms=_col("batch_max_wait_ms", 200),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
