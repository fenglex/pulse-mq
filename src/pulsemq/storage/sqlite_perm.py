"""SQLite PermissionGroupRepo 实现。"""

from __future__ import annotations

import sqlite3
import time

from pulsemq.storage.database import run_sync_locked
from pulsemq.storage.interfaces import (
    GroupPermission,
    PermissionGroup,
    PermissionGroupRepo,
    User,
)


class SqlitePermGroupRepo(PermissionGroupRepo):
    """基于 sqlite3 的 PermissionGroupRepo。"""

    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn

    # ---- 权限组 CRUD ----

    async def create_group(self, name: str) -> PermissionGroup:
        now = time.time()

        def _do():
            cursor = self._conn.execute(
                "INSERT INTO permission_groups (name, created_at) VALUES (?, ?)",
                (name, now),
            )
            self._conn.commit()
            return cursor.lastrowid

        gid = await run_sync_locked(_do)
        return PermissionGroup(id=gid, name=name, created_at=now)

    async def delete_group(self, group_id: int) -> None:
        def _do():
            self._conn.execute("DELETE FROM permission_groups WHERE id = ?", (group_id,))
            self._conn.commit()
        await run_sync_locked(_do)

    async def get_group(self, group_id: int) -> PermissionGroup | None:
        row = await run_sync_locked(
            lambda: self._conn.execute(
                "SELECT * FROM permission_groups WHERE id = ?", (group_id,)
            ).fetchone()
        )
        if row is None:
            return None
        return PermissionGroup(id=row["id"], name=row["name"], created_at=row["created_at"])

    async def list_groups(self) -> list[PermissionGroup]:
        rows = await run_sync_locked(
            lambda: self._conn.execute(
                "SELECT * FROM permission_groups ORDER BY id"
            ).fetchall()
        )
        return [
            PermissionGroup(id=r["id"], name=r["name"], created_at=r["created_at"])
            for r in rows
        ]

    # ---- 权限管理 ----

    async def add_permission(self, group_id: int, topic_pattern: str, action: str) -> None:
        def _do():
            self._conn.execute(
                """INSERT OR IGNORE INTO group_permissions (group_id, topic_pattern, action)
                   VALUES (?, ?, ?)""",
                (group_id, topic_pattern, action),
            )
            self._conn.commit()
        await run_sync_locked(_do)

    async def remove_permission(self, group_id: int, topic_pattern: str, action: str) -> None:
        def _do():
            self._conn.execute(
                "DELETE FROM group_permissions WHERE group_id=? AND topic_pattern=? AND action=?",
                (group_id, topic_pattern, action),
            )
            self._conn.commit()
        await run_sync_locked(_do)

    async def get_permissions(self, group_id: int) -> list[GroupPermission]:
        rows = await run_sync_locked(
            lambda: self._conn.execute(
                "SELECT * FROM group_permissions WHERE group_id = ? ORDER BY id",
                (group_id,),
            ).fetchall()
        )
        return [
            GroupPermission(id=r["id"], group_id=r["group_id"],
                            topic_pattern=r["topic_pattern"], action=r["action"])
            for r in rows
        ]

    # ---- 成员管理 ----

    async def add_member(self, group_id: int, user_id: int) -> None:
        def _do():
            self._conn.execute(
                "INSERT OR IGNORE INTO user_groups (user_id, group_id) VALUES (?, ?)",
                (user_id, group_id),
            )
            self._conn.commit()
        await run_sync_locked(_do)

    async def remove_member(self, group_id: int, user_id: int) -> None:
        def _do():
            self._conn.execute(
                "DELETE FROM user_groups WHERE user_id=? AND group_id=?",
                (user_id, group_id),
            )
            self._conn.commit()
        await run_sync_locked(_do)

    async def get_members(self, group_id: int) -> list[User]:
        rows = await run_sync_locked(
            lambda: self._conn.execute(
                """SELECT u.* FROM users u
                   JOIN user_groups ug ON u.id = ug.user_id
                   WHERE ug.group_id = ? ORDER BY u.id""",
                (group_id,),
            ).fetchall()
        )
        return [self._row_to_user(r) for r in rows]

    async def get_user_groups(self, user_id: int) -> list[PermissionGroup]:
        rows = await run_sync_locked(
            lambda: self._conn.execute(
                """SELECT pg.* FROM permission_groups pg
                   JOIN user_groups ug ON pg.id = ug.group_id
                   WHERE ug.user_id = ? ORDER BY pg.id""",
                (user_id,),
            ).fetchall()
        )
        return [
            PermissionGroup(id=r["id"], name=r["name"], created_at=r["created_at"])
            for r in rows
        ]

    # ---- 权限展开 ----

    async def get_user_expanded_permissions(self, user_id: int) -> dict[str, list[str]]:
        """查询用户所有权限组并展开为 {action: [topic_pattern_list]}。"""
        rows = await run_sync_locked(
            lambda: self._conn.execute(
                """SELECT DISTINCT gp.action, gp.topic_pattern
                   FROM group_permissions gp
                   JOIN user_groups ug ON gp.group_id = ug.group_id
                   WHERE ug.user_id = ?""",
                (user_id,),
            ).fetchall()
        )

        result: dict[str, list[str]] = {}
        for r in rows:
            action = r["action"]
            pattern = r["topic_pattern"]
            if action not in result:
                result[action] = []
            if pattern not in result[action]:
                result[action].append(pattern)
        return result

    async def get_group_all_members(self, group_id: int) -> list[int]:
        rows = await run_sync_locked(
            lambda: self._conn.execute(
                "SELECT user_id FROM user_groups WHERE group_id = ?",
                (group_id,),
            ).fetchall()
        )
        return [r["user_id"] for r in rows]

    @staticmethod
    def _row_to_user(row: sqlite3.Row) -> User:
        return User(
            id=row["id"],
            username=row["username"],
            api_key=row["api_key"],
            role=row["role"],
            namespace=row["namespace"],
            disabled=bool(row["disabled"]),
            max_connections=row["max_connections"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )
