"""SQLite 数据库初始化与连接管理。"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from functools import partial
from pathlib import Path

# 全局写入锁，保证 SQLite 写操作线程安全
_db_write_lock = threading.Lock()

# 建表 DDL
_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    username   TEXT NOT NULL UNIQUE,
    api_key    TEXT NOT NULL UNIQUE,
    role       TEXT NOT NULL DEFAULT 'user',
    namespace  TEXT NOT NULL DEFAULT '',
    disabled   INTEGER NOT NULL DEFAULT 0,
    max_connections INTEGER NOT NULL DEFAULT 10,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS permission_groups (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL UNIQUE,
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS group_permissions (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id       INTEGER NOT NULL,
    topic_pattern  TEXT NOT NULL,
    action         TEXT NOT NULL,
    UNIQUE(group_id, topic_pattern, action),
    FOREIGN KEY(group_id) REFERENCES permission_groups(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS user_groups (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id  INTEGER NOT NULL,
    group_id INTEGER NOT NULL,
    UNIQUE(user_id, group_id),
    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY(group_id) REFERENCES permission_groups(id) ON DELETE CASCADE
);
"""

_DEFAULT_ADMIN_SQL = """
INSERT OR IGNORE INTO users (username, api_key, role, namespace, disabled, max_connections, created_at, updated_at)
VALUES ('admin', 'pulse_sk_admin_default', 'admin', '', 0, 100, ?, ?);
"""


def init_db(db_path: str) -> sqlite3.Connection:
    """初始化数据库：创建表 + 插入默认 admin。

    Args:
        db_path: SQLite 文件路径，如 "pulse_mq.db"

    Returns:
        sqlite3.Connection (同步连接，用于 Repository)
    """
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA_SQL)

    # 插入默认 admin
    now = time.time()
    conn.execute(_DEFAULT_ADMIN_SQL, (now, now))
    conn.commit()
    return conn


def parse_db_url(db_url: str) -> str:
    """解析 db_url 为文件路径。

    支持: "sqlite://./pulse_mq.db" → "./pulse_mq.db"
          "./pulse_mq.db" → "./pulse_mq.db"
    """
    if db_url.startswith("sqlite://"):
        return db_url[len("sqlite://"):]
    return db_url


async def run_sync(func, *args):
    """将同步 IO 操作放入线程池执行，避免阻塞事件循环。"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, partial(func, *args))


async def run_sync_locked(func, *args):
    """将同步 IO 操作放入线程池并在锁保护下执行。"""
    def _locked():
        with _db_write_lock:
            return func(*args)
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _locked)
