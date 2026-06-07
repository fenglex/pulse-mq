"""SqliteUserRepo 单测。

覆盖:
- create / get_by_id / get_by_api_key / update / delete / list_all
- UNIQUE 约束 (重复 username / api_key)
- 空表/非空表查询
- 字段 (disabled / max_connections / role) 正确读写
"""
from __future__ import annotations

import os
import tempfile

import pytest
import pytest_asyncio

from pulsemq.storage.database import init_db
from pulsemq.storage.interfaces import User
from pulsemq.storage.sqlite_user import SqliteUserRepo


@pytest_asyncio.fixture
async def repo():
    """每个测试一个临时 db。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    conn = init_db(path)
    r = SqliteUserRepo(conn)
    try:
        yield r
    finally:
        conn.close()
        os.unlink(path)


def _user(uid: int = 0, username: str = "u", api_key: str = "k", **kw) -> User:
    defaults = dict(
        id=None,
        username=username,
        api_key=api_key,
        role="user",
        namespace="ns",
        disabled=False,
        max_connections=10,
    )
    defaults.update(kw)
    return User(**defaults)


# ---- create + get_by_id ----


@pytest.mark.asyncio
async def test_create_and_get_by_id(repo: SqliteUserRepo):
    """create 后能 get_by_id 取回。"""
    u = await repo.create(_user(username="alice", api_key="k1"))
    assert u.id is not None and u.id > 0
    assert u.created_at > 0
    assert u.updated_at > 0

    got = await repo.get_by_id(u.id)
    assert got is not None
    assert got.username == "alice"
    assert got.api_key == "k1"
    assert got.role == "user"
    assert got.namespace == "ns"
    assert got.disabled is False
    assert got.max_connections == 10


@pytest.mark.asyncio
async def test_get_by_id_missing_returns_none(repo: SqliteUserRepo):
    """不存在的 id 返回 None。"""
    assert await repo.get_by_id(99999) is None


# ---- get_by_api_key ----


@pytest.mark.asyncio
async def test_get_by_api_key(repo: SqliteUserRepo):
    """create 后能按 api_key 查到。"""
    u = await repo.create(_user(username="bob", api_key="secret-key"))
    got = await repo.get_by_api_key("secret-key")
    assert got is not None
    assert got.id == u.id
    assert got.username == "bob"


@pytest.mark.asyncio
async def test_get_by_api_key_missing_returns_none(repo: SqliteUserRepo):
    """不存在的 api_key 返回 None。"""
    assert await repo.get_by_api_key("nope") is None


# ---- UNIQUE 约束 ----


@pytest.mark.asyncio
async def test_duplicate_username_raises(repo: SqliteUserRepo):
    """重复 username 应抛 IntegrityError。"""
    import sqlite3

    await repo.create(_user(username="alice", api_key="k1"))
    with pytest.raises(sqlite3.IntegrityError):
        await repo.create(_user(username="alice", api_key="k2"))


@pytest.mark.asyncio
async def test_duplicate_api_key_raises(repo: SqliteUserRepo):
    """重复 api_key 应抛 IntegrityError。"""
    import sqlite3

    await repo.create(_user(username="alice", api_key="k1"))
    with pytest.raises(sqlite3.IntegrityError):
        await repo.create(_user(username="bob", api_key="k1"))


# ---- update ----


@pytest.mark.asyncio
async def test_update_changes_fields(repo: SqliteUserRepo):
    """update 后字段应生效, updated_at 应刷新。"""
    u = await repo.create(_user(username="alice", api_key="k1"))
    original_updated = u.updated_at
    u.username = "alice2"
    u.role = "admin"
    u.disabled = True
    u.max_connections = 100
    u.namespace = "vip"
    out = await repo.update(u)
    assert out.updated_at >= original_updated

    got = await repo.get_by_id(u.id)
    assert got is not None
    assert got.username == "alice2"
    assert got.role == "admin"
    assert got.disabled is True
    assert got.max_connections == 100
    assert got.namespace == "vip"


# ---- delete ----


@pytest.mark.asyncio
async def test_delete_removes_user(repo: SqliteUserRepo):
    """delete 后 get_by_id 返回 None。"""
    u = await repo.create(_user(username="alice", api_key="k1"))
    await repo.delete(u.id)
    assert await repo.get_by_id(u.id) is None
    assert await repo.get_by_api_key("k1") is None


@pytest.mark.asyncio
async def test_delete_missing_is_noop(repo: SqliteUserRepo):
    """删除不存在 id 不抛错。"""
    # SQLite DELETE 不命中行不报错
    await repo.delete(99999)


# ---- list_all ----


@pytest.mark.asyncio
async def test_list_all_empty(repo: SqliteUserRepo):
    """空表返回空列表 (不算默认 admin, 因 init_db 单独事务先 commit 了)。
    注: init_db 会插一个 admin, 所以 list_all 至少含 admin。
    """
    rows = await repo.list_all()
    assert isinstance(rows, list)
    # init_db 插了一个 admin
    assert len(rows) == 1
    assert rows[0].username == "admin"


@pytest.mark.asyncio
async def test_list_all_returns_in_order(repo: SqliteUserRepo):
    """list_all 按 id 升序。"""
    await repo.create(_user(username="b", api_key="k2"))
    await repo.create(_user(username="c", api_key="k3"))
    rows = await repo.list_all()
    usernames = [r.username for r in rows]
    # admin + b + c
    assert usernames == ["admin", "b", "c"]


# ---- 字段 round-trip ----


@pytest.mark.asyncio
async def test_disabled_flag_persists(repo: SqliteUserRepo):
    """disabled=True 写入后再读出仍是 True。"""
    u = await repo.create(_user(username="alice", api_key="k1", disabled=True))
    got = await repo.get_by_id(u.id)
    assert got is not None
    assert got.disabled is True
