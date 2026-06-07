"""SqlitePermGroupRepo 单测。

覆盖:
- 权限组 CRUD (create_group / get_group / list_groups / delete_group)
- 权限管理 (add_permission / remove_permission / get_permissions)
- 成员管理 (add_member / remove_member / get_members / get_user_groups)
- 权限展开 (get_user_expanded_permissions)
- UNIQUE 约束 (重复 group / 重复 permission)
"""
from __future__ import annotations

import os
import tempfile

import pytest
import pytest_asyncio

from pulsemq.storage.database import init_db
from pulsemq.storage.interfaces import User
from pulsemq.storage.sqlite_perm import SqlitePermGroupRepo
from pulsemq.storage.sqlite_user import SqliteUserRepo


@pytest_asyncio.fixture
async def repos():
    """每个测试一个临时 db, 包含 user_repo + perm_repo。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    conn = init_db(path)
    user_repo = SqliteUserRepo(conn)
    perm_repo = SqlitePermGroupRepo(conn)
    try:
        yield user_repo, perm_repo
    finally:
        conn.close()
        os.unlink(path)


# ---- 权限组 CRUD ----


@pytest.mark.asyncio
async def test_create_group_and_get(repos):
    _, perm = repos
    g = await perm.create_group("admins")
    assert g.id is not None and g.id > 0
    assert g.name == "admins"
    assert g.created_at > 0

    got = await perm.get_group(g.id)
    assert got is not None
    assert got.name == "admins"


@pytest.mark.asyncio
async def test_get_group_missing_returns_none(repos):
    _, perm = repos
    assert await perm.get_group(99999) is None


@pytest.mark.asyncio
async def test_list_groups_empty_after_init(repos):
    """init_db 不创建 group, list_groups 应只含 init 后的空集。"""
    _, perm = repos
    assert await perm.list_groups() == []


@pytest.mark.asyncio
async def test_list_groups_ordered(repos):
    _, perm = repos
    await perm.create_group("g1")
    await perm.create_group("g2")
    groups = await perm.list_groups()
    assert [g.name for g in groups] == ["g1", "g2"]


@pytest.mark.asyncio
async def test_duplicate_group_name_raises(repos):
    import sqlite3

    _, perm = repos
    await perm.create_group("admins")
    with pytest.raises(sqlite3.IntegrityError):
        await perm.create_group("admins")


@pytest.mark.asyncio
async def test_delete_group(repos):
    _, perm = repos
    g = await perm.create_group("g1")
    await perm.delete_group(g.id)
    assert await perm.get_group(g.id) is None
    assert await perm.list_groups() == []


@pytest.mark.asyncio
async def test_delete_group_missing_is_noop(repos):
    """删除不存在 group 不抛错。"""
    _, perm = repos
    await perm.delete_group(99999)


# ---- 权限管理 ----


@pytest.mark.asyncio
async def test_add_and_get_permissions(repos):
    _, perm = repos
    g = await perm.create_group("g1")
    await perm.add_permission(g.id, "topic.*", "pub")
    await perm.add_permission(g.id, "topic.*", "sub")
    await perm.add_permission(g.id, "user.#", "query")

    perms = await perm.get_permissions(g.id)
    assert len(perms) == 3
    actions = sorted([p.action for p in perms])
    assert actions == ["pub", "query", "sub"]
    patterns = sorted([p.topic_pattern for p in perms])
    assert patterns == ["topic.*", "topic.*", "user.#"]


@pytest.mark.asyncio
async def test_add_permission_duplicate_ignored(repos):
    """INSERT OR IGNORE: 重复 (group, pattern, action) 不应抛错也不应重复。"""
    _, perm = repos
    g = await perm.create_group("g1")
    await perm.add_permission(g.id, "topic.*", "pub")
    await perm.add_permission(g.id, "topic.*", "pub")
    perms = await perm.get_permissions(g.id)
    assert len(perms) == 1


@pytest.mark.asyncio
async def test_remove_permission(repos):
    _, perm = repos
    g = await perm.create_group("g1")
    await perm.add_permission(g.id, "topic.*", "pub")
    await perm.add_permission(g.id, "topic.*", "sub")
    await perm.remove_permission(g.id, "topic.*", "pub")
    perms = await perm.get_permissions(g.id)
    assert len(perms) == 1
    assert perms[0].action == "sub"


@pytest.mark.asyncio
async def test_get_permissions_empty(repos):
    _, perm = repos
    g = await perm.create_group("g1")
    assert await perm.get_permissions(g.id) == []


# ---- 成员管理 ----


@pytest.mark.asyncio
async def test_add_and_get_members(repos):
    user_repo, perm = repos
    u1 = await user_repo.create(User(username="alice", api_key="k1"))
    u2 = await user_repo.create(User(username="bob", api_key="k2"))
    g = await perm.create_group("g1")
    await perm.add_member(g.id, u1.id)
    await perm.add_member(g.id, u2.id)

    members = await perm.get_members(g.id)
    ids = sorted([m.id for m in members])
    assert ids == sorted([u1.id, u2.id])


@pytest.mark.asyncio
async def test_add_member_duplicate_ignored(repos):
    user_repo, perm = repos
    u = await user_repo.create(User(username="alice", api_key="k1"))
    g = await perm.create_group("g1")
    await perm.add_member(g.id, u.id)
    await perm.add_member(g.id, u.id)
    members = await perm.get_members(g.id)
    assert len(members) == 1


@pytest.mark.asyncio
async def test_remove_member(repos):
    user_repo, perm = repos
    u1 = await user_repo.create(User(username="alice", api_key="k1"))
    u2 = await user_repo.create(User(username="bob", api_key="k2"))
    g = await perm.create_group("g1")
    await perm.add_member(g.id, u1.id)
    await perm.add_member(g.id, u2.id)
    await perm.remove_member(g.id, u1.id)
    members = await perm.get_members(g.id)
    assert [m.id for m in members] == [u2.id]


@pytest.mark.asyncio
async def test_get_user_groups(repos):
    user_repo, perm = repos
    u = await user_repo.create(User(username="alice", api_key="k1"))
    g1 = await perm.create_group("g1")
    g2 = await perm.create_group("g2")
    await perm.add_member(g1.id, u.id)
    await perm.add_member(g2.id, u.id)
    groups = await perm.get_user_groups(u.id)
    assert sorted([g.name for g in groups]) == ["g1", "g2"]


@pytest.mark.asyncio
async def test_get_group_all_members(repos):
    user_repo, perm = repos
    u1 = await user_repo.create(User(username="alice", api_key="k1"))
    u2 = await user_repo.create(User(username="bob", api_key="k2"))
    g = await perm.create_group("g1")
    await perm.add_member(g.id, u1.id)
    await perm.add_member(g.id, u2.id)
    ids = await perm.get_group_all_members(g.id)
    assert sorted(ids) == sorted([u1.id, u2.id])


# ---- 权限展开 ----


@pytest.mark.asyncio
async def test_get_user_expanded_permissions_single_group(repos):
    user_repo, perm = repos
    u = await user_repo.create(User(username="alice", api_key="k1"))
    g = await perm.create_group("g1")
    await perm.add_member(g.id, u.id)
    await perm.add_permission(g.id, "topic.*", "pub")
    await perm.add_permission(g.id, "topic.*", "sub")
    await perm.add_permission(g.id, "user.#", "query")

    expanded = await perm.get_user_expanded_permissions(u.id)
    assert set(expanded.keys()) == {"pub", "sub", "query"}
    assert "topic.*" in expanded["pub"]
    assert "topic.*" in expanded["sub"]
    assert "user.#" in expanded["query"]


@pytest.mark.asyncio
async def test_get_user_expanded_permissions_multi_group(repos):
    """用户在多个组, 权限合并去重。"""
    user_repo, perm = repos
    u = await user_repo.create(User(username="alice", api_key="k1"))
    g1 = await perm.create_group("g1")
    g2 = await perm.create_group("g2")
    await perm.add_member(g1.id, u.id)
    await perm.add_member(g2.id, u.id)
    await perm.add_permission(g1.id, "topic.*", "pub")
    await perm.add_permission(g2.id, "user.#", "pub")  # 跨 group 同 action 不同 pattern
    await perm.add_permission(g1.id, "topic.*", "pub")  # 重复, 展开后去重
    await perm.add_permission(g2.id, "topic.*", "pub")  # 跨 group 重复, 展开后去重

    expanded = await perm.get_user_expanded_permissions(u.id)
    assert sorted(expanded["pub"]) == ["topic.*", "user.#"]


@pytest.mark.asyncio
async def test_get_user_expanded_permissions_no_group(repos):
    """用户不在任何组, 权限应为空字典。"""
    user_repo, perm = repos
    u = await user_repo.create(User(username="alice", api_key="k1"))
    expanded = await perm.get_user_expanded_permissions(u.id)
    assert expanded == {}


# ---- 删除 group 级联 ----


@pytest.mark.asyncio
async def test_delete_group_cascades_permissions(repos):
    """FK ON DELETE CASCADE: 删除 group 后, group_permissions 应被清空。
    注: SQLite 默认不启用 FK, 但 init_db 显式 PRAGMA foreign_keys=ON, 应当级联。
    """
    user_repo, perm = repos
    u = await user_repo.create(User(username="alice", api_key="k1"))
    g = await perm.create_group("g1")
    await perm.add_member(g.id, u.id)
    await perm.add_permission(g.id, "topic.*", "pub")

    # 先验证插入成功
    assert len(await perm.get_permissions(g.id)) == 1

    await perm.delete_group(g.id)
    # 级联后 group_permissions 应被清空
    # (直接用底层 conn 查询 group_permissions 表, 因为 repo 没暴露 count 接口)
    rows = perm._conn.execute(
        "SELECT COUNT(*) AS c FROM group_permissions WHERE group_id = ?", (g.id,)
    ).fetchone()
    assert rows["c"] == 0
