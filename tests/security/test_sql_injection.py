"""SQL 注入 fuzz 测试。

在 username / api_key / group name / topic_pattern / action 字段注入典型
SQL 注入 payload, 验证全部被参数化绑定, 没有执行注入, 且表结构完整。

注: 实现使用 `?` 占位符 + 参数化绑定, 任何注入 payload 都应被当作普通
字符串处理。我们 fuzz 多个变体确认这一点。
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


# ---- 注入 payload 库 ----

USERNAME_PAYLOADS = [
    "admin' OR '1'='1",
    "admin'; DROP TABLE users; --",
    "admin'/*",
    "' UNION SELECT * FROM users --",
    "admin\x00injected",
    "x' OR 1=1 --",
    "x'); DELETE FROM users; --",
    "'; EXEC xp_cmdshell('dir'); --",
    "' OR ''='",
    "x' AND (SELECT COUNT(*) FROM users) > 0 --",
]

API_KEY_PAYLOADS = [
    "' OR '1'='1",
    "k'; DROP TABLE users; --",
    "k' UNION SELECT api_key FROM users --",
    "k\x00null",
]

GROUP_NAME_PAYLOADS = [
    "g'; DROP TABLE permission_groups; --",
    "g' OR 1=1 --",
    "' UNION SELECT * FROM users --",
    "g\x00null",
]

PERM_PAYLOADS = [
    "topic.*'; DROP TABLE permissions; --",
    "topic' OR 1=1 --",
    "topic.*' UNION SELECT 1,2,3 --",
    "t\x00null",
]

ACTION_PAYLOADS = [
    "pub' OR '1'='1",
    "pub'; DROP TABLE group_permissions; --",
    "'; --",
]


# ---- fixtures ----


@pytest_asyncio.fixture
async def repos():
    """每个测试一个临时 db, 含 user_repo + perm_repo。"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    conn = init_db(path)
    user_repo = SqliteUserRepo(conn)
    perm_repo = SqlitePermGroupRepo(conn)
    try:
        yield user_repo, perm_repo, conn
    finally:
        conn.close()
        os.unlink(path)


def _u(username: str, api_key: str = "k") -> User:
    return User(
        id=None,
        username=username,
        api_key=api_key,
        role="user",
        namespace="ns",
        disabled=False,
        max_connections=10,
    )


def _check_table_intact(conn, table: str) -> bool:
    """表是否仍可查询 (DDL 没被破坏)。"""
    try:
        rows = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
        return rows is not None
    except Exception:
        return False


# ---- username 注入 ----


@pytest.mark.asyncio
@pytest.mark.parametrize("malicious_username", USERNAME_PAYLOADS)
async def test_sql_injection_in_username(repos, malicious_username: str):
    """username 注入应被当作普通字符串处理, 不执行, 表结构完整。"""
    user_repo, _, conn = repos
    try:
        await user_repo.create(_u(username=malicious_username))
    except Exception:
        # 注册失败可接受 (UNIQUE 冲突等), 只要不破坏表
        pass

    # 表结构完整: 仍能正常注册
    await user_repo.create(_u(username="normal", api_key="k-normal"))
    u = await user_repo.get_by_api_key("k-normal")
    assert u is not None
    assert u.username == "normal"

    # 验证表结构没被破坏
    assert _check_table_intact(conn, "users")


@pytest.mark.asyncio
@pytest.mark.parametrize("malicious", USERNAME_PAYLOADS)
async def test_sql_injection_in_username_lookup(repos, malicious: str):
    """get_by_api_key 用注入 payload 当 key 查, 不应返回 admin (除非 payload == admin key)。"""
    _, _, conn = repos
    # admin 用户的 api_key 是 'pulse_sk_admin_default', 注入 payload 不应匹配
    u = await repos[0].get_by_api_key(malicious)
    assert u is None
    # 仍能查到真 admin
    admin = await repos[0].get_by_api_key("pulse_sk_admin_default")
    assert admin is not None
    assert admin.username == "admin"


# ---- api_key 注入 ----


@pytest.mark.asyncio
@pytest.mark.parametrize("malicious", API_KEY_PAYLOADS)
async def test_sql_injection_in_api_key(repos, malicious: str):
    """api_key 注入应被参数化, 表完整。"""
    user_repo, _, conn = repos
    try:
        await user_repo.create(_u(username=f"u_{hash(malicious) & 0xffff:04x}", api_key=malicious))
    except Exception:
        pass

    assert _check_table_intact(conn, "users")
    # 仍能正常注册
    await user_repo.create(_u(username="normal", api_key="k-ok"))
    u = await user_repo.get_by_api_key("k-ok")
    assert u is not None


# ---- group name 注入 ----


@pytest.mark.asyncio
@pytest.mark.parametrize("malicious", GROUP_NAME_PAYLOADS)
async def test_sql_injection_in_group_name(repos, malicious: str):
    """group name 注入应被参数化。"""
    _, perm, conn = repos
    try:
        await perm.create_group(malicious)
    except Exception:
        pass

    # 表结构完整
    assert _check_table_intact(conn, "permission_groups")
    # 仍能正常创建
    g = await perm.create_group("ok-group")
    assert g.id is not None
    assert g.name == "ok-group"


# ---- topic_pattern 注入 ----


@pytest.mark.asyncio
@pytest.mark.parametrize("malicious", PERM_PAYLOADS)
async def test_sql_injection_in_topic_pattern(repos, malicious: str):
    """topic_pattern 注入应被参数化。"""
    user_repo, perm, conn = repos
    u = await user_repo.create(_u(username="alice", api_key="k1"))
    g = await perm.create_group("g1")
    await perm.add_member(g.id, u.id)
    try:
        await perm.add_permission(g.id, malicious, "pub")
    except Exception:
        pass

    # 表结构完整
    assert _check_table_intact(conn, "group_permissions")
    # 仍能正常添加
    await perm.add_permission(g.id, "ok.topic", "pub")
    perms = await perm.get_permissions(g.id)
    patterns = [p.topic_pattern for p in perms]
    assert "ok.topic" in patterns


# ---- action 注入 ----


@pytest.mark.asyncio
@pytest.mark.parametrize("malicious", ACTION_PAYLOADS)
async def test_sql_injection_in_action(repos, malicious: str):
    """action 注入应被参数化。"""
    user_repo, perm, conn = repos
    u = await user_repo.create(_u(username="alice", api_key="k1"))
    g = await perm.create_group("g1")
    await perm.add_member(g.id, u.id)
    try:
        await perm.add_permission(g.id, "topic.*", malicious)
    except Exception:
        pass

    assert _check_table_intact(conn, "group_permissions")
    # 仍能正常添加
    await perm.add_permission(g.id, "topic.*", "ok-action")
    perms = await perm.get_permissions(g.id)
    actions = [p.action for p in perms]
    assert "ok-action" in actions


# ---- 删除路径注入 ----


@pytest.mark.asyncio
@pytest.mark.parametrize("malicious", PERM_PAYLOADS)
async def test_sql_injection_in_remove_permission(repos, malicious: str):
    """remove_permission 用注入 payload 不应误删其他行, 不应破坏表。"""
    user_repo, perm, conn = repos
    u = await user_repo.create(_u(username="alice", api_key="k1"))
    g = await perm.create_group("g1")
    await perm.add_member(g.id, u.id)
    # 真实行
    await perm.add_permission(g.id, "real.topic", "pub")
    # 用注入 payload 删
    try:
        await perm.remove_permission(g.id, malicious, "pub")
    except Exception:
        pass

    # 真行应还在
    perms = await perm.get_permissions(g.id)
    patterns = [p.topic_pattern for p in perms]
    assert "real.topic" in patterns
    assert _check_table_intact(conn, "group_permissions")


# ---- auth bypass 注入 ----


@pytest.mark.asyncio
@pytest.mark.parametrize("malicious", USERNAME_PAYLOADS)
async def test_sql_injection_does_not_bypass_auth_lookup(repos, malicious: str):
    """恶意 username 不应被当成有效用户身份命中 get_by_api_key。
    反过来: 用 admin api_key 应总能查回 admin, 不受其他用户注入影响。
    """
    user_repo, _, _ = repos
    try:
        await user_repo.create(_u(username=malicious, api_key="x"))
    except Exception:
        pass

    # admin 仍能用真实 api_key 查到
    admin = await user_repo.get_by_api_key("pulse_sk_admin_default")
    assert admin is not None
    assert admin.username == "admin"
    assert admin.role == "admin"
