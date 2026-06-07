"""auth/permission.py 单测。

覆盖:
- topic_match 通配符边界
- PermissionCache.has_permission
- PermissionService.check_permission (admin 短路、缓存命中、缓存过期、缓存失效)
- invalidate_user / invalidate_group_members / clear_cache
- perm_repo 异常时不缓存
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from pulsemq.auth.permission import (
    PermissionCache,
    PermissionService,
    topic_match,
)
from pulsemq.models import AuthUser


# ---- topic_match 边界（独立于 test_router.py）----


def test_topic_match_exact_match():
    assert topic_match("a.b.c", "a.b.c")
    assert not topic_match("a.b.c", "a.b.x")
    assert not topic_match("a.b.c", "a")


def test_topic_match_star_middle_one_segment_only():
    """中间 `*` 匹配恰好一段。"""
    assert topic_match("a.*.c", "a.b.c")
    assert not topic_match("a.*.c", "a.b.x.c")
    assert not topic_match("a.*.c", "a.c")  # 缺段


def test_topic_match_star_end_one_or_more():
    """末尾 `*` 匹配一段或多段。"""
    assert topic_match("a.*", "a.b")
    assert topic_match("a.*", "a.b.c")
    assert not topic_match("a.*", "a")  # 至少一段


def test_topic_match_gt_one_or_more():
    """`>` 至少匹配一段。"""
    assert topic_match("a.>", "a.b")
    assert topic_match("a.>", "a.b.c.d")
    assert not topic_match("a.>", "a")  # 零段不允许


def test_topic_match_gt_middle():
    """`>` 在中间: 前面匹配后, 后面必须存在。"""
    assert topic_match("a.>.c", "a.b.c")
    assert topic_match("a.>.c", "a.b.x.c")
    assert not topic_match("a.>.c", "a.c")  # `>` 至少一段


def test_topic_match_empty_pattern_and_topic():
    assert topic_match("", "")
    assert not topic_match("", "x")
    assert not topic_match("x", "")


def test_topic_match_star_in_middle_does_not_match_empty_segment():
    """中间 `*` 段不匹配空段。"""
    assert not topic_match("a.*.c", "a..c")


# ---- PermissionCache ----


def test_permission_cache_has_permission_match():
    cache = PermissionCache(
        user_id=1,
        permissions={"pub": ["a.b", "c.>"], "sub": ["x.*"], "query": []},
        cached_at=time.time(),
        ttl=60,
    )
    assert cache.has_permission("pub", "a.b") is True
    assert cache.has_permission("pub", "c.x.y") is True
    assert cache.has_permission("sub", "x.foo") is True
    assert cache.has_permission("pub", "a.x") is False  # a.b 严格匹配
    assert cache.has_permission("query", "anything") is False  # 空列表


def test_permission_cache_has_permission_action_missing():
    """actions 字典里没有该 action → False。"""
    cache = PermissionCache(
        user_id=1,
        permissions={"pub": ["a.>"]},
        cached_at=time.time(),
        ttl=60,
    )
    assert cache.has_permission("sub", "a.b") is False


def test_permission_cache_is_expired_by_ttl():
    """TTL 到期后 is_expired 为 True。"""
    cache = PermissionCache(
        user_id=1,
        permissions={"pub": ["a"]},
        cached_at=time.time() - 100,
        ttl=10,
    )
    assert cache.is_expired() is True


def test_permission_cache_not_expired_within_ttl():
    cache = PermissionCache(
        user_id=1,
        permissions={"pub": ["a"]},
        cached_at=time.time(),
        ttl=60,
    )
    assert cache.is_expired() is False


# ---- PermissionService ----


class _FakePermRepo:
    """最小 perm_repo mock: get_user_expanded_permissions 返回固定 dict。
    支持 raise_to_fail 控制是否抛异常。"""

    def __init__(self, perms: dict[str, list[str]] | None = None, raise_to_fail: bool = False):
        self._perms = perms or {"pub": [], "sub": [], "query": []}
        self._raise = raise_to_fail
        self.call_count = 0

    async def get_user_expanded_permissions(self, user_id: int) -> dict[str, list[str]]:
        self.call_count += 1
        if self._raise:
            raise RuntimeError("DB down")
        return self._perms


def _user(uid: int = 1, role: str = "user") -> AuthUser:
    return AuthUser(user_id=uid, role=role, groups=[], api_key="k", namespace="")


@pytest.mark.asyncio
async def test_check_permission_admin_short_circuits():
    """admin 直接通过, 不查 repo。"""
    repo = _FakePermRepo(perms={"pub": []})  # 即便没权限也通过
    svc = PermissionService(perm_repo=repo, ttl=60)
    admin = _user(uid=1, role="admin")

    ok = await svc.check_permission(admin, "pub", "anything")
    assert ok is True
    # admin 短路不应触发 DB 调用
    assert repo.call_count == 0


@pytest.mark.asyncio
async def test_check_permission_user_granted_pattern():
    """user 命中授权 pattern → True。"""
    repo = _FakePermRepo(perms={"pub": ["team-a.mkt.*"]})
    svc = PermissionService(perm_repo=repo, ttl=60)

    assert await svc.check_permission(_user(uid=1), "pub", "team-a.mkt.sh.600000") is True
    assert repo.call_count == 1  # 第一次查 DB


@pytest.mark.asyncio
async def test_check_permission_user_denied_pattern():
    """user 未匹配 pattern → False。"""
    repo = _FakePermRepo(perms={"pub": ["team-a.mkt.*"]})
    svc = PermissionService(perm_repo=repo, ttl=60)

    assert await svc.check_permission(_user(uid=1), "pub", "team-b.mkt.x") is False


@pytest.mark.asyncio
async def test_check_permission_caches_result():
    """同 user 二次 check, 不应再查 DB。"""
    repo = _FakePermRepo(perms={"pub": ["a.>"]})
    svc = PermissionService(perm_repo=repo, ttl=60)

    await svc.check_permission(_user(uid=1), "pub", "a.b")
    await svc.check_permission(_user(uid=1), "pub", "a.c.d")
    await svc.check_permission(_user(uid=1), "sub", "anything")  # action 不同, 但用户同一

    assert repo.call_count == 1  # 缓存命中


@pytest.mark.asyncio
async def test_check_permission_cache_expires_and_reloads():
    """TTL 到期后下一次 check 应重新加载。"""
    repo = _FakePermRepo(perms={"pub": ["a.>"]})
    svc = PermissionService(perm_repo=repo, ttl=0.05)  # 50ms TTL

    await svc.check_permission(_user(uid=1), "pub", "a.b")
    assert repo.call_count == 1

    time.sleep(0.1)
    await svc.check_permission(_user(uid=1), "pub", "a.b")
    assert repo.call_count == 2  # 缓存失效, 重新加载


@pytest.mark.asyncio
async def test_invalidate_user_forces_reload():
    """invalidate_user 后再次 check 触发 DB。"""
    repo = _FakePermRepo(perms={"pub": ["a.>"]})
    svc = PermissionService(perm_repo=repo, ttl=60)

    await svc.check_permission(_user(uid=1), "pub", "a.b")
    assert repo.call_count == 1

    svc.invalidate_user(1)
    await svc.check_permission(_user(uid=1), "pub", "a.b")
    assert repo.call_count == 2


@pytest.mark.asyncio
async def test_invalidate_user_for_unknown_user_is_noop():
    """invalidate 不存在的 user_id 不抛异常。"""
    repo = _FakePermRepo()
    svc = PermissionService(perm_repo=repo, ttl=60)
    svc.invalidate_user(9999)  # 静默忽略


@pytest.mark.asyncio
async def test_invalidate_group_members_clears_all_listed():
    """invalidate_group_members 清空给定 user_id 列表的缓存。"""
    repo = _FakePermRepo(perms={"pub": ["a.>"]})
    svc = PermissionService(perm_repo=repo, ttl=60)

    # 缓存 3 个用户
    for uid in (1, 2, 3):
        await svc.check_permission(_user(uid=uid), "pub", "a.b")
    assert repo.call_count == 3

    # 失效 1, 2
    svc.invalidate_group_members([1, 2])

    # 1, 2 重新加载, 3 命中缓存
    for uid in (1, 2, 3):
        await svc.check_permission(_user(uid=uid), "pub", "a.b")
    assert repo.call_count == 5  # +2


@pytest.mark.asyncio
async def test_clear_cache_clears_everyone():
    """clear_cache 之后所有用户都重新加载。"""
    repo = _FakePermRepo(perms={"pub": ["a.>"]})
    svc = PermissionService(perm_repo=repo, ttl=60)

    for uid in (1, 2):
        await svc.check_permission(_user(uid=uid), "pub", "a.b")
    assert repo.call_count == 2

    svc.clear_cache()

    for uid in (1, 2):
        await svc.check_permission(_user(uid=uid), "pub", "a.b")
    assert repo.call_count == 4


@pytest.mark.asyncio
async def test_check_permission_does_not_cache_on_repo_exception():
    """repo 抛异常时, 不应写入缓存 (失败不污染)。"""
    repo = _FakePermRepo(perms={"pub": ["a.>"]}, raise_to_fail=True)
    svc = PermissionService(perm_repo=repo, ttl=60)

    with pytest.raises(RuntimeError):
        await svc.check_permission(_user(uid=1), "pub", "a.b")

    # 改 repo, 不抛
    repo._raise = False
    await svc.check_permission(_user(uid=1), "pub", "a.b")
    # 第一次失败未缓存, 第二次仍调用 DB
    assert repo.call_count == 2


@pytest.mark.asyncio
async def test_different_users_have_independent_caches():
    """不同 user_id 的缓存互相独立。"""
    repo = _FakePermRepo(perms={"pub": ["a.>"]})
    svc = PermissionService(perm_repo=repo, ttl=60)

    await svc.check_permission(_user(uid=1), "pub", "a.b")
    await svc.check_permission(_user(uid=2), "pub", "a.b")
    assert repo.call_count == 2  # 各查一次

    await svc.check_permission(_user(uid=1), "pub", "a.c")
    assert repo.call_count == 2  # 1 命中缓存


# ---- AuthUser.is_admin 与 check_permission 的契约 ----


@pytest.mark.asyncio
async def test_admin_role_string_must_be_exact():
    """非 'admin' 字符串的 role 不视作 admin（防 typo 越权）。"""
    repo = _FakePermRepo(perms={"pub": ["a.>"]})
    svc = PermissionService(perm_repo=repo, ttl=60)

    # 'Admin'/'ADMIN'/'' 均不应短路 → 走 repo → 缓存命中
    # 即便有 cache, 所有 4 个 role 变体都应得到 False（topic "x.y" 不匹配 "a.>"）
    for role in ("Admin", "ADMIN", "root", ""):
        assert await svc.check_permission(_user(uid=1, role=role), "pub", "x.y") is False
    # 同一 user_id 共享缓存, 4 次只查 1 次
    assert repo.call_count == 1


@pytest.mark.asyncio
async def test_role_admin_strict_equality():
    """仅 role 严格 == 'admin' 才短路。"""
    repo = _FakePermRepo(perms={"pub": []})  # 没权限, 用作反向验证
    svc = PermissionService(perm_repo=repo, ttl=60)

    # 真正的 admin 短路, 即便无 pub 权限也通过
    assert await svc.check_permission(_user(uid=1, role="admin"), "pub", "x") is True
    assert repo.call_count == 0  # admin 短路, 不查 repo

    # 非 admin 必须查 repo
    assert await svc.check_permission(_user(uid=2, role="Admin"), "pub", "x") is False
    assert repo.call_count == 1
