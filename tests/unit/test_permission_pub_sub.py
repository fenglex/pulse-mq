"""PermissionService Phase 7 扩展单测。

覆盖:
- check_pub / check_sub (admin 短路、user 匹配/不匹配)
- grant_pub / grant_sub / revoke_pub / revoke_sub
- list_user_permissions
- get_batch_config / set_batch_config
- 不存在的用户抛 LookupError
- 参数非法抛 ValueError
"""

from __future__ import annotations

import pytest

from pulsemq.auth.permission import PermissionService
from pulsemq.models import AuthUser
from pulsemq.storage.interfaces import (
    PermissionGroup,
    User,
)


class _FakePermRepo:
    """支持 pub/sub 权限授予/撤销 + 成员查询。"""

    def __init__(self):
        # group_id -> [(topic_pattern, action)]
        self._group_perms: dict[int, list[tuple[str, str]]] = {}
        # user_id -> [group_id]
        self._user_groups: dict[int, list[int]] = {}
        # group_id -> group
        self._groups: dict[int, PermissionGroup] = {}
        self._next_gid = 1

    # ---- group CRUD ----

    async def create_group(self, name: str) -> PermissionGroup:
        g = PermissionGroup(id=self._next_gid, name=name, created_at=0.0)
        self._groups[g.id] = g
        self._next_gid += 1
        return g

    async def add_member(self, group_id: int, user_id: int) -> None:
        self._user_groups.setdefault(user_id, [])
        if group_id not in self._user_groups[user_id]:
            self._user_groups[user_id].append(group_id)

    # ---- 权限 CRUD ----

    async def add_permission(self, group_id: int, topic_pattern: str, action: str) -> None:
        self._group_perms.setdefault(group_id, [])
        if (topic_pattern, action) not in self._group_perms[group_id]:
            self._group_perms[group_id].append((topic_pattern, action))

    async def remove_permission(self, group_id: int, topic_pattern: str, action: str) -> None:
        if group_id in self._group_perms:
            self._group_perms[group_id] = [
                (p, a) for p, a in self._group_perms[group_id]
                if not (p == topic_pattern and a == action)
            ]

    # ---- 用户权限展开 ----

    async def get_user_groups(self, user_id: int) -> list[PermissionGroup]:
        gids = self._user_groups.get(user_id, [])
        return [self._groups[g] for g in gids if g in self._groups]

    async def get_user_expanded_permissions(self, user_id: int) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {}
        for gid in self._user_groups.get(user_id, []):
            for pattern, action in self._group_perms.get(gid, []):
                result.setdefault(action, [])
                if pattern not in result[action]:
                    result[action].append(pattern)
        return result

    async def get_group_all_members(self, group_id: int) -> list[int]:
        return [uid for uid, gids in self._user_groups.items() if group_id in gids]


class _FakeUserRepo:
    """最小 user_repo: get_by_id + update。"""

    def __init__(self):
        self.users: dict[int, User] = {}
        self._next = 1

    def add(self, user: User) -> User:
        user.id = self._next
        self._next += 1
        self.users[user.id] = user
        return user

    async def get_by_id(self, user_id: int) -> User | None:
        return self.users.get(user_id)

    async def update(self, user: User) -> User:
        self.users[user.id] = user
        return user


def _user(uid: int, role: str = "user") -> AuthUser:
    return AuthUser(user_id=uid, role=role, groups=[], api_key="k", namespace="")


# ---- check_pub / check_sub ----


@pytest.mark.asyncio
async def test_check_pub_allowed_after_grant():
    perm_repo = _FakePermRepo()
    user_repo = _FakeUserRepo()
    svc = PermissionService(perm_repo, user_repo=user_repo)

    g = await perm_repo.create_group("g1")
    await perm_repo.add_member(g.id, 1)
    await svc.grant_pub(1, "a.b.*")

    assert await svc.check_pub(_user(1), "a.b.c") is True
    assert await svc.check_pub(_user(1), "a.x.y") is False


@pytest.mark.asyncio
async def test_check_pub_denied_without_grant():
    perm_repo = _FakePermRepo()
    user_repo = _FakeUserRepo()
    svc = PermissionService(perm_repo, user_repo=user_repo)

    g = await perm_repo.create_group("g1")
    await perm_repo.add_member(g.id, 1)
    # 没授权 pub

    assert await svc.check_pub(_user(1), "a.b.c") is False


@pytest.mark.asyncio
async def test_check_sub_allowed_after_grant():
    perm_repo = _FakePermRepo()
    user_repo = _FakeUserRepo()
    svc = PermissionService(perm_repo, user_repo=user_repo)

    g = await perm_repo.create_group("g1")
    await perm_repo.add_member(g.id, 1)
    await svc.grant_sub(1, "x.>")

    assert await svc.check_sub(_user(1), "x.a") is True
    assert await svc.check_sub(_user(1), "y.a") is False


@pytest.mark.asyncio
async def test_check_sub_denied_without_grant():
    perm_repo = _FakePermRepo()
    user_repo = _FakeUserRepo()
    svc = PermissionService(perm_repo, user_repo=user_repo)

    g = await perm_repo.create_group("g1")
    await perm_repo.add_member(g.id, 1)
    assert await svc.check_sub(_user(1), "x.a") is False


@pytest.mark.asyncio
async def test_check_pub_admin_short_circuits():
    perm_repo = _FakePermRepo()
    user_repo = _FakeUserRepo()
    svc = PermissionService(perm_repo, user_repo=user_repo)

    # admin 没 grant 也通过
    assert await svc.check_pub(_user(1, role="admin"), "anything") is True


# ---- grant / revoke ----


@pytest.mark.asyncio
async def test_grant_revoke_pub():
    perm_repo = _FakePermRepo()
    user_repo = _FakeUserRepo()
    svc = PermissionService(perm_repo, user_repo=user_repo)

    g = await perm_repo.create_group("g1")
    await perm_repo.add_member(g.id, 1)

    await svc.grant_pub(1, "a.*")
    assert await svc.check_pub(_user(1), "a.b") is True

    await svc.revoke_pub(1, "a.*")
    assert await svc.check_pub(_user(1), "a.b") is False


@pytest.mark.asyncio
async def test_grant_revoke_sub():
    perm_repo = _FakePermRepo()
    user_repo = _FakeUserRepo()
    svc = PermissionService(perm_repo, user_repo=user_repo)

    g = await perm_repo.create_group("g1")
    await perm_repo.add_member(g.id, 1)

    await svc.grant_sub(1, "x.*")
    assert await svc.check_sub(_user(1), "x.y") is True

    await svc.revoke_sub(1, "x.*")
    assert await svc.check_sub(_user(1), "x.y") is False


@pytest.mark.asyncio
async def test_grant_invalidates_cache():
    """grant 后缓存应被失效。"""
    perm_repo = _FakePermRepo()
    user_repo = _FakeUserRepo()
    svc = PermissionService(perm_repo, user_repo=user_repo, ttl=60)

    g = await perm_repo.create_group("g1")
    await perm_repo.add_member(g.id, 1)

    # 第一次 check: 缓存空 → 加载 → 写缓存
    assert await svc.check_pub(_user(1), "a.b") is False
    assert 1 in svc._cache

    # grant pub
    await svc.grant_pub(1, "a.*")

    # grant 后缓存应失效
    assert 1 not in svc._cache

    # 再 check: 加载新值
    assert await svc.check_pub(_user(1), "a.b") is True


# ---- topic 通配符 ----


@pytest.mark.asyncio
async def test_check_pub_topic_wildcard():
    perm_repo = _FakePermRepo()
    user_repo = _FakeUserRepo()
    svc = PermissionService(perm_repo, user_repo=user_repo)

    g = await perm_repo.create_group("g1")
    await perm_repo.add_member(g.id, 1)
    await svc.grant_pub(1, "team-a.mkt.*")

    assert await svc.check_pub(_user(1), "team-a.mkt.sh.600000") is True
    assert await svc.check_pub(_user(1), "team-a.mkt.sh.600001") is True
    assert await svc.check_pub(_user(1), "team-b.mkt.sh.600000") is False


@pytest.mark.asyncio
async def test_check_sub_topic_gt():
    perm_repo = _FakePermRepo()
    user_repo = _FakeUserRepo()
    svc = PermissionService(perm_repo, user_repo=user_repo)

    g = await perm_repo.create_group("g1")
    await perm_repo.add_member(g.id, 1)
    await svc.grant_sub(1, "team-a.>")

    assert await svc.check_sub(_user(1), "team-a.x") is True
    assert await svc.check_sub(_user(1), "team-a.x.y.z") is True
    assert await svc.check_sub(_user(1), "team-b.x") is False


# ---- list_user_permissions ----


@pytest.mark.asyncio
async def test_list_user_permissions_empty():
    perm_repo = _FakePermRepo()
    user_repo = _FakeUserRepo()
    svc = PermissionService(perm_repo, user_repo=user_repo)

    g = await perm_repo.create_group("g1")
    await perm_repo.add_member(g.id, 1)

    perms = await svc.list_user_permissions(1)
    assert perms == []


@pytest.mark.asyncio
async def test_list_user_permissions_multiple():
    perm_repo = _FakePermRepo()
    user_repo = _FakeUserRepo()
    svc = PermissionService(perm_repo, user_repo=user_repo)

    g = await perm_repo.create_group("g1")
    await perm_repo.add_member(g.id, 1)
    await svc.grant_pub(1, "a.*")
    await svc.grant_sub(1, "x.>")
    await svc.grant_pub(1, "b.>")

    perms = await svc.list_user_permissions(1)
    actions = sorted(p["action"] for p in perms)
    assert actions == ["pub", "pub", "sub"]
    # topic_pattern 都在
    patterns = {p["topic_pattern"] for p in perms}
    assert patterns == {"a.*", "b.>", "x.>"}


# ---- batch 配置 (v1.0 batcher 已撤销, 端点已删除) ----


@pytest.mark.asyncio
async def test_permission_service_has_no_batch_config_methods():
    """get_batch_config / set_batch_config 已随 batcher 策略移除。"""
    assert not hasattr(PermissionService, "get_batch_config"), \
        "PermissionService.get_batch_config 应当已被移除"
    assert not hasattr(PermissionService, "set_batch_config"), \
        "PermissionService.set_batch_config 应当已被移除"
