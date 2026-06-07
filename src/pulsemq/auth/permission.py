"""权限服务：权限展开 + 缓存 + 通配符匹配。"""

from __future__ import annotations

import fnmatch
import time
from dataclasses import dataclass, field

from pulsemq.models import AuthUser


def topic_match(pattern: str, topic: str) -> bool:
    """通配符匹配 topic。

    规则:
      * = 中间位置匹配恰好一个段，末尾位置匹配一个或多个段
      > = 匹配一个或多个段

    示例:
      "a.*.c"          匹配 "a.b.c"，不匹配 "a.b.x.c"
      "team-a.mkt.*"   匹配 "team-a.mkt.sh.600000"（末尾 * 匹配多段）
      "*.mkt.*"         匹配 "team-a.mkt.sh.600000"
      "team-a.>"        匹配 "team-a.mkt.sh.600000"
      "a.>.c"           匹配 "a.b.c" 和 "a.b.x.c"，不匹配 "a.c"
    """
    if pattern == topic:
        return True

    pat_parts = pattern.split(".")
    topic_parts = topic.split(".")

    return _match_parts(pat_parts, 0, topic_parts, 0)


def _match_parts(pat: list[str], pi: int, topic: list[str], ti: int) -> bool:
    """递归匹配 topic 段。"""
    # 同时到达末尾
    if pi == len(pat) and ti == len(topic):
        return True

    # pattern 用完但 topic 还有
    if pi == len(pat):
        return False

    seg = pat[pi]

    if seg == ">":
        # > 匹配一个或多个段，循环尝试所有可能的消费位置
        for skip in range(ti + 1, len(topic) + 1):
            if _match_parts(pat, pi + 1, topic, skip):
                return True
        return False

    if seg == "*":
        if pi == len(pat) - 1:
            # * 在末尾匹配一个或多个段
            return ti < len(topic) and _match_parts(pat, pi + 1, topic, len(topic))
        else:
            # * 在中间匹配恰好一个非空段
            if ti >= len(topic):
                return False
            if not topic[ti]:
                return False
            return _match_parts(pat, pi + 1, topic, ti + 1)

    # 精确匹配
    if ti >= len(topic):
        return False
    if seg != topic[ti]:
        return False
    return _match_parts(pat, pi + 1, topic, ti + 1)


@dataclass
class PermissionCache:
    """用户权限缓存。"""

    user_id: int
    permissions: dict[str, list[str]]  # {action: [pattern_list]}
    cached_at: float = field(default_factory=time.time)
    ttl: float = 60.0  # 秒

    def is_expired(self) -> bool:
        return time.time() - self.cached_at > self.ttl

    def has_permission(self, action: str, topic: str) -> bool:
        """检查用户是否有某 action 对某 topic 的权限。"""
        patterns = self.permissions.get(action, [])
        for pattern in patterns:
            if topic_match(pattern, topic):
                return True
        return False


class PermissionService:
    """权限服务：查询 + 缓存 + 校验。"""

    def __init__(self, perm_repo, user_repo=None, ttl: float = 60.0):
        self._perm_repo = perm_repo
        self._user_repo = user_repo  # 用于 batch 配置读写（Phase 7）
        self._cache: dict[int, PermissionCache] = {}
        self._ttl = ttl

    async def check_permission(self, user: AuthUser, action: str, topic: str) -> bool:
        """检查用户是否有权限。admin 直接通过。"""
        if user.is_admin:
            return True

        cache = await self._get_or_load(user.user_id)
        return cache.has_permission(action, topic)

    # ---- Phase 7: pub/sub 高层 API ----

    async def check_pub(self, user: AuthUser, topic: str) -> bool:
        """检查用户是否有 topic 的 pub 权限。"""
        return await self.check_permission(user, "pub", topic)

    async def check_sub(self, user: AuthUser, topic: str) -> bool:
        """检查用户是否有 topic 的 sub 权限。"""
        return await self.check_permission(user, "sub", topic)

    async def grant_pub(self, user_id: int, topic_pattern: str) -> None:
        """授予用户 pub 权限（作用于所有 group 上的 pattern）。"""
        await self._grant_for_user(user_id, topic_pattern, "pub")

    async def grant_sub(self, user_id: int, topic_pattern: str) -> None:
        """授予用户 sub 权限。"""
        await self._grant_for_user(user_id, topic_pattern, "sub")

    async def revoke_pub(self, user_id: int, topic_pattern: str) -> None:
        """撤销用户 pub 权限。"""
        await self._revoke_for_user(user_id, topic_pattern, "pub")

    async def revoke_sub(self, user_id: int, topic_pattern: str) -> None:
        """撤销用户 sub 权限。"""
        await self._revoke_for_user(user_id, topic_pattern, "sub")

    async def list_user_permissions(self, user_id: int) -> list[dict]:
        """列出用户所有权限。

        Returns:
            list of {"action": "pub"/"sub"/..., "topic_pattern": "..."}
        """
        perms = await self._perm_repo.get_user_expanded_permissions(user_id)
        result: list[dict] = []
        for action, patterns in perms.items():
            for p in patterns:
                result.append({"action": action, "topic_pattern": p})
        return result

    # ---- Phase 7: batch 配置 API ----

    async def get_batch_config(self, user_id: int) -> dict:
        """读取用户 BATCH 配置。

        Returns:
            {"batch_size": int, "batch_interval_ms": int, "batch_max_wait_ms": int}

        Raises:
            RuntimeError: 未注入 user_repo
            LookupError: 用户不存在
        """
        if self._user_repo is None:
            raise RuntimeError("user_repo 未注入, 无法读取 batch 配置")
        user = await self._user_repo.get_by_id(user_id)
        if user is None:
            raise LookupError(f"用户不存在: {user_id}")
        return {
            "batch_size": user.batch_size,
            "batch_interval_ms": user.batch_interval_ms,
            "batch_max_wait_ms": user.batch_max_wait_ms,
        }

    async def set_batch_config(
        self,
        user_id: int,
        batch_size: int,
        batch_interval_ms: int,
        batch_max_wait_ms: int,
    ) -> None:
        """更新用户 BATCH 配置。

        Raises:
            RuntimeError: 未注入 user_repo
            LookupError: 用户不存在
            ValueError: 参数非法
        """
        if self._user_repo is None:
            raise RuntimeError("user_repo 未注入, 无法更新 batch 配置")
        if batch_size < 1:
            raise ValueError(f"batch_size 必须 >= 1, 收到 {batch_size}")
        if batch_interval_ms < 0:
            raise ValueError(f"batch_interval_ms 必须 >= 0, 收到 {batch_interval_ms}")
        if batch_max_wait_ms < 0:
            raise ValueError(f"batch_max_wait_ms 必须 >= 0, 收到 {batch_max_wait_ms}")

        user = await self._user_repo.get_by_id(user_id)
        if user is None:
            raise LookupError(f"用户不存在: {user_id}")
        user.batch_size = batch_size
        user.batch_interval_ms = batch_interval_ms
        user.batch_max_wait_ms = batch_max_wait_ms
        await self._user_repo.update(user)

    # ---- 内部辅助 ----

    async def _grant_for_user(self, user_id: int, topic_pattern: str, action: str) -> None:
        """在用户的所有权限组上添加 (topic_pattern, action) 规则。"""
        # 找到用户的所有 group_id
        groups = await self._perm_repo.get_user_groups(user_id)
        for g in groups:
            if g.id is None:
                continue
            await self._perm_repo.add_permission(g.id, topic_pattern, action)
            # 失效该 group 所有成员缓存
            member_ids = await self._perm_repo.get_group_all_members(g.id)
            self.invalidate_group_members(member_ids)
        # 同时失效该用户自己的缓存（即便不在任何 group）
        self.invalidate_user(user_id)

    async def _revoke_for_user(self, user_id: int, topic_pattern: str, action: str) -> None:
        """在用户的所有权限组上移除 (topic_pattern, action) 规则。"""
        groups = await self._perm_repo.get_user_groups(user_id)
        for g in groups:
            if g.id is None:
                continue
            await self._perm_repo.remove_permission(g.id, topic_pattern, action)
            member_ids = await self._perm_repo.get_group_all_members(g.id)
            self.invalidate_group_members(member_ids)
        self.invalidate_user(user_id)

    async def _get_or_load(self, user_id: int) -> PermissionCache:
        """获取缓存或从 DB 加载。"""
        cache = self._cache.get(user_id)
        if cache is not None and not cache.is_expired():
            return cache

        # 从 DB 加载
        perms = await self._perm_repo.get_user_expanded_permissions(user_id)
        cache = PermissionCache(
            user_id=user_id,
            permissions=perms,
            ttl=self._ttl,
        )
        self._cache[user_id] = cache
        return cache

    def invalidate_user(self, user_id: int) -> None:
        """失效某用户的缓存。"""
        self._cache.pop(user_id, None)

    def invalidate_group_members(self, user_ids: list[int]) -> None:
        """失效某权限组所有成员的缓存。"""
        for uid in user_ids:
            self._cache.pop(uid, None)

    def clear_cache(self) -> None:
        """清空全部缓存。"""
        self._cache.clear()
