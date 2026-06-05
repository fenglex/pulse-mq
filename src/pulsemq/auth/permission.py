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
    is_last = (pi == len(pat) - 1)

    if seg == ">":
        # > 匹配一个或多个段
        for skip in range(ti + 1, len(topic) + 1):
            if _match_parts(pat, pi + 1, topic, skip):
                return True
        # > 在末尾也可以匹配到 topic 末尾
        if is_last and ti <= len(topic):
            return _match_parts(pat, pi + 1, topic, len(topic))
        return False

    if seg == "*":
        if is_last:
            # * 在末尾匹配一个或多个段
            return ti < len(topic) and _match_parts(pat, pi + 1, topic, len(topic))
        else:
            # * 在中间匹配恰好一个段
            if ti >= len(topic):
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

    def __init__(self, perm_repo, ttl: float = 60.0):
        self._perm_repo = perm_repo
        self._cache: dict[int, PermissionCache] = {}
        self._ttl = ttl

    async def check_permission(self, user: AuthUser, action: str, topic: str) -> bool:
        """检查用户是否有权限。admin 直接通过。"""
        if user.is_admin:
            return True

        cache = await self._get_or_load(user.user_id)
        return cache.has_permission(action, topic)

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
