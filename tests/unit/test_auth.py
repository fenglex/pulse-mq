"""Auth 模块单元测试：内存存储 + ZAP Handler + 权限服务。"""

import pytest
from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.auth.permission import PermissionService, PermissionCache, topic_match
from pulsemq.auth.zap_handler import PulseMQZAPHandler, ZapResponse
from pulsemq.models import AuthUser


class TestAuthMemoryStore:
    def test_register_and_get(self):
        store = AuthMemoryStore()
        user = AuthUser(user_id=1, role="admin", groups=[], api_key="k", namespace="")
        store.register(b"id1", user)
        assert store.get_user(b"id1") == user

    def test_unregister(self):
        store = AuthMemoryStore()
        user = AuthUser(user_id=1, role="admin", groups=[], api_key="k", namespace="")
        store.register(b"id1", user)
        result = store.unregister(b"id1")
        assert result == user
        assert store.get_user(b"id1") is None

    def test_connection_count(self):
        store = AuthMemoryStore()
        user = AuthUser(user_id=1, role="admin", groups=[], api_key="k", namespace="")
        store.register(b"id1", user)
        store.register(b"id2", user)
        assert store.connection_count(1) == 2

    def test_unregister_nonexistent(self):
        store = AuthMemoryStore()
        assert store.unregister(b"noid") is None


class TestTopicMatch:
    @pytest.mark.parametrize("pattern,topic,expected", [
        # 精确匹配
        ("a.b.c", "a.b.c", True),
        # 单层通配 *
        ("a.*.c", "a.b.c", True),
        ("a.*.c", "a.b.x.c", False),
        ("*.mkt.*", "team-a.mkt.sh.600000", True),
        ("*.mkt.*", "team-a.signal.buy", False),
        # 多层通配 >
        ("a.>", "a.b.c", True),
        ("a.>", "a.b.c.d.e", True),
        ("a.>", "a.b", True),        # > 匹配一个或多个段
        ("a.>", "b.c", False),
        # 混合
        ("team-a.mkt.*", "team-a.mkt.sh.600000", True),
        ("team-a.mkt.*", "team-a.mkt.sz.000333", True),
        ("team-a.mkt.*", "team-a.signal.buy", False),
        # 全通配
        (">", "anything.at.all", True),
    ])
    def test_patterns(self, pattern, topic, expected):
        assert topic_match(pattern, topic) is expected


class TestPermissionCache:
    def test_has_permission(self):
        cache = PermissionCache(
            user_id=1,
            permissions={"pub": ["team-a.mkt.*"], "sub": ["*.mkt.*"]},
        )
        assert cache.has_permission("pub", "team-a.mkt.sh.600000") is True
        assert cache.has_permission("pub", "team-b.mkt.sh.600000") is False
        assert cache.has_permission("sub", "team-a.mkt.sh.600000") is True
        assert cache.has_permission("sub", "team-b.signal.buy") is False

    def test_is_expired(self):
        cache = PermissionCache(user_id=1, permissions={}, ttl=0.0)
        assert cache.is_expired() is True
        cache2 = PermissionCache(user_id=1, permissions={}, ttl=60.0)
        assert cache2.is_expired() is False


class TestPermissionService:
    async def test_admin_bypasses(self):
        user = AuthUser(user_id=1, role="admin", groups=[], api_key="k", namespace="")
        svc = PermissionService(perm_repo=None)
        assert await svc.check_permission(user, "pub", "any.topic") is True

    async def test_permission_check_with_cache(self, tmp_path):
        from pulsemq.storage.database import init_db
        from pulsemq.storage.sqlite_user import SqliteUserRepo
        from pulsemq.storage.sqlite_perm import SqlitePermGroupRepo
        from pulsemq.storage.interfaces import User

        conn = init_db(str(tmp_path / "test.db"))
        user_repo = SqliteUserRepo(conn)
        perm_repo = SqlitePermGroupRepo(conn)

        # 创建用户 + 权限组
        u = await user_repo.create(User(username="test", api_key="k_test", role="user"))
        g = await perm_repo.create_group("行情订阅")
        await perm_repo.add_permission(g.id, "*.mkt.*", "sub")
        await perm_repo.add_member(g.id, u.id)

        svc = PermissionService(perm_repo)
        user = AuthUser(user_id=u.id, role="user", groups=[], api_key="k_test", namespace="")

        assert await svc.check_permission(user, "sub", "team-a.mkt.sh.600000") is True
        assert await svc.check_permission(user, "pub", "team-a.mkt.sh.600000") is False
        assert await svc.check_permission(user, "sub", "team-a.signal.buy") is False

        conn.close()

    async def test_invalidate_cache(self, tmp_path):
        from pulsemq.storage.database import init_db
        from pulsemq.storage.sqlite_user import SqliteUserRepo
        from pulsemq.storage.sqlite_perm import SqlitePermGroupRepo
        from pulsemq.storage.interfaces import User

        conn = init_db(str(tmp_path / "test.db"))
        user_repo = SqliteUserRepo(conn)
        perm_repo = SqlitePermGroupRepo(conn)

        u = await user_repo.create(User(username="test2", api_key="k_test2", role="user"))
        g = await perm_repo.create_group("g2")
        await perm_repo.add_permission(g.id, "team-a.*", "pub")
        await perm_repo.add_member(g.id, u.id)

        svc = PermissionService(perm_repo, ttl=60.0)
        user = AuthUser(user_id=u.id, role="user", groups=[], api_key="k_test2", namespace="")

        # 初始有权限
        assert await svc.check_permission(user, "pub", "team-a.mkt.sh.600000") is True

        # 移除权限 + 失效缓存
        await perm_repo.remove_member(g.id, u.id)
        svc.invalidate_user(u.id)

        # 缓存失效后应重新查询
        assert await svc.check_permission(user, "pub", "team-a.mkt.sh.600000") is False

        conn.close()
