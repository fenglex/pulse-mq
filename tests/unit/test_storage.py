"""Storage 层单元测试：SQLite User + PermissionGroup Repository。"""

import pytest
from pulsemq.storage.database import init_db, parse_db_url
from pulsemq.storage.interfaces import User
from pulsemq.storage.sqlite_user import SqliteUserRepo
from pulsemq.storage.sqlite_perm import SqlitePermGroupRepo


@pytest.fixture
def db_conn(tmp_path):
    """创建临时数据库。"""
    db_file = str(tmp_path / "test.db")
    conn = init_db(db_file)
    yield conn
    conn.close()


@pytest.fixture
def user_repo(db_conn):
    return SqliteUserRepo(db_conn)


@pytest.fixture
def perm_repo(db_conn):
    return SqlitePermGroupRepo(db_conn)


class TestDatabaseInit:
    def test_parse_db_url(self):
        assert parse_db_url("sqlite://./pulse_mq.db") == "./pulse_mq.db"
        assert parse_db_url("./pulse_mq.db") == "./pulse_mq.db"

    async def test_default_admin_exists(self, user_repo):
        admin = await user_repo.get_by_api_key("pulse_sk_admin_default")
        assert admin is not None
        assert admin.role == "admin"
        assert admin.username == "admin"


class TestUserRepo:
    async def test_create_and_get(self, user_repo):
        user = await user_repo.create(User(
            username="testuser",
            api_key="pulse_sk_test123",
            role="user",
            namespace="team-a",
        ))
        assert user.id is not None
        assert user.username == "testuser"

        fetched = await user_repo.get_by_id(user.id)
        assert fetched is not None
        assert fetched.username == "testuser"

    async def test_get_by_api_key(self, user_repo):
        user = await user_repo.create(User(
            username="key_user",
            api_key="pulse_sk_abc",
        ))
        found = await user_repo.get_by_api_key("pulse_sk_abc")
        assert found is not None
        assert found.username == "key_user"

    async def test_get_nonexistent(self, user_repo):
        assert await user_repo.get_by_id(9999) is None
        assert await user_repo.get_by_api_key("nonexistent") is None

    async def test_update(self, user_repo):
        user = await user_repo.create(User(
            username="upuser",
            api_key="pulse_sk_up",
            namespace="old",
        ))
        user.namespace = "new"
        await user_repo.update(user)
        fetched = await user_repo.get_by_id(user.id)
        assert fetched.namespace == "new"

    async def test_delete(self, user_repo):
        user = await user_repo.create(User(
            username="deluser",
            api_key="pulse_sk_del",
        ))
        await user_repo.delete(user.id)
        assert await user_repo.get_by_id(user.id) is None

    async def test_list_all(self, user_repo):
        await user_repo.create(User(username="u1", api_key="k1"))
        await user_repo.create(User(username="u2", api_key="k2"))
        users = await user_repo.list_all()
        # 包含默认 admin + 2 个新用户
        assert len(users) >= 3


class TestPermGroupRepo:
    async def test_create_group(self, perm_repo):
        group = await perm_repo.create_group("行情全订阅")
        assert group.id is not None
        assert group.name == "行情全订阅"

    async def test_add_permission(self, perm_repo):
        group = await perm_repo.create_group("test_group")
        await perm_repo.add_permission(group.id, "*.mkt.*", "sub")
        perms = await perm_repo.get_permissions(group.id)
        assert len(perms) == 1
        assert perms[0].topic_pattern == "*.mkt.*"
        assert perms[0].action == "sub"

    async def test_add_member_and_expand(self, perm_repo, user_repo):
        # 创建用户
        user = await user_repo.create(User(username="member1", api_key="k_member"))
        # 创建权限组
        group = await perm_repo.create_group("行情全订阅")
        await perm_repo.add_permission(group.id, "*.mkt.*", "sub")
        await perm_repo.add_permission(group.id, "team-a.mkt.*", "pub")
        # 添加成员
        await perm_repo.add_member(group.id, user.id)
        # 查询用户权限组
        groups = await perm_repo.get_user_groups(user.id)
        assert len(groups) == 1
        assert groups[0].name == "行情全订阅"
        # 展开权限
        expanded = await perm_repo.get_user_expanded_permissions(user.id)
        assert "sub" in expanded
        assert "*.mkt.*" in expanded["sub"]
        assert "pub" in expanded
        assert "team-a.mkt.*" in expanded["pub"]

    async def test_remove_member(self, perm_repo, user_repo):
        user = await user_repo.create(User(username="rm_user", api_key="k_rm"))
        group = await perm_repo.create_group("tmp_group")
        await perm_repo.add_member(group.id, user.id)
        assert len(await perm_repo.get_members(group.id)) == 1
        await perm_repo.remove_member(group.id, user.id)
        assert len(await perm_repo.get_members(group.id)) == 0

    async def test_get_group_all_members(self, perm_repo, user_repo):
        u1 = await user_repo.create(User(username="m1", api_key="km1"))
        u2 = await user_repo.create(User(username="m2", api_key="km2"))
        group = await perm_repo.create_group("multi_members")
        await perm_repo.add_member(group.id, u1.id)
        await perm_repo.add_member(group.id, u2.id)
        member_ids = await perm_repo.get_group_all_members(group.id)
        assert len(member_ids) == 2
        assert u1.id in member_ids
        assert u2.id in member_ids
