from pulsemq.models import (
    AuthUser,
    BufferedMessage,
    ExpandedPermissions,
    TopicInfo,
)


class TestAuthUser:
    def test_create(self):
        user = AuthUser(
            user_id=1, role="admin", groups=["行情全订阅"],
            api_key="pulse_sk_admin_default", namespace="",
        )
        assert user.user_id == 1
        assert user.role == "admin"
        assert user.is_admin is True

    def test_normal_user_is_not_admin(self):
        user = AuthUser(
            user_id=2, role="user", groups=["行情全订阅"],
            api_key="pulse_sk_xxx", namespace="team-a",
        )
        assert user.is_admin is False


class TestTopicInfo:
    def test_create(self):
        info = TopicInfo(
            full_name="team-a.mkt.sh.600000",
            namespace="team-a",
            topic_path="mkt.sh.600000",
            is_wildcard=False,
            subscriber_count=0,
            created_at=1717516800.0,
        )
        assert info.namespace == "team-a"
        assert info.is_wildcard is False

    def test_parse_from_full_name(self):
        info = TopicInfo.from_name("team-a.mkt.sh.600000")
        assert info.namespace == "team-a"
        assert info.topic_path == "mkt.sh.600000"
        assert info.full_name == "team-a.mkt.sh.600000"
        assert info.is_wildcard is False

    def test_wildcard_detection(self):
        info = TopicInfo.from_name("team-a.mkt.*")
        assert info.is_wildcard is True


class TestBufferedMessage:
    def test_create(self):
        msg = BufferedMessage(
            topic="team-a.mkt.sh.600000",
            seq=1,
            record_count=1,
            meta=b"\x02\x01",
            payload=b"\x93\x01\x02\x03",
            timestamp=1717516800.0,
        )
        assert msg.seq == 1
        assert msg.record_count == 1


class TestExpandedPermissions:
    def test_empty(self):
        perms = ExpandedPermissions()
        assert perms.pub == []
        assert perms.sub == []

    def test_from_dict(self):
        perms = ExpandedPermissions.from_dict({
            "pub": ["team-a.mkt.*"],
            "sub": ["*.mkt.*"],
        })
        assert perms.pub == ["team-a.mkt.*"]
        assert perms.sub == ["*.mkt.*"]
