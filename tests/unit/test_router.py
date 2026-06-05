import pytest
from pulsemq.engine.router import MessageRouter
from pulsemq.models import AuthUser


def _make_user(user_id: int = 1, role: str = "user") -> AuthUser:
    return AuthUser(
        user_id=user_id,
        role=role,
        groups=[],
        api_key=f"pulse_sk_test_{user_id}",
        namespace="team-a",
    )


class TestTopicRegistry:
    def test_register_topic(self, router: MessageRouter):
        info = router.register_topic("team-a.mkt.sh.600000")
        assert info.full_name == "team-a.mkt.sh.600000"
        assert info.namespace == "team-a"

    def test_register_idempotent(self, router: MessageRouter):
        info1 = router.register_topic("team-a.mkt.sh.600000")
        info2 = router.register_topic("team-a.mkt.sh.600000")
        assert info1 is info2

    def test_get_topic(self, router: MessageRouter):
        router.register_topic("team-a.mkt.sh.600000")
        info = router.get_topic("team-a.mkt.sh.600000")
        assert info is not None
        assert info.full_name == "team-a.mkt.sh.600000"

    def test_get_nonexistent_topic(self, router: MessageRouter):
        assert router.get_topic("no.such.topic") is None

    def test_remove_topic_if_empty(self, router: MessageRouter):
        router.register_topic("team-a.mkt.sh.600000")
        router.remove_topic_if_empty("team-a.mkt.sh.600000")
        assert router.get_topic("team-a.mkt.sh.600000") is None


class TestSubscriptionManager:
    def test_subscribe(self, router: MessageRouter):
        router.subscribe(b"id1", "team-a.mkt.sh.600000")
        subs = router.get_subscribers("team-a.mkt.sh.600000")
        assert b"id1" in subs

    def test_unsubscribe(self, router: MessageRouter):
        router.subscribe(b"id1", "team-a.mkt.sh.600000")
        router.unsubscribe(b"id1", "team-a.mkt.sh.600000")
        assert b"id1" not in router.get_subscribers("team-a.mkt.sh.600000")

    def test_get_subscriptions(self, router: MessageRouter):
        router.subscribe(b"id1", "topic_a")
        router.subscribe(b"id1", "topic_b")
        subs = router.get_subscriptions(b"id1")
        assert "topic_a" in subs
        assert "topic_b" in subs

    def test_remove_identity(self, router: MessageRouter):
        router.subscribe(b"id1", "topic_a")
        router.subscribe(b"id1", "topic_b")
        router.remove_identity(b"id1")
        assert len(router.get_subscriptions(b"id1")) == 0
        assert b"id1" not in router.get_subscribers("topic_a")
        assert b"id1" not in router.get_subscribers("topic_b")

    def test_multiple_subscribers(self, router: MessageRouter):
        router.subscribe(b"id1", "topic_a")
        router.subscribe(b"id2", "topic_a")
        subs = router.get_subscribers("topic_a")
        assert b"id1" in subs
        assert b"id2" in subs


class TestConnectionManager:
    def test_register_connection(self, router: MessageRouter):
        user = _make_user()
        router.register_connection(b"id1", user)
        assert router.get_user(b"id1") == user

    def test_unregister_connection(self, router: MessageRouter):
        user = _make_user()
        router.register_connection(b"id1", user)
        result = router.unregister_connection(b"id1")
        assert result == user
        assert router.get_user(b"id1") is None

    def test_get_connections(self, router: MessageRouter):
        user = _make_user()
        router.register_connection(b"id1", user)
        router.register_connection(b"id2", user)
        conns = router.get_connections(user.user_id)
        assert b"id1" in conns
        assert b"id2" in conns

    def test_unregister_nonexistent(self, router: MessageRouter):
        assert router.unregister_connection(b"noid") is None


class TestMessageBuffer:
    def test_append_and_latest_seq(self, router: MessageRouter):
        msg = router.append_message("topic_a", b"\x02\x01", 1, b"payload1")
        assert msg.seq == 1
        msg2 = router.append_message("topic_a", b"\x02\x01", 1, b"payload2")
        assert msg2.seq == 2
        assert router.latest_seq("topic_a") == 2

    def test_replay_messages(self, router: MessageRouter):
        for i in range(5):
            router.append_message("topic_a", b"\x02\x01", 1, f"payload{i}".encode())
        msgs = router.replay_messages("topic_a", from_seq=3, limit=10)
        assert len(msgs) == 3
        assert msgs[0].seq == 3

    def test_replay_with_limit(self, router: MessageRouter):
        for i in range(10):
            router.append_message("topic_a", b"\x02\x01", 1, f"p{i}".encode())
        msgs = router.replay_messages("topic_a", from_seq=0, limit=3)
        assert len(msgs) == 3

    def test_replay_empty_topic(self, router: MessageRouter):
        msgs = router.replay_messages("no_topic", from_seq=0, limit=10)
        assert msgs == []

    def test_ring_buffer_overflow(self, router: MessageRouter):
        """超过 MAX_SIZE 时自动淘汰最旧消息。"""
        for i in range(1100):
            router.append_message("topic_a", b"\x02\x01", 1, f"p{i}".encode())
        msgs = router.replay_messages("topic_a", from_seq=0, limit=2000)
        assert len(msgs) == 1000
        assert msgs[0].seq == 101  # 前 100 条被淘汰

    def test_remove_topic_buffer(self, router: MessageRouter):
        router.append_message("topic_a", b"\x02\x01", 1, b"p")
        router.remove_topic_buffer("topic_a")
        assert router.latest_seq("topic_a") == 0


class TestStatistics:
    def test_counts(self, router: MessageRouter):
        user = _make_user()
        router.register_connection(b"id1", user)
        router.register_topic("topic_a")
        router.subscribe(b"id1", "topic_a")
        assert router.topic_count() == 1
        assert router.subscription_count() == 1
        assert router.connection_count() == 1


@pytest.fixture
def router() -> MessageRouter:
    return MessageRouter()
