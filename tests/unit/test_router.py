"""engine/router.py 单元测试。

覆盖:
- topic_match 边界（plan 要求）
- subscribe/unsubscribe 一致性
- 通配符订阅展开
- 通配符缓存一致性
- remove_identity
- append_message/replay_messages 缓冲
"""

from __future__ import annotations

import pytest

from pulsemq.auth.permission import topic_match
from pulsemq.engine.router import MessageRouter
from pulsemq.models import AuthUser, TopicInfo


# ---- topic_match 边界 ----


def test_topic_match_exact():
    assert topic_match("a.b.c", "a.b.c")
    assert not topic_match("a.b.c", "a.b.x")


def test_topic_match_star_middle():
    assert topic_match("a.*.c", "a.b.c")
    assert not topic_match("a.*.c", "a.b.x.c")


def test_topic_match_star_end():
    assert topic_match("team-a.mkt.*", "team-a.mkt.sh.600000")


def test_topic_match_gt():
    assert topic_match("team-a.>", "team-a.mkt.sh.600000")
    assert topic_match("team-a.>", "team-a.x")


def test_topic_match_edge_cases():
    assert topic_match("", "")
    assert not topic_match("", "x")
    assert topic_match("a", "a")
    assert not topic_match("a", "ab")


def test_topic_match_no_false_positive():
    assert not topic_match("a.b", "a.b.c")


def test_topic_match_gt_requires_one_or_more_segments():
    """`>` 必须匹配至少一段, 不能匹配零段。"""
    assert not topic_match("a.>", "a")


def test_topic_match_star_middle_requires_exactly_one_segment():
    """中间位置的 `*` 必须恰好匹配一段, 不能匹配零或多段。"""
    assert not topic_match("a.*.c", "a..c")
    assert not topic_match("a.*.c", "a.b.x.c")


# ---- Topic 注册 ----


def test_register_topic_idempotent():
    r = MessageRouter()
    t1 = r.register_topic("a.b.c")
    t2 = r.register_topic("a.b.c")
    assert t1 is t2
    assert r.topic_count() == 1


def test_register_topic_creates_info():
    r = MessageRouter()
    info = r.register_topic("team-a.mkt.sh.600000")
    assert info.namespace == "team-a"
    assert info.topic_path == "mkt.sh.600000"
    assert info.is_wildcard is False


def test_register_topic_detects_wildcard():
    r = MessageRouter()
    info = r.register_topic("team-a.>")
    assert info.is_wildcard is True


# ---- 精确订阅 ----


def test_subscribe_basic():
    r = MessageRouter()
    r.register_topic("a.b.c")
    r.subscribe(b"client1", "a.b.c")
    subs = r.get_subscribers("a.b.c")
    assert subs == {b"client1"}


def test_subscribe_increments_subscriber_count():
    r = MessageRouter()
    r.register_topic("a.b.c")
    r.subscribe(b"c1", "a.b.c")
    r.subscribe(b"c2", "a.b.c")
    info = r.get_topic("a.b.c")
    assert info.subscriber_count == 2


def test_unsubscribe_removes_subscriber():
    r = MessageRouter()
    r.register_topic("a.b.c")
    r.subscribe(b"c1", "a.b.c")
    r.subscribe(b"c2", "a.b.c")
    r.unsubscribe(b"c1", "a.b.c")
    subs = r.get_subscribers("a.b.c")
    assert subs == {b"c2"}
    info = r.get_topic("a.b.c")
    assert info.subscriber_count == 1


def test_unsubscribe_missing_subscriber_no_error():
    """未订阅就取消订阅: 不应抛错。"""
    r = MessageRouter()
    r.unsubscribe(b"nonexistent", "a.b.c")
    assert r.get_subscribers("a.b.c") == set()


# ---- 通配符订阅 ----


def test_wildcard_subscribe_expands_existing_topics():
    r = MessageRouter()
    r.register_topic("team-a.mkt.sh.600000")
    r.register_topic("team-a.mkt.sh.000001")
    r.register_topic("other.x.y")

    matched = r.subscribe_wildcard(b"c1", "team-a.mkt.>")
    assert set(matched) == {"team-a.mkt.sh.600000", "team-a.mkt.sh.000001"}


def test_wildcard_subscriber_receives_publishes():
    """`team-a.>` 通配符订阅者应收到所有 team-a.* 主题的消息。"""
    r = MessageRouter()
    r.register_topic("team-a.mkt.sh.600000")
    r.subscribe_wildcard(b"c1", "team-a.>")

    # 现在发布到 team-a.mkt.sh.600000
    subs = r.get_subscribers("team-a.mkt.sh.600000")
    assert subs == {b"c1"}


def test_wildcard_cache_invalidated_on_subscribe():
    r = MessageRouter()
    # 预热缓存
    r.get_subscribers("team-a.mkt.sh.600000")
    # 订阅后再查, 必须看到新订阅者
    r.subscribe_wildcard(b"c1", "team-a.>")
    subs = r.get_subscribers("team-a.mkt.sh.600000")
    assert b"c1" in subs


def test_wildcard_unsubscribe_removes():
    r = MessageRouter()
    r.register_topic("team-a.mkt.sh.600000")
    r.subscribe_wildcard(b"c1", "team-a.>")
    r.unsubscribe(b"c1", "team-a.>")
    subs = r.get_subscribers("team-a.mkt.sh.600000")
    assert subs == set()


def test_get_subscriptions_includes_wildcard():
    r = MessageRouter()
    r.subscribe_wildcard(b"c1", "team-a.>")
    r.subscribe(b"c1", "a.b.c")
    subs = r.get_subscriptions(b"c1")
    assert subs == {"team-a.>", "a.b.c"}


# ---- remove_identity ----


def test_remove_identity_clears_exact_and_wildcard():
    r = MessageRouter()
    r.register_topic("a.b.c")
    r.register_topic("team-a.mkt.sh.600000")
    r.subscribe(b"c1", "a.b.c")
    r.subscribe_wildcard(b"c1", "team-a.>")
    r.remove_identity(b"c1")
    assert r.get_subscriptions(b"c1") == set()
    assert r.get_subscribers("a.b.c") == set()
    assert r.get_subscribers("team-a.mkt.sh.600000") == set()


# ---- has_subscribers ----


def test_has_subscribers_exact():
    r = MessageRouter()
    r.register_topic("a.b.c")
    assert r.has_subscribers("a.b.c") is False
    r.subscribe(b"c1", "a.b.c")
    assert r.has_subscribers("a.b.c") is True


def test_has_subscribers_via_wildcard():
    r = MessageRouter()
    r.register_topic("team-a.mkt.sh.600000")
    r.subscribe_wildcard(b"c1", "team-a.>")
    assert r.has_subscribers("team-a.mkt.sh.600000") is True


# ---- 消息缓冲 ----


def test_append_message_assigns_monotonic_seq():
    r = MessageRouter()
    r.append_message("a.b.c", b"\x00", 1, b"payload1")
    r.append_message("a.b.c", b"\x00", 1, b"payload2")
    r.append_message("a.b.c", b"\x00", 1, b"payload3")
    assert r.latest_seq("a.b.c") == 3
    msgs = r.replay_messages("a.b.c", from_seq=0)
    assert [m.seq for m in msgs] == [1, 2, 3]


def test_replay_messages_from_seq():
    r = MessageRouter()
    for i in range(5):
        r.append_message("a.b.c", b"\x00", 1, f"p{i}".encode())
    msgs = r.replay_messages("a.b.c", from_seq=3)
    assert [m.seq for m in msgs] == [3, 4, 5]


def test_replay_messages_respects_limit():
    r = MessageRouter()
    for i in range(10):
        r.append_message("a.b.c", b"\x00", 1, f"p{i}".encode())
    msgs = r.replay_messages("a.b.c", from_seq=0, limit=3)
    assert len(msgs) == 3


def test_replay_messages_empty_topic():
    r = MessageRouter()
    msgs = r.replay_messages("nonexistent")
    assert msgs == []


def test_buffer_disabled_by_default():
    """buffer_enabled 默认 False。"""
    r = MessageRouter()
    assert r.buffer_enabled is False


def test_append_message_still_works_when_disabled():
    """buffer_enabled=False 不阻止 append_message 写入。"""
    r = MessageRouter()
    r.append_message("a.b.c", b"\x00", 1, b"p")
    assert r.latest_seq("a.b.c") == 1


def test_remove_topic_buffer_clears():
    r = MessageRouter()
    r.append_message("a.b.c", b"\x00", 1, b"p")
    r.remove_topic_buffer("a.b.c")
    assert r.latest_seq("a.b.c") == 0
    assert r.replay_messages("a.b.c") == []


def test_buffer_maxlen_truncates_old():
    r = MessageRouter()
    r.max_buffer_size = 3
    for i in range(5):
        r.append_message("a.b.c", b"\x00", 1, f"p{i}".encode())
    msgs = r.replay_messages("a.b.c", from_seq=0)
    assert [m.seq for m in msgs] == [3, 4, 5]


# ---- remove_topic_if_empty ----


def test_remove_topic_if_empty_clears():
    r = MessageRouter()
    r.register_topic("a.b.c")
    r.subscribe(b"c1", "a.b.c")
    # 有订阅者: 不应移除
    r.remove_topic_if_empty("a.b.c")
    assert r.get_topic("a.b.c") is not None
    # 取消订阅
    r.unsubscribe(b"c1", "a.b.c")
    r.remove_topic_if_empty("a.b.c")
    assert r.get_topic("a.b.c") is None


# ---- 连接管理（auth_store 委托模式）----


def test_register_connection_uses_test_mode_when_no_auth_store():
    """无 auth_store 时, 走 _test_connections 兼容模式。"""
    r = MessageRouter()
    user = AuthUser(user_id=42, role="user", groups=[], api_key="k")
    r.register_connection(b"client1", user)
    assert r.get_user(b"client1") is user
    assert r.connection_count() == 1
    assert b"client1" in r.get_connections(42)


def test_unregister_connection_removes():
    r = MessageRouter()
    user = AuthUser(user_id=42, role="user", groups=[], api_key="k")
    r.register_connection(b"client1", user)
    r.unregister_connection(b"client1")
    assert r.get_user(b"client1") is None
    assert r.connection_count() == 0


def test_connection_count_no_store_no_conns():
    r = MessageRouter()
    assert r.connection_count() == 0


def test_subscription_count_sums_all_topics():
    r = MessageRouter()
    r.subscribe(b"c1", "a.b.c")
    r.subscribe(b"c2", "a.b.c")
    r.subscribe(b"c1", "x.y.z")
    assert r.subscription_count() == 3
