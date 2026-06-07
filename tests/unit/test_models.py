"""models 数据类单测。

覆盖:
- AuthUser (含 is_admin 属性)
- TopicInfo.from_name (含通配符检测、边界)
- BufferedMessage (slots, 字段)
- ExpandedPermissions.from_dict
"""

from __future__ import annotations

import time

import pytest

from pulsemq.models import (
    AuthUser,
    BufferedMessage,
    ExpandedPermissions,
    TopicInfo,
)


# ---- AuthUser ----


def test_auth_user_is_admin():
    """is_admin 与 role 一致。"""
    admin = AuthUser(user_id=1, role="admin", groups=[], api_key="k")
    user = AuthUser(user_id=2, role="user", groups=[], api_key="k")
    assert admin.is_admin is True
    assert user.is_admin is False


def test_auth_user_default_namespace():
    """namespace 默认空串。"""
    u = AuthUser(user_id=1, role="user", groups=[], api_key="k")
    assert u.namespace == ""


def test_auth_user_equality():
    """dataclass 相等性。"""
    u1 = AuthUser(user_id=1, role="user", groups=["a"], api_key="k1", namespace="ns")
    u2 = AuthUser(user_id=1, role="user", groups=["a"], api_key="k1", namespace="ns")
    u3 = AuthUser(user_id=2, role="user", groups=["a"], api_key="k1", namespace="ns")
    assert u1 == u2
    assert u1 != u3


# ---- TopicInfo ----


def test_topic_info_from_name_simple():
    """from_name 拆分 namespace 与 topic_path。"""
    t = TopicInfo.from_name("team-a.mkt.sh.600000")
    assert t.full_name == "team-a.mkt.sh.600000"
    assert t.namespace == "team-a"
    assert t.topic_path == "mkt.sh.600000"
    assert t.is_wildcard is False


def test_topic_info_from_name_single_segment():
    """只有一段时 topic_path 为空。"""
    t = TopicInfo.from_name("only")
    assert t.namespace == "only"
    assert t.topic_path == ""
    assert t.is_wildcard is False


def test_topic_info_from_name_wildcard_star():
    """含 `*` 段被识别为通配。"""
    t = TopicInfo.from_name("team-a.mkt.*.600000")
    assert t.is_wildcard is True


def test_topic_info_from_name_wildcard_gt():
    """含 `>` 段被识别为通配。"""
    t = TopicInfo.from_name("team-a.>")
    assert t.is_wildcard is True


def test_topic_info_defaults():
    """默认字段合理。"""
    t = TopicInfo.from_name("a.b.c")
    assert t.subscriber_count == 0
    assert t.created_at <= time.time()
    assert t.created_at > 0


def test_topic_info_empty_name():
    """空 full_name 时不应崩溃。"""
    t = TopicInfo.from_name("")
    assert t.full_name == ""
    assert t.namespace == ""
    assert t.topic_path == ""


# ---- BufferedMessage ----


def test_buffered_message_construction():
    """构造 BufferedMessage 字段保留。"""
    m = BufferedMessage(
        topic="t",
        seq=1,
        record_count=10,
        meta=b"\x00\x01",
        payload=b"\xde\xad\xbe\xef",
        timestamp=1.23,
    )
    assert m.topic == "t"
    assert m.seq == 1
    assert m.record_count == 10
    assert m.meta == b"\x00\x01"
    assert m.payload == b"\xde\xad\xbe\xef"
    assert m.timestamp == 1.23


def test_buffered_message_equality():
    """dataclass 相等。"""
    m1 = BufferedMessage("t", 1, 1, b"", b"", 1.0)
    m2 = BufferedMessage("t", 1, 1, b"", b"", 1.0)
    assert m1 == m2


# ---- ExpandedPermissions ----


def test_expanded_permissions_defaults():
    """默认空列表。"""
    e = ExpandedPermissions()
    assert e.pub == []
    assert e.sub == []
    assert e.query == []


def test_expanded_permissions_from_dict_full():
    """从 dict 构造完整字段。"""
    e = ExpandedPermissions.from_dict(
        {"pub": ["a.b"], "sub": ["a.>"], "query": ["c"]}
    )
    assert e.pub == ["a.b"]
    assert e.sub == ["a.>"]
    assert e.query == ["c"]


def test_expanded_permissions_from_dict_missing():
    """缺失字段默认为空列表。"""
    e = ExpandedPermissions.from_dict({})
    assert e.pub == []
    assert e.sub == []
    assert e.query == []


def test_expanded_permissions_equality():
    """dataclass 相等。"""
    e1 = ExpandedPermissions(pub=["a"], sub=[], query=[])
    e2 = ExpandedPermissions(pub=["a"], sub=[], query=[])
    e3 = ExpandedPermissions(pub=["b"], sub=[], query=[])
    assert e1 == e2
    assert e1 != e3
