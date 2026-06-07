"""ClientTracker 单测。

覆盖:
- 生命周期: on_connect / on_disconnect / on_heartbeat
- 订阅: on_sub / on_unsub
- 流量: on_pub / on_deliver 增计数
- 查询: get / list_online (含心跳超时过滤) / list_all
- 快照: snapshot 字段完整、identity hex 编码
- 边界: on_disconnect 不存在的 identity 不抛错; 重复 on_connect 视为重置
"""

from __future__ import annotations

import time

import pytest

from pulsemq.monitoring.client_tracker import (
    HEARTBEAT_TIMEOUT_SECONDS,
    ClientInfo,
    ClientTracker,
)


# ---- 生命周期 ----


def test_on_connect_registers_client():
    """on_connect 注册新客户端, 字段初值正确。"""
    t = ClientTracker()
    t.on_connect(b"c1", user_id=42)
    info = t.get(b"c1")
    assert info is not None
    assert info.identity == b"c1"
    assert info.user_id == 42
    assert info.connected_at > 0
    assert info.last_heartbeat > 0
    assert info.subscribed_topics == set()
    assert info.msg_in_count == 0
    assert info.msg_out_count == 0


def test_on_connect_user_id_none():
    """未认证连接 user_id=None。"""
    t = ClientTracker()
    t.on_connect(b"anon", user_id=None)
    assert t.get(b"anon").user_id is None


def test_on_disconnect_removes_client():
    """on_disconnect 移除客户端。"""
    t = ClientTracker()
    t.on_connect(b"c1", user_id=1)
    t.on_disconnect(b"c1")
    assert t.get(b"c1") is None


def test_on_disconnect_unknown_is_noop():
    """断开不存在的 identity 不抛错。"""
    t = ClientTracker()
    t.on_disconnect(b"never-existed")  # 不抛错


def test_on_heartbeat_updates_timestamp():
    """on_heartbeat 刷新 last_heartbeat。"""
    t = ClientTracker()
    t.on_connect(b"c1", user_id=1)
    old_hb = t.get(b"c1").last_heartbeat
    time.sleep(0.01)
    t.on_heartbeat(b"c1")
    assert t.get(b"c1").last_heartbeat > old_hb


def test_on_heartbeat_unknown_is_noop():
    """心跳不存在的 identity 不抛错。"""
    t = ClientTracker()
    t.on_heartbeat(b"never-existed")  # 不抛错


def test_reconnect_resets_connected_at():
    """重复 on_connect 视为重连, 刷新 connected_at。"""
    t = ClientTracker()
    t.on_connect(b"c1", user_id=1)
    old_connected = t.get(b"c1").connected_at
    time.sleep(0.01)
    t.on_connect(b"c1", user_id=1)
    assert t.get(b"c1").connected_at > old_connected


# ---- 订阅 ----


def test_on_sub_adds_topic():
    """on_sub 记录订阅 topic。"""
    t = ClientTracker()
    t.on_connect(b"c1", user_id=1)
    t.on_sub(b"c1", "t.a")
    t.on_sub(b"c1", "t.b")
    assert t.get(b"c1").subscribed_topics == {"t.a", "t.b"}


def test_on_unsub_removes_topic():
    """on_unsub 删除订阅 topic。"""
    t = ClientTracker()
    t.on_connect(b"c1", user_id=1)
    t.on_sub(b"c1", "t.a")
    t.on_unsub(b"c1", "t.a")
    assert "t.a" not in t.get(b"c1").subscribed_topics


def test_on_unsub_unknown_topic_is_noop():
    """取消未订阅的 topic 不抛错。"""
    t = ClientTracker()
    t.on_connect(b"c1", user_id=1)
    t.on_unsub(b"c1", "never-subbed")  # 不抛错


# ---- 流量 ----


def test_on_pub_increments_out_counter():
    """on_pub 增 msg_out_count, 更新 EWMA。"""
    t = ClientTracker()
    t.on_connect(b"c1", user_id=1)
    t.on_pub(b"c1", payload_size=10)
    t.on_pub(b"c1", payload_size=20)
    t.on_pub(b"c1", payload_size=30)
    info = t.get(b"c1")
    assert info.msg_out_count == 3
    assert info.msg_out_rate_1min.value > 0


def test_on_deliver_increments_in_counter():
    """on_deliver 增 msg_in_count, 更新 EWMA。"""
    t = ClientTracker()
    t.on_connect(b"c1", user_id=1)
    t.on_deliver(b"c1", payload_size=10)
    t.on_deliver(b"c1", payload_size=20)
    info = t.get(b"c1")
    assert info.msg_in_count == 2
    assert info.msg_in_rate_1min.value > 0


def test_on_pub_unknown_identity_is_noop():
    """on_pub 未注册 identity 不抛错。"""
    t = ClientTracker()
    t.on_pub(b"never-existed", payload_size=10)  # 不抛错


# ---- list_online ----


def test_list_online_within_heartbeat_window():
    """心跳在 60s 内的视为在线。"""
    t = ClientTracker(heartbeat_timeout=1.0)
    t.on_connect(b"c1", user_id=1)
    t.on_connect(b"c2", user_id=2)
    online = t.list_online()
    assert len(online) == 2


def test_list_online_filters_expired():
    """心跳超时的 client 不在 list_online 中。"""
    t = ClientTracker(heartbeat_timeout=0.1)
    t.on_connect(b"c1", user_id=1)
    time.sleep(0.15)
    # c1 心跳已超时
    online = t.list_online()
    assert online == []


def test_list_online_heartbeat_revives():
    """超时后 on_heartbeat 重新激活。"""
    t = ClientTracker(heartbeat_timeout=0.1)
    t.on_connect(b"c1", user_id=1)
    time.sleep(0.15)
    # 即将超时
    t.on_heartbeat(b"c1")
    online = t.list_online()
    assert len(online) == 1
    assert online[0].identity == b"c1"


def test_list_all_includes_all_registered():
    """list_all 列出所有已注册客户端 (含心跳超时的, 但 disconnect 已清掉)。"""
    t = ClientTracker()
    t.on_connect(b"c1", user_id=1)
    t.on_connect(b"c2", user_id=2)
    assert len(t.list_all()) == 2


# ---- snapshot ----


def test_snapshot_structure():
    """snapshot 返回 online_count + clients 列表, identity 是 hex 字符串。"""
    t = ClientTracker()
    t.on_connect(b"\x01\x02\x03", user_id=7)
    t.on_sub(b"\x01\x02\x03", "t.a")
    t.on_pub(b"\x01\x02\x03", 10)

    s = t.snapshot()
    assert s["online_count"] == 1
    assert isinstance(s["clients"], list)
    assert len(s["clients"]) == 1

    c = s["clients"][0]
    assert c["identity"] == "010203"
    assert c["user_id"] == 7
    assert c["subscribed_topics"] == ["t.a"]
    assert c["msg_out_count"] == 1
    assert "msg_in_rate_1min" in c
    assert "msg_out_rate_1min" in c
    assert "connected_at" in c
    assert "last_heartbeat" in c


def test_snapshot_excludes_offline():
    """snapshot 仅含 online 客户端。"""
    t = ClientTracker(heartbeat_timeout=0.1)
    t.on_connect(b"c1", user_id=1)
    time.sleep(0.15)
    s = t.snapshot()
    assert s["online_count"] == 0
    assert s["clients"] == []


# ---- 边界: heartbeat_timeout 默认值 ----


def test_default_heartbeat_timeout_is_60s():
    """默认 heartbeat_timeout 应该是 60 秒。"""
    assert HEARTBEAT_TIMEOUT_SECONDS == 60.0
    t = ClientTracker()
    assert t._heartbeat_timeout == 60.0


# ---- ClientInfo dataclass ----


def test_client_info_default_factories():
    """ClientInfo 默认工厂应每次新建独立 set / EWMA。"""
    a = ClientInfo(identity=b"a", user_id=1, connected_at=0, last_heartbeat=0)
    b = ClientInfo(identity=b"b", user_id=2, connected_at=0, last_heartbeat=0)
    a.subscribed_topics.add("t.x")
    assert "t.x" not in b.subscribed_topics  # 不共享 set
    a.msg_in_rate_1min.update(5)
    assert b.msg_in_rate_1min.value == 0.0  # 不共享 EWMA
