"""MsgType 枚举 单测。"""
from __future__ import annotations

from pulsemq.protocol.msg_type import MsgType


def test_msg_types_distinct():
    """所有 msg_type 值唯一。"""
    values = [getattr(MsgType, name) for name in dir(MsgType)
              if not name.startswith("_") and isinstance(getattr(MsgType, name), int)]
    assert len(values) == len(set(values)), f"重复 msg_type: {values}"


def test_msg_types_in_byte_range():
    """msg_type 必须能用单字节表示。"""
    for name in dir(MsgType):
        if name.startswith("_"):
            continue
        v = getattr(MsgType, name)
        if isinstance(v, int):
            assert 0 <= v < 256, f"{name}={v} 超出单字节范围"


def test_from_byte_known_values():
    """已知 msg_type 字节应正确返回。"""
    for name in ("AUTH", "PUB", "SUB", "PING", "PONG", "BROADCAST"):
        v = getattr(MsgType, name)
        assert MsgType.from_byte(v) == v


def test_from_byte_unknown_returns_none():
    """未知 msg_type 字节返回 None。"""
    assert MsgType.from_byte(0xFF) is None
    assert MsgType.from_byte(0x00) is None  # 0 不在已知 enum 中


def test_batch_msg_type_removed():
    """BATCH 协议已在 v1.0 batcher 后退时移除,MsgType 中不应有 BATCH。"""
    assert not hasattr(MsgType, "BATCH"), "MsgType.BATCH 应当已被移除"
    # 0x0C 不应再识别为有效 msg_type
    assert MsgType.from_byte(0x0C) is None, "0x0C 不应再是有效 msg_type"
