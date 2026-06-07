"""消息类型枚举。"""

from __future__ import annotations


class MsgType:
    """消息类型常量，对应 Frame 3 Byte 0。"""

    AUTH = 0x01
    PUB = 0x02
    SUB = 0x03
    UNSUB = 0x04
    QUERY = 0x05
    PING = 0x06
    PONG = 0x07
    STATUS = 0x08
    ERROR = 0x09
    BROADCAST = 0x0A
    HISTORY_REPLAY = 0x0B
    BATCH = 0x0C  # 批量 PUB：客户端把 N 条 PUB 打包为 1 条 BATCH，server 拆解

    # 控制消息集合（进入 ctrl_buffer）
    _CONTROL_TYPES: frozenset[int] = frozenset({
        AUTH, SUB, UNSUB, QUERY, PING,
    })

    @classmethod
    def is_control(cls, msg_type: int) -> bool:
        """判断是否为控制消息。"""
        return msg_type in cls._CONTROL_TYPES

    @classmethod
    def from_byte(cls, b: int) -> int | None:
        """从字节值获取消息类型，非法值返回 None。"""
        valid = {
            cls.AUTH, cls.PUB, cls.SUB, cls.UNSUB, cls.QUERY,
            cls.PING, cls.PONG, cls.STATUS, cls.ERROR, cls.BROADCAST,
            cls.HISTORY_REPLAY, cls.BATCH,
        }
        return b if b in valid else None
