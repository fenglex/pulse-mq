"""消息类型常量。

v2 简化：无 broker 不需要 AUTH/SUB/UNSUB/QUERY 等控制消息，
仅保留 DATA 和 PING。
"""

from __future__ import annotations


class MsgType:
    """消息类型常量，对应 meta 帧 Byte 0。"""

    DATA = 0x01
    PING = 0x02
