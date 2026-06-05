"""过载保护：双缓冲 + 优先级丢弃。

data_buffer: 行情数据 (PUB)，满了直接丢弃新消息
ctrl_buffer: 控制消息 (AUTH/SUB/QUERY/PING)，满了丢弃最旧的

消费优先级：ctrl_buffer 先于 data_buffer。
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field

from pulsemq.protocol.msg_type import MsgType

logger = logging.getLogger(__name__)


@dataclass
class OverloadStats:
    """过载保护统计。"""

    dropped_total: int = 0
    data_buffer_usage: float = 0.0
    ctrl_buffer_usage: float = 0.0


class DualBuffer:
    """双缓冲过载保护。

    控制消息和数据消息分池存储，控制路径永不饿死。
    """

    def __init__(
        self,
        data_buffer_size: int = 9000,
        ctrl_buffer_size: int = 1000,
    ):
        self.data_buffer: deque = deque(maxlen=data_buffer_size)
        self.ctrl_buffer: deque = deque(maxlen=ctrl_buffer_size)
        self._dropped_total: int = 0

    def enqueue(self, frames: list[bytes]) -> None:
        """根据消息类型分流到对应缓冲区。"""
        # 从 frames 中判断 msg_type
        # 5帧: frames[2] = meta, 6帧: frames[3] = meta
        meta_idx = 2 if len(frames) == 5 else 3
        if len(frames) <= meta_idx:
            return

        msg_type = frames[meta_idx][0]
        is_control = msg_type in (
            MsgType.AUTH, MsgType.SUB, MsgType.UNSUB, MsgType.QUERY, MsgType.PING,
        )

        if is_control:
            self._enqueue_ctrl(frames)
        else:
            self._enqueue_data(frames)

    def _enqueue_ctrl(self, frames: list[bytes]) -> None:
        """控制消息：满了丢弃最旧的。"""
        if len(self.ctrl_buffer) >= self.ctrl_buffer.maxlen:
            self.ctrl_buffer.popleft()  # 丢弃最旧的
        self.ctrl_buffer.append(frames)

    def _enqueue_data(self, frames: list[bytes]) -> None:
        """数据消息：满了直接丢弃新的。"""
        if len(self.data_buffer) >= self.data_buffer.maxlen:
            self._dropped_total += 1
            return  # 不插入，保持 FIFO
        self.data_buffer.append(frames)

    def drain_ctrl(self, limit: int = 0) -> list[list[bytes]]:
        """消费控制消息（优先）。"""
        result = []
        while self.ctrl_buffer:
            result.append(self.ctrl_buffer.popleft())
            if limit and len(result) >= limit:
                break
        return result

    def drain_data(self, limit: int = 0) -> list[list[bytes]]:
        """消费数据消息。"""
        result = []
        while self.data_buffer:
            result.append(self.data_buffer.popleft())
            if limit and len(result) >= limit:
                break
        return result

    @property
    def stats(self) -> OverloadStats:
        data_max = self.data_buffer.maxlen or 1
        ctrl_max = self.ctrl_buffer.maxlen or 1
        return OverloadStats(
            dropped_total=self._dropped_total,
            data_buffer_usage=len(self.data_buffer) / data_max,
            ctrl_buffer_usage=len(self.ctrl_buffer) / ctrl_max,
        )

    @property
    def dropped_total(self) -> int:
        return self._dropped_total
