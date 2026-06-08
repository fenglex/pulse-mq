"""Topic 环形缓存。

每个 topic 一个 deque(maxlen=N)，满时自动淘汰最旧消息。
"""

from __future__ import annotations

import threading
from collections import deque
from dataclasses import dataclass


@dataclass
class CachedMessage:
    """缓存中的消息。"""

    timestamp_ns: int
    frames: list[bytes]       # 原始 4 帧数据（可用于重发）


class TopicBuffer:
    """单个 topic 的环形缓存。"""

    def __init__(self, topic: str, max_size: int = 100_000) -> None:
        self._topic = topic
        self._buf: deque[CachedMessage] = deque(maxlen=max_size)

    def append(self, timestamp_ns: int, frames: list[bytes]) -> None:
        """追加一条消息。满时自动淘汰最旧。"""
        self._buf.append(CachedMessage(timestamp_ns=timestamp_ns, frames=frames))

    def snapshot(self, since_ns: int = 0, limit: int = 100) -> list[CachedMessage]:
        """按时间戳查询（给新 sub 补数据用）。"""
        result: list[CachedMessage] = []
        for msg in self._buf:
            if msg.timestamp_ns > since_ns:
                result.append(msg)
                if len(result) >= limit:
                    break
        return result

    @property
    def size(self) -> int:
        return len(self._buf)


class TopicBufferRegistry:
    """所有 topic 缓存的注册表。"""

    def __init__(self) -> None:
        self._buffers: dict[str, TopicBuffer] = {}

    def get_or_create(self, topic: str, max_size: int = 100_000) -> TopicBuffer:
        """获取或创建 topic 缓存。"""
        if topic not in self._buffers:
            self._buffers[topic] = TopicBuffer(topic, max_size)
        return self._buffers[topic]

    def get(self, topic: str) -> TopicBuffer | None:
        return self._buffers.get(topic)

    def snapshot(self) -> dict[str, int]:
        """所有 topic 的缓存大小快照。"""
        return {topic: buf.size for topic, buf in self._buffers.items()}
