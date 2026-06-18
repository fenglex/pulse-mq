"""Topic 环形缓存。

每个 topic 一个缓存，按**记录数**（record_count）总量淘汰，用于新订阅者补发历史。

设计要点：
- 缓存实体仍是 CachedMessage（一帧 = 一条 ZMQ 消息 = 一次发送），补发时按帧还原
- 淘汰策略按"累计记录数"：当总记录数超过 max_records 时，从队首丢帧
  （DataFrame 一批 1000 条会占 1000 的配额，而非 1）
- max_records 由 producer 注册时的 cache_size 指定（语义从"帧数"改为"记录数"）

向后兼容：
- size 属性返回帧数（测试与调试用）
- snapshot(since_ns, limit) 仍按时间戳查询帧（补发逻辑不变）
- append 的 record_count 默认 1，旧调用方无需改动
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass
class CachedMessage:
    """缓存中的消息。"""

    timestamp_ns: int
    frames: list[bytes]       # 原始 4 帧数据（可用于重发）
    record_count: int = 1     # 本帧包含的记录数


class TopicBuffer:
    """单个 topic 的环形缓存（按记录总数淘汰）。"""

    def __init__(self, topic: str, max_size: int = 100_000) -> None:
        self._topic = topic
        self._max_records = max_size          # 记录数上限
        self._buf: deque[CachedMessage] = deque()
        self._total_records = 0               # 当前累计记录数

    def append(self, timestamp_ns: int, frames: list[bytes], record_count: int = 1) -> None:
        """追加一条消息。

        Args:
            record_count: 本帧包含的记录数（DataFrame 一批 N 行 = N）。
                          累计到 _total_records，超限则从队首丢帧。

        淘汰：当 _total_records + record_count > max_records 时，
              不断从队首 popleft 直到容纳新帧后不超限。
        """
        rc = max(record_count, 1)
        # 先淘汰：直到加入新帧后总记录数 <= max_records
        while self._buf and self._total_records + rc > self._max_records:
            evicted = self._buf.popleft()
            self._total_records -= evicted.record_count
        # 加入新帧（即便超限也至少保留这一帧，避免单帧就超限时缓存为空）
        self._buf.append(CachedMessage(timestamp_ns=timestamp_ns, frames=frames, record_count=rc))
        self._total_records += rc

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
        """缓存中的帧数（向后兼容，测试/调试用）。"""
        return len(self._buf)

    @property
    def total_records(self) -> int:
        """缓存中的累计记录数（用于显示）。"""
        return self._total_records

    @property
    def max_records(self) -> int:
        """记录数上限。"""
        return self._max_records


class TopicBufferRegistry:
    """所有 topic 缓存的注册表。"""

    def __init__(self) -> None:
        self._buffers: dict[str, TopicBuffer] = {}

    def get_or_create(self, topic: str, max_size: int = 100_000) -> TopicBuffer:
        """获取或创建 topic 缓存。已存在时忽略 max_size。"""
        if topic not in self._buffers:
            self._buffers[topic] = TopicBuffer(topic, max_size)
        return self._buffers[topic]

    def get(self, topic: str) -> TopicBuffer | None:
        return self._buffers.get(topic)

    def snapshot(self) -> dict[str, dict]:
        """所有 topic 的缓存快照（当前记录数 + 上限，给 Admin 显示用）。"""
        return {
            topic: {"current": buf.total_records, "max": buf.max_records}
            for topic, buf in self._buffers.items()
        }
