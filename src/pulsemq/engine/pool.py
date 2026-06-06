"""对象池：Message + PipelineContext 复用，减少 GC 压力。"""

from __future__ import annotations

import time


class MessageContextPool:
    """PipelineContext dict 对象池，预分配 + 复用。

    消息主循环每处理一条消息都需要一个上下文 dict，
    高频场景下避免频繁 dict 创建和 GC。
    """

    def __init__(self, size: int = 1000):
        self._pool: list[dict] = [{} for _ in range(size)]
        self._available: int = size

    def acquire(self) -> dict:
        """获取一个干净的上下文 dict。"""
        if self._available > 0:
            self._available -= 1
            return self._pool[self._available]
        return {}  # 池耗尽回退到普通分配

    def release(self, ctx: dict) -> None:
        """归还上下文，清空后放回池。"""
        ctx.clear()
        if self._available < len(self._pool):
            self._pool[self._available] = ctx
            self._available += 1

    @property
    def available(self) -> int:
        return self._available


class PipelineContextPool:
    """PipelineContext 对象池，复用 dataclass 实例减少 GC 压力。

    通过重置字段值而非创建新对象来实现复用。
    """

    def __init__(self, size: int = 4096):
        from pulsemq.engine.pipeline import PipelineContext
        self._pool: list[PipelineContext] = [
            PipelineContext(
                identity=b"", msg_type=0, topic="", meta=b"", payload=b""
            )
            for _ in range(size)
        ]
        self._available: int = size

    def acquire(
        self,
        identity: bytes,
        msg_type: int,
        topic: str,
        meta: bytes,
        payload: bytes,
        record_count: int = 0,
    ):
        """获取一个已填充的 PipelineContext。"""
        from pulsemq.engine.pipeline import PipelineContext
        if self._available > 0:
            self._available -= 1
            ctx = self._pool[self._available]
            ctx.identity = identity
            ctx.msg_type = msg_type
            ctx.topic = topic
            ctx.meta = meta
            ctx.payload = payload
            ctx.record_count = record_count
            ctx.user = None
            ctx.timestamp = time.time()
            return ctx
        # 池耗尽回退
        return PipelineContext(
            identity=identity, msg_type=msg_type, topic=topic,
            meta=meta, payload=payload, record_count=record_count,
        )

    def release(self, ctx) -> None:
        """归还 PipelineContext。"""
        if self._available < len(self._pool):
            ctx.identity = b""
            ctx.topic = ""
            ctx.meta = b""
            ctx.payload = b""
            ctx.user = None
            self._pool[self._available] = ctx
            self._available += 1

    @property
    def available(self) -> int:
        return self._available


class MessagePool:
    """双缓冲中 Message 包装对象的对象池。

    预分配 Message 对象，避免高负载时频繁创建/销毁。
    """

    def __init__(self, size: int = 4096):
        self._pool: list[list] = [[] for _ in range(size)]
        self._available: int = size

    def acquire(self) -> list:
        """获取一个 Message 槽位（list 格式：[identity, topic, meta, payload]）。"""
        if self._available > 0:
            self._available -= 1
            return self._pool[self._available]
        return []  # 池耗尽回退

    def release(self, msg: list) -> None:
        """归还 Message，清空后放回池。"""
        msg.clear()
        if self._available < len(self._pool):
            self._pool[self._available] = msg
            self._available += 1

    @property
    def available(self) -> int:
        return self._available
