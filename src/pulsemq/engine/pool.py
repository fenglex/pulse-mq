"""对象池：Message + PipelineContext 复用，减少 GC 压力。"""

from __future__ import annotations


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
