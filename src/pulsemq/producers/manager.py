"""ProducerManager: 回调注册 + asyncio Task 并发调度。

每个 producer 是独立的 asyncio.Task，固定延迟调度。
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable

logger = logging.getLogger(__name__)

# producer 回调类型：async 函数，返回任意数据
ProducerCallback = Callable[[], Awaitable[Any]]


@dataclass
class ProducerSpec:
    """单个 producer 的配置。"""

    name: str                       # topic 名（同时也是 producer 名）
    callback: ProducerCallback      # async 回调
    interval: float = 5.0           # 推送间隔（秒）
    cache_size: int = 100_000       # 环形缓存大小
    serializer: str = "msgpack"     # 序列化格式
    compression: str = "none"       # 压缩格式


class ProducerManager:
    """管理所有注册的 producer：回调注册 + 并发调度。"""

    def __init__(self) -> None:
        self._specs: dict[str, ProducerSpec] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._running = False

    def register(
        self,
        callback: ProducerCallback,
        name: str,
        interval: float = 5.0,
        cache_size: int = 100_000,
        serializer: str = "msgpack",
        compression: str = "none",
    ) -> None:
        """注册一个 producer。"""
        spec = ProducerSpec(
            name=name,
            callback=callback,
            interval=interval,
            cache_size=cache_size,
            serializer=serializer,
            compression=compression,
        )
        self._specs[name] = spec
        logger.info("Producer 注册: name=%s interval=%.1fs", name, interval)

    @property
    def specs(self) -> dict[str, ProducerSpec]:
        return self._specs

    async def start_all(self, on_message: Any) -> None:
        """启动所有 producer 任务。

        Args:
            on_message: async callback(spec, data) 每次回调返回时调用。
        """
        self._running = True
        for name, spec in self._specs.items():
            task = asyncio.create_task(
                self._run_loop(spec, on_message),
                name=f"producer-{name}",
            )
            self._tasks[name] = task
            logger.info("Producer 启动: %s", name)

    async def stop_all(self) -> None:
        """停止所有 producer 任务。"""
        self._running = False
        for name, task in self._tasks.items():
            task.cancel()
        # 等待所有任务完成
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()
        logger.info("所有 Producer 已停止")

    async def _run_loop(self, spec: ProducerSpec, on_message: Any) -> None:
        """固定延迟调度：执行 → sleep(interval - elapsed) → 执行 → ...

        - elapsed < interval: sleep 剩余时间
        - elapsed >= interval: sleep(0)，不积压
        - 异常不崩溃，warning 日志后继续下一轮
        """
        while self._running:
            start = time.monotonic()
            try:
                data = await spec.callback()
                if data is not None:
                    await on_message(spec, data)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.warning("Producer %s 回调异常", spec.name, exc_info=True)

            elapsed = time.monotonic() - start
            sleep_time = max(0.0, spec.interval - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            else:
                # 不积压，让出控制权
                await asyncio.sleep(0)
