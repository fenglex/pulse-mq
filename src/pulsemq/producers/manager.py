"""ProducerManager: 回调注册 + asyncio Task 并发调度。

两种模式：
- 普通 producer：固定延迟调度（interval > 0）
- burst producer：无间隔连续发送（interval=0），用于极限性能测试
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable, Awaitable

from loguru import logger

from pulsemq.producers.types import ProducerCallback, PubData


@dataclass
class ProducerSpec:
    """单个 producer 的配置。"""

    name: str                       # topic 名（同时也是 producer 名）
    callback: ProducerCallback      # async 回调
    interval: float = 5.0           # 推送间隔（秒）
    serializer: str | None = None   # 序列化格式；None = encode 时按数据类型自动选择
    compression: str = "none"       # 压缩格式


# 消息分发回调：on_message(spec, data) —— ProducerManager 调用
# 必须在 ProducerSpec 定义之后（TypeAlias 右侧在模块加载时即求值，
# from __future__ import annotations 只对函数注解惰性求值，不影响此处）
OnMessageCallback = Callable[[ProducerSpec, PubData], Awaitable[None]]


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
        serializer: str | None = None,
        compression: str = "none",
    ) -> None:
        """注册一个普通 producer。"""
        spec = ProducerSpec(
            name=name,
            callback=callback,
            interval=interval,
            serializer=serializer,
            compression=compression,
        )
        self._specs[name] = spec
        logger.info("Producer 注册: name={} interval={:.1f}s", name, interval)

    def register_burst(
        self,
        callback: ProducerCallback,
        name: str,
        serializer: str | None = None,
        compression: str = "none",
    ) -> None:
        """注册一个 burst producer：无间隔连续发送，用于极限性能测试。"""
        spec = ProducerSpec(
            name=name,
            callback=callback,
            interval=0.0,  # burst 模式标记
            serializer=serializer,
            compression=compression,
        )
        self._specs[name] = spec
        logger.info("Burst Producer 注册: name={}", name)

    @property
    def specs(self) -> dict[str, ProducerSpec]:
        return self._specs

    async def start_all(
        self,
        on_message: OnMessageCallback,
    ) -> None:
        """启动所有 producer 任务。

        Args:
            on_message: async callback(spec, data) 每次回调返回时调用。
        """
        self._running = True
        for name, spec in self._specs.items():
            if spec.interval == 0.0:
                coro = self._run_burst_loop(spec, on_message)
            else:
                coro = self._run_loop(spec, on_message)
            task = asyncio.create_task(coro, name=f"producer-{name}")
            self._tasks[name] = task
            logger.info("Producer 启动: {} (burst={})", name, spec.interval == 0.0)

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

    async def _run_loop(
        self,
        spec: ProducerSpec,
        on_message: OnMessageCallback,
    ) -> None:
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
                logger.warning("Producer {} 回调异常", spec.name, exc_info=True)

            elapsed = time.monotonic() - start
            sleep_time = max(0.0, spec.interval - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
            else:
                # 不积压，让出控制权
                await asyncio.sleep(0)

    async def _run_burst_loop(
        self,
        spec: ProducerSpec,
        on_message: OnMessageCallback,
    ) -> None:
        """Burst 模式：无间隔连续发送，直到 stop 或回调返回 None。

        - 每次循环直接调用回调，无 sleep
        - 异常后短暂冷却 0.1s，避免空转
        """
        while self._running:
            try:
                data = await spec.callback()
                if data is None:
                    break  # 回调主动结束
                await on_message(spec, data)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.warning("Burst Producer {} 回调异常", spec.name, exc_info=True)
                await asyncio.sleep(0.1)
