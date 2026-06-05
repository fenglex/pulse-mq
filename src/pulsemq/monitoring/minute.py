"""分钟聚合指标：每分钟将实时指标聚合并写入 SQLite。"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MinuteSlot:
    """一分钟聚合槽。"""

    msg_count: int = 0
    record_count: int = 0
    bytes_total: int = 0
    latency_sum: float = 0.0
    latency_count: int = 0
    error_count: int = 0
    peak_connections: int = 0
    peak_subscriptions: int = 0


class MinuteAggregator:
    """分钟级指标聚合器。

    每分钟将当前槽位数据写入 SQLite，然后切换到新槽位。
    """

    def __init__(self, metrics_repo=None, retention_days: int = 7):
        self._repo = metrics_repo
        self._retention_days = retention_days
        self._current = MinuteSlot()
        self._slot_start: float = time.time()
        self._task: asyncio.Task | None = None

    def inc_message(self, payload_size: int, elapsed_ms: float) -> None:
        self._current.msg_count += 1
        self._current.bytes_total += payload_size
        self._current.latency_sum += elapsed_ms
        self._current.latency_count += 1

    def inc_record(self, count: int) -> None:
        self._current.record_count += count

    def inc_error(self) -> None:
        self._current.error_count += 1

    def update_peak(self, connections: int, subscriptions: int) -> None:
        self._current.peak_connections = max(self._current.peak_connections, connections)
        self._current.peak_subscriptions = max(self._current.peak_subscriptions, subscriptions)

    async def start(self) -> None:
        """启动分钟聚合定时任务。"""
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _run(self) -> None:
        while True:
            # 等到下一分钟边界
            now = time.time()
            next_minute = (int(now) // 60 + 1) * 60
            await asyncio.sleep(next_minute - now)

            # 切换槽位
            slot = self._current
            self._current = MinuteSlot()
            self._slot_start = time.time()

            # 写入 SQLite（如果有 repo）
            if self._repo is not None:
                try:
                    ts = int(next_minute) // 60  # 分钟精度
                    avg_latency = (
                        slot.latency_sum / slot.latency_count
                        if slot.latency_count > 0 else 0
                    )
                    await self._repo.insert_minute_system_stats({
                        "ts": ts,
                        "total_msgs": slot.msg_count,
                        "total_bytes": slot.bytes_total,
                        "avg_latency_ms": avg_latency,
                        "peak_connections": slot.peak_connections,
                        "peak_subscriptions": slot.peak_subscriptions,
                        "error_total": slot.error_count,
                        "dropped_total": 0,
                    })
                except Exception as e:
                    logger.error("分钟指标写入失败: %s", e)

            # 每小时清理旧数据
            if int(next_minute) % 3600 < 70:
                await self._cleanup()

    async def _cleanup(self) -> None:
        if self._repo is None:
            return
        try:
            count = await self._repo.cleanup_old_data(self._retention_days)
            if count > 0:
                logger.info("清理 %d 条过期监控数据", count)
        except Exception as e:
            logger.error("监控数据清理失败: %s", e)
