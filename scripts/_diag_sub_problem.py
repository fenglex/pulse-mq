"""端到端验证脚本：启动真实 pub 服务 + sub 端实际收消息。

复现并验证修复效果：
- pub 端开启 PLAIN 认证 + 一个 list[dict] producer（0.2s/条）
- sub 端用 username/password 连接、订阅、打印收到的每条消息
- 运行 5 秒后统计 sub 收到的条数，断言 > 0

运行：
    uv run python scripts/_diag_sub_problem.py

退出码：
    0 = sub 正确收到消息（修复生效）
    1 = sub 一条都没收到（仍有问题）

注意：本脚本依赖 pulsemq 包导入时自动设置的 WindowsSelectorEventLoopPolicy
（见 src/pulsemq/__init__.py）。脚本顶部不再手动设置策略，
以此验证用户实际使用场景下的修复效果。
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

# 关键：不在此处手动设置事件循环策略，依赖 import pulsemq 时的自动修正
import pulsemq  # noqa: F401  —— 触发 __init__.py 中的策略修正
from pulsemq.config import PublisherConfig
from pulsemq.publisher import PulsePublisher
from pulsemq.subscriber import PulseSubscriber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("diag")

PUB_BIND = "tcp://127.0.0.1:15555"
ADMIN_BIND = "127.0.0.1:19090"
TOPIC = "diag_topic"
USER = "alice"
PASSWORD = "pulse_sk_diag"
STATS_DB = "sqlite://./_diag_stats.sqlite"

RUN_SECONDS = 5.0
PRODUCER_INTERVAL = 0.2


async def _run_pub(stop_event: asyncio.Event) -> None:
    """启动 publisher，每 0.2s 推一条 list[dict]。"""
    pub = PulsePublisher(
        config=PublisherConfig(
            bind=PUB_BIND,
            admin_bind=ADMIN_BIND,
            stats_db=STATS_DB,
        ),
        api_keys={USER: PASSWORD},
    )
    counter = {"n": 0}

    async def _factory():
        counter["n"] += 1
        return [{"seq": counter["n"], "msg": f"hello-{counter['n']}"}]

    pub.register_producer(fn=_factory, name=TOPIC, interval=PRODUCER_INTERVAL)
    logger.info("Publisher 启动: bind=%s admin=%s", PUB_BIND, ADMIN_BIND)
    try:
        run_task = asyncio.create_task(pub._run())
        await stop_event.wait()
    finally:
        pub._running = False
        await asyncio.sleep(0.3)
        run_task.cancel()
        with __import__("contextlib").suppress(asyncio.CancelledError, Exception):
            await run_task
        logger.info("Publisher 已停止")

    # 清理临时 db
    for ext in ("", "-shm", "-wal"):
        try:
            os.unlink("./_diag_stats.sqlite" + ext)
        except OSError:
            pass


async def _run_sub() -> int:
    """sub 端连接、订阅、收消息，返回收到的条数。"""
    sub = PulseSubscriber(PUB_BIND, username=USER, password=PASSWORD)
    await sub.connect()
    received = 0
    try:
        async def _consume():
            nonlocal received
            async for msg in sub.subscribe(TOPIC):
                received += 1
                print(
                    f"  [SUB] #{received} topic={msg.topic} "
                    f"record_count={msg.record_count} payload={msg.payload}",
                    file=sys.stderr,
                )

        # 给 sub 足够时间收消息（producer interval 0.2s，至少能收 ~20 条）
        await asyncio.wait_for(_consume(), timeout=RUN_SECONDS)
    except asyncio.TimeoutError:
        # 正常退出：达到运行时长
        pass
    finally:
        await sub.close()
    return received


async def main() -> int:
    logger.info("=" * 60)
    logger.info("端到端验证：pub 服务 + sub 订阅收消息")
    logger.info("平台: %s | 事件循环策略: %s",
                sys.platform, type(asyncio.get_event_loop_policy()).__name__)
    logger.info("=" * 60)

    stop_event = asyncio.Event()
    pub_task = asyncio.create_task(_run_pub(stop_event))

    # 等 pub 起来并已发出几条
    await asyncio.sleep(1.0)

    try:
        received = await _run_sub()
    finally:
        stop_event.set()
        with __import__("contextlib").suppress(asyncio.CancelledError, Exception):
            await pub_task

    logger.info("=" * 60)
    logger.info("结果：sub 共收到 %d 条消息", received)
    if received > 0:
        logger.info("✅ 修复生效：sub 端能正常收到消息")
        return 0
    else:
        logger.error("❌ sub 一条都没收到，仍存在问题")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
