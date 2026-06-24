"""验证脚本：错误密码连接后应打日志并静默结束迭代，而非卡死或抛异常。

修复前：sub.recv() 无限等待（pyzmq 在 ZAP 拒绝后不抛错、后台重连）。
修复后：PulseSubscriber 通过 monitor 检测 EVENT_HANDSHAKE_FAILED_AUTH，
        自行打 error 日志并结束迭代，``async for`` 自然退出，
        **无需用户 try/except**。

运行：
    uv run python scripts/_diag_auth_hang.py

退出码：
    0 = sub 静默结束迭代（修复生效）
    1 = sub 卡死或抛了异常（仍有问题）
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time

import pulsemq  # noqa: F401  —— 触发 Windows 事件循环修正
from pulsemq import PulseSubscriber
from pulsemq.config import PublisherConfig
from pulsemq.publisher import PulsePublisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("diag")

PUB_BIND = "tcp://127.0.0.1:15557"
ADMIN_BIND = "127.0.0.1:19092"
USER = "alice"
RIGHT_PWD = "pulse_sk_diag"
WRONG_PWD = "wrong_pwd"


async def _pub(stop_event: asyncio.Event) -> None:
    pub = PulsePublisher(
        config=PublisherConfig(
            bind=PUB_BIND, admin_bind=ADMIN_BIND,
            stats_db="sqlite://./_diag_stats3.sqlite",
        ),
        api_keys={USER: RIGHT_PWD},
    )

    async def _f():
        return [{"x": 1}]

    pub.register_producer(fn=_f, name="t", interval=0.3)
    try:
        task = asyncio.create_task(pub._run())
        await stop_event.wait()
    finally:
        pub._running = False
        await asyncio.sleep(0.3)
        task.cancel()
        with __import__("contextlib").suppress(asyncio.CancelledError, Exception):
            await task


async def main() -> int:
    stop = asyncio.Event()
    pub_task = asyncio.create_task(_pub(stop))
    await asyncio.sleep(1.0)

    logger.info("=" * 60)
    logger.info("场景：用错误密码连接，验证 sub 静默结束迭代（不卡死、不抛异常）")
    logger.info("=" * 60)

    sub = PulseSubscriber(PUB_BIND, username=USER, password=WRONG_PWD)
    await sub.connect()

    start = time.monotonic()
    result = {"finished": False, "exc_type": None, "msg": None, "received": 0}

    async def _consume() -> None:
        async for _msg in sub.subscribe("t"):
            result["received"] += 1

    try:
        # 最多等 5s，期望 ~1s 内就静默结束（async for 正常退出）
        await asyncio.wait_for(_consume(), timeout=5.0)
        result["finished"] = True
    except asyncio.TimeoutError:
        result["exc_type"] = "TimeoutError(卡死)"
    except Exception as e:
        result["exc_type"] = type(e).__name__
        result["msg"] = str(e)
    finally:
        await sub.close()

    elapsed = time.monotonic() - start

    logger.info("=" * 60)
    logger.info("结果（耗时 %.2fs）：", elapsed)
    logger.info("  收到消息数: %d（预期 0）", result["received"])
    if result["exc_type"]:
        logger.info("  抛出异常: %s %s", result["exc_type"], result["msg"] or "")
    if result["finished"] and result["received"] == 0 and result["exc_type"] is None:
        logger.info("✅ 修复生效：sub 打日志后静默结束迭代，未卡死、未抛异常")
        ok = True
    else:
        logger.error("❌ 不符合预期（应静默结束，收到 0 条，无异常）")
        ok = False
    logger.info("=" * 60)

    stop.set()
    with __import__("contextlib").suppress(asyncio.CancelledError, Exception):
        await pub_task

    for ext in ("", "-shm", "-wal"):
        try:
            os.unlink("./_diag_stats3.sqlite" + ext)
        except OSError:
            pass

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
