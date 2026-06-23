"""认证失败场景验证：错误密码的 sub 连接应被拒绝并打印失败日志。"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

import pulsemq  # noqa: F401
from pulsemq.config import PublisherConfig
from pulsemq.publisher import PulsePublisher
from pulsemq.subscriber import PulseSubscriber

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("diag")

PUB_BIND = "tcp://127.0.0.1:15556"
ADMIN_BIND = "127.0.0.1:19091"
USER = "alice"
RIGHT_PWD = "pulse_sk_diag"
WRONG_PWD = "wrong_pwd"


async def _make_data():
    return [{"x": 1}]


async def main() -> int:
    pub = PulsePublisher(
        config=PublisherConfig(
            bind=PUB_BIND, admin_bind=ADMIN_BIND,
            stats_db="sqlite://./_diag_stats2.sqlite",
        ),
        api_keys={USER: RIGHT_PWD},
    )
    pub.register_producer(
        fn=_make_data, name="t", interval=0.3,
    )

    pub_task = asyncio.create_task(pub._run())
    await asyncio.sleep(1.0)

    logger.info("=" * 60)
    logger.info("场景：用错误密码连接，应看到 [SUB 认证失败] 日志")
    logger.info("=" * 60)

    bad_sub = PulseSubscriber(PUB_BIND, username=USER, password=WRONG_PWD)
    await bad_sub.connect()
    received = 0
    try:
        async def _consume():
            nonlocal received
            async for _ in bad_sub.subscribe("t"):
                received += 1
                if received >= 1:
                    break
        await asyncio.wait_for(_consume(), timeout=2.5)
    except asyncio.TimeoutError:
        pass
    finally:
        await bad_sub.close()

    logger.info("错误密码的 sub 收到 %d 条（预期 0，ZAP 拒绝后收不到消息）", received)

    pub._running = False
    await asyncio.sleep(0.3)
    pub_task.cancel()
    with __import__("contextlib").suppress(asyncio.CancelledError, Exception):
        await pub_task

    for ext in ("", "-shm", "-wal"):
        try:
            os.unlink("./_diag_stats2.sqlite" + ext)
        except OSError:
            pass

    return 0 if received == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
