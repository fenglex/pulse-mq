"""统一启动顺序与优雅关闭 + 信号处理。"""
from __future__ import annotations

import asyncio
import signal

from pulsemq.logging_setup import logger


async def run_server(server) -> int:
    """启动 Server，监听 SIGINT/SIGTERM 触发优雅关闭，返回退出码。"""
    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        if not server.is_shutting_down():
            logger.info("收到终止信号，开始优雅关闭")
            asyncio.create_task(server.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
        except (NotImplementedError, RuntimeError):
            # Windows 不支持 add_signal_handler；非主线程无信号。
            pass

    await server.start()
    await server.wait_for_shutdown()  # server.stop() 设置 _stop 后返回
    return 0
