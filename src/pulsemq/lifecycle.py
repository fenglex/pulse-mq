"""统一启动顺序与优雅关闭 + 信号处理。"""
from __future__ import annotations

import asyncio
import signal

from pulsemq.logging_setup import logger

# 优雅关闭超时（秒）：超时后强制退出，避免 stop() 卡住导致进程无法终止。
_SHUTDOWN_TIMEOUT = 10.0


async def run_server(server) -> int:
    """启动 Server，监听 SIGINT/SIGTERM 触发优雅关闭，返回退出码。"""
    loop = asyncio.get_running_loop()
    shutdown_task: asyncio.Task | None = None

    def _request_shutdown() -> None:
        nonlocal shutdown_task
        if not server.is_shutting_down():
            logger.info("收到终止信号，开始优雅关闭")
            shutdown_task = asyncio.create_task(server.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_shutdown)
        except (NotImplementedError, RuntimeError):
            # Windows 不支持 add_signal_handler；非主线程无信号。
            pass

    await server.start()
    await server.wait_for_shutdown()  # server.stop() 设置 _stop 后返回
    # 等待 server.stop() 完成所有清理（取消任务、关闭 transport 等）。
    # 超时强制退出，避免某个关闭步骤卡住导致进程无法终止。
    if shutdown_task is not None:
        try:
            await asyncio.wait_for(shutdown_task, timeout=_SHUTDOWN_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning("优雅关闭超时（{}s），强制退出", _SHUTDOWN_TIMEOUT)
        except Exception:
            logger.debug("server.stop() 异常", exc_info=True)
    return 0
