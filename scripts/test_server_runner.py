#!/usr/bin/env python3
"""PulseMQ 测试服务端（端到端测试专用）。

与 scripts/test_server.py 的差异：
- 关闭指标（不需要 HTTP 端口）
- 启动后向 stdout 写 "READY\n"，便于 e2e 脚本同步
- 默认端口 15555（避开开发端口 5555/5556）

用法:
    python scripts/test_server_runner.py
    python scripts/test_server_runner.py --port 15555
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from pulsemq.config import ServerConfig
from pulsemq.event_loop import install_event_loop
from pulsemq.server import PulseServer


def main() -> None:
    logging.basicConfig(
        level=logging.WARNING,  # 测试环境降噪
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=15555, help="ROUTER 端口 (XPUB = port+1)")
    args = parser.parse_args()

    config = ServerConfig(
        bind=f"tcp://*:{args.port}",
        xpub_bind=f"tcp://*:{args.port + 1}",
        auth_enabled=False,
        metrics_enabled=False,
        admin_enabled=False,  # e2e 不需要 Web UI
        max_concurrency=100,
        data_buffer_size=50_000,
        ctrl_buffer_size=5_000,
        zmq_sndhwm=200_000,  # 100k 压测需要更大的 XPUB 队列
        zmq_xpub_nodrop=True,  # 队列满时阻塞 pub, 不丢消息
    )

    install_event_loop(config.use_uvloop)
    server = PulseServer(config)
    loop = asyncio.new_event_loop()

    def _shutdown() -> None:
        loop.call_soon_threadsafe(lambda: asyncio.ensure_future(server.stop()))

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown)

    async def _run() -> None:
        # 启动 server
        start_task = asyncio.create_task(server.start())
        # 等到 transport 完成 bind（约几十 ms），再打印 READY
        await asyncio.sleep(0.3)
        print("READY", flush=True)
        await start_task

    try:
        loop.run_until_complete(_run())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(server.stop())
        loop.close()


if __name__ == "__main__":
    main()
