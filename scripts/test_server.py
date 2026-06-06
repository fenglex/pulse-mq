#!/usr/bin/env python3
"""PulseMQ 测试服务端。

启动一个禁用认证的服务端，方便本地测试。

用法:
    python scripts/test_server.py
    python scripts/test_server.py --port 5555
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

from pulsemq.config import ServerConfig
from pulsemq.event_loop import install_event_loop
from pulsemq.server import PulseServer


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    # 可通过命令行参数自定义端口
    port = 5555
    if len(sys.argv) > 1 and sys.argv[1] == "--port":
        port = int(sys.argv[2])

    config = ServerConfig(
        bind=f"tcp://*:{port}",
        xpub_bind=f"tcp://*:{port + 1}",
        auth_enabled=False,          # 测试环境关闭认证
        metrics_enabled=True,
        metrics_bind="0.0.0.0:9090",
        max_concurrency=100,
        data_buffer_size=50_000,
        ctrl_buffer_size=5_000,
    )

    loop_type = install_event_loop(config.use_uvloop)
    logging.getLogger(__name__).info("事件循环: %s", loop_type)

    server = PulseServer(config)
    loop = asyncio.new_event_loop()

    def _shutdown():
        loop.call_soon_threadsafe(lambda: asyncio.ensure_future(server.stop()))

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _shutdown)

    print(f"{'=' * 50}")
    print(f"  PulseMQ 测试服务端")
    print(f"  ROUTER:  tcp://*:{port}")
    print(f"  XPUB:    tcp://*:{port + 1}")
    print(f"  指标:    http://0.0.0.0:9090/metrics")
    print(f"  认证:    关闭")
    print(f"{'=' * 50}")
    print("按 Ctrl+C 停止")

    try:
        loop.run_until_complete(server.start())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(server.stop())
        loop.close()


if __name__ == "__main__":
    main()
