"""CLI 入口：pulsemq 启动 Server。"""
from __future__ import annotations

import argparse
import asyncio
import sys

from pulsemq._version import __version__
from pulsemq.errors import PulseMQError, exit_code_for
from pulsemq.lifecycle import run_server
from pulsemq.logging_setup import setup_logging
from pulsemq.server import Server


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="pulsemq",
        description="PulseMQ 消息中间件服务器"
                    "（配置经 TOML / 环境变量，默认端口 5555/5556/9090）")
    parser.add_argument("--version", action="version",
                        version=f"pulsemq {__version__}")
    parser.parse_args(argv)
    setup_logging()
    try:
        server = Server()
        return asyncio.run(run_server(server))
    except PulseMQError as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        return exit_code_for(e)
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
