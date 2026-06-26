"""CLI 入口：python -m pulsemq 启动 Server。"""
from __future__ import annotations

import asyncio
import sys

from pulsemq.errors import PulseMQError, exit_code_for
from pulsemq.lifecycle import run_server
from pulsemq.logging_setup import setup_logging
from pulsemq.server import Server


def main() -> int:
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
