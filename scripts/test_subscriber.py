#!/usr/bin/env python3
"""PulseMQ 测试接收端。

连接到服务端，订阅指定 topic 并打印收到的消息。
支持通配符匹配（* 和 >）。

用法:
    python scripts/test_subscriber.py
    python scripts/test_subscriber.py --port 5555 --topics "test.>" "market.*"
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time

from pulsemq.client.async_client import PulseClient, PulseMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def format_payload(msg: PulseMessage) -> str:
    """格式化消息内容用于显示。"""
    if isinstance(msg.payload, str):
        preview = msg.payload[:200]
        if len(msg.payload) > 200:
            preview += "..."
        return preview
    if isinstance(msg.payload, bytes):
        return f"<bytes {len(msg.payload)}B>"
    if isinstance(msg.payload, dict):
        return str(msg.payload)
    if msg.payload is None:
        return f"<raw {len(msg.raw_payload)}B>"
    return str(msg.payload)[:200]


async def subscribe_loop(
    address: str,
    xpub_address: str,
    topics: list[str],
) -> None:
    """持续接收消息。"""
    async with PulseClient(
        address=address,
        xpub_address=xpub_address,
        auto_reconnect=True,
        heartbeat_interval=10.0,
    ) as client:
        count = 0
        logger.info("已连接 %s，订阅 topic: %s", address, ", ".join(topics))
        print("等待消息中... 按 Ctrl+C 停止\n")

        async for msg in client.subscribe(*topics):
            count += 1
            elapsed = time.time() - msg.timestamp
            print(
                f"[#{count:>6}] "
                f"topic={msg.topic:<30} "
                f"type=0x{msg.msg_type:02X} "
                f"payload={format_payload(msg)}"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="PulseMQ 测试接收端")
    parser.add_argument("--host", default="localhost", help="服务端地址 (默认 localhost)")
    parser.add_argument("--port", type=int, default=5555, help="服务端端口 (默认 5555)")
    parser.add_argument(
        "--topics",
        nargs="+",
        default=["test.>"],
        help="订阅 topic 列表，支持通配符 (默认 'test.>')",
    )
    args = parser.parse_args()

    address = f"tcp://{args.host}:{args.port}"
    xpub_address = f"tcp://{args.host}:{args.port + 1}"

    print(f"{'=' * 50}")
    print(f"  PulseMQ 测试接收端")
    print(f"  服务端:  {address}")
    print(f"  Topics:  {', '.join(args.topics)}")
    print(f"{'=' * 50}")

    asyncio.run(subscribe_loop(address, xpub_address, args.topics))


if __name__ == "__main__":
    main()
