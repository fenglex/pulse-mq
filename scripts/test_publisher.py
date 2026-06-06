#!/usr/bin/env python3
"""PulseMQ 测试发送端。

连接到服务端，持续发布消息到指定 topic。
支持 str / bytes / DataFrame 三种消息类型。

用法:
    python scripts/test_publisher.py
    python scripts/test_publisher.py --port 5555 --topic test.demo --interval 1.0
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time

from pulsemq.client.async_client import PulseClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def publish_loop(
    address: str,
    xpub_address: str,
    topic: str,
    interval: float,
) -> None:
    """持续发布消息。"""
    async with PulseClient(
        address=address,
        xpub_address=xpub_address,
        auto_reconnect=True,
        heartbeat_interval=10.0,
    ) as client:
        seq = 0
        logger.info("已连接 %s，开始发布到 topic=%s", address, topic)
        logger.info("发送间隔: %.2fs，按 Ctrl+C 停止", interval)

        while True:
            seq += 1

            # ---- 1. 发布字符串消息 ----
            text = f'{{"seq": {seq}, "ts": {time.time():.3f}, "msg": "hello from publisher"}}'
            await client.publish(topic, text)
            logger.info("[#%d] str  → %s (%d bytes)", seq, topic, len(text))

            # ---- 2. 发布二进制消息 ----
            raw_topic = f"{topic}.raw"
            raw = seq.to_bytes(4, "big") + b"\x00" * 16
            await client.publish(raw_topic, raw)
            logger.info("[#%d] bytes → %s (%d bytes)", seq, raw_topic, len(raw))

            # ---- 3. 发布字典消息（msgpack 序列化）----
            dict_topic = f"{topic}.dict"
            data = {
                "seq": seq,
                "code": "600519",
                "price": round(1800 + seq * 0.01, 2),
                "volume": 1000 + seq,
                "ts": time.time(),
            }
            await client.publish(dict_topic, data, format="msgpack")
            logger.info("[#%d] dict → %s", seq, dict_topic)

            await asyncio.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="PulseMQ 测试发送端")
    parser.add_argument("--host", default="localhost", help="服务端地址 (默认 localhost)")
    parser.add_argument("--port", type=int, default=5555, help="服务端端口 (默认 5555)")
    parser.add_argument("--topic", default="test.demo", help="发布 topic (默认 test.demo)")
    parser.add_argument("--interval", type=float, default=1.0, help="发送间隔秒数 (默认 1.0)")
    args = parser.parse_args()

    address = f"tcp://{args.host}:{args.port}"
    xpub_address = f"tcp://{args.host}:{args.port + 1}"

    print(f"{'=' * 50}")
    print(f"  PulseMQ 测试发送端")
    print(f"  服务端:  {address}")
    print(f"  Topic:   {args.topic}")
    print(f"  间隔:    {args.interval}s")
    print(f"{'=' * 50}")

    asyncio.run(publish_loop(address, xpub_address, args.topic, args.interval))


if __name__ == "__main__":
    main()
