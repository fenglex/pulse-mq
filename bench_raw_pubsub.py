"""Raw ZeroMQ PUB/SUB 基准测试 —— v1 等价基线，与 PulseMQ v2 Client/Server 对比。

v1 时代 PulseMQ 内部就是 zmq PUB/SUB（+ PLAIN 认证）。本脚本用裸 zmq.asyncio 复现
v1 的拓扑（无服务器中继，发布者直接广播给所有订阅者），用与 v2 基准完全相同的
帧编码（pulsemq.protocol.frames.encode/decode），从而隔离架构差异。
"""
from __future__ import annotations

import argparse
import asyncio
import random
import time

import zmq
import zmq.asyncio

from pulsemq.protocol.frames import encode, decode

SYMBOLS = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "JNJ"]
_BASE = {"AAPL": 180, "GOOG": 140, "MSFT": 420, "AMZN": 185, "TSLA": 250,
         "NVDA": 900, "META": 500, "JPM": 200, "V": 280, "JNJ": 160}


def make_tick(seq: int) -> dict:
    sym = random.choice(SYMBOLS)
    p = _BASE[sym] + random.uniform(-2, 2)
    return {
        "symbol": sym, "price": round(p, 2),
        "volume": random.randint(1, 500),
        "bid": round(p - random.uniform(0, 0.05), 2),
        "ask": round(p + random.uniform(0, 0.05), 2),
        "seq": seq,
    }


async def run_publisher(port: int, duration: float, ready: asyncio.Event) -> None:
    ctx = zmq.asyncio.Context()
    pub = ctx.socket(zmq.PUB)
    pub.setsockopt(zmq.LINGER, 1000)
    pub.bind(f"tcp://127.0.0.1:{port}")
    # 等待订阅者 connect + 设置 SUBSCRIBE 过滤器
    await ready.wait()
    # 再额外等一小段时间让订阅消息真正送达 PUB（zmq 内部）
    await asyncio.sleep(0.1)

    sent = 0
    seq = 0
    t0 = time.monotonic()
    while time.monotonic() - t0 < duration:
        # 主题仅为帧内的元数据；raw PUB-SUB 不做路由，所有订阅者都收到
        topic = f"market.stock.{random.choice(SYMBOLS)}"
        await pub.send(encode(topic, make_tick(seq)))
        sent += 1
        seq += 1
        if sent % 500 == 0:
            await asyncio.sleep(0)
    elapsed = time.monotonic() - t0
    rate = sent / elapsed if elapsed > 0 else 0.0
    print(
        f"[raw publisher] sent={sent} elapsed={elapsed:.2f}s rate={rate:.0f} msg/s",
        flush=True,
    )
    pub.close(linger=1000)
    ctx.term()


async def run_subscriber(port: int, duration: float, ready: asyncio.Event) -> None:
    ctx = zmq.asyncio.Context()
    sub = ctx.socket(zmq.SUB)
    sub.setsockopt(zmq.LINGER, 1000)
    # 空过滤 = 订阅所有
    sub.setsockopt(zmq.SUBSCRIBE, b"")
    sub.connect(f"tcp://127.0.0.1:{port}")
    # 通知 publisher 可以开始灌数据
    ready.set()

    received_ts: list[float] = []
    latencies_ns: list[int] = []
    end = time.monotonic() + duration + 1.0
    while time.monotonic() < end:
        remaining = end - time.monotonic()
        try:
            frame = await asyncio.wait_for(sub.recv(), timeout=min(remaining, 0.5))
        except asyncio.TimeoutError:
            continue
        msg = decode(frame)
        received_ts.append(time.monotonic())
        latencies_ns.append(time.time_ns() - msg.timestamp_ns)

    if not received_ts:
        print("[raw subscriber] received=0", flush=True)
    else:
        t0 = received_ts[0]
        t1 = received_ts[-1]
        elapsed = max(t1 - t0, 1e-9)
        rate = len(received_ts) / elapsed
        latencies_ns.sort()

        def pct(p: float) -> float:
            i = min(len(latencies_ns) - 1, int(p * len(latencies_ns)))
            return latencies_ns[i] / 1e6

        print(
            f"[raw subscriber] received={len(received_ts)} elapsed={elapsed:.2f}s "
            f"rate={rate:.0f} msg/s",
            flush=True,
        )
        print(
            f"[raw subscriber] latency p50={pct(0.50):.3f} ms "
            f"p95={pct(0.95):.3f} ms p99={pct(0.99):.3f} ms "
            f"max={latencies_ns[-1]/1e6:.3f} ms",
            flush=True,
        )
    sub.close(linger=1000)
    ctx.term()


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=5560)
    ap.add_argument("--duration", type=float, default=10.0)
    args = ap.parse_args()
    ready = asyncio.Event()
    await asyncio.gather(
        run_publisher(args.port, args.duration, ready),
        run_subscriber(args.port, args.duration, ready),
    )


if __name__ == "__main__":
    asyncio.run(main())