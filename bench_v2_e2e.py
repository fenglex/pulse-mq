"""PulseMQ v2 端到端基准测试：consumer + producer 在同一进程中并发运行。

用法：uv run python bench_v2_e2e.py [--duration 10]
"""
from __future__ import annotations

import argparse
import asyncio
import random
import time

from pulsemq.client import ConsumerClient, ProducerClient

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


async def run_consumer(ready: asyncio.Event, duration: float) -> None:
    c = ConsumerClient(data_endpoint="tcp://127.0.0.1:5555",
                       control_endpoint="tcp://127.0.0.1:5556",
                       username="subscriber", password="subpw",
                       client_id="bench-cons")
    await c.start()
    received_ts: list[float] = []
    latencies_ns: list[int] = []

    def on_msg(m) -> None:
        received_ts.append(time.monotonic())
        latencies_ns.append(time.time_ns() - m.timestamp_ns)

    await c.subscribe("market.stock.*", on_msg)
    ready.set()  # 通知 producer 可以开始了
    await asyncio.sleep(duration + 1.0)

    if not received_ts:
        print("[consumer] received=0", flush=True)
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
            f"[consumer] received={len(received_ts)} elapsed={elapsed:.2f}s "
            f"rate={rate:.0f} msg/s",
            flush=True,
        )
        print(
            f"[consumer] latency p50={pct(0.50):.3f} ms "
            f"p95={pct(0.95):.3f} ms p99={pct(0.99):.3f} ms "
            f"max={latencies_ns[-1]/1e6:.3f} ms",
            flush=True,
        )
    await c.stop()


async def run_producer(ready: asyncio.Event, duration: float) -> None:
    await ready.wait()  # 等待消费者完成订阅
    p = ProducerClient(data_endpoint="tcp://127.0.0.1:5555",
                       control_endpoint="tcp://127.0.0.1:5556",
                       username="publisher", password="pubpw",
                       client_id="bench-prod")
    await p.start()
    sent = 0
    seq = 0
    t0 = time.monotonic()
    while time.monotonic() - t0 < duration:
        await p.publish(f"market.stock.{random.choice(SYMBOLS)}", make_tick(seq))
        sent += 1
        seq += 1
        if sent % 500 == 0:
            await asyncio.sleep(0)
    elapsed = time.monotonic() - t0
    rate = sent / elapsed if elapsed > 0 else 0.0
    print(
        f"[producer] sent={sent} elapsed={elapsed:.2f}s rate={rate:.0f} msg/s",
        flush=True,
    )
    await p.stop()


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=10.0)
    args = ap.parse_args()
    ready = asyncio.Event()
    await asyncio.gather(run_consumer(ready, args.duration), run_producer(ready, args.duration))


if __name__ == "__main__":
    asyncio.run(main())