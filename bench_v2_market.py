"""PulseMQ v2 基准测试：模拟行情数据 producer + consumer，吞吐量与延迟。

用法：
  uv run python bench_v2_market.py producer [--duration 10]
  uv run python bench_v2_market.py consumer [--duration 13]
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
        "symbol": sym,
        "price": round(p, 2),
        "volume": random.randint(1, 500),
        "bid": round(p - random.uniform(0, 0.05), 2),
        "ask": round(p + random.uniform(0, 0.05), 2),
        "seq": seq,
    }


async def run_producer(data_ep, ctrl_ep, user, pw, duration):
    p = ProducerClient(data_endpoint=data_ep, control_endpoint=ctrl_ep,
                       username=user, password=pw, client_id="bench-prod")
    await p.start()
    # 预热（建立连接、认证、REGISTER），避免计入基准。
    await asyncio.sleep(0.5)
    sent = 0
    seq = 0
    t0 = time.monotonic()
    while time.monotonic() - t0 < duration:
        await p.publish(f"market.stock.{random.choice(SYMBOLS)}", make_tick(seq))
        seq += 1
        sent += 1
        if sent % 500 == 0:
            await asyncio.sleep(0)  # 让出事件循环，处理控制面心跳/REPLY
    elapsed = time.monotonic() - t0
    rate = sent / elapsed if elapsed > 0 else 0.0
    print(f"[producer] sent={sent} elapsed={elapsed:.2f}s rate={rate:.0f} msg/s", flush=True)
    await p.stop()


async def run_consumer(data_ep, ctrl_ep, user, pw, duration):
    c = ConsumerClient(data_endpoint=data_ep, control_endpoint=ctrl_ep,
                       username=user, password=pw, client_id="bench-cons")
    await c.start()

    received_ts: list[float] = []
    latencies_ns: list[int] = []

    def on_msg(m) -> None:
        received_ts.append(time.monotonic())
        latencies_ns.append(time.time_ns() - m.timestamp_ns)

    await c.subscribe("market.stock.*", on_msg)
    # 运行 duration 秒；producer 可能已结束，多睡 1s 收尾。
    await asyncio.sleep(duration + 1.0)

    if not received_ts:
        print("[consumer] received=0", flush=True)
    else:
        t_first = received_ts[0]
        t_last = received_ts[-1]
        elapsed = max(t_last - t_first, 1e-9)
        rate = len(received_ts) / elapsed
        latencies_ns.sort()

        def pct(p: float) -> float:
            i = min(len(latencies_ns) - 1, int(p * len(latencies_ns)))
            return latencies_ns[i] / 1e6  # ns -> ms

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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("role", choices=["producer", "consumer"])
    ap.add_argument("--data", default="tcp://127.0.0.1:5555")
    ap.add_argument("--control", default="tcp://127.0.0.1:5556")
    ap.add_argument("--user", default="admin")
    ap.add_argument("--password", default="adminpw")
    ap.add_argument("--duration", type=float, default=10.0)
    args = ap.parse_args()
    if args.role == "producer":
        asyncio.run(run_producer(args.data, args.control, args.user, args.password, args.duration))
    else:
        # consumer 多跑一点以接收 producer 的尾部
        asyncio.run(run_consumer(args.data, args.control, args.user, args.password, args.duration + 3.0))


if __name__ == "__main__":
    main()