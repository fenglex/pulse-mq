"""PulseMQ 实时压测脚本。

启动方式:
    # 终端 1: 启动 Broker
    pulse-mq

    # 终端 2: 运行压测（默认 10w 条，2 个订阅者，全格式矩阵）
    python scripts/bench_live.py
    python scripts/bench_live.py --ser msgpack --comp lz4
    python scripts/bench_live.py --msgs 50000 --clients 3

覆盖的格式组合:
    msgpack x (none, snappy, lz4, zstd) + raw x none = 5 种
"""

from __future__ import annotations

import argparse
import asyncio
import random
import sys
import time
from dataclasses import dataclass, field

# Windows 下 pyzmq 需要 Selector 事件循环
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 将项目 src 加入 path
sys.path.insert(0, "src")

from pulsemq.client.async_client import PulseClient


# ---- 内嵌 A 股行情数据生成器 ----

_STOCK_POOL: list[tuple[str, str, float]] = [
    ("sh.600000", "浦发银行", 7.50), ("sh.600036", "招商银行", 35.20),
    ("sh.600519", "贵州茅台", 1700.00), ("sh.601318", "中国平安", 48.30),
    ("sh.601398", "工商银行", 5.10), ("sh.600276", "恒瑞医药", 42.80),
    ("sh.601012", "隆基绿能", 22.50), ("sh.600887", "伊利股份", 30.60),
    ("sh.601888", "中国中免", 85.40), ("sh.600030", "中信证券", 21.30),
    ("sz.000001", "平安银行", 11.20), ("sz.000333", "美的集团", 62.50),
    ("sz.000858", "五粮液", 155.00), ("sz.002415", "海康威视", 32.40),
    ("sz.000568", "泸州老窖", 210.00), ("sz.002714", "牧原股份", 42.00),
    ("sz.000725", "京东方A", 4.20), ("sz.002230", "科大讯飞", 55.80),
    ("sz.300750", "宁德时代", 195.00), ("sz.002594", "比亚迪", 260.00),
]


def _gen_snapshot() -> dict:
    """生成一条模拟 A 股行情快照。"""
    code, name, base = random.choice(_STOCK_POOL)
    pct = random.uniform(-0.02, 0.02)
    close = round(base * (1 + pct), 2)
    spread = base * 0.005
    open_ = round(base + random.uniform(-spread, spread), 2)
    high = round(max(open_, close) + random.uniform(0, spread), 2)
    low = round(min(open_, close) - random.uniform(0, spread), 2)
    tick = 0.01 if base < 10 else (0.05 if base < 100 else 0.10)
    return {
        "code": code, "name": name,
        "open": open_, "high": high, "low": low, "close": close,
        "volume": random.randint(100_000, 50_000_000),
        "amount": round(random.randint(100_000, 50_000_000) * close, 2),
        "ts": time.time(),
        "bid_p": [round(close - tick * (i + 1), 2) for i in range(5)],
        "bid_v": [random.randint(10, 5000) for _ in range(5)],
        "ask_p": [round(close + tick * (i + 1), 2) for i in range(5)],
        "ask_v": [random.randint(10, 5000) for _ in range(5)],
    }


# ---- 格式矩阵 ----

FORMAT_MATRIX: list[tuple[str, str]] = [
    ("msgpack", "none"),
    ("msgpack", "snappy"),
    ("msgpack", "lz4"),
    ("msgpack", "zstd"),
    ("raw", "none"),
]


# ---- 结果数据结构 ----

@dataclass
class ClientResult:
    client_id: int
    received: int = 0
    latency_samples: list[float] = field(default_factory=list)
    elapsed_s: float = 0.0

    @property
    def msg_per_sec(self) -> float:
        return self.received / self.elapsed_s if self.elapsed_s > 0 else 0

    def latency_p(self, p: float) -> float:
        if not self.latency_samples:
            return 0
        s = sorted(self.latency_samples)
        idx = min(round((len(s) - 1) * p / 100), len(s) - 1)
        return s[idx]


@dataclass
class ComboResult:
    ser: str
    comp: str
    total_msgs: int
    send_elapsed_s: float = 0.0
    client_results: list[ClientResult] = field(default_factory=list)

    @property
    def send_rate(self) -> float:
        return self.total_msgs / self.send_elapsed_s if self.send_elapsed_s > 0 else 0


# ---- 核心逻辑 ----

async def _run_subscriber(
    client_id: int,
    addr: str,
    xpub_addr: str,
    topic: str,
    expected: int,
    ser: str,
    comp: str,
    timeout_s: float,
    identity: bytes,
) -> ClientResult:
    """运行一个订阅客户端。"""
    result = ClientResult(client_id=client_id)
    client = PulseClient(
        address=addr, xpub_address=xpub_addr,
        serializer=ser, compressor=comp, identity=identity,
        heartbeat_interval=30.0, recv_timeout=2.0,
        auto_reconnect=False,
    )
    await client.connect()
    # 提升 SUB socket 缓冲区（默认 RCVHWM=1000 太小）
    client._sub.setsockopt(zmq.RCVHWM, 5_000_000)

    # 清空 DEALER 缓冲区中可能残留的消息
    try:
        while True:
            await asyncio.wait_for(client._dealer.recv_multipart(), timeout=0.05)
    except (asyncio.TimeoutError, Exception):
        pass
    try:
        start = time.monotonic()
        deadline = start + timeout_s
        async for msg in client.subscribe(topic):
            result.received += 1
            # 每 1000 条采样一次延迟
            if result.received % 1000 == 0 and msg.payload is not None:
                try:
                    data = msg.payload
                    if isinstance(data, dict) and "_send_ts" in data:
                        lat = (time.time() - data["_send_ts"]) * 1_000_000
                        result.latency_samples.append(lat)
                except Exception:
                    pass
            if result.received >= expected:
                break
            if time.monotonic() > deadline:
                break
        result.elapsed_s = time.monotonic() - start
    finally:
        await client.disconnect()
    return result


async def _run_publisher(
    addr: str, xpub_addr: str,
    topic: str, n_msgs: int, ser: str, comp: str,
) -> float:
    """运行发布者，返回发送耗时。"""
    client = PulseClient(
        address=addr, xpub_address=xpub_addr,
        serializer=ser, compressor=comp, identity=b"bench_pub",
        heartbeat_interval=30.0, auto_reconnect=False,
    )
    await client.connect()
    # 清空 DEALER 缓冲区残留消息
    try:
        while True:
            await asyncio.wait_for(client._dealer.recv_multipart(), timeout=0.05)
    except (asyncio.TimeoutError, Exception):
        pass
    try:
        start = time.monotonic()
        for i in range(n_msgs):
            if ser == "raw":
                data = b"market_binary_snapshot_data" * 10
            else:
                snap = _gen_snapshot()
                if i % 100 == 0:
                    snap["_send_ts"] = time.time()
                data = snap
            await client.publish(topic, data, record_count=1)
        return time.monotonic() - start
    finally:
        # 清空 DEALER 回复缓冲区
        try:
            while True:
                await asyncio.wait_for(client._dealer.recv_multipart(), timeout=0.05)
        except (asyncio.TimeoutError, Exception):
            pass
        await client.disconnect()


async def run_combo(
    ser: str, comp: str,
    addr: str, xpub_addr: str,
    n_msgs: int, n_clients: int, topic: str, timeout_s: float,
) -> ComboResult:
    """运行单个格式组合的压测。"""
    combo_topic = f"{topic}.{ser}.{comp}"
    result = ComboResult(ser=ser, comp=comp, total_msgs=n_msgs)

    # 1. 启动订阅者
    sub_tasks = []
    for i in range(n_clients):
        task = asyncio.create_task(_run_subscriber(
            client_id=i, addr=addr, xpub_addr=xpub_addr,
            topic=combo_topic, expected=n_msgs,
            ser=ser, comp=comp, timeout_s=timeout_s,
            identity=f"bench_sub_{i}".encode(),
        ))
        sub_tasks.append(task)

    await asyncio.sleep(0.5)  # 等待订阅生效

    # 2. 启动发布者
    print(f"  Publishing {n_msgs:,} msgs ({ser}+{comp})...", end=" ", flush=True)
    send_elapsed = await _run_publisher(
        addr=addr, xpub_addr=xpub_addr,
        topic=combo_topic, n_msgs=n_msgs, ser=ser, comp=comp,
    )
    result.send_elapsed_s = send_elapsed
    print(f"{result.send_rate:,.0f} msg/s")

    # 3. 等待订阅者完成
    done, pending = await asyncio.wait(sub_tasks, timeout=timeout_s)
    for task in pending:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    for task in done:
        try:
            result.client_results.append(task.result())
        except Exception:
            pass

    return result


# ---- 输出 ----

def print_header(args, combos):
    print()
    print("=" * 62)
    print("  PulseMQ Live Benchmark")
    print("=" * 62)
    print(f"  Broker:     {args.addr}")
    print(f"  Messages:   {args.msgs:,} per combo")
    print(f"  Clients:    {args.clients} subscribers")
    print(f"  Topic:      {args.topic}.<ser>.<comp>")
    print(f"  Combos:     {len(combos)} ({', '.join(s+'+'+c for s, c in combos)})")
    print("=" * 62)


def print_combo_result(result: ComboResult):
    print(f"\n  Publisher:  {result.send_rate:,.0f} msg/s "
          f"({result.total_msgs:,} msgs in {result.send_elapsed_s:.2f}s)")

    for cr in result.client_results:
        recv_rate = cr.msg_per_sec
        loss = (1 - cr.received / result.total_msgs) * 100 \
            if result.total_msgs > 0 else 0
        line = (f"  Client {cr.client_id}: "
                f"{recv_rate:,.0f} msg/s | "
                f"recv {cr.received:,}/{result.total_msgs:,} | "
                f"loss {loss:.1f}%")
        if cr.latency_samples:
            p50 = cr.latency_p(50)
            p99 = cr.latency_p(99)
            line += f" | P50 {p50:.0f}us P99 {p99:.0f}us"
        print(line)


def print_summary(all_results: list[ComboResult], n_clients: int):
    print()
    print("=" * 62)
    print("  Summary")
    print("=" * 62)
    print(f"  {'Format':<20} {'Send msg/s':>12} {'Recv msg/s':>12} {'Loss':>8}")
    print("  " + "-" * 54)

    for r in all_results:
        avg_recv = sum(cr.msg_per_sec for cr in r.client_results) / \
            max(len(r.client_results), 1)
        total_recv = sum(cr.received for cr in r.client_results)
        total_expected = r.total_msgs * n_clients
        loss = (1 - total_recv / total_expected) * 100 if total_expected > 0 else 0
        label = f"{r.ser}+{r.comp}"
        print(f"  {label:<20} {r.send_rate:>12,.0f} {avg_recv:>12,.0f} {loss:>7.1f}%")

    print("=" * 62)


# ---- CLI ----

def parse_args():
    parser = argparse.ArgumentParser(description="PulseMQ 实时压测脚本")
    parser.add_argument("--addr", default="tcp://localhost:5555")
    parser.add_argument("--xpub", default="tcp://localhost:5556")
    parser.add_argument("--msgs", type=int, default=100_000)
    parser.add_argument("--clients", type=int, default=2)
    parser.add_argument("--topic", default="bench")
    parser.add_argument("--ser", default="all", choices=["msgpack", "raw", "all"])
    parser.add_argument("--comp", default="all",
                        choices=["none", "snappy", "lz4", "zstd", "all"])
    parser.add_argument("--timeout", type=float, default=60)
    return parser.parse_args()


def build_matrix(ser: str, comp: str) -> list[tuple[str, str]]:
    return [
        (s, c) for s, c in FORMAT_MATRIX
        if (ser == "all" or s == ser) and (comp == "all" or c == comp)
    ]


async def main():
    args = parse_args()
    matrix = build_matrix(args.ser, args.comp)
    if not matrix:
        print("Error: no matching format combo")
        sys.exit(1)

    print_header(args, matrix)

    all_results: list[ComboResult] = []
    for idx, (ser, comp) in enumerate(matrix, 1):
        print(f"\n--- [{idx}/{len(matrix)}] {ser} + {comp} ---")
        try:
            result = await run_combo(
                ser=ser, comp=comp,
                addr=args.addr, xpub_addr=args.xpub,
                n_msgs=args.msgs, n_clients=args.clients,
                topic=args.topic, timeout_s=args.timeout,
            )
            all_results.append(result)
            print_combo_result(result)
        except Exception as e:
            print(f"  ERROR: {e}")
        # 组合间等待，让服务端排空残留消息和连接
        if idx < len(matrix):
            await asyncio.sleep(2.0)

    print_summary(all_results, args.clients)


if __name__ == "__main__":
    asyncio.run(main())
