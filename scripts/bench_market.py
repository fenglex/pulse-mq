"""模拟行情性能测试。

Publisher: 3 个模拟行情 producer（沪市 A 股、深市 A 股、期货）
Subscriber: 2 个客户端，分别订阅不同 topic
运行 30 秒后报告延迟、吞吐量统计。

用法:
    python scripts/bench_market.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import signal
import sys
import time
import statistics

# 确保能 import pulsemq
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pulsemq.publisher import PulsePublisher
from pulsemq.subscriber import PulseSubscriber
from pulsemq.config import PublisherConfig

# ---------------------------------------------------------------------------
# 模拟行情数据生成器
# ---------------------------------------------------------------------------

# 沪市 A 股代码池
SH_CODES = [f"600{i:04d}" for i in range(1, 51)]  # 50 只股票
# 深市 A 股代码池
SZ_CODES = [f"000{i:04d}" for i in range(1, 51)]
# 期货合约
FUTURES = ["IF2401", "IC2401", "IH2401", "IM2401", "TF2401", "T2401"]

# 基础价格
_base_prices: dict[str, float] = {}


def _get_base(code: str) -> float:
    if code not in _base_prices:
        _base_prices[code] = random.uniform(5.0, 200.0)
    return _base_prices[code]


def _tick_price(base: float) -> float:
    """模拟价格波动 ±0.5%。"""
    return round(base * (1 + random.gauss(0, 0.005)), 3)


def _gen_sh_snapshot() -> list[dict]:
    """沪市快照：50 只股票，每只一条。"""
    return [
        {
            "code": code,
            "price": _tick_price(_get_base(code)),
            "volume": random.randint(100, 50000),
            "turnover": round(random.uniform(100000, 50000000), 2),
            "bid": [_tick_price(_get_base(code)) - i * 0.01 for i in range(5)],
            "ask": [_tick_price(_get_base(code)) + i * 0.01 for i in range(5)],
            "ts": time.time_ns(),
        }
        for code in SH_CODES
    ]


def _gen_sz_snapshot() -> list[dict]:
    """深市快照：50 只股票。"""
    return [
        {
            "code": code,
            "price": _tick_price(_get_base(code)),
            "volume": random.randint(100, 50000),
            "turnover": round(random.uniform(100000, 50000000), 2),
            "bid": [_tick_price(_get_base(code)) - i * 0.01 for i in range(5)],
            "ask": [_tick_price(_get_base(code)) + i * 0.01 for i in range(5)],
            "ts": time.time_ns(),
        }
        for code in SZ_CODES
    ]


def _gen_futures_tick() -> list[dict]:
    """期货逐笔：每批 20 条。"""
    return [
        {
            "symbol": random.choice(FUTURES),
            "price": round(random.uniform(3000, 4500), 2),
            "volume": random.randint(1, 50),
            "side": random.choice(["B", "S"]),
            "ts": time.time_ns(),
        }
        for _ in range(20)
    ]


# ---------------------------------------------------------------------------
# 性能统计
# ---------------------------------------------------------------------------


class BenchStats:
    """延迟与吞吐统计。"""

    def __init__(self, name: str) -> None:
        self.name = name
        self.latencies_ms: list[float] = []
        self.msg_count = 0
        self.record_count = 0
        self.bytes_received = 0
        self.start_time = 0.0
        self._lock = asyncio.Lock()

    def record(self, latency_ms: float, records: int, payload_bytes: int) -> None:
        self.latencies_ms.append(latency_ms)
        self.msg_count += 1
        self.record_count += records
        self.bytes_received += payload_bytes

    def report(self) -> str:
        if not self.latencies_ms:
            return f"[{self.name}] 无数据"
        elapsed = time.monotonic() - self.start_time
        lats = self.latencies_ms
        lines = [
            f"\n===== [{self.name}] 性能报告 =====",
            f"运行时间:         {elapsed:.1f}s",
            f"消息帧数:         {self.msg_count}",
            f"记录总数:         {self.record_count}",
            f"总接收字节:       {self.bytes_received / 1024 / 1024:.2f} MB",
            f"消息吞吐:         {self.msg_count / elapsed:.1f} msg/s",
            f"记录吞吐:         {self.record_count / elapsed:.1f} records/s",
            f"带宽:             {self.bytes_received / elapsed / 1024:.1f} KB/s",
            f"延迟 (p50):       {self._percentile(lats, 50):.3f} ms",
            f"延迟 (p90):       {self._percentile(lats, 90):.3f} ms",
            f"延迟 (p99):       {self._percentile(lats, 99):.3f} ms",
            f"延迟 (max):       {max(lats):.3f} ms",
            f"延迟 (min):       {min(lats):.3f} ms",
            f"延迟 (mean):      {statistics.mean(lats):.3f} ms",
        ]
        return "\n".join(lines)

    @staticmethod
    def _percentile(data: list[float], pct: float) -> float:
        s = sorted(data)
        idx = int(len(s) * pct / 100)
        return s[min(idx, len(s) - 1)]


# ---------------------------------------------------------------------------
# 主测试流程
# ---------------------------------------------------------------------------

BENCH_DURATION = 30  # 秒


async def run_bench() -> None:
    print("=" * 60)
    print("PulseMQ v2 模拟行情性能测试")
    print(f"  Producer: 3 (沪市50股快照 / 深市50股快照 / 期货逐笔)")
    print(f"  Subscriber: 2 (客户端A: 沪市+期货, 客户端B: 深市)")
    print(f"  持续时间: {BENCH_DURATION}s")
    print("=" * 60)

    port = 25555
    admin_port = 29090

    # ---- Publisher ----
    cfg = PublisherConfig(
        bind=f"tcp://127.0.0.1:{port}",
        admin_bind=f"127.0.0.1:{admin_port}",
        stats_db="sqlite://./bench_stats.sqlite",
    )
    pub = PulsePublisher(config=cfg)

    # 沪市 A 股快照：每 1 秒一批（50 条/批）
    @pub.producer(name="sh_stock_snapshot", interval=1.0, serializer="msgpack", compression="lz4")
    async def sh_producer():
        return _gen_sh_snapshot()

    # 深市 A 股快照：每 1 秒一批（50 条/批）
    @pub.producer(name="sz_stock_snapshot", interval=1.0, serializer="msgpack", compression="lz4")
    async def sz_producer():
        return _gen_sz_snapshot()

    # 期货逐笔：每 0.5 秒一批（20 条/批）
    @pub.producer(name="futures_tick", interval=0.5, serializer="msgpack", compression="lz4")
    async def futures_producer():
        return _gen_futures_tick()

    # 启动 publisher
    pub_task = asyncio.create_task(pub._run())
    await asyncio.sleep(1.0)  # 等 publisher 就绪

    # ---- Subscriber A: 沪市 + 期货 ----
    stats_a = BenchStats("客户端A (沪市+期货)")
    sub_a = PulseSubscriber(f"tcp://127.0.0.1:{port}")

    # ---- Subscriber B: 深市 ----
    stats_b = BenchStats("客户端B (深市)")
    sub_b = PulseSubscriber(f"tcp://127.0.0.1:{port}")

    await sub_a.connect()
    await sub_b.connect()

    async def subscriber_loop(sub: PulseSubscriber, stats: BenchStats, *topics: str):
        stats.start_time = time.monotonic()
        async for msg in sub.subscribe(*topics):
            now_ns = time.time_ns()
            latency_ms = (now_ns - msg.timestamp_ns) / 1_000_000
            stats.record(latency_ms, msg.record_count, len(msg.raw_payload))

    # 启动两个 subscriber
    task_a = asyncio.create_task(subscriber_loop(sub_a, stats_a, "sh_stock_snapshot", "futures_tick"))
    task_b = asyncio.create_task(subscriber_loop(sub_b, stats_b, "sz_stock_snapshot"))

    # ---- 运行指定时间 ----
    print(f"\n测试运行中... ({BENCH_DURATION}s)")
    for i in range(BENCH_DURATION):
        await asyncio.sleep(1.0)
        # 每 5 秒打印进度
        if (i + 1) % 5 == 0:
            print(f"  [{i + 1}s] A: {stats_a.msg_count} msgs / {stats_a.record_count} records | "
                  f"B: {stats_b.msg_count} msgs / {stats_b.record_count} records")

    # ---- 停止 ----
    print("\n测试结束，正在生成报告...")
    task_a.cancel()
    task_b.cancel()
    try:
        await asyncio.gather(task_a, task_b, return_exceptions=True)
    except Exception:
        pass

    await sub_a.close()
    await sub_b.close()

    pub._running = False
    await asyncio.sleep(0.5)
    pub_task.cancel()
    try:
        await pub_task
    except (asyncio.CancelledError, Exception):
        pass

    # ---- 报告 ----
    print(stats_a.report())
    print(stats_b.report())

    total_msgs = stats_a.msg_count + stats_b.msg_count
    total_records = stats_a.record_count + stats_b.record_count
    total_bytes = stats_a.bytes_received + stats_b.bytes_received
    all_lats = stats_a.latencies_ms + stats_b.latencies_ms

    print(f"\n===== [汇总] =====")
    print(f"总消息帧数:       {total_msgs}")
    print(f"总记录数:         {total_records}")
    print(f"总字节:           {total_bytes / 1024 / 1024:.2f} MB")
    if all_lats:
        print(f"全局延迟 (p50):   {BenchStats._percentile(all_lats, 50):.3f} ms")
        print(f"全局延迟 (p99):   {BenchStats._percentile(all_lats, 99):.3f} ms")
        print(f"全局延迟 (max):   {max(all_lats):.3f} ms")

    # 清理临时 SQLite
    try:
        os.unlink("./bench_stats.sqlite")
    except Exception:
        pass


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    # Windows 下设置事件循环策略以消除 ZMQ 警告
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(run_bench())
    except KeyboardInterrupt:
        print("\n中断")


if __name__ == "__main__":
    main()
