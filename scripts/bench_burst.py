"""Burst producer 极限性能测试。

测试 pub→sub 管道的极限吞吐量。
每次回调返回一批消息（BATCH_SIZE 条），减少逐帧开销。

用法:
    python scripts/bench_burst.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pulsemq.publisher import PulsePublisher
from pulsemq.subscriber import PulseSubscriber
from pulsemq.config import PublisherConfig

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

TOTAL_RECORDS = 100_000     # 总记录数
BATCH_SIZE = 1_000          # 每批记录数（每次回调返回多少条）
PAYLOAD_SIZE = 64           # 每条记录中随机数据字节数

# 预生成随机数据，避免 os.urandom 在热路径上
_RANDOM_DATA = os.urandom(PAYLOAD_SIZE).hex()[:PAYLOAD_SIZE]


# ---------------------------------------------------------------------------
# 延迟统计
# ---------------------------------------------------------------------------

class LatencyStats:
    def __init__(self, name: str) -> None:
        self.name = name
        self.frames = 0          # 接收帧数
        self.records = 0         # 接收记录数
        self.bytes_recv = 0
        self.latencies: list[float] = []  # 帧级延迟
        self.t_start = 0.0

    def record(self, lat_ms: float, record_count: int, nbytes: int) -> None:
        self.frames += 1
        self.records += record_count
        self.bytes_recv += nbytes
        self.latencies.append(lat_ms)

    def report(self) -> str:
        elapsed = time.monotonic() - self.t_start
        if not self.latencies:
            return f"  [{self.name}] 无数据"
        s = sorted(self.latencies)
        lines = [
            f"  [{self.name}]",
            f"    帧数:         {self.frames:,}",
            f"    记录总数:     {self.records:,}",
            f"    接收字节:     {self.bytes_recv / 1024 / 1024:.2f} MB",
            f"    帧吞吐:       {self.frames / elapsed:,.0f} frames/s",
            f"    记录吞吐:     {self.records / elapsed:,.0f} records/s",
            f"    帧延迟 p50:   {s[len(s)//2]:.3f} ms",
            f"    帧延迟 p90:   {s[int(len(s)*0.9)]:.3f} ms",
            f"    帧延迟 p99:   {s[int(len(s)*0.99)]:.3f} ms",
            f"    帧延迟 max:   {s[-1]:.3f} ms",
        ]
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

async def run_bench() -> None:
    total_batches = TOTAL_RECORDS // BATCH_SIZE
    print("=" * 60)
    print("PulseMQ v2 Burst 极限性能测试")
    print(f"  总记录数:     {TOTAL_RECORDS:,}")
    print(f"  每批大小:     {BATCH_SIZE:,}")
    print(f"  总批数:       {total_batches}")
    print(f"  Payload/条:   {PAYLOAD_SIZE} bytes")
    print(f"  Subscriber:   2 个客户端")
    print("=" * 60)

    port = 35555
    aport = 39090

    go = asyncio.Event()
    sent_batches = 0

    async def burst_callback():
        nonlocal sent_batches
        await go.wait()
        if sent_batches >= total_batches:
            return None
        sent_batches += 1
        # 返回一批消息（list[dict]）
        return [
            {"seq": sent_batches * BATCH_SIZE + i, "data": _RANDOM_DATA, "ts": time.time_ns()}
            for i in range(BATCH_SIZE)
        ]

    # ---- 创建 Publisher ----
    pub = PulsePublisher(config=PublisherConfig(
        bind=f"tcp://127.0.0.1:{port}",
        admin_bind=f"127.0.0.1:{aport}",
        stats_db="sqlite://:memory:",
    ))

    pub._producer_mgr.register_burst(
        callback=burst_callback,
        name="burst_topic",
        serializer="msgpack",
        compression="none",
    )
    pub._buffers.get_or_create("burst_topic", 200_000)

    pub_task = asyncio.create_task(pub._run())
    await asyncio.sleep(0.5)

    # ---- 启动 Subscriber ----
    sub_a = PulseSubscriber(f"tcp://127.0.0.1:{port}")
    sub_b = PulseSubscriber(f"tcp://127.0.0.1:{port}")
    await sub_a.connect()
    await sub_b.connect()

    stats_a = LatencyStats("Subscriber A")
    stats_b = LatencyStats("Subscriber B")

    async def collect(sub, stats, target_records):
        stats.t_start = time.monotonic()
        async for msg in sub.subscribe("burst_topic"):
            now_ns = time.time_ns()
            lat_ms = (now_ns - msg.timestamp_ns) / 1_000_000
            stats.record(lat_ms, msg.record_count, len(msg.raw_payload))
            if stats.records >= target_records:
                break

    task_a = asyncio.create_task(collect(sub_a, stats_a, TOTAL_RECORDS))
    task_b = asyncio.create_task(collect(sub_b, stats_b, TOTAL_RECORDS))

    await asyncio.sleep(0.3)

    # ---- 开始 ----
    print(f"\n开始发送 {total_batches} 批 x {BATCH_SIZE} 条/批 ...")
    t_start = time.monotonic()
    go.set()

    done, pending = await asyncio.wait(
        [task_a, task_b], timeout=120.0
    )
    t_end = time.monotonic()
    elapsed = t_end - t_start

    for t in pending:
        t.cancel()
    try:
        await asyncio.gather(*pending, return_exceptions=True)
    except Exception:
        pass

    pub._running = False
    await asyncio.sleep(0.3)
    pub_task.cancel()
    try:
        await pub_task
    except (asyncio.CancelledError, Exception):
        pass

    await sub_a.close()
    await sub_b.close()

    # ---- 报告 ----
    total_sent = sent_batches * BATCH_SIZE
    print(f"\n===== [发送端] =====")
    print(f"  总发送:       {total_sent:,} 记录 ({sent_batches} 批)")
    print(f"  耗时:         {elapsed:.2f}s")
    print(f"  发送吞吐:     {total_sent / elapsed:,.0f} records/s")
    print(f"  批发送吞吐:   {sent_batches / elapsed:,.0f} batches/s")

    print()
    print(stats_a.report())
    print()
    print(stats_b.report())

    all_lats = stats_a.latencies + stats_b.latencies
    total_recv = stats_a.records + stats_b.records
    if all_lats:
        s = sorted(all_lats)
        print(f"\n===== [全局汇总] =====")
        print(f"  总接收:       {total_recv:,} 记录")
        print(f"  记录吞吐:     {total_recv / elapsed:,.0f} records/s")
        print(f"  帧延迟 p50:   {s[len(s)//2]:.3f} ms")
        print(f"  帧延迟 p99:   {s[int(len(s)*0.99)]:.3f} ms")
        print(f"  帧延迟 max:   {s[-1]:.3f} ms")


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(run_bench())
    except KeyboardInterrupt:
        print("\n中断")


if __name__ == "__main__":
    main()
