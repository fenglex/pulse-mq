"""可运行的端到端基准：producer → server → consumer 单机压测。

用法:
    python scripts/bench_simple.py
    python scripts/bench_simple.py --duration 10 --records-per-frame 1000 --serializer pyarrow --compression lz4
"""
from __future__ import annotations

import argparse
import asyncio
import statistics
import sys
import time

import pandas as pd

from pulsemq import Server
from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.protocol import frames
from pulsemq.protocol.msg_type import DataType

DATA_EP = "tcp://127.0.0.1:35555"
CTRL_EP = "tcp://127.0.0.1:35556"
ADMIN_BIND = "127.0.0.1:39090"
CREDS = {"pub": "secret", "sub": "secret"}


async def run_bench(duration: float, records_per_frame: int,
                    serializer: str, compression: str) -> None:
    print("=" * 64)
    print("PulseMQ 端到端基准 (producer -> server -> consumer)")
    print(f"  duration:           {duration}s")
    print(f"  records/frame:     {records_per_frame}")
    print(f"  serializer:        {serializer}")
    print(f"  compression:       {compression}")
    print("=" * 64)

    srv = Server(data_endpoint=DATA_EP, control_endpoint=CTRL_EP,
                 admin_endpoint=ADMIN_BIND, credentials=CREDS)
    await srv.start()

    latencies: list[float] = []
    frames_recv = 0
    records_recv = 0

    cons = ConsumerClient(DATA_EP, CTRL_EP, username="sub", password="secret")
    await cons.start()

    def on_msg(msg):
        nonlocal frames_recv, records_recv
        frames_recv += 1
        records_recv += msg.record_count
        latencies.append((time.time_ns() - msg.timestamp_ns) / 1_000_000)

    await cons.subscribe("bench.*", on_msg)

    prod = ProducerClient(DATA_EP, CTRL_EP, username="pub", password="secret")
    await prod.start()

    # 预构建负载
    if records_per_frame > 1:
        payload = pd.DataFrame({
            "seq": list(range(records_per_frame)),
            "val": [i * 1.5 for i in range(records_per_frame)],
        })
        dtype = DataType.DATAFRAME
        rc = records_per_frame
    else:
        payload = {"seq": 0, "val": 0.0}
        dtype = DataType.DICT
        rc = 1

    async def produce():
        end = time.monotonic() + duration
        while time.monotonic() < end:
            # publish() 使用默认 serializer/compression；自定义需直接 encode + send。
            frame = frames.encode(
                "bench.topic", payload,
                serializer=serializer, compression=compression,
                record_count=rc, data_type=dtype,
            )
            await prod._transport.send(b"", frame, role="data")

    t0 = time.monotonic()
    prod_task = asyncio.create_task(produce())
    await prod_task
    # 给 consumer 一点时间收尾
    await asyncio.sleep(1.0)
    elapsed = time.monotonic() - t0

    await prod.stop()
    await cons.stop()
    await srv.stop()

    print("-" * 64)
    print(f"  帧数:        {frames_recv:,}")
    print(f"  记录总数:    {records_recv:,}")
    print(f"  耗时:        {elapsed:.2f}s")
    if frames_recv:
        print(f"  帧吞吐:      {frames_recv / elapsed:,.0f} frames/s")
        print(f"  记录吞吐:    {records_recv / elapsed:,.0f} records/s")
        s = sorted(latencies)
        print(f"  帧延迟 p50:  {s[len(s) // 2]:.3f} ms")
        print(f"  帧延迟 p90:  {s[int(len(s) * 0.9)]:.3f} ms")
        print(f"  帧延迟 p99:  {s[min(len(s) - 1, int(len(s) * 0.99))]:.3f} ms")
        print(f"  帧延迟 max:  {s[-1]:.3f} ms")
    else:
        print("  无数据")


def main() -> None:
    p = argparse.ArgumentParser(description="PulseMQ 端到端基准")
    p.add_argument("--duration", type=float, default=5.0, help="压测秒数")
    p.add_argument("--records-per-frame", type=int, default=1,
                   help="每帧行数（>1 使用 DataFrame）")
    p.add_argument("--serializer", default="msgpack",
                   choices=["msgpack", "json", "pyarrow", "str", "bytes"])
    p.add_argument("--compression", default="none",
                   choices=["none", "snappy", "lz4", "zstd"])
    args = p.parse_args()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(run_bench(args.duration, args.records_per_frame,
                              args.serializer, args.compression))
    except KeyboardInterrupt:
        print("\n中断")


if __name__ == "__main__":
    main()
