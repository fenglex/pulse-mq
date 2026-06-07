"""Cython df-msgpack 100k 端到端压测 (启用 Batcher, 只跑 df-msgpack 4 组合).

用于对比 Cython 优化前后, df-msgpack 路径的吞吐.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import subprocess
import sys
import time

sys.path.insert(0, "src")

from pulsemq.event_loop import install_event_loop

if sys.platform == "win32":
    install_event_loop(use_uvloop=False)

from pulsemq.client.async_client import PulseClient  # noqa: E402

from pulsemq.serialization._df_msgpack_loader import is_using_cython  # noqa: E402

DATA_TYPES = ["df-msgpack"]
COMPRESSIONS = ["none", "snappy", "lz4", "zstd"]
N_MESSAGES = 100_000
BATCH_SIZE = 10
BATCH_INTERVAL_MS = 10.0
BATCH_MAX_WAIT_MS = 50.0


def start_server(port: int) -> subprocess.Popen:
    proc = subprocess.Popen(
        [sys.executable, "scripts/test_server_runner.py", "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                stderr = proc.stderr.read() if proc.stderr else ""
                raise RuntimeError(
                    f"server_runner 提前退出 (rc={proc.returncode})\nstderr: {stderr}"
                )
            time.sleep(0.05)
            continue
        if line.strip() == "READY":
            return proc
    proc.kill()
    raise RuntimeError("server not ready")


def stop_server(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)


def build_payload(data_type: str, idx: int):
    import pandas as pd
    return pd.DataFrame({"i": [idx], "s": [f"row{idx}"]})


async def bench_one(port: int, data_type: str, comp: str, n_messages: int) -> dict:
    address = f"tcp://localhost:{port}"
    xpub_address = f"tcp://localhost:{port + 1}"
    latencies: list[float] = []
    ser = "msgpack"  # df-msgpack
    topic = f"bench.{data_type}.{comp}"
    fmt_arg = ser
    received: list = []
    recv_done = asyncio.Event()

    async def pub() -> None:
        async with PulseClient(
            address=address,
            xpub_address=xpub_address,
            auto_reconnect=False,
            batch_size=BATCH_SIZE,
            batch_interval_ms=BATCH_INTERVAL_MS,
            batch_max_wait_ms=BATCH_MAX_WAIT_MS,
        ) as c:
            await asyncio.sleep(0.3)
            for i in range(n_messages):
                payload = build_payload(data_type, i)
                t0 = time.perf_counter()
                await c.publish(topic, payload, format=fmt_arg, compression=comp)
                latencies.append((time.perf_counter() - t0) * 1000)
                if i > 0 and i % 200 == 0:
                    await asyncio.sleep(0.001)

    async def sub() -> None:
        async with PulseClient(
            address=address,
            xpub_address=xpub_address,
            auto_reconnect=False,
        ) as c:
            try:
                async for msg in c.subscribe("bench.>"):
                    received.append(msg)
                    if len(received) >= n_messages:
                        recv_done.set()
                        return
                    if len(received) % 200 == 0:
                        await asyncio.sleep(0)
            except (asyncio.TimeoutError, Exception):
                pass

    t0 = time.perf_counter()
    timeout_s = max(300, n_messages / 100)
    try:
        await asyncio.wait_for(
            asyncio.gather(pub(), sub()), timeout=timeout_s
        )
    except asyncio.TimeoutError:
        recv_done.set()
        raise
    elapsed = time.perf_counter() - t0

    if len(latencies) < n_messages:
        raise RuntimeError(
            f"{data_type}/{comp}: pub 仅记录 {len(latencies)}/{n_messages} 条延迟"
        )
    if len(received) < n_messages:
        raise RuntimeError(
            f"{data_type}/{comp}: sub 仅收到 {len(received)}/{n_messages} 条消息"
        )

    sorted_lat = sorted(latencies)
    p50 = statistics.median(sorted_lat)
    p99 = sorted_lat[int(n_messages * 0.99)]
    return {
        "data_type": data_type,
        "compression": comp,
        "n": n_messages,
        "elapsed_s": round(elapsed, 3),
        "throughput_msg_s": round(n_messages / elapsed, 0),
        "p50_ms": round(p50, 3),
        "p99_ms": round(p99, 3),
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description="Cython df-msgpack 100k 压测 (Batcher)")
    parser.add_argument("--port", type=int, default=17000)
    parser.add_argument(
        "--output", type=str, default="docs/perf-100k-cython-data.md"
    )
    parser.add_argument("--n-messages", type=int, default=100_000)
    args = parser.parse_args()

    print("=== Cython df-msgpack 100k 压测 (Batcher) ===", flush=True)
    print(f"  端口:     {args.port}/{args.port + 1}", flush=True)
    print(f"  消息数:   {args.n_messages}", flush=True)
    print(f"  Batcher:  size={BATCH_SIZE}, interval={BATCH_INTERVAL_MS}ms", flush=True)
    print(f"  Cython:   {is_using_cython()}", flush=True)
    print(f"  输出:     {args.output}", flush=True)
    print(flush=True)

    proc = start_server(args.port)
    try:
        results = []
        for comp in COMPRESSIONS:
            r = await bench_one(args.port, "df-msgpack", comp, args.n_messages)
            results.append(r)
            print(
                f"  df-msgpack {comp:6s} → {r['throughput_msg_s']:>8.0f} msg/s, "
                f"p50={r['p50_ms']:>6.2f} ms, p99={r['p99_ms']:>6.2f} ms",
                flush=True,
            )
    finally:
        stop_server(proc)

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write("# Cython df-msgpack 100k 压测结果 (Batcher 启用)\n\n")
        f.write(f"**Cython 启用**: {is_using_cython()}\n\n")
        f.write(
            f"**场景**: 1 pub + 1 sub（同一台机器，loopback），"
            f"df-msgpack 4 压缩组合, 每组合 {args.n_messages} 条\n\n"
        )
        f.write(
            f"**Batcher**: size={BATCH_SIZE}, interval={BATCH_INTERVAL_MS}ms, "
            f"max_wait={BATCH_MAX_WAIT_MS}ms\n\n"
        )
        f.write("| data_type | compression | throughput (msg/s) | p50 (ms) | p99 (ms) |\n")
        f.write("|-----------|-------------|-------------------|----------|----------|\n")
        for r in results:
            f.write(
                f"| {r['data_type']} | {r['compression']} | "
                f"{r['throughput_msg_s']:.0f} | {r['p50_ms']:.2f} | {r['p99_ms']:.2f} |\n"
            )
    print(f"\n结果写入 {args.output}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))

