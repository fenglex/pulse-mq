#!/usr/bin/env python3
"""性能基线测试。

启 server + 1 pub + 1 sub, 跑 16 组合各 N 条消息, 测吞吐 / p50 / p99。
输出: 表格打印 + 写 docs/bench-baseline.md。
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

DATA_TYPES = ["str", "bytes", "df-msgpack", "df-pyarrow"]
COMPRESSIONS = ["none", "snappy", "lz4", "zstd"]
N_MESSAGES = 10_000


def start_server(port: int) -> subprocess.Popen:
    """启动 test_server_runner 子进程，等待 READY 后返回。"""
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
    """构造与 data_type 匹配的负载。"""
    if data_type == "str":
        return f"msg-{idx}-世界-🚀"
    if data_type == "bytes":
        return os.urandom(128)
    if data_type in ("df-msgpack", "df-pyarrow"):
        import pandas as pd

        return pd.DataFrame({"i": [idx], "s": [f"row{idx}"]})
    raise ValueError(data_type)


def _ser_for(data_type: str) -> str:
    """data_type → publish() 的 format 参数。"""
    if data_type == "str":
        return "str"
    if data_type == "bytes":
        return "bytes"
    if data_type.startswith("df-"):
        return data_type[len("df-"):]  # msgpack / pyarrow
    raise ValueError(data_type)


async def bench_one(port: int, data_type: str, comp: str, n_messages: int) -> dict:
    """单组合基准测试：1 pub 顺序发 N 条，1 sub 收 N 条，统计 p50/p99/吞吐。"""
    address = f"tcp://localhost:{port}"
    xpub_address = f"tcp://localhost:{port + 1}"
    latencies: list[float] = []
    ser = _ser_for(data_type)
    topic = f"bench.{data_type}.{comp}"
    # str/bytes 必须 format=None，DataFrame 走 ser（msgpack/pyarrow）
    fmt_arg = None if data_type in ("str", "bytes") else ser
    received: list = []
    recv_done = asyncio.Event()

    async def pub() -> None:
        async with PulseClient(
            address=address,
            xpub_address=xpub_address,
            auto_reconnect=False,
        ) as c:
            # 等订阅就绪
            await asyncio.sleep(0.3)
            for i in range(n_messages):
                payload = build_payload(data_type, i)
                t0 = time.perf_counter()
                await c.publish(topic, payload, format=fmt_arg, compression=comp)
                latencies.append((time.perf_counter() - t0) * 1000)
                # 每 200 条 yield 一次，避免事件循环饥饿 (避免 sub 收不到 100k 全集)
                if i > 0 and i % 200 == 0:
                    await asyncio.sleep(0.001)  # 1ms 让 sub 跟上

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
                # 100k 收完或 cleanup 阶段的异常, 忽略
                pass

    t0 = time.perf_counter()
    # 动态 timeout: 按 100 msg/s 下界, 至少 5 分钟
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


async def run_all(port: int, n_messages: int) -> list[dict]:
    """跑完 4 data_type × 4 compression = 16 组合。"""
    results: list[dict] = []
    for dt in DATA_TYPES:
        for comp in COMPRESSIONS:
            r = await bench_one(port, dt, comp, n_messages)
            results.append(r)
            print(
                f"  {dt:12s} {comp:6s} → {r['throughput_msg_s']:>8.0f} msg/s, "
                f"p50={r['p50_ms']:>6.2f} ms, p99={r['p99_ms']:>6.2f} ms",
                flush=True,
            )
    return results


def write_markdown(results: list[dict], output: str, n_messages: int) -> None:
    """把结果写成 markdown 表格。"""
    os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        f.write("# PulseMQ 性能基线\n\n")
        f.write(f"**测试环境**: Python {sys.version.split()[0]}, ")
        f.write(f"{sys.platform}\n\n")
        f.write(
            "**场景**: 1 pub + 1 sub（同一台机器，loopback），"
            "每组合 N 条消息，pub 顺序发布、sub 异步接收。\n\n"
        )
        f.write(f"**消息数**: 每组合 {n_messages} 条\n\n")
        f.write("**组合**: 4 data_type × 4 compression = 16 组合\n\n")
        f.write("| data_type | compression | throughput (msg/s) | p50 (ms) | p99 (ms) |\n")
        f.write("|-----------|-------------|-------------------|----------|----------|\n")
        for r in results:
            f.write(
                f"| {r['data_type']} | {r['compression']} | "
                f"{r['throughput_msg_s']:.0f} | {r['p50_ms']:.2f} | {r['p99_ms']:.2f} |\n"
            )
    print(f"\n结果写入 {output}", flush=True)


async def main() -> int:
    parser = argparse.ArgumentParser(description="PulseMQ 性能基线")
    parser.add_argument("--port", type=int, default=16000, help="server 端口 (默认 16000)")
    parser.add_argument(
        "--output", type=str, default="docs/bench-baseline.md", help="markdown 输出路径"
    )
    parser.add_argument(
        "--n-messages", type=int, default=N_MESSAGES, help=f"每组合消息数 (默认 {N_MESSAGES})"
    )
    args = parser.parse_args()

    print(f"=== PulseMQ 性能基线 ===")
    print(f"  端口:     {args.port}/{args.port + 1}")
    print(f"  每组合:   {args.n_messages} 条")
    print(f"  组合数:   {len(DATA_TYPES) * len(COMPRESSIONS)}")
    print(f"  输出:     {args.output}")
    print(flush=True)

    proc = start_server(args.port)
    try:
        results = await run_all(args.port, args.n_messages)
    finally:
        stop_server(proc)

    write_markdown(results, args.output, args.n_messages)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
