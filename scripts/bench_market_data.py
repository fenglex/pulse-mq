#!/usr/bin/env python3
"""市场行情数据 100k 压测。

3 data_types (str/df-100/df-1000) × 4 compressions = 12 组合
每组合 100,000 条消息, 测吞吐/p50/p99。
数据: 模拟 A 股行情, 8 字段, 20 股票池, seed=42 可复现。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics
import subprocess
import sys
import time

sys.path.insert(0, "src")

from pulsemq.event_loop import install_event_loop

if sys.platform == "win32":
    install_event_loop(use_uvloop=False)

from pulsemq.client.async_client import PulseClient  # noqa: E402

# ---- 数据生成器 ----
STOCKS = [
    ("600000", "浦发银行"), ("000001", "平安银行"), ("300750", "宁德时代"),
    ("600519", "贵州茅台"), ("000858", "五粮液"), ("601318", "中国平安"),
    ("000333", "美的集团"), ("002594", "比亚迪"), ("600276", "恒瑞医药"),
    ("000568", "泸州老窖"), ("601012", "隆基绿能"), ("002475", "立讯精密"),
    ("600030", "中信证券"), ("601888", "中国中免"), ("000063", "中兴通讯"),
    ("002714", "牧原股份"), ("600887", "伊利股份"), ("601166", "兴业银行"),
    ("000002", "万科A"), ("600585", "海螺水泥"),
]

# 测试矩阵
DATA_TYPES = ["str", "df-100", "df-1000"]
COMPRESSIONS = ["none", "snappy", "lz4", "zstd"]
N_MESSAGES = 100_000

random.seed(42)


def gen_quote(idx: int) -> dict:
    """生成单条行情 quote, seed=42 保证可复现。"""
    code, name = STOCKS[idx % len(STOCKS)]
    base_price = 10.0 + (idx % 1000) * 0.1
    open_ = base_price + random.uniform(-0.5, 0.5)
    close = open_ + random.uniform(-0.3, 0.3)
    high = max(open_, close) + random.uniform(0, 0.2)
    low = min(open_, close) - random.uniform(0, 0.2)
    volume = random.randint(10000, 1000000)
    turnover = volume * (high + low) / 2
    return {
        "code": code, "name": name,
        "open": round(open_, 2), "high": round(high, 2),
        "low": round(low, 2), "close": round(close, 2),
        "volume": volume, "turnover": round(turnover, 2),
    }


def build_payload(data_type: str, idx: int):
    """构造与 data_type 匹配的负载。"""
    if data_type == "str":
        return json.dumps(gen_quote(idx), ensure_ascii=False)
    if data_type == "df-100":
        import pandas as pd
        rows = [gen_quote(idx * 100 + j) for j in range(100)]
        return pd.DataFrame(rows)
    if data_type == "df-1000":
        import pandas as pd
        rows = [gen_quote(idx * 1000 + j) for j in range(1000)]
        return pd.DataFrame(rows)
    raise ValueError(data_type)


def start_server(port: int) -> subprocess.Popen:
    """启 server_runner 子进程, 等 READY 后返回。"""
    proc = subprocess.Popen(
        [sys.executable, "scripts/test_server_runner.py", "--port", str(port)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            time.sleep(0.05)
            if proc.poll() is not None:
                stderr = proc.stderr.read() if proc.stderr else ""
                raise RuntimeError(
                    f"server_runner 提前退出 rc={proc.returncode}\nstderr: {stderr}"
                )
            continue
        if line.strip() == "READY":
            return proc
    proc.kill()
    raise TimeoutError("server_runner 启动超时")


def stop_server(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)


async def bench_one(port: int, data_type: str, comp: str, n_messages: int) -> dict:
    """跑单组合: 1 pub × 1 sub × N 条。"""
    address = f"tcp://localhost:{port}"
    xpub_address = f"tcp://localhost:{port + 1}"
    latencies: list[float] = []
    received: list = []

    # str 不传 format (走 StringSerializer); DataFrame 不传 format (走默认 json)
    fmt_arg = None
    topic = f"bench.{data_type}.{comp}"

    async def pub() -> None:
        async with PulseClient(
            address=address, xpub_address=xpub_address, auto_reconnect=False,
        ) as c:
            await asyncio.sleep(0.3)  # 等订阅就绪
            for i in range(n_messages):
                t0 = time.perf_counter()
                payload = build_payload(data_type, i)
                await c.publish(topic, payload, format=fmt_arg, compression=comp)
                latencies.append((time.perf_counter() - t0) * 1000)
                # 每 200 条 yield 1ms, 避免 sub 饥饿
                if i > 0 and i % 200 == 0:
                    await asyncio.sleep(0.001)

    async def sub() -> None:
        async with PulseClient(
            address=address, xpub_address=xpub_address, auto_reconnect=False,
        ) as c:
            try:
                async for msg in c.subscribe("bench.>"):
                    received.append(msg)
                    if len(received) >= n_messages:
                        return
                    if len(received) % 200 == 0:
                        await asyncio.sleep(0)
            except (asyncio.TimeoutError, Exception):
                pass  # 收完或 cleanup 异常忽略

    t0 = time.perf_counter()
    timeout_s = max(300, n_messages / 50)  # 50 msg/s 下界, 至少 5 分钟
    try:
        await asyncio.wait_for(
            asyncio.gather(pub(), sub()), timeout=timeout_s,
        )
    except asyncio.TimeoutError:
        raise
    elapsed = time.perf_counter() - t0

    if len(latencies) < n_messages:
        raise RuntimeError(f"{data_type}/{comp}: pub 仅记录 {len(latencies)}/{n_messages} 条延迟")
    if len(received) < n_messages:
        raise RuntimeError(f"{data_type}/{comp}: sub 仅收到 {len(received)}/{n_messages} 条消息")

    sorted_lat = sorted(latencies)
    return {
        "data_type": data_type,
        "compression": comp,
        "n": n_messages,
        "elapsed_s": round(elapsed, 3),
        "throughput_msg_s": round(n_messages / elapsed, 0),
        "p50_ms": round(statistics.median(sorted_lat), 3),
        "p99_ms": round(sorted_lat[int(n_messages * 0.99)], 3),
    }


def write_results_md(results: list[dict], output: str) -> None:
    """把结果写成 markdown 表格。"""
    os.makedirs(os.path.dirname(output) or ".", exist_ok=True)
    with open(output, "w", encoding="utf-8") as f:
        f.write("# PulseMQ 真实行情 100k 压测 — 原始数据\n\n")
        f.write("| data_type | compression | n | elapsed (s) | throughput (msg/s) | p50 (ms) | p99 (ms) |\n")
        f.write("|---|---|---|---|---|---|---|\n")
        for r in results:
            f.write(
                f"| {r['data_type']} | {r['compression']} | {r['n']} | "
                f"{r['elapsed_s']:.2f} | {r['throughput_msg_s']:.0f} | "
                f"{r['p50_ms']:.2f} | {r['p99_ms']:.2f} |\n"
            )
    print(f"\n结果写入 {output}", flush=True)


async def main() -> int:
    parser = argparse.ArgumentParser(description="PulseMQ 真实行情 100k 压测")
    parser.add_argument("--port", type=int, default=18000, help="server 端口 (默认 18000)")
    parser.add_argument("--n-messages", type=int, default=N_MESSAGES,
                        help=f"每组合消息数 (默认 {N_MESSAGES:,})")
    parser.add_argument("--output", type=str, default="docs/perf-market-data-results.md",
                        help="原始数据 markdown 输出")
    args = parser.parse_args()

    print(f"=== PulseMQ 真实行情 {args.n_messages} 压测 ===")
    print(f"  端口:   {args.port} / {args.port + 1}")
    print(f"  消息数: {args.n_messages:,} / 组合")
    print(f"  组合:   {len(DATA_TYPES)} data_types × {len(COMPRESSIONS)} compressions = {len(DATA_TYPES) * len(COMPRESSIONS)}")
    print(f"  数据:   8 字段 OHLCV 行情, 20 股票池, seed=42")
    print(f"  Batcher: 关闭 (batch_size=1)")
    print()

    proc = start_server(args.port)
    try:
        results: list[dict] = []
        for dt in DATA_TYPES:
            for comp in COMPRESSIONS:
                t_start = time.time()
                r = await bench_one(args.port, dt, comp, args.n_messages)
                results.append(r)
                elapsed_total = time.time() - t_start
                print(
                    f"  {dt:8s} {comp:6s} → {r['throughput_msg_s']:>8.0f} msg/s, "
                    f"p50={r['p50_ms']:>6.2f} ms, p99={r['p99_ms']:>6.2f} ms, "
                    f"({elapsed_total:.0f}s)",
                    flush=True,
                )
    finally:
        stop_server(proc)

    print()
    write_results_md(results, args.output)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
