#!/usr/bin/env python3
"""并发压测: N pub × M sub × 16 组合。

测最大吞吐, 写 docs/bench-baseline.md 追加段。
"""
from __future__ import annotations

import argparse
import asyncio
import os
import subprocess
import sys
import time

sys.path.insert(0, "src")

from pulsemq.event_loop import install_event_loop

if sys.platform == "win32":
    install_event_loop(use_uvloop=False)

from pulsemq.client.async_client import PulseClient  # noqa: E402


def start_server(port: int) -> subprocess.Popen:
    """启动 test_server_runner 子进程,等待 READY 后返回。"""
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


def _ser_for(data_type: str) -> str:
    """data_type → publish() 的 format 参数。"""
    if data_type == "str":
        return "str"
    if data_type == "bytes":
        return "bytes"
    if data_type.startswith("df-"):
        return data_type[len("df-"):]  # msgpack / pyarrow
    raise ValueError(data_type)


async def bench_concurrent(
    port: int, n_pub: int, n_sub: int, n_per_pub: int, data_type: str, comp: str
) -> dict:
    """并发压测: N 个 publisher × M 个 subscriber, 测最大吞吐。"""
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    ser = _ser_for(data_type)
    # str/bytes 必须 format=None, DataFrame 走 ser (msgpack/pyarrow)
    fmt_arg = None if data_type in ("str", "bytes") else ser
    total = n_pub * n_per_pub
    received: list = []
    received_lock = asyncio.Lock()

    async def pub(idx: int) -> None:
        async with PulseClient(
            address=address, xpub_address=xpub, auto_reconnect=False
        ) as c:
            for i in range(n_per_pub):
                topic = f"bench.cc.{idx}"
                if data_type.startswith("df-"):
                    import pandas as pd

                    payload = pd.DataFrame({"i": [i]})
                    await c.publish(topic, payload, format=fmt_arg, compression=comp)
                elif data_type == "bytes":
                    await c.publish(topic, os.urandom(64), compression=comp)
                else:
                    payload = f"pub{idx}-msg{i}"
                    await c.publish(topic, payload, compression=comp)
                # 偶尔 yield, 避免事件循环饥饿
                if i > 0 and i % 200 == 0:
                    await asyncio.sleep(0)

    async def sub(idx: int) -> None:
        async with PulseClient(
            address=address, xpub_address=xpub, auto_reconnect=False
        ) as c:
            async for msg in c.subscribe("bench.cc.>"):
                async with received_lock:
                    received.append(msg)
                if len(received) >= total:
                    return
                # 偶尔 yield
                if len(received) % 200 == 0:
                    await asyncio.sleep(0)

    t0 = time.perf_counter()
    pub_tasks = [asyncio.create_task(pub(i)) for i in range(n_pub)]
    sub_tasks = [asyncio.create_task(sub(i)) for i in range(n_sub)]
    try:
        # 先给 sub 一点时间建立订阅
        await asyncio.sleep(0.3)
        await asyncio.wait_for(
            asyncio.gather(*pub_tasks, *sub_tasks), timeout=300
        )
    except asyncio.TimeoutError:
        # 让 sub 任务退出 gather
        for t in sub_tasks:
            t.cancel()
        raise
    finally:
        for t in pub_tasks + sub_tasks:
            if not t.done():
                t.cancel()
    elapsed = time.perf_counter() - t0
    # PUB-SUB 广播: 每个 sub 都会收到全部消息; 总收 = 收量 / n_sub 即为"系统吞吐"
    per_sub_received = len(received) // n_sub if n_sub > 0 else len(received)
    return {
        "n_pub": n_pub,
        "n_sub": n_sub,
        "n_per_pub": n_per_pub,
        "received_total": len(received),
        "received_per_sub": per_sub_received,
        "expected": total,
        "elapsed_s": round(elapsed, 2),
        "throughput_msg_s": round(per_sub_received / elapsed, 0),
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description="PulseMQ 并发压测")
    parser.add_argument("--port", type=int, default=16010, help="server 端口 (默认 16010)")
    parser.add_argument("--n-pub", type=int, default=4, help="publisher 数 (默认 4)")
    parser.add_argument("--n-sub", type=int, default=4, help="subscriber 数 (默认 4)")
    parser.add_argument(
        "--n-per-pub", type=int, default=5000, help="每 publisher 消息数 (默认 5000)"
    )
    parser.add_argument(
        "--data-type",
        type=str,
        default="str",
        choices=["str", "bytes", "df-msgpack", "df-pyarrow"],
    )
    parser.add_argument(
        "--compression",
        type=str,
        default="none",
        choices=["none", "snappy", "lz4", "zstd"],
    )
    parser.add_argument(
        "--output",
        type=str,
        default="docs/bench-baseline.md",
        help="markdown 输出路径 (默认 docs/bench-baseline.md)",
    )
    args = parser.parse_args()

    print("=== PulseMQ 并发压测 ===", flush=True)
    print(f"  端口:        {args.port}/{args.port + 1}", flush=True)
    print(f"  pub 数:      {args.n_pub}", flush=True)
    print(f"  sub 数:      {args.n_sub}", flush=True)
    print(f"  每 pub:      {args.n_per_pub}", flush=True)
    print(f"  data_type:   {args.data_type}", flush=True)
    print(f"  compression: {args.compression}", flush=True)
    print(flush=True)

    proc = start_server(args.port)
    try:
        result = await bench_concurrent(
            args.port,
            args.n_pub,
            args.n_sub,
            args.n_per_pub,
            args.data_type,
            args.compression,
        )
        print(f"  结果: {result}", flush=True)
        # 追加到 bench-baseline.md
        with open(args.output, "a", encoding="utf-8") as f:
            f.write(
                f"\n## 并发压测 ({args.n_pub} pub × {args.n_sub} sub, "
                f"{args.data_type}/{args.compression})\n\n"
            )
            f.write(
                f"- 收/发: {result['received_per_sub']}/{result['expected']} "
                f"(总收 {result['received_total']} = {result['n_sub']} sub × 广播)\n"
            )
            f.write(f"- 吞吐: {result['throughput_msg_s']:.0f} msg/s\n")
            f.write(f"- 耗时: {result['elapsed_s']}s\n")
        print(f"\n结果已追加到 {args.output}", flush=True)
        return 0 if result["received_per_sub"] >= result["expected"] else 1
    finally:
        stop_server(proc)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
