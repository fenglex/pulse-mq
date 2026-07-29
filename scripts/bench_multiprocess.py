"""PulseMQ 多进程基准测试：生产端 / 服务端 / 消费端各自独立进程。

覆盖全部合法的「消息类型 × 序列化器 × 压缩格式」组合（28 组），
记录每组的端到端延迟分布（p50/p90/p99/max/avg/min）。

用法:
    uv run python scripts/bench_multiprocess.py                  # 跑全部 28 组合
    uv run python scripts/bench_multiprocess.py --count 3000     # 每组合发送帧数（默认 3000）
    uv run python scripts/bench_multiprocess.py --data-type dict # 只测指定数据类型
    uv run python scripts/bench_multiprocess.py --timeout 60     # consumer 等待超时（默认 30s）

结果: 写入项目根 bench_multiprocess_results.md。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

import pulsemq
from pulsemq import Server
from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.config import ServerConfig
from pulsemq.protocol import frames
from pulsemq.protocol.msg_type import DataType

# 固定端口（避免与开发环境 5555/5556/9090 冲突）
DATA_EP = "tcp://127.0.0.1:45555"
CTRL_EP = "tcp://127.0.0.1:45556"
ADMIN_BIND = "127.0.0.1:49090"
CREDS = {"pub": "s", "sub": "s"}

RESULT_FILE = Path(__file__).resolve().parent.parent / "bench_multiprocess_results.md"

# 合法组合矩阵（28 组）= 数据类型 × 序列化器 × 压缩格式
# 序列化器与数据类型的对应关系来自 frames._SERIALIZER_RULES
_SERIALIZER_PAIRS = [
    ("dict", "msgpack"),
    ("dict", "json"),
    ("dataframe", "msgpack"),
    ("dataframe", "json"),
    ("dataframe", "pyarrow"),
    ("str", "str"),
    ("bytes", "bytes"),
]
COMPRESSIONS = ["none", "snappy", "lz4", "zstd"]
MATRIX = [(dt, ser, comp) for (dt, ser) in _SERIALIZER_PAIRS for comp in COMPRESSIONS]


def _pct(sorted_list: list[float], pct: float) -> float:
    """已排序列表取分位（线性索引，足够基准用途）。"""
    if not sorted_list:
        return 0.0
    idx = min(len(sorted_list) - 1, int(len(sorted_list) * pct))
    return sorted_list[idx]


def _make_payload(data_type: str) -> tuple:
    """构造测试数据，返回 (payload, DataType, record_count)。"""
    if data_type == "dict":
        return {"seq": 0, "val": 1.5, "sym": "AAPL", "ok": True}, DataType.DICT, 1
    if data_type == "dataframe":
        df = pd.DataFrame({
            "seq": list(range(100)),
            "val": [i * 1.5 for i in range(100)],
            "sym": ["AAPL"] * 100,
        })
        return df, DataType.DATAFRAME, 100
    if data_type == "str":
        return "hello pulse-mq benchmark " * 10, DataType.STR, 1
    if data_type == "bytes":
        return b"hello pulse-mq benchmark " * 10, DataType.BYTES, 1
    raise ValueError(f"未知 data_type: {data_type}")


# ---------------------------------------------------------------------------
# 角色: server — 独立进程，启动后运行直到被编排器终止
# ---------------------------------------------------------------------------

async def role_server() -> None:
    """启动服务端，输出 SERVER_READY 后运行直到被编排器终止。"""
    cfg = ServerConfig(
        data_endpoint=DATA_EP,
        control_endpoint=CTRL_EP,
        admin_endpoint=ADMIN_BIND,
        sndhwm=100_000,   # 放大 HWM，避免基准期间背压丢消息
        rcvhwm=100_000,
        admin_token="bench-token",
        stats_db="sqlite:///./bench_stats.sqlite",
    )
    srv = Server(
        data_endpoint=DATA_EP,
        control_endpoint=CTRL_EP,
        admin_endpoint=ADMIN_BIND,
        credentials=CREDS,
        admin_token="bench-token",
        config=cfg,
    )
    await srv.start()
    print("SERVER_READY", flush=True)
    await asyncio.Event().wait()  # 运行直到被编排器 terminate


# ---------------------------------------------------------------------------
# 角色: consumer — 独立进程，收够 count 条消息或超时后输出 JSON 结果
# ---------------------------------------------------------------------------

async def role_consumer(data_type: str, serializer: str, compression: str,
                        count: int, timeout: float) -> None:
    """启动消费端，订阅 topic，收够 count 条消息或超时后输出 JSON 结果行。"""
    cons = ConsumerClient(DATA_EP, CTRL_EP, "sub", "s")
    await cons.start()

    latencies: list[float] = []
    frames_recv = 0
    records_recv = 0
    # 接收吞吐量计时：第一条与最后一条消息的 monotonic 时间
    t_first: float | None = None
    t_last: float | None = None

    def on_msg(msg):
        nonlocal frames_recv, records_recv, t_first, t_last
        frames_recv += 1
        records_recv += msg.record_count
        now = time.monotonic()
        if t_first is None:
            t_first = now
        t_last = now
        # 端到端延迟 = 消费端墙钟 - 帧内时间戳（encode 时写入）
        latencies.append((time.time_ns() - msg.timestamp_ns) / 1_000_000)

    await cons.subscribe("bench.*", on_msg)
    print("READY", flush=True)

    # 等待收够消息或超时
    deadline = time.monotonic() + timeout
    while frames_recv < count and time.monotonic() < deadline:
        await asyncio.sleep(0.05)

    await cons.stop()

    latencies.sort()
    recv_elapsed = (t_last - t_first) if t_first is not None and t_last is not None else 0.0
    recv_fps = round(frames_recv / recv_elapsed) if recv_elapsed > 0 else 0
    result = {
        "data_type": data_type,
        "serializer": serializer,
        "compression": compression,
        "frames_sent": count,
        "frames_recv": frames_recv,
        "records_recv": records_recv,
        "recv_fps": recv_fps,
        "recv_elapsed_s": round(recv_elapsed, 3),
        "p50_ms": round(_pct(latencies, 0.50), 3),
        "p90_ms": round(_pct(latencies, 0.90), 3),
        "p99_ms": round(_pct(latencies, 0.99), 3),
        "max_ms": round(latencies[-1] if latencies else 0, 3),
        "avg_ms": round(sum(latencies) / len(latencies), 3) if latencies else 0,
        "min_ms": round(latencies[0] if latencies else 0, 3),
    }
    print("RESULT " + json.dumps(result, ensure_ascii=False), flush=True)


# ---------------------------------------------------------------------------
# 角色: producer — 独立进程，发送 count 条消息后退出
# ---------------------------------------------------------------------------

async def role_producer(data_type: str, serializer: str, compression: str,
                        count: int) -> None:
    """启动生产端，发送 count 条消息后输出 JSON 吞吐结果并退出。"""
    prod = ProducerClient(DATA_EP, CTRL_EP, "pub", "s")
    await prod.start()

    payload, dtype, rc = _make_payload(data_type)

    t_start = time.monotonic()
    for i in range(count):
        # 每帧重新 encode 以获取新的 timestamp_ns（延迟测量基准）
        frame = frames.encode(
            "bench.topic", payload,
            serializer=serializer,
            compression=compression,
            data_type=dtype,
            record_count=rc,
        )
        await prod._transport.send(b"", frame, role="consumer")
        if i % 100 == 0:
            await asyncio.sleep(0)  # 让出事件循环给 ZMQ IO
    t_end = time.monotonic()

    await prod.stop()

    elapsed = t_end - t_start
    send_fps = round(count / elapsed) if elapsed > 0 else 0
    print(json.dumps({"send_fps": send_fps, "send_elapsed_s": round(elapsed, 3)},
                     ensure_ascii=False), flush=True)


# ---------------------------------------------------------------------------
# 编排器 — 启动子进程，逐组合测试，收集结果
# ---------------------------------------------------------------------------

def run_orchestrator(count: int, timeout: float,
                     data_type_filter: str | None = None) -> None:
    """启动 server/consumer/producer 子进程，逐组合测试，收集结果并生成报告。"""
    env = dict(os.environ, PYTHONUNBUFFERED="1")
    py = [sys.executable, str(Path(__file__).resolve())]

    matrix = [m for m in MATRIX
              if data_type_filter is None or m[0] == data_type_filter]
    results: list[dict] = []
    total = len(matrix)

    # 1. 启动 server 子进程
    print("[orchestrator] 启动 server...", flush=True)
    server_proc = subprocess.Popen(
        py + ["--role", "server"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,  # 丢弃 server 日志（loguru 输出到 stderr）
        env=env,
        text=True,
    )
    # 等待 server 就绪信号
    ready_line = server_proc.stdout.readline()
    if "SERVER_READY" not in ready_line:
        print(f"[orchestrator] server 启动失败: {ready_line.strip()}", flush=True)
        server_proc.kill()
        return
    print("[orchestrator] server 就绪", flush=True)

    try:
        for i, (dtype, ser, comp) in enumerate(matrix):
            print(f"\n[{i+1}/{total}] {dtype}/{ser}/{comp} (count={count})...",
                  flush=True)

            # 2. 启动 consumer，等待 READY 信号
            cons_proc = subprocess.Popen(
                py + ["--role", "consumer",
                       "--data-type", dtype,
                       "--serializer", ser,
                       "--compression", comp,
                       "--count", str(count),
                       "--timeout", str(timeout)],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                env=env,
                text=True,
            )
            ready = cons_proc.stdout.readline()
            if "READY" not in ready:
                print(f"  consumer 启动失败: {ready.strip()}", flush=True)
                cons_proc.kill()
                continue

            # 3. 启动 producer，等发送完毕退出
            prod_proc = subprocess.Popen(
                py + ["--role", "producer",
                       "--data-type", dtype,
                       "--serializer", ser,
                       "--compression", comp,
                       "--count", str(count)],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                env=env,
                text=True,
            )
            try:
                prod_proc.wait(timeout=120)
            except subprocess.TimeoutExpired:
                prod_proc.kill()
                prod_proc.wait()

            # 读取 producer 的吞吐量 JSON
            prod_result: dict = {}
            if prod_proc.stdout:
                for line in prod_proc.stdout.read().strip().splitlines():
                    if line.startswith("{"):
                        prod_result = json.loads(line)
                        break

            # 4. 等 consumer 退出（收够消息或超时后自动退出）
            try:
                cons_proc.wait(timeout=timeout + 10)
            except subprocess.TimeoutExpired:
                cons_proc.kill()
                cons_proc.wait()

            # 读取 consumer stdout 剩余内容，解析 RESULT JSON
            remaining = cons_proc.stdout.read() if cons_proc.stdout else ""
            result = None
            for line in remaining.strip().splitlines():
                if line.startswith("RESULT "):
                    result = json.loads(line[7:])
                    break

            if result:
                result["send_fps"] = prod_result.get("send_fps", 0)
                result["send_elapsed_s"] = prod_result.get("send_elapsed_s", 0)
                results.append(result)
                print(f"  recv={result['frames_recv']}/{count}, "
                      f"send={result['send_fps']:,} f/s, recv={result['recv_fps']:,} f/s, "
                      f"p50={result['p50_ms']}ms, p99={result['p99_ms']}ms, "
                      f"max={result['max_ms']}ms", flush=True)
            else:
                print("  consumer 无结果输出", flush=True)

    finally:
        # 5. 停止 server
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server_proc.kill()
            server_proc.wait()
        print("\n[orchestrator] server 已停止", flush=True)

    # 6. 生成报告
    write_report(results, count)


def write_report(results: list[dict], count: int) -> None:
    """生成 Markdown 报告。"""
    lines: list[str] = []
    lines.append("# PulseMQ 多进程基准报告")
    lines.append("")
    lines.append(f"- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- Python: {platform.python_version()} | 平台: {platform.platform()}")
    lines.append(f"- pulsemq 版本: {pulsemq.__version__}")
    lines.append(f"- 每组合发送帧数: {count}")
    lines.append("- 生产端 / 服务端 / 消费端: **独立进程**")
    lines.append("- 端口: data=45555 control=45556 admin=49090")
    lines.append("")
    lines.append("> 单机 localhost 压测，三进程独立运行，数值仅作量级参考。")
    lines.append("")

    if results:
        lines.append("| data_type | serializer | compression | frames_recv | send_fps | recv_fps | p50_ms | p90_ms | p99_ms | max_ms | avg_ms | min_ms |")
        lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|")
        for r in results:
            lines.append(
                f"| {r['data_type']} | {r['serializer']} | {r['compression']} "
                f"| {r['frames_recv']}/{count} "
                f"| {r.get('send_fps', 0):,} | {r.get('recv_fps', 0):,} "
                f"| {r['p50_ms']} | {r['p90_ms']} | {r['p99_ms']} "
                f"| {r['max_ms']} | {r['avg_ms']} | {r['min_ms']} |"
            )
    else:
        lines.append("_无结果_")
    lines.append("")

    RESULT_FILE.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n报告已写入: {RESULT_FILE}", flush=True)


# ---------------------------------------------------------------------------
# CLI 入口
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="PulseMQ 多进程基准测试")
    p.add_argument("--role", choices=["server", "consumer", "producer"],
                   default=None, help="子角色（内部用；不指定则运行编排器）")
    p.add_argument("--count", type=int, default=3000,
                   help="每组合发送帧数（默认 3000）")
    p.add_argument("--data-type", default=None,
                   choices=["dict", "dataframe", "str", "bytes"],
                   help="只测指定数据类型")
    p.add_argument("--serializer", default=None,
                   help="序列化器（consumer/producer 角色用）")
    p.add_argument("--compression", default=None,
                   help="压缩格式（consumer/producer 角色用）")
    p.add_argument("--timeout", type=float, default=30.0,
                   help="consumer 等待超时秒数（默认 30）")
    args = p.parse_args()

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        # 防止 Windows 在基准期间休眠（ES_CONTINUOUS | ES_SYSTEM_REQUIRED）
        try:
            import ctypes
            ctypes.windll.kernel32.SetThreadExecutionState(0x80000001)
        except Exception:
            pass

    try:
        if args.role == "server":
            asyncio.run(role_server())
        elif args.role == "consumer":
            asyncio.run(role_consumer(
                args.data_type, args.serializer, args.compression,
                args.count, args.timeout,
            ))
        elif args.role == "producer":
            asyncio.run(role_producer(
                args.data_type, args.serializer, args.compression, args.count,
            ))
        else:
            run_orchestrator(args.count, args.timeout, args.data_type)
    finally:
        # 恢复：清除连续保持标志，允许系统正常休眠
        if sys.platform == "win32":
            try:
                import ctypes
                ctypes.windll.kernel32.SetThreadExecutionState(0x80000000)
            except Exception:
                pass


if __name__ == "__main__":
    main()
