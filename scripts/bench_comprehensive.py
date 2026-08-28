"""PulseMQ 全面性能基准：类型 × 序列化器 × 压缩 的发送/接收全矩阵 + 尺寸扩展 + 扇出。

五个部分（每部分独立成表，增量写入结果文件，中断也有部分结果）：

  Part 1  协议层微基准 — 28 组合 encode/decode 纯 CPU 性能（无网络，编排器进程内）
  Part 2  发送性能     — 28 组合，producer 独立进程持续 D 秒全速发送（无订阅者，
                         测 encode + DEALER 发送 + 服务端接收转发路径）
  Part 3  接收性能     — 28 组合，三进程端到端（producer/server/consumer），
                         消费端全速解码，测最大持续接收吞吐 + 端到端延迟
  Part 4  尺寸扩展     — dict 1KB/10KB/100KB/1MB、DataFrame 100/1k/10k 行
                         的发送 + 接收随 payload 尺寸的变化
  Part 5  扇出         — 1→1/3/5 订阅者广播扩展性（dict + DataFrame）

用法::

    uv run python scripts/bench_comprehensive.py                 # 全部
    uv run python scripts/bench_comprehensive.py --part 3        # 只跑 Part 3
    uv run python scripts/bench_comprehensive.py --duration 3 --iters 1200

结果: 项目根 ``bench_comprehensive_results.md``。

角色子进程（由编排器拉起）::

    --role server                          # 服务端，SERVER_READY 后常驻
    --role producer  --data-type ...       # 全速发送 D 秒（或 --max-frames 上限）
    --role consumer  [--fanout N]          # N 个订阅者收满/静默 2s 后输出 RESULT

内存安全：大 payload 场景用 --max-frames 限制在途帧数，避免弱机 OOM。
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

# Windows：zmq.asyncio 需要 SelectorEventLoop（Linux 无影响）
if sys.platform == "win32" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

DATA_EP = "tcp://127.0.0.1:46666"
CTRL_EP = "tcp://127.0.0.1:46667"
ADMIN_BIND = "127.0.0.1:49690"
PASSWORD = "s"
CREDS = {"pub": PASSWORD, **{f"sub{i}": PASSWORD for i in range(5)}}

RESULT_FILE = Path(__file__).resolve().parent.parent / "bench_comprehensive_results.md"

# 28 组合矩阵（与 bench_multiprocess 一致，便于对照）
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

# Part 4 尺寸场景：(data_type, serializer, compression, pad_bytes, df_rows, max_frames)
SIZE_SCENES = [
    ("dict", "msgpack", "none", 1_000, 0, 20_000),
    ("dict", "msgpack", "none", 10_000, 0, 5_000),
    ("dict", "msgpack", "none", 100_000, 0, 1_000),
    ("dict", "msgpack", "none", 1_000_000, 0, 200),
    ("dataframe", "pyarrow", "lz4", 0, 1_000, 1_500),
    ("dataframe", "pyarrow", "lz4", 0, 10_000, 300),
]

# Part 5 扇出场景：(data_type, serializer, compression) × fanout
FANOUT_SCENES = [("dict", "msgpack", "none"), ("dataframe", "pyarrow", "lz4")]
FANOUTS = [1, 3, 5]


# ---------------------------------------------------------------------------
# 共享：payload 构造
# ---------------------------------------------------------------------------

def build_payload(data_type: str, pad: int = 0, df_rows: int = 100):
    """返回 (payload, DataType, record_count)。"""
    if data_type == "dict":
        if pad:
            return {"seq": 0, "blob": "x" * pad}, DataType.DICT, 1
        return {"seq": 0, "val": 1.5, "sym": "AAPL", "ok": True}, DataType.DICT, 1
    if data_type == "dataframe":
        n = df_rows
        df = pd.DataFrame({
            "seq": list(range(n)),
            "val": [i * 1.5 for i in range(n)],
            "sym": ["AAPL"] * n,
        })
        return df, DataType.DATAFRAME, n
    if data_type == "str":
        return "hello pulse-mq benchmark " * 10, DataType.STR, 1
    if data_type == "bytes":
        return b"hello pulse-mq benchmark " * 10, DataType.BYTES, 1
    raise ValueError(data_type)


def _pct(sorted_list: list[float], p: float) -> float:
    if not sorted_list:
        return 0.0
    return sorted_list[min(len(sorted_list) - 1, int(len(sorted_list) * p))]


# ---------------------------------------------------------------------------
# 角色: server
# ---------------------------------------------------------------------------

async def role_server() -> None:
    cfg = ServerConfig(
        data_endpoint=DATA_EP, control_endpoint=CTRL_EP,
        admin_endpoint=ADMIN_BIND,
        sndhwm=100_000, rcvhwm=100_000, admin_token="bench-token",
        stats_db="sqlite:///./bench_comp_stats.sqlite",
    )
    srv = Server(data_endpoint=DATA_EP, control_endpoint=CTRL_EP,
                 admin_endpoint=ADMIN_BIND, credentials=CREDS,
                 admin_token="bench-token", config=cfg)
    await srv.start()
    print("SERVER_READY", flush=True)
    await asyncio.Event().wait()


# ---------------------------------------------------------------------------
# 角色: producer — 全速发送 duration 秒（或 max_frames 帧）
# ---------------------------------------------------------------------------

async def role_producer(args) -> None:
    prod = ProducerClient(DATA_EP, CTRL_EP, "pub", PASSWORD)
    await prod.start()
    payload, dtype, rc = build_payload(args.data_type, args.pad, args.df_rows)

    total = 0
    enc_ns = 0
    bytes_total = 0
    t_start = time.monotonic()
    end = t_start + args.duration
    while time.monotonic() < end and (args.max_frames <= 0 or total < args.max_frames):
        t0 = time.perf_counter_ns()
        frame = frames.encode("bench.topic", payload, serializer=args.serializer,
                              compression=args.compression, data_type=dtype,
                              record_count=rc)
        enc_ns += time.perf_counter_ns() - t0
        await prod._transport.send(b"", frame, role="data")
        total += 1
        bytes_total += len(frame)
        if total % 256 == 0:
            await asyncio.sleep(0)
    elapsed = time.monotonic() - t_start
    await prod.stop()
    print("RESULT " + json.dumps({
        "role": "producer",
        "frames": total,
        "elapsed_s": round(elapsed, 3),
        "send_fps": round(total / elapsed) if elapsed else 0,
        "enc_us_per_frame": round(enc_ns / 1000 / max(total, 1), 1),
        "frame_bytes": bytes_total // max(total, 1),
        "mbps": round(bytes_total / elapsed / 1e6, 1) if elapsed else 0,
    }, ensure_ascii=False), flush=True)


# ---------------------------------------------------------------------------
# 角色: consumer — N 个订阅者；收到首批后静默 2s 或总超时退出
# ---------------------------------------------------------------------------

async def role_consumer(args) -> None:
    n = args.fanout
    stats = [{"frames": 0, "records": 0, "lats": [], "first": None, "last": None}
             for _ in range(n)]
    clients = []

    def make_cb(i):
        def cb(msg):
            s = stats[i]
            s["frames"] += 1
            s["records"] += msg.record_count
            now = time.monotonic()
            if s["first"] is None:
                s["first"] = now
            s["last"] = now
            s["lats"].append((time.time_ns() - msg.timestamp_ns) / 1e6)
        return cb

    for i in range(n):
        c = ConsumerClient(DATA_EP, CTRL_EP, f"sub{i}", PASSWORD,
                           decode_queue_size=args.decode_queue)
        await c.start()
        await c.subscribe("bench.*", make_cb(i))
        clients.append(c)
    print("READY", flush=True)

    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        await asyncio.sleep(0.1)
        any_started = any(s["first"] is not None for s in stats)
        if any_started and all(
                s["last"] is not None and time.monotonic() - s["last"] > 2.0
                for s in stats):
            break

    for c in clients:
        await c.stop()

    per = []
    for s in stats:
        recv_elapsed = (s["last"] - s["first"]) if s["first"] is not None else 0.0
        lats = sorted(s["lats"])
        per.append({
            "frames": s["frames"],
            "records": s["records"],
            "fps": round(s["frames"] / recv_elapsed) if recv_elapsed > 0 else 0,
            "p50_ms": round(_pct(lats, 0.50), 3),
            "p99_ms": round(_pct(lats, 0.99), 3),
        })
    print("RESULT " + json.dumps({
        "role": "consumer",
        "fanout": n,
        "clients": per,
        "frames_total": sum(p["frames"] for p in per),
    }, ensure_ascii=False), flush=True)


# ---------------------------------------------------------------------------
# 编排器
# ---------------------------------------------------------------------------

class Orchestrator:
    def __init__(self, duration: float, iters: int, decode_queue: int = 0) -> None:
        self.duration = duration
        self.iters = iters
        self.decode_queue = decode_queue
        self.py = [sys.executable, str(Path(__file__).resolve())]
        self.server: subprocess.Popen | None = None

    def _spawn(self, extra: list[str], ready_marker: str) -> subprocess.Popen:
        env = dict(os.environ, PYTHONUNBUFFERED="1")
        p = subprocess.Popen(self.py + extra, stdout=subprocess.PIPE,
                             stderr=subprocess.DEVNULL, env=env, text=True)
        line = p.stdout.readline()
        if ready_marker not in line:
            print(f"  子进程启动失败: {line.strip()}", flush=True)
            p.kill()
            raise RuntimeError("child start failed")
        return p

    @staticmethod
    def _read_result_tail(proc: subprocess.Popen, marker: str) -> dict | None:
        out = proc.stdout.read() if proc.stdout else ""
        for line in out.strip().splitlines():
            if line.startswith(marker):
                return json.loads(line[len(marker):])
        return None

    def _spawn_noready(self, extra: list[str]) -> subprocess.Popen:
        """启动无 READY 信号的子进程（producer：结束时才输出 RESULT）。"""
        env = dict(os.environ, PYTHONUNBUFFERED="1")
        return subprocess.Popen(self.py + extra, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL, env=env, text=True)

    def _run_producer(self, dt: str, ser: str, comp: str, pad: int = 0,
                      df_rows: int = 100, max_frames: int = 0) -> dict | None:
        p = self._spawn_noready([
            "--role", "producer", "--data-type", dt, "--serializer", ser,
            "--compression", comp, "--duration", str(self.duration),
            "--pad", str(pad), "--df-rows", str(df_rows),
            "--max-frames", str(max_frames),
        ])
        try:
            p.wait(timeout=max(60, self.duration * 3 + 60))
        except subprocess.TimeoutExpired:
            p.kill()
            p.wait()
        return self._read_result_tail(p, "RESULT ")

    def _run_consumer(self, timeout: float, fanout: int = 1) -> subprocess.Popen:
        return self._spawn([
            "--role", "consumer", "--timeout", str(timeout),
            "--fanout", str(fanout),
            "--decode-queue", str(self.decode_queue),
        ], "READY")

    # ---- 各部分 ----

    def part1_micro(self) -> None:
        rows = []
        print(f"\n[Part1] 协议层微基准 iters={self.iters} ...", flush=True)
        for dt, ser, comp in MATRIX:
            payload, dtype, rc = build_payload(dt)
            # encode
            t0 = time.perf_counter()
            for _ in range(self.iters):
                frame = frames.encode("bench.topic", payload, serializer=ser,
                                      compression=comp, data_type=dtype,
                                      record_count=rc)
            enc = time.perf_counter() - t0
            # decode
            t0 = time.perf_counter()
            for _ in range(self.iters):
                frames.decode(frame)
            dec = time.perf_counter() - t0
            rows.append((dt, ser, comp, self.iters / enc, self.iters / dec,
                         len(frame)))
            print(f"  {dt}/{ser}/{comp}: enc={self.iters / enc:,.0f} f/s "
                  f"dec={self.iters / dec:,.0f} f/s frame={len(frame)}B", flush=True)
        with open(RESULT_FILE, "a", encoding="utf-8") as f:
            f.write("\n## Part 1: 协议层微基准（纯 CPU，无网络）\n\n")
            f.write("| data_type | serializer | compression | encode f/s | decode f/s | 帧大小 |\n")
            f.write("|---|---|---|---|---|---|\n")
            for dt, ser, comp, e, d, fb in rows:
                f.write(f"| {dt} | {ser} | {comp} | {e:,.0f} | {d:,.0f} | {fb:,} B |\n")

    def part2_send(self) -> None:
        print(f"\n[Part2] 发送性能（producer 独立进程，{self.duration}s/组合）...",
              flush=True)
        rows = []
        for i, (dt, ser, comp) in enumerate(MATRIX):
            r = self._run_producer(dt, ser, comp)
            if r is None:
                continue
            rc = build_payload(dt)[2]
            rows.append((dt, ser, comp, r["send_fps"], r["send_fps"] * rc,
                         r["mbps"], r["enc_us_per_frame"], r["frame_bytes"]))
            print(f"  [{i + 1}/{len(MATRIX)}] {dt}/{ser}/{comp}: "
                  f"{r['send_fps']:,} f/s {r['mbps']} MB/s "
                  f"enc={r['enc_us_per_frame']}us", flush=True)
        with open(RESULT_FILE, "a", encoding="utf-8") as f:
            f.write("\n## Part 2: 发送性能（encode + 发送，无订阅者）\n\n")
            f.write("| data_type | serializer | compression | 发送 f/s | 发送 records/s | MB/s | encode µs/帧 | 帧大小 |\n")
            f.write("|---|---|---|---|---|---|---|---|\n")
            for dt, ser, comp, fps, rps, mbps, enc, fb in rows:
                f.write(f"| {dt} | {ser} | {comp} | {fps:,} | {rps:,} | {mbps} | "
                        f"{enc} | {fb:,} B |\n")

    def _run_e2e(self, dt: str, ser: str, comp: str, pad: int = 0,
                 df_rows: int = 100, max_frames: int = 0,
                 fanout: int = 1, cons_timeout: float = 60.0) -> dict | None:
        cons = self._run_consumer(cons_timeout, fanout)
        prod_r = self._run_producer(dt, ser, comp, pad, df_rows, max_frames)
        try:
            cons.wait(timeout=cons_timeout + 30)
        except subprocess.TimeoutExpired:
            cons.kill()
            cons.wait()
        cons_r = self._read_result_tail(cons, "RESULT ")
        if prod_r is None or cons_r is None:
            return None
        return {"prod": prod_r, "cons": cons_r}

    def part3_e2e(self) -> None:
        print(f"\n[Part3] 接收性能（三进程端到端，{self.duration}s/组合）...",
              flush=True)
        rows = []
        for i, (dt, ser, comp) in enumerate(MATRIX):
            r = self._run_e2e(dt, ser, comp, cons_timeout=90)
            if r is None:
                print(f"  [{i + 1}] {dt}/{ser}/{comp}: FAILED", flush=True)
                continue
            c = r["cons"]["clients"][0]
            p = r["prod"]
            ratio = (c["frames"] / p["frames"] * 100) if p["frames"] else 0
            rc = build_payload(dt)[2]
            rows.append((dt, ser, comp, p["send_fps"], c["fps"],
                         c["fps"] * rc, ratio, c["p50_ms"], c["p99_ms"]))
            print(f"  [{i + 1}/{len(MATRIX)}] {dt}/{ser}/{comp}: "
                  f"send={p['send_fps']:,} recv={c['fps']:,} f/s "
                  f"({ratio:.0f}%) p50={c['p50_ms']}ms", flush=True)
        with open(RESULT_FILE, "a", encoding="utf-8") as f:
            f.write("\n## Part 3: 接收性能（端到端，消费端全速解码；p50/p99 含过载排队）\n\n")
            f.write("| data_type | serializer | compression | 发送 f/s | 接收 f/s | 接收 records/s | 送达率 | p50 ms | p99 ms |\n")
            f.write("|---|---|---|---|---|---|---|---|---|\n")
            for dt, ser, comp, sf, cf, cr, ratio, p50, p99 in rows:
                f.write(f"| {dt} | {ser} | {comp} | {sf:,} | {cf:,} | {cr:,} | "
                        f"{ratio:.0f}% | {p50} | {p99} |\n")

    def part4_sizes(self) -> None:
        print("\n[Part4] 尺寸扩展 ...", flush=True)
        rows = []
        scenes = [(f"dict {s[3] // 1000}KB", *s) for s in SIZE_SCENES if s[0] == "dict"] + \
                 [(f"dataframe {s[4]} 行", *s) for s in SIZE_SCENES if s[0] == "dataframe"]
        for i, (label, dt, ser, comp, pad, df_rows, max_frames) in enumerate(scenes):
            r = self._run_e2e(dt, ser, comp, pad, df_rows, max_frames,
                              cons_timeout=90)
            if r is None:
                continue
            c = r["cons"]["clients"][0]
            p = r["prod"]
            rows.append((label, p["send_fps"], p["mbps"], c["fps"], c["p50_ms"],
                         c["p99_ms"]))
            print(f"  [{i + 1}/{len(scenes)}] {label}: send={p['send_fps']:,} f/s "
                  f"{p['mbps']}MB/s recv={c['fps']:,} f/s p50={c['p50_ms']}ms",
                  flush=True)
        with open(RESULT_FILE, "a", encoding="utf-8") as f:
            f.write("\n## Part 4: 尺寸扩展（发送/接收随 payload 大小变化）\n\n")
            f.write("| 场景 | 发送 f/s | 发送 MB/s | 接收 f/s | p50 ms | p99 ms |\n")
            f.write("|---|---|---|---|---|---|\n")
            for label, sf, mbps, cf, p50, p99 in rows:
                f.write(f"| {label} | {sf:,} | {mbps} | {cf:,} | {p50} | {p99} |\n")

    def part5_fanout(self) -> None:
        print("\n[Part5] 扇出 ...", flush=True)
        rows = []
        for dt, ser, comp in FANOUT_SCENES:
            for n in FANOUTS:
                r = self._run_e2e(dt, ser, comp, fanout=n, cons_timeout=90)
                if r is None:
                    continue
                per = r["cons"]["clients"]
                fps_avg = sum(c["fps"] for c in per) / len(per)
                p50 = sum(c["p50_ms"] for c in per) / len(per)
                rows.append((f"{dt}/{ser}/{comp}", n, fps_avg, fps_avg * n, p50))
                print(f"  {dt}/{comp} fanout={n}: 每订阅者 {fps_avg:,.0f} f/s "
                      f"总投递 {fps_avg * n:,.0f} f/s", flush=True)
        with open(RESULT_FILE, "a", encoding="utf-8") as f:
            f.write("\n## Part 5: 扇出扩展性（1 publisher → N subscriber）\n\n")
            f.write("| 场景 | fanout | 每订阅者 f/s | 总投递 f/s | p50 ms |\n")
            f.write("|---|---|---|---|---|\n")
            for scene, n, fps, total, p50 in rows:
                f.write(f"| {scene} | {n} | {fps:,.0f} | {total:,.0f} | {p50:.1f} |\n")

    def start_server(self) -> bool:
        try:
            self.server = self._spawn(["--role", "server"], "SERVER_READY")
            print("[orchestrator] server 就绪", flush=True)
            return True
        except RuntimeError:
            return False

    def stop_server(self) -> None:
        if self.server:
            self.server.kill()
            self.server.wait()


def main() -> None:
    p = argparse.ArgumentParser(description="PulseMQ 全面性能基准")
    p.add_argument("--role", choices=["server", "producer", "consumer"])
    p.add_argument("--part", default="all",
                   help="1|2|3|4|5|all（all 按 1→5 顺序）")
    p.add_argument("--duration", type=float, default=3.0, help="每场景发送秒数")
    p.add_argument("--iters", type=int, default=1200, help="Part1 每组合迭代数")
    p.add_argument("--data-type", default="dict")
    p.add_argument("--serializer", default="msgpack")
    p.add_argument("--compression", default="none")
    p.add_argument("--pad", type=int, default=0)
    p.add_argument("--df-rows", type=int, default=100)
    p.add_argument("--max-frames", type=int, default=0, help=">0 时限制发送帧数")
    p.add_argument("--timeout", type=float, default=60.0, help="consumer 总超时")
    p.add_argument("--fanout", type=int, default=1)
    p.add_argument("--decode-queue", type=int, default=0,
                   help="consumer 解码队列长度（>0 启用 worker 线程模式）")
    args = p.parse_args()

    if args.role == "server":
        asyncio.run(role_server())
        return
    if args.role == "producer":
        asyncio.run(role_producer(args))
        return
    if args.role == "consumer":
        asyncio.run(role_consumer(args))
        return

    # 编排器：全量运行时重写结果文件；单部分补跑时追加（保留已完成部分）
    if args.part == "all":
        with open(RESULT_FILE, "w", encoding="utf-8") as f:
            f.write("# PulseMQ 全面性能基准报告\n\n")
            f.write(f"- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"- Python: {platform.python_version()} | {platform.platform()}\n")
            f.write(f"- pulsemq: {pulsemq.__version__} | 每场景 {args.duration}s\n")
            f.write("- 拓扑: producer / server / consumer 独立进程, localhost\n")
    parts = {
        "1": ["1"], "2": ["2"], "3": ["3"], "4": ["4"], "5": ["5"],
        "all": ["1", "2", "3", "4", "5"],
    }[args.part]
    orch = Orchestrator(args.duration, args.iters, args.decode_queue)
    if "2" in parts or "3" in parts or "4" in parts or "5" in parts:
        if not orch.start_server():
            return
    try:
        if "1" in parts:
            orch.part1_micro()
        if "2" in parts:
            orch.part2_send()
        if "3" in parts:
            orch.part3_e2e()
        if "4" in parts:
            orch.part4_sizes()
        if "5" in parts:
            orch.part5_fanout()
    finally:
        orch.stop_server()
    print(f"\n报告已写入: {RESULT_FILE}", flush=True)


if __name__ == "__main__":
    main()
