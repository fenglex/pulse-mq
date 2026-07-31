"""PulseMQ 跨机器基准：Server 在远程 Linux，Producer/Consumer 在本地。

拓扑（B 方案）::

    本地 Producer/Consumer  ──tcp──→  远程 Server (ROUTER, bind 0.0.0.0)
    消息过两次局域网，测跨网络吞吐/延迟/扇出/payload 扩展性。

用法::

    # 远程（由编排器通过 ssh 自动启动，无需手动）:
    uv run python scripts/bench_dist.py --role server

    # 本地编排器（ssh 启远程 server，跑 Part A/B/C/D）:
    uv run python scripts/bench_dist.py                 # 全部
    uv run python scripts/bench_dist.py --part a        # 只跑 Part A
    uv run python scripts/bench_dist.py --remote 172.16.1.74 --ssh root@172.16.1.74

结果: 写入项目根 ``bench_dist_results.md``。

设计要点:
- Server 默认绑 ``0.0.0.0``，天然支持远程；Client 构造时传远程 endpoint 即可。
- Client 全在本地编排器进程内 asyncio 起（1 个 Producer + N 个 Consumer），
  扇出 N=50 也只占一个 Python 进程，避免多进程内存爆炸。
- 延迟测量有效：producer/consumer 同机同进程，``timestamp_ns`` 与接收墙钟同源，
  端到端延迟 = encode + 两次网络 + server 转发 + decode。
"""
from __future__ import annotations

import argparse
import asyncio
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

DEFAULT_REMOTE = "172.16.1.74"
DEFAULT_SSH = "root@172.16.1.74"
DEFAULT_REMOTE_DIR = "~/pulse-mq"
DATA_PORT = 45555
CTRL_PORT = 45556
ADMIN_BIND = "127.0.0.1:49090"
PASSWORD = "s"
# 1 publisher + 50 subscriber 凭据（扇出上限 50）
CREDS = {"pub": PASSWORD, **{f"sub{i}": PASSWORD for i in range(50)}}

RESULT_FILE = Path(__file__).resolve().parent.parent / "bench_dist_results.md"

# Part A 矩阵：data_type × serializer × compression（28 组）
_SERIALIZER_PAIRS = [
    ("dict", "msgpack"), ("dict", "json"),
    ("dataframe", "msgpack"), ("dataframe", "json"), ("dataframe", "pyarrow"),
    ("str", "str"), ("bytes", "bytes"),
]
COMPRESSIONS = ["none", "snappy", "lz4", "zstd"]
MATRIX = [(dt, ser, comp) for (dt, ser) in _SERIALIZER_PAIRS for comp in COMPRESSIONS]

# Part B 扇出档位 / Part C payload 档位
FANOUT_NS = [1, 5, 10, 20, 50]
PAYLOAD_SIZES = [64, 1024, 10_240, 102_400, 1_048_576]
SIZE_LABELS = {"64": "64B", "1024": "1KB", "10240": "10KB",
               "102400": "100KB", "1048576": "1MB"}


def _pct(sorted_list: list[float], pct: float) -> float:
    if not sorted_list:
        return 0.0
    return sorted_list[min(len(sorted_list) - 1, int(len(sorted_list) * pct))]


def _make_payload(data_type: str, payload_size: int | None = None):
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
        if payload_size:
            return b"x" * payload_size, DataType.BYTES, 1
        return b"hello pulse-mq benchmark " * 10, DataType.BYTES, 1
    raise ValueError(f"未知 data_type: {data_type}")


# ---------------------------------------------------------------------------
# server role —— 远程跑，绑 0.0.0.0
# ---------------------------------------------------------------------------

async def role_server(data_port: int, ctrl_port: int) -> None:
    """启动服务端，输出 SERVER_READY 后运行直到被编排器 pkill 终止。"""
    cfg = ServerConfig(
        data_endpoint=f"tcp://0.0.0.0:{data_port}",
        control_endpoint=f"tcp://0.0.0.0:{ctrl_port}",
        admin_endpoint=ADMIN_BIND,
        sndhwm=100_000, rcvhwm=100_000,
        admin_token="bench-token",
        stats_db="sqlite:///./bench_dist_stats.sqlite",
        admin_thread=False, ui_enabled=False,
    )
    srv = Server(
        data_endpoint=f"tcp://0.0.0.0:{data_port}",
        control_endpoint=f"tcp://0.0.0.0:{ctrl_port}",
        admin_endpoint=ADMIN_BIND,
        credentials=CREDS, admin_token="bench-token", config=cfg,
    )
    await srv.start()
    print("SERVER_READY", flush=True)
    await asyncio.Event().wait()  # 运行直到被 pkill


# ---------------------------------------------------------------------------
# 本地编排器 —— 进程内 asyncio 起 client，连远程 server
# ---------------------------------------------------------------------------

class SceneState:
    """单 consumer 的接收累计状态。"""
    __slots__ = ("frames", "records", "bytes_recv", "lats", "t_first", "t_last")

    def __init__(self) -> None:
        self.frames = 0
        self.records = 0
        self.bytes_recv = 0
        self.lats: list[float] = []
        self.t_first: float | None = None
        self.t_last: float | None = None


async def _drain(state: SceneState, drain: float, hard_cap: float) -> None:
    """drain 秒内无新消息则返回；总等待不超过 hard_cap 秒。"""
    start = time.monotonic()
    last = state.t_last
    idle = 0.0
    while idle < drain and time.monotonic() - start < hard_cap:
        await asyncio.sleep(0.2)
        if state.t_last == last:
            idle += 0.2
        else:
            idle = 0.0
            last = state.t_last


async def run_scene(remote: str, data_port: int, ctrl_port: int,
                    data_type: str, serializer: str, compression: str,
                    duration: float, drain: float,
                    payload_size: int | None = None) -> dict:
    """单 producer + 单 consumer，跨机器端到端一场景。"""
    dep = f"tcp://{remote}:{data_port}"
    cep = f"tcp://{remote}:{ctrl_port}"
    cons = ConsumerClient(dep, cep, "sub0", PASSWORD, sndhwm=100_000, rcvhwm=100_000)
    await cons.start()
    state = SceneState()

    def on_msg(msg):
        state.frames += 1
        state.records += msg.record_count
        if isinstance(msg.payload, (bytes, bytearray)):
            state.bytes_recv += len(msg.payload)
        now = time.monotonic()
        if state.t_first is None:
            state.t_first = now
        state.t_last = now
        state.lats.append((time.time_ns() - msg.timestamp_ns) / 1_000_000)

    await cons.subscribe("bench.*", on_msg)
    await asyncio.sleep(0.5)  # 订阅同步

    prod = ProducerClient(dep, cep, "pub", PASSWORD, sndhwm=100_000, rcvhwm=100_000)
    await prod.start()

    payload, dtype, rc = _make_payload(data_type, payload_size)
    # 采样一帧取实际大小；背压按积压字节数（8MB）限制——大 payload 自动强限速到
    # consumer 速率，避免 producer 突发超前导致 DEALER 队列残留消息在 stop 时丢失、
    # 以及 drain 排不空造成的延迟失真。小消息（<1KB）几乎不受限。
    sample = frames.encode("bench.topic", payload, serializer=serializer,
                           compression=compression, data_type=dtype, record_count=rc)
    est_bytes = len(sample) or 1
    BACKLOG_BYTES = 8_000_000

    t0 = time.monotonic()
    end = t0 + duration
    sent = 0
    while time.monotonic() < end:
        if (sent - state.frames) * est_bytes > BACKLOG_BYTES:
            await asyncio.sleep(0.001)
            continue
        frame = frames.encode("bench.topic", payload, serializer=serializer,
                              compression=compression, data_type=dtype, record_count=rc)
        await prod._transport.send(b"", frame, role="data")
        sent += 1
        if sent % 100 == 0:
            await asyncio.sleep(0)
    send_elapsed = time.monotonic() - t0

    await _drain(state, drain, hard_cap=duration + drain * 3 + 10)
    await prod.stop()
    await cons.stop()

    state.lats.sort()
    recv_elapsed = (state.t_last - state.t_first) if state.t_first and state.t_last else 0.0
    return {
        "data_type": data_type, "serializer": serializer, "compression": compression,
        "payload_size": payload_size or 0,
        "sent": sent, "frames_recv": state.frames, "records_recv": state.records,
        "send_fps": round(sent / send_elapsed) if send_elapsed else 0,
        "recv_fps": round(state.frames / recv_elapsed) if recv_elapsed else 0,
        "bytes_per_s": round(state.bytes_recv / recv_elapsed) if recv_elapsed else 0,
        "p50_ms": round(_pct(state.lats, 0.50), 3),
        "p90_ms": round(_pct(state.lats, 0.90), 3),
        "p99_ms": round(_pct(state.lats, 0.99), 3),
        "max_ms": round(state.lats[-1] if state.lats else 0, 3),
        "avg_ms": round(sum(state.lats) / len(state.lats), 3) if state.lats else 0,
        "min_ms": round(state.lats[0] if state.lats else 0, 3),
    }


async def run_fanout(remote: str, data_port: int, ctrl_port: int, n: int,
                     duration: float, drain: float) -> dict:
    """1 producer → N consumer 扇出。进程内 N 个 ConsumerClient（各独立 username）。"""
    dep = f"tcp://{remote}:{data_port}"
    cep = f"tcp://{remote}:{ctrl_port}"
    states = [SceneState() for _ in range(n)]
    consumers: list[ConsumerClient] = []

    def mk_cb(s: SceneState):
        def cb(msg):
            s.frames += 1
            now = time.monotonic()
            if s.t_first is None:
                s.t_first = now
            s.t_last = now
            s.lats.append((time.time_ns() - msg.timestamp_ns) / 1_000_000)
        return cb

    for i in range(n):
        c = ConsumerClient(dep, cep, f"sub{i}", PASSWORD, sndhwm=100_000, rcvhwm=100_000)
        await c.start()
        await c.subscribe("bench.*", mk_cb(states[i]))
        consumers.append(c)
    await asyncio.sleep(0.8)  # 全部订阅同步

    prod = ProducerClient(dep, cep, "pub", PASSWORD, sndhwm=100_000, rcvhwm=100_000)
    await prod.start()

    payload, dtype, rc = _make_payload("dict")
    sample = frames.encode("bench.topic", payload, serializer="msgpack",
                           compression="none", data_type=dtype, record_count=rc)
    est_bytes = len(sample) or 1
    BACKLOG_BYTES = 8_000_000
    t0 = time.monotonic()
    end = t0 + duration
    sent = 0
    while time.monotonic() < end:
        if (sent - min(s.frames for s in states)) * est_bytes > BACKLOG_BYTES:
            await asyncio.sleep(0.001)
            continue
        frame = frames.encode("bench.topic", payload, serializer="msgpack",
                              compression="none", data_type=dtype, record_count=rc)
        await prod._transport.send(b"", frame, role="data")
        sent += 1
        if sent % 100 == 0:
            await asyncio.sleep(0)
    send_elapsed = time.monotonic() - t0

    # drain：所有 consumer 均静默 drain 秒才停
    start = time.monotonic()
    last = [s.t_last for s in states]
    idle = 0.0
    while idle < drain and time.monotonic() - start < duration + drain * 3 + 10:
        await asyncio.sleep(0.2)
        if any(states[i].t_last != last[i] for i in range(n)):
            idle = 0.0
            last = [s.t_last for s in states]
        else:
            idle += 0.2

    await prod.stop()
    for c in consumers:
        await c.stop()

    total = sum(s.frames for s in states)
    all_lats = sorted(l for s in states for l in s.lats)
    per_consumer = [
        round(s.frames / ((s.t_last - s.t_first) if s.t_first and s.t_last else 1.0))
        for s in states
    ]
    return {
        "consumers": n, "sent": sent, "total_delivered": total,
        "send_fps": round(sent / send_elapsed) if send_elapsed else 0,
        "total_rps": round(total / send_elapsed) if send_elapsed else 0,
        "per_consumer_rps": per_consumer,
        "p50_ms": round(_pct(all_lats, 0.50), 3),
        "p99_ms": round(_pct(all_lats, 0.99), 3),
        "max_ms": round(all_lats[-1] if all_lats else 0, 3),
    }


async def run_all(args: argparse.Namespace) -> dict:
    state: dict[str, list] = {"partA": [], "partB": [], "partC": [], "partD": []}
    run_all_parts = args.part == "all"

    if run_all_parts or args.part == "a":
        print(f"[Part A] 28 组合跨机器矩阵（{args.duration_a}s/组）...", flush=True)
        for i, (dt, ser, comp) in enumerate(MATRIX):
            r = await run_scene(args.remote, args.data_port, args.ctrl_port,
                                dt, ser, comp, args.duration_a, args.drain_a)
            state["partA"].append(r)
            print(f"  [{i + 1}/28] {dt}/{ser}/{comp}: "
                  f"send={r['send_fps']:,} recv={r['recv_fps']:,} f/s "
                  f"p50={r['p50_ms']} p99={r['p99_ms']}ms", flush=True)
            await asyncio.sleep(0.3)

    if run_all_parts or args.part == "b":
        print(f"\n[Part B] 扇出 N={FANOUT_NS}（{args.duration_b}s/档）...", flush=True)
        for n in FANOUT_NS:
            r = await run_fanout(args.remote, args.data_port, args.ctrl_port, n,
                                 args.duration_b, args.drain_b)
            state["partB"].append(r)
            print(f"  N={n}: total={r['total_delivered']:,} ({r['total_rps']:,} r/s) "
                  f"p50={r['p50_ms']} p99={r['p99_ms']}ms", flush=True)
            await asyncio.sleep(0.3)

    if run_all_parts or args.part == "c":
        sizes_lbl = [SIZE_LABELS[str(s)] for s in PAYLOAD_SIZES]
        print(f"\n[Part C] payload {sizes_lbl}（{args.duration_c}s/档）...", flush=True)
        for sz in PAYLOAD_SIZES:
            r = await run_scene(args.remote, args.data_port, args.ctrl_port,
                                "bytes", "bytes", "none", args.duration_c, args.drain_c,
                                payload_size=sz)
            r["size_label"] = SIZE_LABELS[str(sz)]
            state["partC"].append(r)
            print(f"  {r['size_label']}: send={r['send_fps']:,} recv={r['recv_fps']:,} f/s "
                  f"{r['bytes_per_s'] / 1_000_000:.1f} MB/s p50={r['p50_ms']}ms", flush=True)
            await asyncio.sleep(0.3)

    if run_all_parts or args.part == "d":
        print(f"\n[Part D] 长稳 dict/msgpack/none（{args.duration_d}s）...", flush=True)
        r = await run_scene(args.remote, args.data_port, args.ctrl_port,
                            "dict", "msgpack", "none", args.duration_d, args.drain_d)
        state["partD"].append(r)
        print(f"  send={r['send_fps']:,} recv={r['recv_fps']:,} f/s "
              f"p50={r['p50_ms']} p99={r['p99_ms']} max={r['max_ms']}ms", flush=True)
    return state


# ---------------------------------------------------------------------------
# 远程 server ssh 启停
# ---------------------------------------------------------------------------

def start_remote_server(ssh: str, remote_dir: str, data_port: int, ctrl_port: int,
                        timeout: float = 120.0) -> subprocess.Popen:
    """ssh 启动远程 server，等 SERVER_READY。"""
    # 清理上次残留进程 + 旧 stats 文件
    subprocess.run(
        ["ssh", "-o", "StrictHostKeyChecking=accept-new", ssh,
         f"pkill -f 'bench_dist.py.*--role server' || true; "
         f"cd {remote_dir} && rm -f bench_dist_stats.sqlite"],
        capture_output=True, timeout=30,
    )
    cmd = (f"cd {remote_dir} && uv run python scripts/bench_dist.py "
           f"--role server --data-port {data_port} --ctrl-port {ctrl_port}")
    proc = subprocess.Popen(
        ["ssh", "-o", "StrictHostKeyChecking=accept-new", ssh, cmd],
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        line = proc.stdout.readline() if proc.stdout else ""
        if not line:
            if proc.poll() is not None:
                raise RuntimeError("远程 server ssh 进程提前退出（查看远程日志 data/logs/）")
            continue
        if "SERVER_READY" in line:
            return proc
    raise RuntimeError(f"远程 server {timeout:.0f}s 内未就绪")


def stop_remote_server(ssh: str, proc: subprocess.Popen) -> None:
    try:
        subprocess.run(
            ["ssh", ssh, "pkill -f 'bench_dist.py.*--role server' || true"],
            capture_output=True, timeout=15,
        )
    except Exception:
        pass
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# 报告
# ---------------------------------------------------------------------------

def write_report(args: argparse.Namespace, state: dict) -> None:
    L: list[str] = ["# PulseMQ 跨机器基准报告", ""]
    L.append(f"- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    L.append(f"- 本地: Python {platform.python_version()} | {platform.platform()}")
    L.append(f"- 远程: {args.remote}（ssh {args.ssh}, dir {args.remote_dir}）")
    L.append(f"- pulsemq: {pulsemq.__version__}")
    L.append("- 拓扑: 本地 Producer/Consumer → 远程 Server（消息过两次局域网）")
    L.append(f"- 端口: data={args.data_port} control={args.ctrl_port}")
    L.append("")
    L.append("> 跨机器局域网压测；延迟为端到端（producer→server→consumer，含两次网络）。")
    L.append("")

    if pA := state.get("partA"):
        L.append("## Part A: 28 组合跨机器矩阵")
        L.append("")
        L.append("| data_type | serializer | compression | send f/s | recv f/s | "
                 "p50 ms | p90 ms | p99 ms | max ms | avg ms |")
        L.append("|---|---|---|---|---|---|---|---|---|---|")
        for r in pA:
            L.append(f"| {r['data_type']} | {r['serializer']} | {r['compression']} "
                     f"| {r['send_fps']:,} | {r['recv_fps']:,} "
                     f"| {r['p50_ms']} | {r['p90_ms']} | {r['p99_ms']} "
                     f"| {r['max_ms']} | {r['avg_ms']} |")
        L.append("")

    if pB := state.get("partB"):
        L.append("## Part B: 扇出（1 producer → N consumer, msgpack/none, dict）")
        L.append("")
        L.append("| consumers | send f/s | total delivered | total r/s | "
                 "per-consumer r/s | p50 ms | p99 ms | max ms |")
        L.append("|---|---|---|---|---|---|---|---|")
        for r in pB:
            L.append(f"| {r['consumers']} | {r['send_fps']:,} | {r['total_delivered']:,} "
                     f"| {r['total_rps']:,} | {r['per_consumer_rps']} "
                     f"| {r['p50_ms']} | {r['p99_ms']} | {r['max_ms']} |")
        L.append("")

    if pC := state.get("partC"):
        L.append("## Part C: payload 大小扩展性（bytes/none）")
        L.append("")
        L.append("| size | send f/s | recv f/s | MB/s | p50 ms | p99 ms | max ms |")
        L.append("|---|---|---|---|---|---|---|")
        for r in pC:
            L.append(f"| {r['size_label']} | {r['send_fps']:,} | {r['recv_fps']:,} "
                     f"| {r['bytes_per_s'] / 1_000_000:.2f} "
                     f"| {r['p50_ms']} | {r['p99_ms']} | {r['max_ms']} |")
        L.append("")

    if pD := state.get("partD"):
        L.append("## Part D: 长稳压测（dict/msgpack/none）")
        L.append("")
        L.append("| duration | send f/s | recv f/s | sent | recv | "
                 "p50 ms | p90 ms | p99 ms | max ms |")
        L.append("|---|---|---|---|---|---|---|---|---|")
        for r in pD:
            L.append(f"| {args.duration_d}s | {r['send_fps']:,} | {r['recv_fps']:,} "
                     f"| {r['sent']:,} | {r['frames_recv']:,} "
                     f"| {r['p50_ms']} | {r['p90_ms']} | {r['p99_ms']} | {r['max_ms']} |")
        L.append("")

    RESULT_FILE.write_text("\n".join(L), encoding="utf-8")
    print(f"\n报告已写入: {RESULT_FILE}", flush=True)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    p = argparse.ArgumentParser(description="PulseMQ 跨机器基准")
    p.add_argument("--role", choices=["server"], default=None,
                   help="子角色（内部用；不指定则运行本地编排器）")
    p.add_argument("--remote", default=DEFAULT_REMOTE, help="远程 server host")
    p.add_argument("--data-port", type=int, default=DATA_PORT)
    p.add_argument("--ctrl-port", type=int, default=CTRL_PORT)
    p.add_argument("--ssh", default=DEFAULT_SSH, help="ssh 目标（user@host）")
    p.add_argument("--remote-dir", default=DEFAULT_REMOTE_DIR)
    p.add_argument("--part", choices=["a", "b", "c", "d", "all"], default="all")
    p.add_argument("--duration-a", type=float, default=5.0)
    p.add_argument("--drain-a", type=float, default=2.0)
    p.add_argument("--duration-b", type=float, default=10.0)
    p.add_argument("--drain-b", type=float, default=3.0)
    p.add_argument("--duration-c", type=float, default=5.0)
    p.add_argument("--drain-c", type=float, default=3.0)
    p.add_argument("--duration-d", type=float, default=30.0)
    p.add_argument("--drain-d", type=float, default=3.0)
    args = p.parse_args()

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    if args.role == "server":
        asyncio.run(role_server(args.data_port, args.ctrl_port))
        return

    proc = start_remote_server(args.ssh, args.remote_dir, args.data_port, args.ctrl_port)
    print(f"[orchestrator] 远程 server 就绪 ({args.remote})", flush=True)
    try:
        state = asyncio.run(run_all(args))
    finally:
        stop_remote_server(args.ssh, proc)
        print("[orchestrator] 远程 server 已停止", flush=True)
    write_report(args, state)


if __name__ == "__main__":
    main()
