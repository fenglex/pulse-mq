#!/usr/bin/env python3
"""PulseMQ 全矩阵压测 — 数据格式 × 压缩格式 × 消息类型。

数据格式:
  - single: 单条 dict (msgpack 序列化)
  - batch_msgpack: DataFrame 多条 (msgpack 序列化)
  - batch_pyarrow: DataFrame 多条 (pyarrow IPC 序列化)

压缩格式: none, snappy, lz4, zstd
消息类型: PUB(含SUB接收), PING, QUERY, SUB, UNSUB

用法:
    python scripts/bench_1m.py --msgs 100000 --port 58555
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import pyarrow as pa
import zmq
import zmq.asyncio

from pulsemq.config import BrokerConfig
from pulsemq.event_loop import install_event_loop
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType
from pulsemq.server import PulseServer

# ---------------------------------------------------------------------------
# 数据生成
# ---------------------------------------------------------------------------

_CODES = ["600000", "000001", "600036", "000858", "601318", "000333", "600519", "002415"]


def gen_single_dict(seq: int) -> dict:
    """单条行情快照 dict。"""
    code = _CODES[seq % len(_CODES)]
    base = 10 + (seq % 100)
    return {
        "seq": seq, "code": code, "name": f"STOCK_{code}",
        "open": round(base + np.random.random(), 2),
        "high": round(base + 1 + np.random.random(), 2),
        "low": round(base - 1 + np.random.random(), 2),
        "close": round(base + 0.5 + np.random.random(), 2),
        "volume": int(np.random.randint(10000, 9999999)),
        "ts": time.time(),
    }


def gen_batch_df(seq_start: int, batch_size: int) -> pd.DataFrame:
    """生成 DataFrame 批量行情。"""
    rows = []
    for i in range(batch_size):
        seq = seq_start + i
        code = _CODES[seq % len(_CODES)]
        base = 10 + (seq % 100)
        rows.append({
            "seq": seq, "code": code, "name": f"STOCK_{code}",
            "open": round(base + np.random.random(), 2),
            "high": round(base + 1 + np.random.random(), 2),
            "low": round(base - 1 + np.random.random(), 2),
            "close": round(base + 0.5 + np.random.random(), 2),
            "volume": int(np.random.randint(10000, 9999999)),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# 数据格式定义
# ---------------------------------------------------------------------------

DATA_FORMATS = ["single", "batch_msgpack", "batch_pyarrow"]
COMPS = ["none", "snappy", "lz4", "zstd"]
BATCH_SIZE = 8  # 每次 DataFrame 包含 8 只股票

# 格式 → (ser_fmt, record_count, 生成函数签名)
# single: msgpack 序列化单条 dict, record_count=1
# batch_msgpack: msgpack 序列化整个 DataFrame.to_dict(), record_count=batch_size
# batch_pyarrow: pyarrow IPC 序列化, record_count=batch_size


# ---------------------------------------------------------------------------
# 结果
# ---------------------------------------------------------------------------


@dataclass
class CellResult:
    """单个测试结果。"""
    data_fmt: str = ""
    comp: str = ""
    msg_type: str = ""
    n: int = 0
    elapsed: float = 0.0
    rate: float = 0.0
    sub_count: int = 0
    sub_rate: float = 0.0
    lat_p50: float = 0.0
    lat_p99: float = 0.0
    lat_samples: list[float] = field(default_factory=list)
    payload_size: int = 0

    def compute(self) -> None:
        if self.elapsed > 0:
            self.rate = self.n / self.elapsed
        if self.lat_samples:
            s = sorted(self.lat_samples)
            self.lat_p50 = s[min(round((len(s) - 1) * 0.50), len(s) - 1)]
            self.lat_p99 = s[min(round((len(s) - 1) * 0.99), len(s) - 1)]


# ---------------------------------------------------------------------------
# Broker 生命周期
# ---------------------------------------------------------------------------


async def start_broker(port: int) -> tuple[PulseServer, asyncio.Task]:
    config = BrokerConfig(
        bind=f"tcp://*:{port}", xpub_bind=f"tcp://*:{port + 1}",
        auth_enabled=False, max_concurrency=200, max_batch_size=128,
        zmq_rcvhwm=0, zmq_sndhwm=0,
        data_buffer_size=50_000, ctrl_buffer_size=5_000,
        metrics_enabled=False, default_compressor="none",
    )
    server = PulseServer(config)
    task = asyncio.create_task(server.start())
    await asyncio.sleep(1.5)
    return server, task


async def stop_broker(server: PulseServer, task: asyncio.Task) -> None:
    try:
        server._engine._running = False
        server._running = False
        server._transport._router.close(linger=0)
        server._transport._xpub.close(linger=0)
    except Exception:
        pass
    task.cancel()
    try:
        await asyncio.wait_for(task, timeout=3.0)
    except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
        pass
    try:
        server._transport._context.term()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# payload 编码
# ---------------------------------------------------------------------------


def encode_payload_single(snap: dict, comp: str) -> tuple[bytes, int]:
    """单条 dict → msgpack 序列化 + 压缩, record_count=1"""
    payload = FrameCodec.encode_payload(snap, "msgpack", comp)
    return payload, 1


def encode_payload_batch_msgpack(df: pd.DataFrame, comp: str) -> tuple[bytes, int]:
    """DataFrame 多条 → msgpack 序列化 + 压缩, record_count=len(df)"""
    payload = FrameCodec.encode_payload(df.to_dict(orient="records"), "msgpack", comp)
    return payload, len(df)


def encode_payload_batch_pyarrow(df: pd.DataFrame, comp: str) -> tuple[bytes, int]:
    """DataFrame 多条 → pyarrow IPC 序列化 + 压缩, record_count=len(df)"""
    payload = FrameCodec.encode_payload(df, "pyarrow", comp)
    return payload, len(df)


def _encode(data_fmt: str, seq_start: int, comp: str) -> tuple[bytes, int]:
    """根据数据格式生成并编码 payload。"""
    if data_fmt == "single":
        snap = gen_single_dict(seq_start)
        return encode_payload_single(snap, comp)
    elif data_fmt == "batch_msgpack":
        df = gen_batch_df(seq_start, BATCH_SIZE)
        return encode_payload_batch_msgpack(df, comp)
    elif data_fmt == "batch_pyarrow":
        df = gen_batch_df(seq_start, BATCH_SIZE)
        return encode_payload_batch_pyarrow(df, comp)
    else:
        raise ValueError(f"未知数据格式: {data_fmt}")


def _ser_fmt(data_fmt: str) -> str:
    return "pyarrow" if data_fmt == "batch_pyarrow" else "msgpack"


# ---------------------------------------------------------------------------
# PUB 吞吐测试
# ---------------------------------------------------------------------------


async def bench_pub(port: int, data_fmt: str, comp: str, n_batches: int) -> CellResult:
    """PUB 吞吐测试（并发 SUB 接收）。
    
    n_batches: 发送的消息帧数（每帧 record_count 取决于数据格式）
    """
    r = CellResult(data_fmt=data_fmt, comp=comp, msg_type="PUB")
    addr = f"tcp://localhost:{port}"
    xpub_addr = f"tcp://localhost:{port + 1}"
    topic = "bench.pub"
    ser = _ser_fmt(data_fmt)

    # SUB socket
    sub_ctx = zmq.asyncio.Context()
    sub_sock = sub_ctx.socket(zmq.SUB)
    sub_sock.setsockopt(zmq.RCVHWM, 0)
    sub_sock.setsockopt(zmq.SUBSCRIBE, topic.encode())
    sub_sock.connect(xpub_addr)
    await asyncio.sleep(0.5)

    # DEALER
    dealer_ctx = zmq.asyncio.Context()
    dealer = dealer_ctx.socket(zmq.DEALER)
    dealer.setsockopt(zmq.IDENTITY, f"bench_{data_fmt}_{comp}".encode())
    dealer.connect(addr)
    await asyncio.sleep(0.5)

    # SUB 注册
    sub_frames = FrameCodec.encode(MsgType.SUB, topic, 0, b"", "msgpack", "none")
    await dealer.send_multipart(sub_frames)
    try:
        await asyncio.wait_for(dealer.recv_multipart(), timeout=2.0)
    except asyncio.TimeoutError:
        pass
    await asyncio.sleep(0.5)

    # 诊断消息
    diag_payload, diag_rc = _encode(data_fmt, 0, comp)
    diag_frames = FrameCodec.encode(MsgType.PUB, topic, diag_rc, diag_payload, ser, comp)
    await dealer.send_multipart(diag_frames)
    try:
        await asyncio.wait_for(sub_sock.recv_multipart(), timeout=3.0)
    except asyncio.TimeoutError:
        await asyncio.sleep(1.0)
        try:
            await asyncio.wait_for(sub_sock.recv_multipart(), timeout=3.0)
        except asyncio.TimeoutError:
            pass

    # 并发接收
    recv_done = asyncio.Event()
    recv_count = [0]

    async def _recv():
        while not recv_done.is_set():
            try:
                frames = await asyncio.wait_for(sub_sock.recv_multipart(), timeout=1.0)
                if len(frames) >= 4:
                    recv_count[0] += 1
            except asyncio.TimeoutError:
                continue
            except zmq.ZMQError:
                break

    recv_task = asyncio.create_task(_recv())

    # PUB 发送
    t0 = time.monotonic()
    for i in range(n_batches):
        payload, rc = _encode(data_fmt, i * BATCH_SIZE, comp)
        frames = FrameCodec.encode(MsgType.PUB, topic, rc, payload, ser, comp)
        await dealer.send_multipart(frames)
        if i == 0:
            r.payload_size = len(payload)
    r.elapsed = time.monotonic() - t0
    r.n = n_batches

    # 等接收完成
    total_records = n_batches * (BATCH_SIZE if "batch" in data_fmt else 1)
    deadline = time.monotonic() + 60
    while recv_count[0] < n_batches and time.monotonic() < deadline:
        await asyncio.sleep(0.1)
    recv_done.set()
    try:
        await asyncio.wait_for(recv_task, timeout=3.0)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        recv_task.cancel()

    r.sub_count = recv_count[0]
    r.sub_rate = recv_count[0] / (time.monotonic() - t0) if r.elapsed > 0 else 0

    dealer.close(linger=0); dealer_ctx.term()
    sub_sock.close(linger=0); sub_ctx.term()
    r.compute()
    return r


# ---------------------------------------------------------------------------
# PING / QUERY / SUB / UNSUB 测试（固定用 msgpack 单条）
# ---------------------------------------------------------------------------


async def bench_ping(port: int, comp: str, n: int) -> CellResult:
    r = CellResult(comp=comp, msg_type="PING")
    dealer_ctx = zmq.asyncio.Context()
    dealer = dealer_ctx.socket(zmq.DEALER)
    dealer.setsockopt(zmq.IDENTITY, b"bench_ping")
    dealer.connect(f"tcp://localhost:{port}")
    await asyncio.sleep(0.3)

    t0 = time.monotonic()
    for _ in range(n):
        payload = FrameCodec.encode_payload({"client_ts": time.time()}, "msgpack", comp)
        frames = FrameCodec.encode(MsgType.PING, "", 0, payload, "msgpack", comp)
        ts = time.monotonic()
        await dealer.send_multipart(frames)
        try:
            await asyncio.wait_for(dealer.recv_multipart(), timeout=2.0)
            r.lat_samples.append((time.monotonic() - ts) * 1000)
        except asyncio.TimeoutError:
            pass
    r.elapsed = time.monotonic() - t0
    r.n = n
    dealer.close(linger=0); dealer_ctx.term()
    r.compute()
    return r


async def bench_query(port: int, comp: str, n: int) -> CellResult:
    r = CellResult(comp=comp, msg_type="QUERY")
    dealer_ctx = zmq.asyncio.Context()
    dealer = dealer_ctx.socket(zmq.DEALER)
    dealer.setsockopt(zmq.IDENTITY, b"bench_query")
    dealer.connect(f"tcp://localhost:{port}")
    await asyncio.sleep(0.3)

    t0 = time.monotonic()
    for _ in range(n):
        payload = FrameCodec.encode_payload({"action": "system_status"}, "msgpack", comp)
        frames = FrameCodec.encode(MsgType.QUERY, "", 0, payload, "msgpack", comp)
        ts = time.monotonic()
        await dealer.send_multipart(frames)
        try:
            await asyncio.wait_for(dealer.recv_multipart(), timeout=2.0)
            r.lat_samples.append((time.monotonic() - ts) * 1000)
        except asyncio.TimeoutError:
            pass
    r.elapsed = time.monotonic() - t0
    r.n = n
    dealer.close(linger=0); dealer_ctx.term()
    r.compute()
    return r


async def bench_sub_unsub(port: int, comp: str, n: int) -> tuple[CellResult, CellResult]:
    r_sub = CellResult(comp=comp, msg_type="SUB")
    r_unsub = CellResult(comp=comp, msg_type="UNSUB")
    dealer_ctx = zmq.asyncio.Context()
    dealer = dealer_ctx.socket(zmq.DEALER)
    dealer.setsockopt(zmq.IDENTITY, b"bench_subunsub")
    dealer.connect(f"tcp://localhost:{port}")
    await asyncio.sleep(0.3)

    n_half = n // 2
    t0 = time.monotonic()
    for i in range(n_half):
        topic = f"bench.sub.{i % 100}"
        frames = FrameCodec.encode(MsgType.SUB, topic, 0, b"")
        await dealer.send_multipart(frames)
        try:
            await asyncio.wait_for(dealer.recv_multipart(), timeout=2.0)
        except asyncio.TimeoutError:
            pass
    r_sub.elapsed = time.monotonic() - t0
    r_sub.n = n_half

    t0 = time.monotonic()
    for i in range(n_half):
        topic = f"bench.sub.{i % 100}"
        frames = FrameCodec.encode(MsgType.UNSUB, topic, 0, b"")
        await dealer.send_multipart(frames)
        try:
            await asyncio.wait_for(dealer.recv_multipart(), timeout=2.0)
        except asyncio.TimeoutError:
            pass
    r_unsub.elapsed = time.monotonic() - t0
    r_unsub.n = n_half

    dealer.close(linger=0); dealer_ctx.term()
    r_sub.compute(); r_unsub.compute()
    return r_sub, r_unsub


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------


async def run_all(n_batches: int, n_ctrl: int, base_port: int) -> list[CellResult]:
    results: list[CellResult] = []
    port_idx = 0

    for data_fmt in DATA_FORMATS:
        for comp in COMPS:
            port = base_port + port_idx * 2
            port_idx += 1
            print(f"\n{'━' * 100}")
            print(f"  {data_fmt} × {comp}  (port={port})")
            print(f"{'━' * 100}")

            server, task = await start_broker(port)

            try:
                # 1. PUB
                print(f"  ▶ PUB ...", end="", flush=True)
                r = await bench_pub(port, data_fmt, comp, n_batches)
                results.append(r)
                loss = (1 - r.sub_count / r.n) * 100 if r.n > 0 else 0
                print(f"  PUB {r.rate:>7,.0f}/s  SUB {r.sub_rate:>7,.0f}/s  丢 {loss:.1f}%  payload={r.payload_size}B")

                # 2. PING
                print(f"  ▶ PING ...", end="", flush=True)
                r = await bench_ping(port, comp, n_ctrl)
                results.append(r)
                print(f"  {r.rate:>7,.0f}/s  P50 {r.lat_p50:.2f}ms  P99 {r.lat_p99:.2f}ms")

                # 3. QUERY
                print(f"  ▶ QUERY ...", end="", flush=True)
                r = await bench_query(port, comp, n_ctrl)
                results.append(r)
                print(f"  {r.rate:>7,.0f}/s  P50 {r.lat_p50:.2f}ms  P99 {r.lat_p99:.2f}ms")

                # 4. SUB/UNSUB
                print(f"  ▶ SUB/UNSUB ...", end="", flush=True)
                r_sub, r_unsub = await bench_sub_unsub(port, comp, n_ctrl)
                results.append(r_sub)
                results.append(r_unsub)
                print(f"  SUB {r_sub.rate:>7,.0f}/s  UNSUB {r_unsub.rate:>7,.0f}/s")

            except Exception as e:
                print(f"\n  错误: {e}")
            finally:
                await stop_broker(server, task)

    return results


def print_matrix(results: list[CellResult], n_batches: int) -> None:
    grid: dict[tuple[str, str, str], CellResult] = {}
    for r in results:
        key = (r.data_fmt or "single", r.comp, r.msg_type)
        grid[key] = r

    print(f"\n{'━' * 130}")
    print(f"  PulseMQ 全矩阵性能报告  ({n_batches:,} 批次/组, 每批 {BATCH_SIZE} 条行情)")
    print(f"{'━' * 130}")

    # --- PUB 吞吐量 ---
    print(f"\n  ■ PUB 吞吐量 (batches/s)")
    print(f"  {'数据格式':<16}", end="")
    for c in COMPS:
        print(f"│{c:>12}", end="")
    print()
    print(f"  {'─' * 16}", end="")
    for _ in COMPS:
        print(f"┼{'─' * 12}", end="")
    print()
    for df in DATA_FORMATS:
        label = df
        print(f"  {label:<16}", end="")
        for c in COMPS:
            cell = grid.get((df, c, "PUB"))
            if cell and cell.rate > 0:
                print(f"│{cell.rate:>11,.0f}", end="")
            else:
                print(f"│{'—':>12}", end="")
        print()

    # --- SUB 接收率 ---
    print(f"\n  ■ PUB→SUB 接收率 (batches/s)")
    print(f"  {'数据格式':<16}", end="")
    for c in COMPS:
        print(f"│{c:>12}", end="")
    print()
    print(f"  {'─' * 16}", end="")
    for _ in COMPS:
        print(f"┼{'─' * 12}", end="")
    print()
    for df in DATA_FORMATS:
        print(f"  {df:<16}", end="")
        for c in COMPS:
            cell = grid.get((df, c, "PUB"))
            if cell and cell.sub_rate > 0:
                print(f"│{cell.sub_rate:>11,.0f}", end="")
            else:
                print(f"│{'—':>12}", end="")
        print()

    # --- Payload 大小 ---
    print(f"\n  ■ Payload 大小 (bytes)")
    print(f"  {'数据格式':<16}", end="")
    for c in COMPS:
        print(f"│{c:>12}", end="")
    print()
    print(f"  {'─' * 16}", end="")
    for _ in COMPS:
        print(f"┼{'─' * 12}", end="")
    print()
    for df in DATA_FORMATS:
        print(f"  {df:<16}", end="")
        for c in COMPS:
            cell = grid.get((df, c, "PUB"))
            if cell and cell.payload_size > 0:
                print(f"│{cell.payload_size:>11,}", end="")
            else:
                print(f"│{'—':>12}", end="")
        print()

    # --- 丢包率 ---
    print(f"\n  ■ PUB 丢包率")
    print(f"  {'数据格式':<16}", end="")
    for c in COMPS:
        print(f"│{c:>12}", end="")
    print()
    print(f"  {'─' * 16}", end="")
    for _ in COMPS:
        print(f"┼{'─' * 12}", end="")
    print()
    for df in DATA_FORMATS:
        print(f"  {df:<16}", end="")
        for c in COMPS:
            cell = grid.get((df, c, "PUB"))
            if cell and cell.n > 0:
                loss = (1 - cell.sub_count / cell.n) * 100
                print(f"│{loss:>10.2f}%", end="")
            else:
                print(f"│{'—':>12}", end="")
        print()

    # --- PING 延迟 ---
    print(f"\n  ■ PING 往返延迟 (ms) — P50 / P99")
    print(f"  {'数据格式':<16}", end="")
    for c in COMPS:
        print(f"│{c:>24}", end="")
    print()
    print(f"  {'─' * 16}", end="")
    for _ in COMPS:
        print(f"┼{'─' * 24}", end="")
    print()
    print(f"  {'*':<16}", end="")
    for c in COMPS:
        cell = grid.get(("single", c, "PING"))
        if cell and cell.lat_p50 > 0:
            print(f"│{cell.lat_p50:>10.2f}/{cell.lat_p99:>10.2f}", end="")
        else:
            print(f"│{'—':>24}", end="")
    print()

    # --- QUERY 吞吐 ---
    print(f"\n  ■ QUERY 吞吐量 (msg/s)")
    print(f"  {'数据格式':<16}", end="")
    for c in COMPS:
        print(f"│{c:>12}", end="")
    print()
    print(f"  {'─' * 16}", end="")
    for _ in COMPS:
        print(f"┼{'─' * 12}", end="")
    print()
    print(f"  {'*':<16}", end="")
    for c in COMPS:
        cell = grid.get(("single", c, "QUERY"))
        if cell and cell.rate > 0:
            print(f"│{cell.rate:>11,.0f}", end="")
        else:
            print(f"│{'—':>12}", end="")
    print()

    print(f"{'━' * 130}")


def main() -> None:
    parser = argparse.ArgumentParser(description="PulseMQ 全矩阵压测")
    parser.add_argument("--msgs", type=int, default=50_000, help="PUB 批次数/组")
    parser.add_argument("--ctrl", type=int, default=3_000, help="PING/QUERY/SUB/UNSUB 消息数")
    parser.add_argument("--port", type=int, default=58555, help="基础端口号")
    args = parser.parse_args()

    loop_type = install_event_loop()
    n_batches = args.msgs
    n_ctrl = args.ctrl

    print(f"{'═' * 130}")
    print(f"  PulseMQ 全矩阵压测 — 数据格式 × 压缩格式 × 消息类型")
    print(f"{'═' * 130}")
    print(f"  数据格式: {', '.join(DATA_FORMATS)}")
    print(f"  压缩格式: {', '.join(COMPS)}")
    print(f"  PUB 批次数: {n_batches:,} × {BATCH_SIZE}条/批 = {n_batches * BATCH_SIZE:,} 条")
    print(f"  PING/QUERY/SUB/UNSUB: {n_ctrl:,}")
    print(f"  事件循环: {loop_type}")

    results = asyncio.run(run_all(n_batches, n_ctrl, args.port))
    print_matrix(results, n_batches)


if __name__ == "__main__":
    main()
