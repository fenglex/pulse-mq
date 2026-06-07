#!/usr/bin/env python3
"""PulseMQ 全参数 PUB 基准测试 — 数据形态 × 序列化 × 压缩。

20 个测试单元（5 data_shape × 4 compression）:
  × none, snappy, lz4, zstd

每组 100 万条记录为基准，批量模式每批 2000 条。

用法:
    python scripts/bench_1m.py --records 1000000 --port 58555
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from dataclasses import dataclass

import numpy as np
import pandas as pd
import zmq
import zmq.asyncio

from pulsemq.client.async_client import PulseClient
from pulsemq.config import ServerConfig
from pulsemq.event_loop import install_event_loop
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType
from pulsemq.server import PulseServer

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

DATA_SHAPES = [
    "single_msgpack",
    "batch_msgpack",
    "single_pyarrow",
    "batch_pyarrow",
]
COMPS = ["none", "snappy", "lz4", "zstd"]
BATCH_SIZE = 2000
CODES = [
    "600000", "000001", "600036", "000858", "601318",
    "000333", "600519", "002415", "600276", "601398",
]


# ---------------------------------------------------------------------------
# 数据生成
# ---------------------------------------------------------------------------


def gen_single_dict(seq: int) -> dict:
    """生成单条 A 股行情快照。"""
    code = CODES[seq % len(CODES)]
    base = 10 + (seq % 100)
    return {
        "seq": seq,
        "code": code,
        "name": f"STOCK_{code}",
        "open": round(base + np.random.random(), 2),
        "high": round(base + 1 + np.random.random(), 2),
        "low": round(base - 1 + np.random.random(), 2),
        "close": round(base + 0.5 + np.random.random(), 2),
        "volume": int(np.random.randint(10000, 9999999)),
        "amount": round(base * 10000 * (1 + np.random.random()), 2),
        "ts": time.time(),
    }


def gen_batch_df(seq_start: int, batch_size: int) -> pd.DataFrame:
    """生成批量 DataFrame 行情。"""
    rows = []
    for i in range(batch_size):
        seq = seq_start + i
        code = CODES[seq % len(CODES)]
        base = 10 + (seq % 100)
        rows.append({
            "seq": seq,
            "code": code,
            "name": f"STOCK_{code}",
            "open": round(base + np.random.random(), 2),
            "high": round(base + 1 + np.random.random(), 2),
            "low": round(base - 1 + np.random.random(), 2),
            "close": round(base + 0.5 + np.random.random(), 2),
            "volume": int(np.random.randint(10000, 9999999)),
            "amount": round(base * 10000 * (1 + np.random.random()), 2),
            "ts": time.time(),
        })
    return pd.DataFrame(rows)


_RAW_BYTES = b"\x01" + b"market_binary_snapshot_data" * 10


# ---------------------------------------------------------------------------
# 结果
# ---------------------------------------------------------------------------


@dataclass
class CellResult:
    """单个测试单元的结果。"""

    data_shape: str = ""
    comp: str = ""
    total_records: int = 0
    send_frames: int = 0
    recv_frames: int = 0
    send_elapsed: float = 0.0
    recv_elapsed: float = 0.0
    payload_size: int = 0

    @property
    def send_rate(self) -> float:
        """发送吞吐 (records/s)。"""
        return self.total_records / self.send_elapsed if self.send_elapsed > 0 else 0

    @property
    def recv_rate(self) -> float:
        """接收吞吐 (records/s)。"""
        return (
            self.recv_frames * self._records_per_frame / self.recv_elapsed
            if self.recv_elapsed > 0
            else 0
        )

    @property
    def loss_pct(self) -> float:
        """丢包率。"""
        if self.send_frames == 0:
            return 0.0
        return (1 - self.recv_frames / self.send_frames) * 100

    @property
    def _records_per_frame(self) -> int:
        if "batch" in self.data_shape:
            return BATCH_SIZE
        return 1


# ---------------------------------------------------------------------------
# 服务端生命周期
# ---------------------------------------------------------------------------


async def start_server(port: int) -> tuple[PulseServer, asyncio.Task]:
    """启动独立服务端。"""
    config = ServerConfig(
        bind=f"tcp://*:{port}",
        xpub_bind=f"tcp://*:{port + 1}",
        auth_enabled=False,
        max_concurrency=200,
        zmq_rcvhwm=0,
        zmq_sndhwm=0,
        data_buffer_size=100_000,
        ctrl_buffer_size=5_000,
        metrics_enabled=False,
        default_compressor="none",
    )
    server = PulseServer(config)
    task = asyncio.create_task(server.start())
    await asyncio.sleep(1.5)
    return server, task


async def stop_server(server: PulseServer, task: asyncio.Task) -> None:
    """停止服务端。"""
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
# 序列化格式映射
# ---------------------------------------------------------------------------


def _ser_fmt(data_shape: str) -> str:
    """根据 data_shape 返回序列化格式参数。"""
    if "pyarrow" in data_shape:
        return "pyarrow"
    if "bytes" in data_shape:
        return "bytes"
    return "msgpack"


# ---------------------------------------------------------------------------
# 测试单元
# ---------------------------------------------------------------------------


async def run_cell(
    cell_idx: int,
    data_shape: str,
    comp: str,
    port: int,
    total_records: int,
) -> CellResult:
    """运行单个测试单元。

    PUB 端使用 PulseClient API，SUB 端使用原始 ZMQ SUB socket 避免解码开销。
    """
    result = CellResult(
        data_shape=data_shape, comp=comp, total_records=total_records,
    )
    ser = _ser_fmt(data_shape)
    is_batch = "batch" in data_shape
    topic = f"bench.{data_shape}.{comp}"

    # 计算发送次数
    if is_batch:
        n_sends = total_records // BATCH_SIZE
    else:
        n_sends = total_records
    result.send_frames = n_sends

    # 1. 启动服务端
    server, server_task = await start_server(port)

    try:
        # 2. SUB 端：原始 ZMQ SUB socket + DEALER 注册
        sub_ctx = zmq.asyncio.Context()
        sub_sock = sub_ctx.socket(zmq.SUB)
        sub_sock.setsockopt(zmq.RCVHWM, 5_000_000)
        sub_sock.connect(f"tcp://localhost:{port + 1}")
        sub_sock.setsockopt(zmq.SUBSCRIBE, topic.encode())

        # DEALER 用于 SUB 注册（让服务端知道订阅关系）
        sub_dealer = sub_ctx.socket(zmq.DEALER)
        sub_dealer.setsockopt(zmq.IDENTITY, f"bench_sub_{cell_idx}".encode())
        sub_dealer.connect(f"tcp://localhost:{port}")
        await asyncio.sleep(0.3)

        sub_frames = FrameCodec.encode(MsgType.SUB, topic, 0, b"")
        await sub_dealer.send_multipart(sub_frames)
        try:
            await asyncio.wait_for(sub_dealer.recv_multipart(), timeout=2.0)
        except asyncio.TimeoutError:
            pass
        await asyncio.sleep(0.3)

        # 3. SUB 接收协程（原始帧计数，不做解码，不用 wait_for 减少开销）
        recv_count = [0]
        recv_start = [0.0]
        recv_done = asyncio.Event()

        async def recv_loop():
            recv_start[0] = time.monotonic()
            try:
                while recv_count[0] < n_sends:
                    await sub_sock.recv_multipart()
                    recv_count[0] += 1
            except zmq.ZMQError:
                pass
            except asyncio.CancelledError:
                pass
            recv_done.set()

        recv_task = asyncio.create_task(recv_loop())
        await asyncio.sleep(0.2)

        # 4. PUB 客户端（PulseClient）
        pub_client = PulseClient(
            address=f"tcp://localhost:{port}",
            xpub_address=f"tcp://localhost:{port + 1}",
            heartbeat_interval=30.0,
            recv_timeout=5.0,
            auto_reconnect=False,
            identity=f"bench_pub_{cell_idx}".encode(),
        )
        await pub_client.connect()
        # 排空 DEALER 中 服务端推送的 AUTH 消息
        try:
            while True:
                await asyncio.wait_for(
                    pub_client._dealer.recv_multipart(), timeout=0.05
                )
        except (asyncio.TimeoutError, Exception):
            pass

        # 5. 计算 payload 大小
            sample_data = _RAW_BYTES
        elif data_shape == "single_bytes":
                data = _RAW_BYTES
                _fmt = "bytes"
            elif is_batch:
            sample_df = gen_batch_df(0, BATCH_SIZE)
            if True:
                sample_data = sample_df.to_dict(orient="records")
            else:
                sample_data = sample_df
        else:
            sample_data = gen_single_dict(0)

        result.payload_size = len(
            FrameCodec.encode_payload(sample_data, ser, comp)
        )

        # 6. 发送并计时
        t0 = time.monotonic()
        for i in range(n_sends):
            if data_shape == "single_bytes":
                data = _RAW_BYTES
                _fmt = "bytes"
            elif is_batch:
                df = gen_batch_df(i * BATCH_SIZE, BATCH_SIZE)
                if ser == "msgpack":
                    data = df.to_dict(orient="records")
                else:
                    data = df
                _fmt = ser
            else:
                data = gen_single_dict(i)
                _fmt = "msgpack"

            await pub_client.publish(topic, data, format=_fmt, compression=comp)

            # 每 1000 次主动 yield，防止事件循环饥饿
            if i > 0 and i % 1000 == 0:
                await asyncio.sleep(0)

        result.send_elapsed = time.monotonic() - t0

        # 排空 PUB DEALER
        try:
            while True:
                await asyncio.wait_for(
                    pub_client._dealer.recv_multipart(), timeout=0.05
                )
        except (asyncio.TimeoutError, Exception):
            pass

        # 7. 等待 SUB 接收完成（超时通过 task cancel 处理）
        deadline = time.monotonic() + 300
        while not recv_done.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(0.5)

        result.recv_frames = recv_count[0]
        result.recv_elapsed = time.monotonic() - recv_start[0]

        # 取消未完成的接收协程
        if not recv_done.is_set():
            recv_task.cancel()
            try:
                await recv_task
            except (asyncio.CancelledError, Exception):
                pass

        # 8. 断开
        try:
            await pub_client.disconnect()
        except Exception:
            pass
        sub_dealer.close(linger=0)
        sub_sock.close(linger=0)
        sub_ctx.term()

    except Exception as e:
        print(f"错误: {e}")
    finally:
        await stop_server(server, server_task)

    return result


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------


async def run_all(total_records: int, base_port: int) -> list[CellResult]:
    """运行全部 20 个测试单元。"""
    results: list[CellResult] = []
    cell_idx = 0

    for data_shape in DATA_SHAPES:
        for comp in COMPS:
            cell_idx += 1
            port = base_port + (cell_idx - 1) * 2
            print(
                f"[{cell_idx:>2}/20] {data_shape:<16} × {comp:<6} ",
                end="",
                flush=True,
            )
            r = await run_cell(cell_idx, data_shape, comp, port, total_records)
            results.append(r)
            loss = r.loss_pct
            print(
                f"PUB {r.send_rate:>10,.0f} rec/s │ "
                f"SUB {r.recv_rate:>10,.0f} rec/s │ "
                f"loss {loss:>5.1f}% │ "
                f"payload {r.payload_size:>6}B"
            )

    return results


# ---------------------------------------------------------------------------
# 矩阵输出
# ---------------------------------------------------------------------------


def print_matrix(results: list[CellResult]) -> None:
    """打印矩阵汇总。"""
    grid: dict[tuple[str, str], CellResult] = {
        (r.data_shape, r.comp): r for r in results
    }

    def _table(title: str, val_fn) -> None:
        print(f"\n  ■ {title}")
        print(f"  {'':>16}", end="")
        for c in COMPS:
            print(f" │ {c:>12}", end="")
        print()
        print(f"  {'─' * 16}", end="")
        for _ in COMPS:
            print(f"─┼{'─' * 13}", end="")
        print()
        for ds in DATA_SHAPES:
            print(f"  {ds:>16}", end="")
            for c in COMPS:
                cell = grid.get((ds, c))
                v = val_fn(cell)
                print(f" │ {v:>12}", end="")
            print()

    _table(
        "发送吞吐 (records/s)",
        lambda c: f"{c.send_rate:,.0f}" if c and c.send_rate > 0 else "—",
    )
    _table(
        "接收吞吐 (records/s)",
        lambda c: f"{c.recv_rate:,.0f}" if c and c.recv_rate > 0 else "—",
    )
    _table(
        "丢包率 (%)",
        lambda c: f"{c.loss_pct:.1f}%" if c and c.send_frames > 0 else "—",
    )
    _table(
        "Payload 大小 (bytes)",
        lambda c: f"{c.payload_size:,}" if c and c.payload_size > 0 else "—",
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="PulseMQ 全参数 PUB 基准测试")
    parser.add_argument(
        "--records", type=int, default=1_000_000, help="每组总记录数 (默认 1,000,000)"
    )
    parser.add_argument(
        "--port", type=int, default=58555, help="基础端口号 (默认 58555)"
    )
    args = parser.parse_args()

    loop_type = install_event_loop()

    print(f"{'═' * 100}")
    print(f"  PulseMQ 全参数 PUB 基准测试")
    print(f"{'═' * 100}")
    print(f"  数据形态: {', '.join(DATA_SHAPES)}")
    print(f"  压缩格式: {', '.join(COMPS)}")
    print(f"  每组记录: {args.records:,}")
    print(f"  批量大小: {BATCH_SIZE}")
    print(f"  事件循环: {loop_type}")
    print()

    results = asyncio.run(run_all(args.records, args.port))
    print_matrix(results)
    print(f"\n{'═' * 100}")


if __name__ == "__main__":
    main()
