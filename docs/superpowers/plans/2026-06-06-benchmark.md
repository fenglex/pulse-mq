# 全参数 PUB 基准测试 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 重写 `scripts/bench_1m.py`，使用 PulseClient 高级 API 覆盖 5 种数据形态 × 4 种压缩 = 20 个测试单元的 PUB 吞吐基准测试。

**Architecture:** 单文件自包含脚本。每测试单元独立启动 服务端 → SUB 客户端接收 → PUB 客户端发送 100 万记录 → 采集指标 → 关闭 服务端。终端输出进度 + 4 张矩阵表格。

**Tech Stack:** Python 3.12+, asyncio, pyzmq, msgpack, pyarrow, pandas, numpy

---

## File Structure

- **Rewrite:** `scripts/bench_1m.py` — 完整基准测试脚本（约 300 行）

---

### Task 1: 编写完整的 bench_1m.py 脚本

**Files:**
- Rewrite: `scripts/bench_1m.py`

- [ ] **Step 1: 写入完整脚本**

```python
#!/usr/bin/env python3
"""PulseMQ 全参数 PUB 基准测试 — 数据形态 × 序列化 × 压缩。

20 个测试单元（5 data_shape × 4 compression）:
  single_msgpack, batch_msgpack, single_pyarrow, batch_pyarrow, single_raw
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

from pulsemq.client.async_client import PulseClient
from pulsemq.config import ServerConfig
from pulsemq.event_loop import install_event_loop
from pulsemq.protocol.frames import FrameCodec
from pulsemq.server import PulseServer

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

DATA_SHAPES = [
    "single_msgpack",
    "batch_msgpack",
    "single_pyarrow",
    "batch_pyarrow",
    "single_raw",
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
    decode_fails: int = 0
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
# 服务端 生命周期
# ---------------------------------------------------------------------------


async def start_server(port: int) -> tuple[PulseServer, asyncio.Task]:
    """启动独立 服务端。"""
    config = ServerConfig(
        bind=f"tcp://*:{port}",
        xpub_bind=f"tcp://*:{port + 1}",
        auth_enabled=False,
        max_concurrency=200,
        max_batch_size=256,
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
    """停止 服务端。"""
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
# DEALER 排空
# ---------------------------------------------------------------------------


async def drain_dealer(client: PulseClient, timeout: float = 0.05) -> None:
    """排空 DEALER 残留消息。"""
    try:
        while True:
            await asyncio.wait_for(client._dealer.recv_multipart(), timeout=timeout)
    except (asyncio.TimeoutError, Exception):
        pass


# ---------------------------------------------------------------------------
# 序列化格式映射
# ---------------------------------------------------------------------------


def _ser_fmt(data_shape: str) -> str:
    """根据 data_shape 返回序列化格式参数。"""
    if "pyarrow" in data_shape:
        return "pyarrow"
    if "bytes" in data_shape:
        return "none"
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
    """运行单个测试单元。"""
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

    # 1. 启动 服务端
    server, 服务端_task = await start_server(port)

    try:
        # 2. SUB 客户端
        sub_client = PulseClient(
            address=f"tcp://localhost:{port}",
            xpub_address=f"tcp://localhost:{port + 1}",
            heartbeat_interval=30.0,
            recv_timeout=5.0,
            auto_reconnect=False,
            identity=f"bench_sub_{cell_idx}".encode(),
        )
        await sub_client.connect()
        sub_client._sub.setsockopt(zmq.RCVHWM, 5_000_000)

        # 3. 接收协程
        recv_count = [0]
        decode_fails = [0]
        recv_start = [0.0]
        recv_done = asyncio.Event()

        async def recv_loop():
            recv_start[0] = time.monotonic()
            async for msg in sub_client.subscribe(topic):
                recv_count[0] += 1
                if msg.payload is None:
                    decode_fails[0] += 1
                if recv_count[0] >= n_sends:
                    break
            recv_done.set()

        recv_task = asyncio.create_task(recv_loop())
        await asyncio.sleep(0.5)

        # 4. PUB 客户端
        pub_client = PulseClient(
            address=f"tcp://localhost:{port}",
            xpub_address=f"tcp://localhost:{port + 1}",
            heartbeat_interval=30.0,
            recv_timeout=5.0,
            auto_reconnect=False,
            identity=f"bench_pub_{cell_idx}".encode(),
        )
        await pub_client.connect()
        await drain_dealer(pub_client)

        # 5. 计算 payload 大小
        if data_shape == "single_raw":
            sample_data = _RAW_BYTES
        elif is_batch:
            sample_df = gen_batch_df(0, BATCH_SIZE)
            if ser == "msgpack":
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
            if data_shape == "single_raw":
                data = _RAW_BYTES
            elif is_batch:
                df = gen_batch_df(i * BATCH_SIZE, BATCH_SIZE)
                if ser == "msgpack":
                    data = df.to_dict(orient="records")
                else:
                    data = df
            else:
                data = gen_single_dict(i)

            await pub_client.publish(topic, data, format=ser, compression=comp)

        result.send_elapsed = time.monotonic() - t0

        # 排空 PUB DEALER（ACK 等）
        await drain_dealer(pub_client)

        # 7. 等待 SUB 接收完成
        deadline = time.monotonic() + 60
        while not recv_done.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(0.1)

        result.recv_frames = recv_count[0]
        result.decode_fails = decode_fails[0]
        result.recv_elapsed = time.monotonic() - recv_start[0]

        # 取消未完成的接收协程
        if not recv_done.is_set():
            recv_task.cancel()
            try:
                await recv_task
            except (asyncio.CancelledError, Exception):
                pass

        # 8. 断开客户端
        try:
            await pub_client.disconnect()
        except Exception:
            pass
        try:
            await sub_client.disconnect()
        except Exception:
            pass

    except Exception as e:
        print(f"错误: {e}")
    finally:
        # 关闭 服务端
        await stop_server(server, 服务端_task)

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
```

---

### Task 2: 冒烟测试（少量记录验证脚本可运行）

**Files:**
- Verify: `scripts/bench_1m.py`

- [ ] **Step 1: 用少量记录运行冒烟测试**

Run:
```bash
cd D:/workflow/pulse-mq && uv run python scripts/bench_1m.py --records 2000 --port 58555
```

Expected:
- 20 个测试单元依次执行，每单元打印一行进度
- 进度行包含 PUB/SUB 吞吐、丢包率、payload 大小
- 全部完成后打印 4 张矩阵表格（发送吞吐、接收吞吐、丢包率、Payload 大小）
- 无报错或异常退出

- [ ] **Step 2: 检查输出合理性**

验证点：
- batch 模式的 payload 大小应大于 single 模式（批量编码更大数据）
- 压缩后 payload 大小应小于 none 压缩
- 丢包率应接近 0%（少量记录不应丢包）

---

### Task 3: 提交

- [ ] **Step 1: 提交脚本**

```bash
cd D:/workflow/pulse-mq && git add scripts/bench_1m.py && git commit -m "feat: 重写全参数 PUB 基准测试脚本，使用 PulseClient API，覆盖 20 种组合"
```
