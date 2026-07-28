"""PulseMQ 全面基准：协议层微基准 + 端到端吞吐矩阵 + 扇出基准。

用法:
    uv run python scripts/bench_full.py                  # 跑全部，结果写 bench_results.md
    uv run python scripts/bench_full.py --duration 10    # 端到端/扇出每场景秒数（默认 10）
    uv run python scripts/bench_full.py --iters 300      # 协议层每组合迭代次数（默认 300）

结果: 写入项目根 bench_results.md（每完成一部分即刷新，中途中断也有部分结果）。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import platform
import socket
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

import pulsemq
from pulsemq import Server
from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.protocol import frames
from pulsemq.protocol.msg_type import DataType

# 结果文件：项目根目录
RESULT_FILE = Path(__file__).resolve().parent.parent / "bench_results.md"
# 状态文件：累积各组合结果，支持逐组合增量运行
STATE_FILE = Path(__file__).resolve().parent.parent / "bench_state.json"

# Part2 端到端矩阵：数据类型 × 序列化器 × 压缩 的完整合法组合（28 组）
E2E_MATRIX = [
    (dtype, ser, comp)
    for (dtype, ser) in [
        ("dict", "msgpack"),
        ("dict", "json"),
        ("dataframe", "msgpack"),
        ("dataframe", "json"),
        ("dataframe", "pyarrow"),
        ("str", "str"),
        ("bytes", "bytes"),
    ]
    for comp in ["none", "snappy", "lz4", "zstd"]
]
# Part3 扇出场景
FANOUT_SCENES = [1, 5, 10]


def _port() -> int:
    """分配一个空闲端口。"""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def _pct(sorted_list: list[float], pct: float) -> float:
    """已排序列表取分位（线性索引，足够基准用途）。"""
    if not sorted_list:
        return 0.0
    idx = min(len(sorted_list) - 1, int(len(sorted_list) * pct))
    return sorted_list[idx]


def _make_server() -> Server:
    """构造一个基准用 Server（固定 token 避免写 token 文件）。"""
    dp, cp, ap = _port(), _port(), _port()
    return Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"pub": "s", "sub": "s"},
        admin_token="bench-token",  # 禁用随机生成，避免写 pulsemq_admin.token
    )


# ---------------------------------------------------------------------------
# Part 1: 协议层微基准（无 ZMQ，隔离协议性能）
# ---------------------------------------------------------------------------

def run_protocol(iters: int, index: int | None = None) -> list[dict]:
    print(f"[Part 1] 协议层微基准（iters={iters}/组合）...", flush=True)
    rows: list[dict] = []
    # 数据类型 × 序列化器 × 压缩 矩阵
    payloads = [
        ("dict", {"seq": 0, "val": 1.5, "sym": "AAPL", "ok": True}, DataType.DICT, 1),
        ("dataframe", pd.DataFrame({
            "seq": list(range(1000)),
            "val": [i * 1.5 for i in range(1000)],
            "sym": ["AAPL"] * 1000,
        }), DataType.DATAFRAME, 1000),
    ]
    serializers = ["msgpack", "json", "pyarrow"]
    compressions = ["none", "snappy", "lz4", "zstd"]
    # 扁平化组合列表，供 --index 寻址
    combos = [(dn, d, dt, rc, ser, comp)
              for (dn, d, dt, rc) in payloads
              for ser in serializers
              for comp in compressions]
    total = len(combos)
    indices = [index] if index is not None else range(total)
    for i in indices:
        dtype_name, data, dtype, rc, ser, comp = combos[i]
        # 预编码取帧大小；失败则记录错误跳过
        try:
            frame = frames.encode("bench.topic", data, serializer=ser,
                                  compression=comp, data_type=dtype, record_count=rc)
        except Exception as e:  # noqa: BLE001 - 基准不应因单组合失败中断
            rows.append({"data_type": dtype_name, "serializer": ser,
                         "compression": comp,
                         "error": f"{type(e).__name__}: {e}"[:80]})
            print(f"  [{i+1}/{total}] {dtype_name}/{ser}/{comp}: ERR {type(e).__name__}",
                  flush=True)
            continue
        # encode 耗时
        t0 = time.perf_counter_ns()
        for _ in range(iters):
            frames.encode("bench.topic", data, serializer=ser,
                          compression=comp, data_type=dtype, record_count=rc)
        enc_us = (time.perf_counter_ns() - t0) / iters / 1000
        # decode 耗时
        t0 = time.perf_counter_ns()
        for _ in range(iters):
            frames.decode(frame)
        dec_us = (time.perf_counter_ns() - t0) / iters / 1000
        # decode_header 耗时（服务端路由路径）
        t0 = time.perf_counter_ns()
        for _ in range(iters):
            frames.decode_header(frame)
        hdr_us = (time.perf_counter_ns() - t0) / iters / 1000
        rows.append({
            "data_type": dtype_name, "serializer": ser, "compression": comp,
            "frame_bytes": len(frame),
            "encode_us": round(enc_us, 2),
            "decode_us": round(dec_us, 2),
            "decode_header_us": round(hdr_us, 3),
        })
        print(f"  [{i+1}/{total}] {dtype_name}/{ser}/{comp}: "
              f"enc={enc_us:.2f}us dec={dec_us:.2f}us hdr={hdr_us:.3f}us "
              f"size={len(frame)}B", flush=True)
    return rows


# ---------------------------------------------------------------------------
# Part 2: 端到端吞吐矩阵（producer -> server -> consumer）
# ---------------------------------------------------------------------------

async def run_e2e(duration: float, index: int | None = None) -> list[dict]:
    print(f"\n[Part 2] 端到端吞吐矩阵（{duration}s/组合，复用单 Server）...", flush=True)
    rows: list[dict] = []
    srv = _make_server()
    await srv.start()
    try:
        dp, cp = srv._data_endpoint, srv._control_endpoint
        cons = ConsumerClient(dp, cp, "sub", "s")
        await cons.start()
        state = {"frames": 0, "records": 0, "lats": []}

        def on_msg(msg):
            state["frames"] += 1
            state["records"] += msg.record_count
            state["lats"].append((time.time_ns() - msg.timestamp_ns) / 1_000_000)

        await cons.subscribe("bench.*", on_msg)

        prod = ProducerClient(dp, cp, "pub", "s")
        await prod.start()

        matrix = E2E_MATRIX
        total = len(matrix)
        indices = [index] if index is not None else range(total)

        async def produce(payload, ser, comp, dtype, rc, dtype_name):
            end = time.monotonic() + duration
            sent = 0
            while time.monotonic() < end:
                # 背压：未处理帧数超阈值时等待 consumer 追上，避免 ZMQ 队列积压导致 send 阻塞
                if sent - state["frames"] > 100:
                    await asyncio.sleep(0.001)
                    continue
                frame = frames.encode("bench.topic", payload, serializer=ser,
                                      compression=comp, data_type=dtype, record_count=rc)
                await prod._transport.send(b"", frame, role="consumer")
                sent += 1
                if sent % 1000 == 0:
                    # 强制让出 event loop，避免 produce 协程饿死 server _data_loop
                    # 与 consumer _recv_loop（pyzmq DONTWAIT send 成功时 await 不 yield）
                    await asyncio.sleep(0)
                if sent % 20000 == 0:
                    print(f"    ... {dtype_name}/{ser}/{comp} sent={sent:,}", flush=True)

        for i in indices:
            dtype_name, ser, comp = matrix[i]
            if dtype_name == "dict":
                payload = {"seq": 0, "val": 1.0}
                dtype = DataType.DICT
                rc = 1
            elif dtype_name == "dataframe":
                payload = pd.DataFrame({
                    "seq": list(range(1000)),
                    "val": [j * 1.5 for j in range(1000)],
                })
                dtype = DataType.DATAFRAME
                rc = 1000
            elif dtype_name == "str":
                payload = "hello pulse-mq benchmark " * 10
                dtype = DataType.STR
                rc = 1
            else:  # bytes
                payload = b"hello pulse-mq benchmark " * 10
                dtype = DataType.BYTES
                rc = 1
            state["frames"] = 0
            state["records"] = 0
            state["lats"] = []

            t0 = time.monotonic()
            await produce(payload, ser, comp, dtype, rc, dtype_name)
            # 等待 consumer 处理完积压消息（每秒检查帧数是否还在增长，最多等 5 秒）
            prev = state["frames"]
            for _ in range(5):
                await asyncio.sleep(1.0)
                curr = state["frames"]
                if curr == prev:
                    break
                prev = curr
            elapsed = time.monotonic() - t0
            fr = state["frames"]
            rec = state["records"]
            lats = sorted(state["lats"])
            p50 = _pct(lats, 0.50)
            p99 = _pct(lats, 0.99)
            mx = lats[-1] if lats else 0.0
            fps = fr / elapsed if elapsed else 0
            rps = rec / elapsed if elapsed else 0
            rows.append({
                "data_type": dtype_name, "serializer": ser, "compression": comp, "rc": rc,
                "frames": fr, "records": rec, "elapsed": round(elapsed, 2),
                "frames_per_s": round(fps),
                "records_per_s": round(rps),
                "p50_ms": round(p50, 3), "p99_ms": round(p99, 3), "max_ms": round(mx, 3),
            })
            print(f"  [{i+1}/{total}] {dtype_name}/{ser}/{comp}: "
                  f"{fps:,.0f} f/s, {rps:,.0f} r/s, p50={p50:.3f}ms p99={p99:.3f}ms",
                  flush=True)
        await prod.stop()
        await cons.stop()
    finally:
        await srv.stop()
    return rows


# ---------------------------------------------------------------------------
# Part 3: 扇出基准（1 producer -> N consumer）
# ---------------------------------------------------------------------------

async def run_fanout(duration: float, index: int | None = None) -> list[dict]:
    print(f"\n[Part 3] 扇出基准（1->N, msgpack/none, dict, {duration}s/场景）...", flush=True)
    rows: list[dict] = []
    scenes = FANOUT_SCENES
    indices = [index] if index is not None else range(len(scenes))
    for si in indices:
        n = scenes[si]
        srv = _make_server()
        await srv.start()
        try:
            dp, cp = srv._data_endpoint, srv._control_endpoint
            counts = [0] * n
            consumers: list[ConsumerClient] = []
            for i in range(n):
                c = ConsumerClient(dp, cp, "sub", "s")
                await c.start()

                def mk_cb(idx):
                    def cb(msg):
                        counts[idx] += 1
                    return cb

                await c.subscribe("bench.*", mk_cb(i))
                consumers.append(c)
            prod = ProducerClient(dp, cp, "pub", "s")
            await prod.start()
            await asyncio.sleep(0.3)  # 订阅同步

            async def produce():
                end = time.monotonic() + duration
                sent = 0
                while time.monotonic() < end:
                    await prod.publish("bench.topic", {"seq": 0, "val": 1.0})
                    sent += 1
                    if sent % 1000 == 0:
                        await asyncio.sleep(0)  # 防止饿死 server/consumer 协程

            t0 = time.monotonic()
            await produce()
            await asyncio.sleep(1.0)  # 收尾
            elapsed = time.monotonic() - t0
            total = sum(counts)
            rps = total / elapsed if elapsed else 0
            per = [round(c / elapsed) if elapsed else 0 for c in counts]
            rows.append({
                "consumers": n, "total_delivered": total,
                "elapsed": round(elapsed, 2),
                "total_records_per_s": round(rps),
                "per_consumer_rps": per,
            })
            print(f"  N={n}: total={total:,} ({rps:,.0f} r/s), per-consumer={per}",
                  flush=True)
            await prod.stop()
            for c in consumers:
                await c.stop()
        finally:
            await srv.stop()
    return rows


# ---------------------------------------------------------------------------
# 报告生成
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    """加载累积状态；不存在则初始化。"""
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"meta": {}, "part1": [], "part2": [], "part3": []}


def _save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2),
                          encoding="utf-8")


def _merge_rows(existing: list[dict], key_fields: list[str],
                new_rows: list[dict]) -> list[dict]:
    """按 key 去重合并：新结果覆盖同 key 旧结果，保持既有顺序，新 key 追加末尾。"""
    by_key: dict[tuple, dict] = {tuple(r[k] for k in key_fields): r for r in existing}
    for r in new_rows:
        by_key[tuple(r[k] for k in key_fields)] = r
    merged: list[dict] = []
    seen: set[tuple] = set()
    for r in existing:
        k = tuple(r[k] for k in key_fields)
        if k in seen:
            continue
        seen.add(k)
        merged.append(by_key[k])
    for r in new_rows:
        k = tuple(r[k] for k in key_fields)
        if k not in seen:
            seen.add(k)
            merged.append(r)
    return merged


def write_report(state: dict) -> None:
    meta = state.get("meta", {})
    p1 = state.get("part1", [])
    p2 = state.get("part2", [])
    p3 = state.get("part3", [])
    lines: list[str] = []
    lines.append("# PulseMQ 全面基准报告")
    lines.append("")
    lines.append(f"- 生成时间: {meta.get('generated', '-')}")
    lines.append(f"- Python: {meta.get('python', '-')} | 平台: {meta.get('platform', '-')}")
    lines.append(f"- 端到端/扇出每场景时长: {meta.get('duration', '-')}s | 协议层每组合迭代: {meta.get('iters', '-')}")
    lines.append(f"- pulsemq 版本: {meta.get('version', '-')}")
    lines.append("")
    lines.append("> 单机 localhost 压测，数值仅作量级参考，受机器/负载影响。")
    lines.append("")

    # Part 1
    lines.append("## Part 1: 协议层微基准（无 ZMQ）")
    lines.append("")
    if p1:
        lines.append("| data_type | serializer | compression | frame_bytes | encode_us | decode_us | decode_header_us |")
        lines.append("|---|---|---|---|---|---|---|")
        for r in p1:
            if "error" in r:
                lines.append(f"| {r['data_type']} | {r['serializer']} | {r['compression']} | - | - | - | ERR: {r['error']} |")
            else:
                lines.append(f"| {r['data_type']} | {r['serializer']} | {r['compression']} | {r['frame_bytes']} | {r['encode_us']} | {r['decode_us']} | {r['decode_header_us']} |")
    else:
        lines.append("_未运行_")
    lines.append("")

    # Part 2
    lines.append("## Part 2: 端到端吞吐矩阵（producer→server→consumer）")
    lines.append("")
    if p2:
        lines.append("| data_type | serializer | compression | rc/frame | frames/s | records/s | p50_ms | p99_ms | max_ms |")
        lines.append("|---|---|---|---|---|---|---|---|---|")
        for r in p2:
            lines.append(f"| {r['data_type']} | {r['serializer']} | {r['compression']} | {r['rc']} | {r['frames_per_s']:,} | {r['records_per_s']:,} | {r['p50_ms']} | {r['p99_ms']} | {r['max_ms']} |")
    else:
        lines.append("_未运行_")
    lines.append("")

    # Part 3
    lines.append("## Part 3: 扇出基准（1 producer → N consumer, msgpack/none, dict）")
    lines.append("")
    if p3:
        lines.append("| consumers | total_delivered | total_r/s | per_consumer r/s |")
        lines.append("|---|---|---|---|")
        for r in p3:
            lines.append(f"| {r['consumers']} | {r['total_delivered']:,} | {r['total_records_per_s']:,} | {r['per_consumer_rps']} |")
    else:
        lines.append("_未运行_")
    lines.append("")

    RESULT_FILE.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n报告已写入: {RESULT_FILE}", flush=True)


async def main_async(duration: float, iters: int, part: str, index: int | None) -> None:
    state = _load_state()
    state["meta"] = {
        "duration": duration, "iters": iters,
        "python": platform.python_version(), "platform": platform.platform(),
        "version": pulsemq.__version__,
        "generated": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }
    run_all = part == "all"
    if run_all or part == "1":
        p1 = run_protocol(iters, index if not run_all else None)
        state["part1"] = _merge_rows(state["part1"], ["data_type", "serializer", "compression"], p1)
        _save_state(state)
        write_report(state)
    if run_all or part == "2":
        p2 = await run_e2e(duration, index if not run_all else None)
        state["part2"] = _merge_rows(state["part2"], ["data_type", "serializer", "compression"], p2)
        _save_state(state)
        write_report(state)
    if run_all or part == "3":
        p3 = await run_fanout(duration, index if not run_all else None)
        state["part3"] = _merge_rows(state["part3"], ["consumers"], p3)
        _save_state(state)
        write_report(state)


def main() -> None:
    p = argparse.ArgumentParser(description="PulseMQ 全面基准")
    p.add_argument("--duration", type=float, default=10.0,
                   help="端到端/扇出每场景秒数（默认 10）")
    p.add_argument("--iters", type=int, default=300,
                   help="协议层每组合迭代次数（默认 300）")
    p.add_argument("--part", choices=["1", "2", "3", "all"], default="all",
                   help="运行部分：1/2/3/all（默认 all）")
    p.add_argument("--index", type=int, default=None,
                   help="单组合索引（Part1:0-23, Part2:0-14, Part3:0-2）；仅 --part 指定单部分时生效")
    args = p.parse_args()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        # 防止 Windows 在长基准期间休眠（ES_CONTINUOUS | ES_SYSTEM_REQUIRED）。
        try:
            import ctypes
            ctypes.windll.kernel32.SetThreadExecutionState(0x80000001)
        except Exception:
            pass
    try:
        asyncio.run(main_async(args.duration, args.iters, args.part, args.index))
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
