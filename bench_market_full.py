"""全矩阵行情性能基准测试 — 模拟高开低收 + 成交额/量。

所有 ser/comp 组合共享同一对 Producer/Consumer，回调通过全局标志区分轮次。
用法：uv run python bench_market_full.py [--duration 5]
"""
from __future__ import annotations

import argparse
import asyncio
import random
import socket
import sys
import time

sys.path.insert(0, "src")

from pulsemq import Server
from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.protocol.frames import encode
from pulsemq.protocol.msg_type import DataType


# ---- 行情数据 ----

SYMBOLS = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "JNJ"]
_BASE = {"AAPL": 180, "GOOG": 140, "MSFT": 420, "AMZN": 185, "TSLA": 250,
         "NVDA": 900, "META": 500, "JPM": 200, "V": 280, "JNJ": 160}


def make_tick(seq: int) -> dict:
    """模拟行情 tick：高开低收 + 成交额/成交量。"""
    sym = random.choice(SYMBOLS)
    base = _BASE[sym]
    open_p = round(base + random.uniform(-1, 1), 2)
    high_p = round(open_p + random.uniform(0, 0.5), 2)
    low_p = round(open_p - random.uniform(0, 0.5), 2)
    close_p = round(random.uniform(low_p, high_p), 2)
    volume = random.randint(100, 100_000)
    turnover = round(volume * close_p, 2)
    return {
        "symbol": sym,
        "open": open_p,
        "high": high_p,
        "low": low_p,
        "close": close_p,
        "volume": volume,
        "turnover": turnover,
        "seq": seq,
    }


SERS = ["msgpack", "json", "pyarrow"]
COMPS = ["none", "snappy", "lz4", "zstd"]


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


def fmt_rate(n: float) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f} M/s"
    if n >= 1_000:
        return f"{n/1_000:.1f} K/s"
    return f"{n:.1f} /s"


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=5.0)
    args = ap.parse_args()

    dp, cp, ap_ = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap_}",
        credentials={"pub": "pw", "sub": "pw"}, admin_token="",
    )
    await srv.start()
    await asyncio.sleep(0.3)

    cons = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
                          "sub", "pw", client_id="bench-cons")
    prod = ProducerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
                          "pub", "pw", client_id="bench-prod")
    await cons.start()
    await prod.start()
    await asyncio.sleep(0.2)

    # ---- 全局 collector + 重入标志（回调内只能赋值，不能 for/await） ----
    _bench = {"active": False, "rts": None, "lts": None, "seq_filter": None}

    def on_msg(m) -> None:
        if _bench["active"]:
            _bench["rts"].append(time.monotonic())
            _bench["lts"].append(time.time_ns() - m.timestamp_ns)

    await cons.subscribe("market.*", on_msg)
    await asyncio.sleep(0.3)

    print(f"\n{'='*95}")
    print(f"  PulseMQ v2 行情性能全矩阵测试 ({len(SERS)} ser x {len(COMPS)} comp = {len(SERS)*len(COMPS)} 组合)")
    print(f"  行情字段: symbol, open, high, low, close, volume, turnover")
    print(f"{'='*95}")
    hdr = f"{'序列化':>8} {'压缩':>6} {'发送':>8} {'接收':>8}  {'吞吐量':>10}  {'P50(ms)':>8} {'P95(ms)':>8} {'P99(ms)':>8} {'最大(ms)':>8}"
    print(hdr)
    print(f"{'-'*95}")

    results = []
    for ser in SERS:
        for comp in COMPS:
            received_ts: list[float] = []
            latencies_ns: list[int] = []

            # 激活本轮 collector
            _bench["rts"] = received_ts
            _bench["lts"] = latencies_ns

            # 预热 500 条（此时 _bench["active"]=False，不采集）
            for i in range(500):
                frame = encode("market.tick", make_tick(i),
                               serializer=ser, compression=comp, data_type=DataType.DICT)
                await prod._transport.send(b"", frame, role="consumer")
            await asyncio.sleep(0.2)

            # 正式测试：激活采集
            _bench["active"] = True
            seq = 0
            t0 = time.monotonic()
            while time.monotonic() - t0 < args.duration:
                frame = encode("market.tick", make_tick(seq),
                               serializer=ser, compression=comp, data_type=DataType.DICT)
                await prod._transport.send(b"", frame, role="consumer")
                seq += 1
                if seq % 1000 == 0:
                    await asyncio.sleep(0)
            sent = seq
            _bench["active"] = False  # 停止采集

            # 等尾部消息到达
            await asyncio.sleep(1.0)

            # 计算指标
            r = {"ser": ser, "comp": comp, "sent": sent}
            if received_ts:
                ts_first = received_ts[0]
                ts_last = received_ts[-1]
                run_elapsed = max(ts_last - ts_first, 1e-9)
                rate = len(received_ts) / run_elapsed
                latencies_ns.sort()

                def pct(p):
                    i = min(len(latencies_ns) - 1, int(p * len(latencies_ns)))
                    return latencies_ns[i] / 1e6

                r["received"] = len(received_ts)
                r["rate"] = rate
                r["p50"] = pct(0.50)
                r["p95"] = pct(0.95)
                r["p99"] = pct(0.99)
                r["max"] = latencies_ns[-1] / 1e6
            else:
                r.update(received=0, rate=0, p50=0, p95=0, p99=0, max=0)

            results.append(r)
            print(f"  {ser:>8} {comp:>6} {r['sent']:>8} {r['received']:>8}  "
                  f"{fmt_rate(r['rate']):>10}  {r['p50']:>8.3f} {r['p95']:>8.3f} "
                  f"{r['p99']:>8.3f} {r['max']:>8.3f}")

    await prod.stop()
    await cons.stop()
    await srv.stop()
    print(f"{'-'*95}")

    # 按序列化器汇总
    print(f"\n  按序列化器 (all comps averaged):")
    for ser in SERS:
        rows = [r for r in results if r["ser"] == ser and r["rate"] > 0]
        if rows:
            avg_rate = sum(r["rate"] for r in rows) / len(rows)
            avg_p50 = sum(r["p50"] for r in rows) / len(rows)
            print(f"    {ser:>8}  →  {fmt_rate(avg_rate):>10}  avg P50={avg_p50:.3f}ms")

    print(f"\n  按压缩格式 (all sers averaged):")
    for comp in COMPS:
        rows = [r for r in results if r["comp"] == comp and r["rate"] > 0]
        if rows:
            avg_rate = sum(r["rate"] for r in rows) / len(rows)
            avg_p50 = sum(r["p50"] for r in rows) / len(rows)
            print(f"    {comp:>8}  →  {fmt_rate(avg_rate):>10}  avg P50={avg_p50:.3f}ms")

    best = max(results, key=lambda r: r["rate"])
    print(f"\n  最佳组合: {best['ser']}+{best['comp']}  →  {fmt_rate(best['rate'])}  "
          f"P50={best['p50']:.3f}ms")
    print(f"{'='*95}\n")


if __name__ == "__main__":
    asyncio.run(main())
