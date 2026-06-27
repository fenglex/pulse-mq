"""DataFrame 批量行情性能基准 — 每帧 1000 条记录。

模拟行情字段：symbol, open, high, low, close, volume, turnover
每帧一个 DataFrame（1000 行），测试 ser×comp 全矩阵。
用法：uv run python bench_df_batch.py [--duration 5]
"""
from __future__ import annotations

import argparse
import asyncio
import random
import socket
import sys
import time

import pandas as pd

sys.path.insert(0, "src")

from pulsemq import Server
from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.protocol.frames import decode, encode
from pulsemq.protocol.msg_type import DataType

# ---- 行情字段 ----
SYMBOLS = ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM", "V", "JNJ"]
_BASE = {"AAPL": 180, "GOOG": 140, "MSFT": 420, "AMZN": 185, "TSLA": 250,
         "NVDA": 900, "META": 500, "JPM": 200, "V": 280, "JNJ": 160}
BATCH = 1000  # 每帧行数


def make_batch(seq: int) -> pd.DataFrame:
    """生成 1000 行行情 DataFrame。"""
    rows = []
    for _ in range(BATCH):
        sym = random.choice(SYMBOLS)
        base = _BASE[sym]
        open_p = round(base + random.uniform(-1, 1), 2)
        high_p = round(open_p + random.uniform(0, 0.5), 2)
        low_p = round(open_p - random.uniform(0, 0.5), 2)
        close_p = round(random.uniform(low_p, high_p), 2)
        volume = random.randint(100, 100_000)
        turnover = round(volume * close_p, 2)
        rows.append({"symbol": sym, "open": open_p, "high": high_p,
                     "low": low_p, "close": close_p, "volume": volume,
                     "turnover": turnover})
    return pd.DataFrame(rows)


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
                          "sub", "pw", client_id="df-cons")
    prod = ProducerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
                          "pub", "pw", client_id="df-prod")
    await cons.start()
    await prod.start()
    await asyncio.sleep(0.2)

    # ---- 全局 collector ----
    _bench = {"active": False, "rts": None, "lts": None}

    def on_msg(m) -> None:
        if _bench["active"]:
            _bench["rts"].append(time.monotonic())
            _bench["lts"].append(time.time_ns() - m.timestamp_ns)

    await cons.subscribe("market.*", on_msg)
    await asyncio.sleep(0.3)

    print(f"\n{'='*105}")
    print(f"  DataFrame 批量行情性能 ({BATCH} 行/帧)  3 ser x 4 comp = 12 组合")
    print(f"  字段: symbol, open, high, low, close, volume, turnover")
    print(f"{'='*105}")
    hdr = (f"{'序列化':>8} {'压缩':>6} {'帧数':>6} {'记录数':>10}  "
           f"{'帧/s':>10} {'记录/s':>12}  {'P50(ms)':>8} {'P95(ms)':>8} {'P99(ms)':>8}")
    print(hdr)
    print(f"{'-'*105}")

    results = []
    for ser in SERS:
        for comp in COMPS:
            received_ts: list[float] = []
            latencies_ns: list[int] = []

            _bench["rts"] = received_ts
            _bench["lts"] = latencies_ns

            _bench["active"] = False

            # 预热 5 帧
            for i in range(5):
                df = make_batch(i)
                # msgpack/json 不能直接序列化 DataFrame，转 list[dict]；
                # pyarrow 可以直接序列化 DataFrame。
                prep = df if ser == "pyarrow" else df.to_dict(orient="records")
                frame = encode("market.tick", prep, serializer=ser, compression=comp,
                               record_count=BATCH, data_type=DataType.DATAFRAME)
                await prod._transport.send(b"", frame, role="consumer")
            await asyncio.sleep(0.3)

            # 正式测试
            _bench["active"] = True
            frames_sent = 0
            records_sent = 0
            seq = 0
            t0 = time.monotonic()
            while time.monotonic() - t0 < args.duration:
                df = make_batch(seq)
                prep = df if ser == "pyarrow" else df.to_dict(orient="records")
                frame = encode("market.tick", prep, serializer=ser, compression=comp,
                               record_count=BATCH, data_type=DataType.DATAFRAME)
                await prod._transport.send(b"", frame, role="consumer")
                frames_sent += 1
                records_sent += BATCH
                seq += 1
                if frames_sent % 100 == 0:
                    await asyncio.sleep(0)
            _bench["active"] = False

            await asyncio.sleep(1.0)

            r = {"ser": ser, "comp": comp, "frames": frames_sent, "records": records_sent}
            if received_ts:
                ts_first = received_ts[0]
                ts_last = received_ts[-1]
                run_elapsed = max(ts_last - ts_first, 1e-9)
                frame_rate = len(received_ts) / run_elapsed
                record_rate = len(received_ts) * BATCH / run_elapsed
                latencies_ns.sort()

                def pct(p):
                    i = min(len(latencies_ns) - 1, int(p * len(latencies_ns)))
                    return latencies_ns[i] / 1e6

                r.update(frames_recv=len(received_ts), frame_rate=frame_rate,
                         record_rate=record_rate, p50=pct(0.50), p95=pct(0.95),
                         p99=pct(0.99), max=latencies_ns[-1] / 1e6)
            else:
                r.update(frames_recv=0, frame_rate=0, record_rate=0,
                         p50=0, p95=0, p99=0, max=0)

            results.append(r)
            print(f"  {ser:>8} {comp:>6} {r['frames']:>6} {r['records']:>10,}  "
                  f"{r.get('frame_rate',0):>10.1f} {fmt_rate(r.get('record_rate',0)):>12}  "
                  f"{r.get('p50',0):>8.3f} {r.get('p95',0):>8.3f} {r.get('p99',0):>8.3f}")

    await prod.stop()
    await cons.stop()
    await srv.stop()
    print(f"{'-'*105}")

    for ser in SERS:
        rows = [r for r in results if r["ser"] == ser]
        avg_fr = sum(r.get("frame_rate", 0) for r in rows) / len(rows)
        avg_rr = sum(r.get("record_rate", 0) for r in rows) / len(rows)
        avg_p50 = sum(r.get("p50", 0) for r in rows) / len(rows)
        print(f"  {ser:>8}  →  {avg_fr:>8.1f} 帧/s  {fmt_rate(avg_rr):>12}  records/s  avg P50={avg_p50:.3f}ms")

    best = max(results, key=lambda r: r.get("record_rate", 0))
    print(f"\n  最佳组合: {best['ser']}+{best['comp']}  →  {fmt_rate(best['record_rate'])}  records/s  "
          f"P50={best['p50']:.3f}ms")
    print(f"{'='*105}\n")


if __name__ == "__main__":
    asyncio.run(main())
