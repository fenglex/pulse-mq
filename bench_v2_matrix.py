"""PulseMQ v2 消息类型 x 压缩格式 全矩阵 E2E 验证。
"""
from __future__ import annotations

import asyncio
import socket as _sock
import sys

import pandas as pd

from pulsemq import Server
from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.protocol import frames
from pulsemq.protocol.msg_type import DataType


def _free_port():
    s = _sock.socket()
    s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


_STR = "你好 PulseMQ"
_BYTES = b"\x00\x01\x02\xFF"
_DF = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
_DICT = {"price": 12.345, "symbol": "AAPL", "volume": 1_000_000}


def make_data(dt: str, seq: int):
    if dt == "str":   return f"{_STR} seq={seq}"
    if dt == "bytes": return _BYTES + seq.to_bytes(4, "big")
    if dt == "df":    return _DF.copy()
    if dt == "dict":  return _DICT.copy() | {"seq": seq}
    raise ValueError(dt)


SERS = ["msgpack", "json", "str", "bytes", "pyarrow"]
COMPS = ["none", "snappy", "lz4", "zstd"]
DTYPES = ["str", "bytes", "df", "dict"]


def valid(ser: str, dt: str) -> bool:
    if dt == "str" and ser != "str":   return False
    if dt == "bytes" and ser != "bytes": return False
    if dt in ("df", "dict") and ser in ("str", "bytes"): return False
    return True


def assert_ok(got, exp, dt: str, ser: str):
    if dt == "df":
        assert isinstance(got, pd.DataFrame), f"df type lost: {type(got).__name__}"
        pd.testing.assert_frame_equal(
            got.reset_index(drop=True), exp.reset_index(drop=True),
            check_dtype=False, check_like=True)
        return
    assert got == exp, f"payload mismatch: got={str(got)[:120]} != exp={str(exp)[:120]}"


async def run():
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}",
                 control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}",
                 credentials={"pub": "pw", "sub": "pw"}, admin_token="")
    await srv.start()
    await asyncio.sleep(0.3)

    try:
        cons = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
                              "sub", "pw", client_id="mx-sub")
        prod = ProducerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
                              "pub", "pw", client_id="mx-prod")
        await cons.start()
        await prod.start()
        await asyncio.sleep(0.2)

        total = passed = 0
        fails: list[str] = []

        for dt in DTYPES:
            for ser in SERS:
                for comp in COMPS:
                    if not valid(ser, dt):
                        continue
                    total += 1
                    lbl = f"{dt:>5} / {ser:>8} / {comp:>6}"

                    exp = make_data(dt, seq=total)
                    prep = exp
                    dtf = DataType.UNKNOWN
                    rc = 1
                    if dt == "df":
                        dtf = DataType.DATAFRAME
                        rc = len(exp)
                        if ser != "pyarrow":
                            prep = exp.to_dict(orient="records")
                    elif dt == "dict":
                        dtf = DataType.DICT

                    frame = frames.encode("t.ok", prep, serializer=ser,
                                          compression=comp, record_count=rc,
                                          data_type=dtf)
                    got: list = []
                    await cons.subscribe("t.*", lambda m: got.append(m))
                    await asyncio.sleep(0.15)

                    await prod._transport.send(b"", frame, role="consumer")
                    await asyncio.sleep(1.5)

                    if not got:
                        fails.append(f"{lbl}  timeout")
                        print(f"  FAIL  {lbl}  timeout")
                    else:
                        try:
                            assert_ok(got[0].payload, exp, dt, ser)
                            assert got[0].serializer == ser
                            assert got[0].compression == comp
                            passed += 1
                            print(f"  PASS  {lbl}")
                        except AssertionError as e:
                            fails.append(f"{lbl}  {e}")
                            print(f"  FAIL  {lbl}  {e}")

                    cons._subscriptions.clear()
                    await asyncio.sleep(0.1)

        await prod.stop()
        await cons.stop()
    finally:
        await srv.stop()

    print(f"\n{'='*50}")
    print(f"  total={total}  passed={passed}  failed={len(fails)}")
    if fails:
        for f in fails:
            print(f"  FAIL: {f}")
    else:
        print(f"  ALL PASSED")
    return passed == total


def main():
    ok = asyncio.run(run())
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()