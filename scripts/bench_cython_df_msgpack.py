"""Cython 优化前后 micro-benchmark。

测: 1000 行 × 5 列 DataFrame, 编码 1000 次, 比较 Cython vs 纯 Python 用时.
"""
from __future__ import annotations

import time

import pandas as pd

from pulsemq.serialization._df_msgpack_loader import (
    encode_dataframe_to_msgpack,
    is_using_cython,
)

N_ROWS = 1000
N_COLS = 5
N_ITERS = 1000

df = pd.DataFrame(
    {
        "id": list(range(N_ROWS)),
        "price": [round(100.0 + i * 0.01, 4) for i in range(N_ROWS)],
        "volume": [1000 + i for i in range(N_ROWS)],
        "symbol": [f"SYM{i % 100:03d}" for i in range(N_ROWS)],
        "ts": [time.time()] * N_ROWS,
    }
)

# Warmup
for _ in range(10):
    encode_dataframe_to_msgpack(df)

# Cython / fallback
t0 = time.perf_counter()
for _ in range(N_ITERS):
    encode_dataframe_to_msgpack(df)
cython_time = time.perf_counter() - t0

# Pure Python (to_dict + packb)
import msgpack

t0 = time.perf_counter()
for _ in range(N_ITERS):
    msgpack.packb(df.to_dict(orient="records"), use_bin_type=True)
py_time = time.perf_counter() - t0

print(f"DataFrame: {N_ROWS} rows x {N_COLS} cols, {N_ITERS} iters")
print(f"Pure Python:  {py_time*1000:>8.1f} ms  ({N_ITERS/py_time:>7.0f} ops/s)")
print(f"Cython:       {cython_time*1000:>8.1f} ms  ({N_ITERS/cython_time:>7.0f} ops/s)")
print(f"Speedup:      {py_time/cython_time:>7.2f}x")
print(f"Using Cython: {is_using_cython()}")
