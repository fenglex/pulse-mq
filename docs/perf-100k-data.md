# PulseMQ 性能基线

**测试环境**: Python 3.13.5, win32

**场景**: 1 pub + 1 sub（同一台机器，loopback），每组合 N 条消息，pub 顺序发布、sub 异步接收。

**消息数**: 每组合 100000 条

**组合**: 4 data_type × 4 compression = 16 组合

| data_type | compression | throughput (msg/s) | p50 (ms) | p99 (ms) |
|-----------|-------------|-------------------|----------|----------|
| str | none | 23446 | 0.01 | 0.04 |
| str | snappy | 19628 | 0.01 | 0.06 |
| str | lz4 | 19632 | 0.01 | 0.06 |
| str | zstd | 18758 | 0.01 | 0.06 |
| bytes | none | 19771 | 0.01 | 0.06 |
| bytes | snappy | 18925 | 0.01 | 0.07 |
| bytes | lz4 | 19362 | 0.01 | 0.07 |
| bytes | zstd | 17457 | 0.01 | 0.10 |
| df-msgpack | none | 3132 | 0.17 | 0.40 |
| df-msgpack | snappy | 3225 | 0.17 | 0.38 |
| df-msgpack | lz4 | 3264 | 0.17 | 0.37 |
| df-msgpack | zstd | 3216 | 0.17 | 0.38 |
| df-pyarrow | none | 2822 | 0.19 | 0.39 |
| df-pyarrow | snappy | 2731 | 0.20 | 0.40 |
| df-pyarrow | lz4 | 2749 | 0.20 | 0.40 |
| df-pyarrow | zstd | 2584 | 0.21 | 0.43 |
