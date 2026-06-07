# PulseMQ 性能基线

**测试环境**: Python 3.13.5, win32

**场景**: 1 pub + 1 sub（同一台机器，loopback），每组合 N 条消息，pub 顺序发布、sub 异步接收。

**消息数**: 每组合 100000 条

**组合**: 5 data_type × 4 compression = 20 组合

| data_type | compression | throughput (msg/s) | p50 (ms) | p99 (ms) |
|-----------|-------------|-------------------|----------|----------|
| str | none | 21689 | 0.01 | 0.05 |
| str | snappy | 18891 | 0.01 | 0.06 |
| str | lz4 | 19778 | 0.01 | 0.05 |
| str | zstd | 19089 | 0.01 | 0.06 |
| bytes | none | 20342 | 0.01 | 0.05 |
| bytes | snappy | 18450 | 0.01 | 0.06 |
| bytes | lz4 | 19661 | 0.01 | 0.05 |
| bytes | zstd | 18083 | 0.01 | 0.07 |
| df-json | none | 2674 | 0.20 | 0.47 |
| df-json | snappy | 2644 | 0.21 | 0.46 |
| df-json | lz4 | 2651 | 0.21 | 0.46 |
| df-json | zstd | 2579 | 0.22 | 0.45 |
| df-msgpack | none | 2713 | 0.20 | 0.46 |
| df-msgpack | snappy | 2654 | 0.20 | 0.46 |
| df-msgpack | lz4 | 2682 | 0.20 | 0.45 |
| df-msgpack | zstd | 2627 | 0.21 | 0.48 |
| df-pyarrow | none | 2326 | 0.23 | 0.48 |
| df-pyarrow | snappy | 2279 | 0.24 | 0.47 |
| df-pyarrow | lz4 | 2299 | 0.23 | 0.47 |
| df-pyarrow | zstd | 2158 | 0.26 | 0.52 |
