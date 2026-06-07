# PulseMQ 性能基线

**测试环境**: Python 3.13.5, win32

**场景**: 1 pub + 1 sub（同一台机器，loopback），每组合 N 条消息，pub 顺序发布、sub 异步接收。

**消息数**: 每组合 100000 条

**组合**: 4 data_type × 4 compression = 16 组合

| data_type | compression | throughput (msg/s) | p50 (ms) | p99 (ms) |
|-----------|-------------|-------------------|----------|----------|
| str | none | 21878 | 0.01 | 0.04 |
| str | snappy | 19768 | 0.01 | 0.06 |
| str | lz4 | 23473 | 0.01 | 0.03 |
| str | zstd | 23694 | 0.01 | 0.04 |
| bytes | none | 23899 | 0.01 | 0.03 |
| bytes | snappy | 23172 | 0.01 | 0.04 |
| bytes | lz4 | 23497 | 0.01 | 0.03 |
| bytes | zstd | 23316 | 0.01 | 0.04 |
| df-msgpack | none | 3440 | 0.16 | 0.35 |
| df-msgpack | snappy | 3379 | 0.16 | 0.36 |
| df-msgpack | lz4 | 3400 | 0.16 | 0.35 |
| df-msgpack | zstd | 3383 | 0.16 | 0.35 |
| df-pyarrow | none | 2858 | 0.19 | 0.38 |
| df-pyarrow | snappy | 2761 | 0.20 | 0.39 |
| df-pyarrow | lz4 | 2790 | 0.19 | 0.39 |
| df-pyarrow | zstd | 2655 | 0.21 | 0.40 |
