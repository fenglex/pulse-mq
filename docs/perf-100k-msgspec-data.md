# PulseMQ 性能基线

**测试环境**: Python 3.13.5, win32

**场景**: 1 pub + 1 sub（同一台机器，loopback），每组合 N 条消息，pub 顺序发布、sub 异步接收。

**消息数**: 每组合 100000 条

**组合**: 4 data_type × 4 compression = 16 组合

| data_type | compression | throughput (msg/s) | p50 (ms) | p99 (ms) |
|-----------|-------------|-------------------|----------|----------|
| str | none | 22011 | 0.01 | 0.05 |
| str | snappy | 20079 | 0.01 | 0.06 |
| str | lz4 | 20863 | 0.01 | 0.05 |
| str | zstd | 20849 | 0.01 | 0.06 |
| bytes | none | 22081 | 0.01 | 0.05 |
| bytes | snappy | 19932 | 0.01 | 0.06 |
| bytes | lz4 | 21013 | 0.01 | 0.06 |
| bytes | zstd | 19379 | 0.01 | 0.07 |
| df-msgpack | none | 4094 | 0.10 | 0.28 |
| df-msgpack | snappy | 3965 | 0.10 | 0.29 |
| df-msgpack | lz4 | 4022 | 0.10 | 0.28 |
| df-msgpack | zstd | 3956 | 0.10 | 0.29 |
| df-pyarrow | none | 2533 | 0.20 | 0.47 |
| df-pyarrow | snappy | 2441 | 0.21 | 0.49 |
| df-pyarrow | lz4 | 2479 | 0.21 | 0.48 |
| df-pyarrow | zstd | 2335 | 0.23 | 0.51 |
