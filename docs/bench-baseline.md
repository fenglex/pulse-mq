# PulseMQ 性能基线

**测试环境**: Python 3.13.5, win32

**场景**: 1 pub + 1 sub（同一台机器，loopback），每组合 N 条消息，pub 顺序发布、sub 异步接收。

**消息数**: 每组合 1000 条

**组合**: 4 data_type × 4 compression = 16 组合

| data_type | compression | throughput (msg/s) | p50 (ms) | p99 (ms) |
|-----------|-------------|-------------------|----------|----------|
| str | none | 2858 | 0.01 | 0.04 |
| str | snappy | 2699 | 0.01 | 0.06 |
| str | lz4 | 2672 | 0.01 | 0.06 |
| str | zstd | 2812 | 0.01 | 0.05 |
| bytes | none | 2701 | 0.01 | 0.01 |
| bytes | snappy | 2880 | 0.01 | 0.03 |
| bytes | lz4 | 2612 | 0.01 | 0.03 |
| bytes | zstd | 2762 | 0.01 | 0.06 |
| df-msgpack | none | 1495 | 0.20 | 0.45 |
| df-msgpack | snappy | 1462 | 0.20 | 0.43 |
| df-msgpack | lz4 | 1509 | 0.19 | 0.44 |
| df-msgpack | zstd | 1449 | 0.19 | 0.57 |
| df-pyarrow | none | 1413 | 0.21 | 0.42 |
| df-pyarrow | snappy | 1411 | 0.21 | 0.39 |
| df-pyarrow | lz4 | 1410 | 0.21 | 0.40 |
| df-pyarrow | zstd | 1370 | 0.23 | 0.38 |

## 并发压测 (4 pub × 4 sub, str/none)

- 收/发: 500/2000 (总收 2003 = 4 sub × 广播)
- 吞吐: 1611 msg/s
- 耗时: 0.31s
