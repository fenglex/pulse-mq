# PulseMQ 100k 批量压测数据

**测试环境**: Python 3.13.5, win32

**场景**: 1 pub + 1 sub (同机 loopback), 客户端启用 Batcher, 每组合 N 条消息, pub 顺序发布、sub 异步接收。

**Batcher 配置**: batch_size=10, batch_interval_ms=10.0, batch_max_wait_ms=50.0

**消息数**: 每组合 100000 条

**组合**: 4 data_type × 4 compression = 16 组合

| data_type | compression | throughput (msg/s) | p50 (ms) | p99 (ms) |
|-----------|-------------|-------------------|----------|----------|
| str | none | 38513 | 0.00 | 0.04 |
| str | snappy | 26930 | 0.00 | 0.05 |
| str | lz4 | 29699 | 0.00 | 0.04 |
| str | zstd | 27652 | 0.01 | 0.05 |
| bytes | none | 31244 | 0.00 | 0.04 |
| bytes | snappy | 26804 | 0.00 | 0.05 |
| bytes | lz4 | 29259 | 0.00 | 0.04 |
| bytes | zstd | 26256 | 0.01 | 0.07 |
| df-msgpack | none | 3571 | 0.13 | 0.39 |
| df-msgpack | snappy | 3585 | 0.13 | 0.41 |
| df-msgpack | lz4 | 3625 | 0.13 | 0.39 |
| df-msgpack | zstd | 3633 | 0.13 | 0.39 |
| df-pyarrow | none | 3264 | 0.14 | 0.40 |
| df-pyarrow | snappy | 3161 | 0.14 | 0.42 |
| df-pyarrow | lz4 | 3131 | 0.14 | 0.42 |
| df-pyarrow | zstd | 2959 | 0.16 | 0.44 |
