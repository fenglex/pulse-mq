# Cython df-msgpack 100k 压测结果 (Batcher 启用)

**Cython 启用**: False

**场景**: 1 pub + 1 sub（同一台机器，loopback），df-msgpack 4 压缩组合, 每组合 100000 条

**Batcher**: size=10, interval=10.0ms, max_wait=50.0ms

| data_type | compression | throughput (msg/s) | p50 (ms) | p99 (ms) |
|-----------|-------------|-------------------|----------|----------|
| df-msgpack | none | 2823 | 0.18 | 0.52 |
| df-msgpack | snappy | 2673 | 0.18 | 0.56 |
| df-msgpack | lz4 | 2779 | 0.18 | 0.53 |
| df-msgpack | zstd | 2653 | 0.19 | 0.56 |
