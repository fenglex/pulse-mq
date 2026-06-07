# Cython df-msgpack 100k 压测结果 (Batcher 启用)

**Cython 启用**: True

**场景**: 1 pub + 1 sub（同一台机器，loopback），df-msgpack 4 压缩组合, 每组合 100000 条

**Batcher**: size=10, interval=10.0ms, max_wait=50.0ms

| data_type | compression | throughput (msg/s) | p50 (ms) | p99 (ms) |
|-----------|-------------|-------------------|----------|----------|
| df-msgpack | none | 3445 | 0.10 | 0.40 |
| df-msgpack | snappy | 3984 | 0.09 | 0.29 |
| df-msgpack | lz4 | 3699 | 0.09 | 0.40 |
| df-msgpack | zstd | 3831 | 0.09 | 0.34 |
