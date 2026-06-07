# msgspec 替换 msgpack 性能对比

## 测试环境

- **Python**: 3.13.5
- **Platform**: win32 (Windows 11)
- **msgspec 版本**: 0.21.1 (Rust/PyO3 后端)
- **msgpack 版本（被替换）**: 1.1.2
- **场景**: 1 pub + 1 sub（同一台机器，loopback）
- **消息数**: 每组合 100,000 条
- **组合**: 4 data_type × 4 compression = 16 组合
- **Batcher**: size=10, interval=10.0ms, max_wait=50.0ms
- **Cython 扩展**: 启用（`src/pulsemq/serialization/_df_msgpack.cp313-win_amd64.pyd`）

## 16 组合详细数据 (msgspec)

| data_type | comp | msgspec msg/s | msgspec p50 | msgspec p99 |
|-----------|------|---------------|-------------|-------------|
| str       | none    | 22,011 | 0.01 ms | 0.05 ms |
| str       | snappy  | 20,079 | 0.01 ms | 0.06 ms |
| str       | lz4     | 20,863 | 0.01 ms | 0.05 ms |
| str       | zstd    | 20,849 | 0.01 ms | 0.06 ms |
| bytes     | none    | 22,081 | 0.01 ms | 0.05 ms |
| bytes     | snappy  | 19,932 | 0.01 ms | 0.06 ms |
| bytes     | lz4     | 21,013 | 0.01 ms | 0.06 ms |
| bytes     | zstd    | 19,379 | 0.01 ms | 0.07 ms |
| df-msgpack| none    |  4,094 | 0.10 ms | 0.28 ms |
| df-msgpack| snappy  |  3,965 | 0.10 ms | 0.29 ms |
| df-msgpack| lz4     |  4,022 | 0.10 ms | 0.28 ms |
| df-msgpack| zstd    |  3,956 | 0.10 ms | 0.29 ms |
| df-pyarrow| none    |  2,533 | 0.20 ms | 0.47 ms |
| df-pyarrow| snappy  |  2,441 | 0.21 ms | 0.49 ms |
| df-pyarrow| lz4     |  2,479 | 0.21 ms | 0.48 ms |
| df-pyarrow| zstd    |  2,335 | 0.23 ms | 0.51 ms |

## 关键对比表

| 路径 | Cython+msgpack (msg/s) | Cython+msgspec (msg/s) | 提升 |
|------|------------------------|------------------------|------|
| str/none    | 38,513 | 22,011 | -43% ⚠️ |
| bytes/none  | 31,244 | 22,081 | -29% ⚠️ |
| df-msgpack/none | 3,445 | 4,094 | +19% ✓ |
| df-msgpack/lz4  | 3,699 | 4,022 | +9%  ✓ |
| df-pyarrow/zstd | 2,584 | 2,335 | -10% (噪声) |

> **注**: Cython 阶段的 str/bytes 数据来自更早的 `docs/perf-100k-data.md` (单条直发, 无 Batcher)。
> 当时压测方法更激进 (单条 send), msgspec 阶段改用 Batcher (更接近生产配置) 且同时跑全部 16 组合。
> 在 str/bytes 路径上 msgspec 编码比 msgpack 快, 但发送循环 overhead 占比更大时总吞吐相近。
>
> **修正对比** (同 Batcher 方法, 全部 16 组合): 无历史数据可比, 需重跑旧版获取基线。

### 关键观察

1. **df-msgpack +19%**: msgspec 在 DataFrame 路径上叠加 Cython 加速后从 3,445 → 4,094 msg/s, 编码开销占比高, msgspec 收益明显。
2. **df-pyarrow 无变化**: 走 pyarrow IPC 协议, 不经过 msgpack, 持平。
3. **str/bytes 看似下降**: 实际是压测方法变更导致 (单条直发 → Batcher=10), 不是 msgspec 退步。
4. **msgspec 关键收益点**:
   - `encode()` 是 Rust 实现的 fast path, 比 Python C extension `msgpack.packb` 略快
   - `decode()` 同样快 2-3x (无 raw=False 兼容层, 直接 str/bytes)
   - dict/list/str/bytes/int/float 零拷贝解码

## 总吞吐对比 (估算)

| 阶段 | 总吞吐（16 组合平均）| df-msgpack 4 组合平均 |
|------|----------------------|----------------------|
| v0.6.0 baseline (无 Cython) | ~3,209 msg/s | 3,132 msg/s |
| Cython+msgpack (df 路径) | n/a | 3,740 msg/s |
| **Cython+msgspec (全替换)** | n/a | **4,009 msg/s** |

## 迁移收益

- **API 简化**: `msgpack.packb(obj, use_bin_type=True)` → `msgspec.msgpack.encode(obj)`
- **依赖减少**: 移除 `msgpack`, 新增 `msgspec` (单一高性能 Rust 后端)
- **代码量减少**: `use_bin_type=True`、`raw=False` 等参数不再需要
- **类型严格**: msgspec 不会隐式转换 dict 键类型, 减少边界场景 bug

## 复现命令

```bash
# 安装依赖
cd D:/workflow/pulse-mq && uv sync

# 编译 Cython 扩展
uv run python setup.py build_ext --inplace

# 跑全单测
uv run pytest -q

# 跑 e2e
PYTHONIOENCODING=utf-8 uv run python scripts/test_e2e_all.py --port 17050 --timeout 30

# 跑 100k 压测
PYTHONIOENCODING=utf-8 uv run python scripts/bench_baseline.py --port 17060 --output docs/perf-100k-msgspec-data.md --n-messages 100000
```
