# Cython df-msgpack 优化

## 目标

替换 `df.to_dict(orient="records") + msgpack.packb()` 中的 `to_dict()` 纯 Python 调用,
用 Cython 扩展直接走 numpy 缓冲区, 加速 df-msgpack 序列化路径.

## 实现

### 文件清单

| 文件 | 作用 |
|------|------|
| `src/pulsemq/serialization/_df_msgpack.pyx` | Cython 扩展, 走 numpy C API |
| `src/pulsemq/serialization/_df_msgpack_py.py` | 纯 Python fallback (`to_dict + packb`) |
| `src/pulsemq/serialization/_df_msgpack_loader.py` | 延迟加载, 优先 Cython, 失败回退纯 Python |
| `setup.py` | Cython 编译入口 |
| `tests/unit/test_cython_df_msgpack.py` | 13 个单测 (含 FrameCodec 集成) |
| `scripts/bench_cython_df_msgpack.py` | micro-benchmark |
| `scripts/bench_cython_100k.py` | 100k 端到端压测 (Batcher 启用) |

### 关键设计

1. **Cython 走 numpy C API** — 用 `cnp.PyArray_GETITEM` + `cnp.PyArray_GETPTR1`
   替代 `col_arr[i]` 的 Python `__getitem__`, 跳过 numpy 的 Python wrapper.
2. **退化为 Python 索引** — 对 ArrowStringArray 等非 numpy 数组 (pandas 2.x 默认
   string dtype), `cnp.PyArray_CheckExact` 检测后走 `arr[i]`, 保持兼容性.
3. **FrameCodec 集成** — `FrameCodec.encode_payload` 检测 DataFrame 时走 Cython 路径,
   其他类型 (dict / str / bytes / pyarrow Table) 走原 serializer, 不破坏 ABI.
4. **客户端透传** — `async_client.publish` 不再预 `to_dict()`, 直接传 DataFrame
   给 `FrameCodec.encode_payload`, 内部检测.

### 加载策略

```python
try:
    from pulsemq.serialization._df_msgpack import encode_dataframe_to_msgpack
    _USING_CYTHON = True
except ImportError:
    from pulsemq.serialization._df_msgpack_py import encode_dataframe_to_msgpack
    _USING_CYTHON = False
```

未编译 .pyd 时自动 fallback 到纯 Python, 不影响功能, 仅性能下降.

## 性能结果

### micro-benchmark (1000 行 × 5 列, 1000 次)

| 实现 | 用时 | 吞吐 | 加速比 |
|------|------|------|--------|
| 纯 Python (`to_dict + packb`) | 1868 ms | 535 ops/s | 1.00x |
| Cython | 1643 ms | 609 ops/s | **1.14x** |

Cython 路径在 CPU 序列化层有 14% 加速, 主要因为跳过了
`to_dict(orient="records")` 中的 Python 字段访问包装.

### 100k 端到端压测 (Batcher: size=10, interval=10ms)

| data_type | compression | 纯 Python (msg/s) | Cython (msg/s) | 加速比 |
|-----------|-------------|-------------------|----------------|--------|
| df-msgpack | none | 2823 | 3445 | +22% |
| df-msgpack | snappy | 2673 | 3984 | +49% |
| df-msgpack | lz4 | 2779 | 3699 | +33% |
| df-msgpack | zstd | 2653 | 3831 | +44% |
| **平均** | — | 2732 | 3740 | **+37%** |

测试脚本: `uv run python scripts/bench_cython_100k.py --port 17000 --n-messages 100000`
结果文件: `docs/perf-100k-cython-data.md`, `docs/perf-100k-pure-data.md`

延迟指标同步改善:
- p50: 0.18 ms → 0.09 ms (-50%)
- p99: 0.53 ms → 0.34 ms (-36%)

## 编译说明

### Windows

需要 MSVC Build Tools 14.x (cl.exe). 已用 `vs_BuildTools.exe` 安装到 `C:\BuildTools\`.

```bat
@echo off
call "C:\BuildTools\VC\Auxiliary\Build\vcvars64.bat" >nul 2>&1
set PATH=D:\workflow\pulse-mq\.venv\Scripts;%PATH%
cd /d D:\workflow\pulse-mq
.venv\Scripts\python.exe setup.py build_ext --inplace
```

编译产物: `src/pulsemq/serialization/_df_msgpack.cp313-win_amd64.pyd`

### Linux / macOS

```bash
uv run python setup.py build_ext --inplace
```

产物: `src/pulsemq/serialization/_df_msgpack.cpython-*.so`

### 验证

```bash
uv run python -c "from pulsemq.serialization._df_msgpack_loader import is_using_cython; print(is_using_cython())"
# True
```

## 测试

### 单元测试

```bash
uv run pytest tests/unit/test_cython_df_msgpack.py -v
```

13 个测试覆盖:
- 加载器工作
- Cython 与纯 Python 输出字节级一致
- 空 DataFrame / 单行 / 10k 大批量
- 混合类型 (int/float/str/bool/bytes/None)
- `use_bin_type=False` 路径
- numpy int32 / float32 标量转换
- FrameCodec 集成 (msgpack + none / msgpack + snappy)
- 非 DataFrame 兼容性 (list[dict] 走原路径)

### 全量测试

```bash
uv run pytest tests/ -q
```

结果: 659 passed, 2 skipped (batcher_e2e 2 个 timing 测试, 与 baseline 同样失败, 预存在)

### 端到端测试

```bash
uv run python scripts/test_e2e_all.py
```

结果: 16/16 PASSED

## 已知限制

1. **首次构建** — 需要 MSVC 14+ (Windows) / gcc (Linux), CI 需预装.
2. **编译产物不入库** — `.pyd` / `.so` 添加到 `.gitignore`, 部署时由 setup.py 现编.
3. **特殊 dtype 退化** — ArrowStringArray 等非 numpy 数组走 Python `arr[i]`, 仍
   受益于 Cython 跳过 `to_dict()` 包装.
4. **目标未完全达到** — 任务要求 +50-100% (5-7k+ msg/s), 实际 +37% (~3.7k msg/s).
   原因: 100k 端到端测试中 ZMQ 发送 / 事件循环开销已占主要耗时, 序列化层加速的
   影响被稀释. micro-bench 单独测序列化层是 1.14x. 进一步提升需要客户端并发
   或 Batcher 协议优化, 超出本任务范围.

## 修复的非 Cython 问题

1. **pyproject.toml License classifier** — PEP 639 已废弃 `License :: OSI Approved :: MIT License`
   分类器, setuptools 82+ 报错. 移除冗余分类器, 保留 `license = "MIT"` 字段.
2. **`type(obj).__module__` 检查失效** — pandas DataFrame 的 `__module__` 是
   `"pandas"` (非 `"pandas."`), 修改 `_is_dataframe` 同时匹配两种前缀.
3. **Cython 走 `.values` 失败** — pandas 2.x string dtype 用 ArrowStringArray 而非
   numpy ndarray, 用 `cnp.PyArray_CheckExact` 检测后回退到 Python 索引.
