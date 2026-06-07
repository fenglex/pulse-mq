# PulseMQ 100k 压测: DataFrame JSON 默认 + 5×4 组合

**测试日期**: 2026-06-07
**版本**: 5 data_type × 4 compression = 20 组合
**改动**: 新增 `JsonSerializer` (msgspec.json, Rust 后端), DataFrame 默认格式 `msgpack → json`
**Bug 修复**: `FrameFlags._SER_MAP` 缺少 `json` 编码 (默认 0b000 → msgpack), wire 上 ser_fmt 信息丢失

## 测试环境

- Python 3.13.5 (Windows 11 Pro, win32)
- msgspec 0.21.1 (Rust 后端, JSON/msgpack 路径)
- 1 pub + 1 sub, loopback
- 每组合 100,000 条消息
- `bench_baseline.py`, server 用 `scripts/test_server_runner.py`

## 改动总览

### 新增 `JsonSerializer`

`src/pulsemq/serialization/registry.py`:

```python
class JsonSerializer(Serializer):
    """JSON 文本序列化 (msgspec.json, Rust 后端)."""
    def serialize(self, obj: Any) -> bytes:
        import msgspec
        return msgspec.json.encode(obj)

    def deserialize(self, data: bytes) -> Any:
        import msgspec
        return msgspec.json.decode(data)
```

注册: `SerializationRegistry.register("json", JsonSerializer())`

### DataFrame 默认走 json

`src/pulsemq/client/async_client.py`:

```python
# _resolve_format
if isinstance(data, pd.DataFrame):
    _fmt = format or "json"  # ← 改: 默认从 msgpack 改为 json
    if _fmt not in ("json", "msgpack", "pyarrow"):
        raise ValueError(...)
    return _fmt

# publish() payload 预转换
if isinstance(data, pd.DataFrame) and ser_fmt in ("json", "msgpack"):
    payload_obj = data.to_dict(orient="records")
```

### Bug 修复: `FrameFlags` 缺 json 编码

`src/pulsemq/protocol/flags.py`:

修改前: `_SER_MAP` 只含 msgpack/bytes/pyarrow/protobuf/str
修改后: 增加 `"json": 0b101`

未修复时: wire 上 `ser_fmt="json"` 的帧会被错误地编码为 `0b000` (msgpack),
subscriber 端拿到 JSON bytes 用 msgpack 解码失败 → `payload=None`。
e2e 调试中发现, 修源码后 4/4 json e2e 全部通过。

## 测试结果 (20 组合)

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
| **df-json** | **none** | **2674** | **0.20** | **0.47** |
| **df-json** | **snappy** | **2644** | **0.21** | **0.46** |
| **df-json** | **lz4** | **2651** | **0.21** | **0.46** |
| **df-json** | **zstd** | **2579** | **0.22** | **0.45** |
| df-msgpack | none | 2713 | 0.20 | 0.46 |
| df-msgpack | snappy | 2654 | 0.20 | 0.46 |
| df-msgpack | lz4 | 2682 | 0.20 | 0.45 |
| df-msgpack | zstd | 2627 | 0.21 | 0.48 |
| df-pyarrow | none | 2326 | 0.23 | 0.48 |
| df-pyarrow | snappy | 2279 | 0.24 | 0.47 |
| df-pyarrow | lz4 | 2299 | 0.23 | 0.47 |
| df-pyarrow | zstd | 2158 | 0.26 | 0.52 |

## 横向对比 (DataFrame 三种 ser_fmt)

| ser_fmt | 序列化器 | none 吞吐 | none p50 | snappy 吞吐 | lz4 吞吐 | zstd 吞吐 |
|---------|---------|----------|----------|------------|----------|----------|
| **df-msgpack** | msgspec.msgpack | **2713** | 0.20ms | 2654 | 2682 | 2627 |
| **df-json** | msgspec.json | **2674** | 0.20ms | 2644 | 2651 | 2579 |
| **df-pyarrow** | pa.ipc | 2326 | 0.23ms | 2279 | 2299 | 2158 |

**对比 ratio (以 msgpack = 1.0x)**:

| 压缩 | df-msgpack | df-json | df-pyarrow |
|------|------------|---------|------------|
| none | 1.00x | 0.99x | 0.86x |
| snappy | 1.00x | 1.00x | 0.86x |
| lz4 | 1.00x | 0.99x | 0.86x |
| zstd | 1.00x | 0.98x | 0.82x |

## 关键发现

### 1. df-json 与 df-msgpack 吞吐几乎一致 (差异 ≤ 2%)

**反预期**: 通常认为 msgspec.msgpack 比 msgspec.json 快 2-3x。
实测在本场景下 1-row DataFrame 吞吐差距 ≤ 1.5%。

**原因分析**:
- 1-row DataFrame 体积小 (~40 bytes), 序列化开销被 `to_dict` 主导
- msgspec.json 与 msgpack 同样走 Rust 后端, 在小对象上性能差距收窄
- p50/p99 都在 0.20-0.50 ms 区间, 系统瓶颈在事件循环 + ZMQ 收发而非序列化
- 200 条 yield 一次的 sleep(0.001) 让 sub 跟得上, 进一步削弱序列化侧差距

### 2. DataFrame 压缩几乎不改善吞吐 (none ≈ snappy ≈ lz4)

- 1-row DataFrame payload 极小 (~40-60 bytes), 压缩器开销与节省相互抵消
- 100k 条 1-row DataFrame 性能主要由 IPC 路径 + `to_dict` 主导, 压缩不是瓶颈
- 大批量 DataFrame (>10 rows) 才会显著受压缩影响 (本压测固定 1 row)

### 3. str/bytes 比 DataFrame 快 ~8x

- str/bytes 直传, 跳过 `to_dict` 转换
- DataFrame 路径多了: `isinstance` 校验 + `to_dict(orient="records")` + 预转 list[dict]
- str/bytes ~22k msg/s, DataFrame ~2.6-2.7k msg/s

### 4. df-pyarrow 慢 ~14% vs msgpack/json

- pyarrow IPC 流式协议有 schema 元数据开销
- 1-row DataFrame 场景下 schema 开销占比大
- 大批量 (>100 rows) 才是 pyarrow 优势区间

## 兼容性影响

- DataFrame 显式 `format="msgpack"` / `format="pyarrow"` 完全兼容 (4/4 e2e 通过)
- BATCH 协议自动支持新 ser_fmt (外层 msgpack 包装内携带每条 (ser_fmt, payload))
- wire 协议二进制兼容: FrameFlags bit 0b101 已分配, 老客户端(无 fix)无法识别 → 默认回退到 msgpack 路径 → 拿到 JSON bytes 尝试 msgpack 解码失败
- 客户端需升级 (含本次 flags 修复) 才能正确收发 json ser_fmt 帧

## 复现命令

```bash
cd D:/workflow/pulse-mq
PYTHONIOENCODING=utf-8 uv run python scripts/bench_baseline.py --port 17200 --output docs/perf-100k-json-default-data.md --n-messages 100000
```

## 单测 + e2e 覆盖

### 单测 (10 个新增, `tests/unit/test_json_serializer.py`)

- `test_json_serializer_registered`: 注册表查找
- `test_json_dict_roundtrip`: dict 含中文+emoji
- `test_json_list_roundtrip`: list of dict
- `test_json_dataframe`: DataFrame → list[dict] → JSON roundtrip
- `test_json_special_chars`: emoji / 中 / 引号 / tab
- `test_json_nested`: 3 层嵌套
- `test_json_empty`: 空 dict / list
- `test_json_numbers`: int / float / 负数 / 0 / 极大值
- `test_json_unicode`: 日文 / 阿拉伯文 / emoji
- `test_json_via_framecodec`: 4 种压缩 × json 完整路径

### e2e (4 个新增, `tests/integration/test_json_e2e.py`)

- `test_dataframe_json_default`: DataFrame 默认走 json
- `test_dataframe_explicit_msgpack`: 显式 msgpack 向后兼容
- `test_dataframe_explicit_pyarrow`: 显式 pyarrow 向后兼容
- `test_dataframe_json_with_compression`: json + lz4 压缩端到端

### 完整测试结果

```
pytest: 662 passed, 2 skipped
integration: 71 passed (16 原有 e2e + 4 新增 json e2e + 51 其它)
```

## JSON 默认化的设计权衡 (v1.0 决策)

**选择 json 为 DataFrame 默认**:
- 优点: 可读性高 (调试/日志/wireshark 友好) + 跨语言互操作 (任意 JSON 消费者可读)
- 缺点: 1-row 场景下与 msgpack 性能几乎一致 (无明显代价); 但 wire 上字节数可能略增 (字段名重复)
- 缓解: 用户可显式 `format="msgpack"` 切回高性能路径 (向后兼容已验证)
- Batcher + 压缩后: 网络占用与 msgpack 路径差距进一步缩小

**未来优化方向** (非本次范围):
- 多行 DataFrame 场景 (10+ rows) 验证 JSON vs msgpack 真实差距
- 客户端本地缓存 DataFrame schema, 减少 wire 字段名重复
- 提供 `format="auto"` 让 server 根据 payload 体积自动选
