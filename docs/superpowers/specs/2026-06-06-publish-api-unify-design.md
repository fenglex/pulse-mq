# 统一 Publish API 设计

## 目标

统一 `PulseClient` 的消息推送 API，提供清晰、类型安全的单条/批量发布接口，自动推断消息数量。

## 设计

### `publish` 单条发布

```python
async def publish(
    self,
    topic: str,
    data: bytes | str | dict | list[dict] | pd.DataFrame,
    format: str = "msgpack",
    compression: str = "none",
    retry: int = 0,
    retry_delay: float = 0.1,
) -> None:
```

| 参数 | 类型 | 必填 | 默认值 | 说明 |
|---|---|---|---|---|
| `topic` | `str` | 是 | - | 消息主题路径 |
| `data` | `bytes \| str \| dict \| list[dict] \| pd.DataFrame` | 是 | - | 消息数据 |
| `format` | `str` | 否 | `"msgpack"` | 序列化格式：`none` / `msgpack` / `pyarrow` |
| `compression` | `str` | 否 | `"none"` | 压缩算法：`none` / `lz4` / `zstd` / `snappy` |
| `retry` | `int` | 否 | `0` | 重试次数 |
| `retry_delay` | `float` | 否 | `0.1` | 重试间隔（秒），指数退避 |

### `record_count` 自动推断规则

| `data` 类型 | `record_count` | 说明 |
|---|---|---|
| `pd.DataFrame` | `len(df)` | 按实际行数 |
| `list[dict]` | `1` | 整体作为一条消息 |
| `dict` | `1` | |
| `str` | `1` | |
| `bytes` | `1` | |

### `format` 格式说明

| format | 说明 |
|---|---|
| `"msgpack"` | 默认格式，支持 dict / list[dict] / str |
| `"pyarrow"` | DataFrame 高效传输，支持 DataFrame / dict（自动转 1 行表） |
| `"none"` | 直接透传 bytes，data 必须是 `bytes` 类型，否则抛 `TypeError` |

### `str` 类型 data 的处理

- `format="none"` → `data.encode("utf-8")` 后透传
- 其他格式 → 正常序列化

### `publish_batch` 批量发布

```python
async def publish_batch(
    self,
    messages: list[dict],
    format: str = "msgpack",
    compression: str = "none",
    retry: int = 0,
    retry_delay: float = 0.1,
) -> None:
```

`messages` 中每个元素是一个 dict，每条消息可单独覆盖外层默认参数：

```python
await client.publish_batch(
    messages=[
        {
            "topic": "mkt.sh.600000",
            "data": {"price": 15.8},
            # format/compression 未指定，使用外层默认
        },
        {
            "topic": "mkt.sh.600001",
            "data": df,
            "format": "pyarrow",       # 覆盖外层 format
            "compression": "lz4",      # 覆盖外层 compression
        },
    ],
    format="msgpack",
    compression="none",
)
```

### 构造函数变化

移除 `serializer` / `compressor` 全局默认参数，改为在 `publish` 调用时显式指定。构造函数中移除：

- `serializer` → 由 `publish(format=...)` 控制
- `compressor` → 由 `publish(compression=...)` 控制

> 注：`subscribe` / `query` / `ping` 内部仍需默认序列化方式，内部硬编码为 `"msgpack"` + `"none"`。

## 涉及文件

| 文件 | 变更 |
|---|---|
| `src/pulsemq/client/async_client.py` | 重写 `publish` / `publish_batch`，移除构造函数 `serializer`/`compressor` 参数 |
| `src/pulsemq/serialization/registry.py` | 注册 `"none"` → `BytesSerializer`（别名） |
| `tests/unit/test_client.py` | 更新初始化测试，新增 publish 参数推断测试 |

## 不涉及

- Protobuf / FlatBuffers：不做支持
- 服务端 handler / 路由：无变更
- 协议帧格式：无变更
