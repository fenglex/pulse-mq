# publish() API 简化设计

## 目标

将 publish API 简化为三层消息模型：数据类型决定序列化方式，压缩对所有类型可选。

## 消息模型

| data 类型 | 序列化方式 | format 参数 | 大小限制（压缩前） |
|-----------|-----------|-------------|-------------------|
| `str` | StringSerializer（UTF-8） | 忽略 | ≤ 16MB |
| `bytes` | BytesSerializer（透传） | 忽略 | ≤ 128MB |
| `DataFrame` | msgpack 或 pyarrow | `"msgpack"` / `"pyarrow"` | 无限制 |

## publish() 签名

```python
async def publish(
    self,
    topic: str,
    data: bytes | str | DataFrame,
    format: str = "msgpack",       # 仅 DataFrame 有效
    compression: str = "none",     # 所有类型可选
    retry: int = 0,
    retry_delay: float = 0.1,
) -> None:
```

## 行为规则

1. `data` 类型自动推断序列化方式，`format` 仅在 DataFrame 时生效
2. `str` 传入时自动 UTF-8 编码，接收端自动解码回 str
3. `bytes` 透传，接收端返回 bytes
4. DataFrame 支持 `format="msgpack"`（默认）或 `format="pyarrow"`
5. 三种类型均支持 compression（none/snappy/lz4/zstd）
6. 大小限制在 `_prepare_data` 中校验，超限抛出 `ValueError`

## 常量

```python
MAX_STR_SIZE = 16 * 1024 * 1024    # 16MB
MAX_BYTES_SIZE = 128 * 1024 * 1024  # 128MB
```

## 受影响文件

- `src/pulsemq/client/async_client.py` — publish 签名、_prepare_data、_infer_record_count
- `scripts/bench_1m.py` — 更新 bench 调用
- `scripts/bench_live.py` — 更新 bench 调用
- `docs/api-reference.md` — 更新 API 文档
