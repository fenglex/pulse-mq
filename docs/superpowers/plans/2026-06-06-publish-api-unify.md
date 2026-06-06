# 统一 Publish API 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 统一 `PulseClient.publish` API，支持自动推断 record_count，单条/批量发布，移除构造函数中的 serializer/compressor 全局参数。

**Architecture:** 在 `PulseClient` 中新增 `_infer_record_count` 和 `_prepare_data` 两个内部方法处理数据类型适配和行数推断。`publish` 方法接收 `format`/`compression`/`retry`/`retry_delay` 参数，`publish_batch` 接收消息列表，每条消息可覆盖外层默认值。序列化注册表新增 `"none"` 别名。

**Tech Stack:** Python 3.12+, msgpack, pyarrow(可选), zmq

---

### Task 1: 注册 "none" 序列化器别名

**Files:**
- Modify: `src/pulsemq/serialization/registry.py:77-105`

- [ ] **Step 1: 在 `_init_builtins()` 中注册 `"none"` 别名**

在 `src/pulsemq/serialization/registry.py` 的 `_init_builtins()` 函数中，在注册 `"raw"` 之后添加 `"none"` 别名：

```python
def _init_builtins() -> None:
    """注册内置序列化器和压缩器。"""
    from pulsemq.serialization.msgpack_ser import MsgpackSerializer
    from pulsemq.serialization.raw_ser import RawSerializer

    SerializationRegistry.register("msgpack", MsgpackSerializer())
    SerializationRegistry.register("raw", RawSerializer())
    SerializationRegistry.register("none", RawSerializer())  # none 别名，等价 raw

    try:
        from pulsemq.serialization.pyarrow_ser import PyArrowSerializer
        SerializationRegistry.register("pyarrow", PyArrowSerializer())
    except ImportError:
        pass  # pyarrow 未安装，跳过

    from pulsemq.serialization.compressors import (
        NoneCompressor,
        SnappyCompressor,
        Lz4Compressor,
        ZstdCompressor,
    )

    CompressionRegistry.register("none", NoneCompressor())
    CompressionRegistry.register("snappy", SnappyCompressor())
    CompressionRegistry.register("lz4", Lz4Compressor())
    CompressionRegistry.register("zstd", ZstdCompressor())
```

- [ ] **Step 2: 验证注册成功**

Run: `cd D:/workflow/pulse-mq && uv run python -c "from pulsemq.serialization.registry import SerializationRegistry; print(SerializationRegistry.list())"`

Expected: 输出包含 `'none'`

- [ ] **Step 3: 提交**

```bash
git add src/pulsemq/serialization/registry.py
git commit -m "feat: 注册 none 序列化器别名，等价 raw 透传"
```

---

### Task 2: 重写 PulseClient publish/publish_batch 及构造函数

**Files:**
- Modify: `src/pulsemq/client/async_client.py`

这是核心变更。分几个子步骤：

- [ ] **Step 1: 修改构造函数，移除 serializer/compressor 参数**

在 `async_client.py` 的 `PulseClient.__init__` 中，移除 `serializer` 和 `compressor` 参数及对应属性。内部使用的默认值改为类常量：

```python
# 类顶部新增常量
_DEFAULT_SER = "msgpack"
_DEFAULT_COMP = "none"

def __init__(
    self,
    address: str,
    api_key: str | None = None,
    xpub_address: str | None = None,
    auto_reconnect: bool = True,
    reconnect_initial_delay: float = 1.0,
    reconnect_max_delay: float = 30.0,
    reconnect_backoff: float = 2.0,
    heartbeat_interval: float = 10.0,
    recv_timeout: float = 5.0,
    connect_timeout: float = 5.0,
    identity: bytes | None = None,
):
    self._address = address
    self._xpub_address = xpub_address or address.replace("5555", "5556")
    self._api_key = api_key
    self._auto_reconnect = auto_reconnect
    self._reconnect_initial_delay = reconnect_initial_delay
    self._reconnect_max_delay = reconnect_max_delay
    self._reconnect_backoff = reconnect_backoff
    self._heartbeat_interval = heartbeat_interval
    self._recv_timeout = recv_timeout
    self._connect_timeout = connect_timeout
    self._identity = identity or f"client_{id(self)}".encode()

    self._ctx: zmq.asyncio.Context | None = None
    self._dealer: zmq.asyncio.Socket | None = None
    self._sub: zmq.asyncio.Socket | None = None
    self._connected = False
    self._reconnect_count = 0
    self._user_info: dict | None = None
```

- [ ] **Step 2: 添加 `_infer_record_count` 静态方法**

在 `PulseClient` 类的内部方法区域（`_send_with_retry` 之前）添加：

```python
@staticmethod
def _infer_record_count(data: Any) -> int:
    """根据 data 类型推断 record_count。

    DataFrame 按实际行数，其余都算 1 条。
    """
    try:
        import pandas as pd
        if isinstance(data, pd.DataFrame):
            return len(data)
    except ImportError:
        pass
    return 1
```

- [ ] **Step 3: 添加 `_prepare_data` 静态方法**

```python
@staticmethod
def _prepare_data(data: Any, format: str) -> Any:
    """预处理 data，处理 str 类型转换和 format 校验。"""
    if format == "none" and not isinstance(data, bytes):
        if isinstance(data, str):
            return data.encode("utf-8")
        raise TypeError(
            f"format='none' 只接受 bytes 或 str 类型数据，收到 {type(data).__name__}"
        )
    return data
```

- [ ] **Step 4: 重写 `publish` 方法**

替换现有的 `publish` 方法（约第 180-204 行）：

```python
async def publish(
    self,
    topic: str,
    data: Any,
    format: str = "msgpack",
    compression: str = "none",
    retry: int = 0,
    retry_delay: float = 0.1,
) -> None:
    """发布消息。

    Args:
        topic: topic 路径（必填）
        data: 消息数据，支持 bytes/str/dict/list[dict]/DataFrame
        format: 序列化格式，none/msgpack/pyarrow
        compression: 压缩算法，none/lz4/zstd/snappy
        retry: 重试次数，默认 0
        retry_delay: 重试间隔（秒），默认 0.1
    """
    data = self._prepare_data(data, format)
    record_count = self._infer_record_count(data)
    payload = FrameCodec.encode_payload(data, format, compression)
    frames = FrameCodec.encode(
        MsgType.PUB, topic, record_count, payload, format, compression
    )
    await self._send_with_retry(frames, retry, retry_delay)
```

- [ ] **Step 5: 重写 `publish_batch` 方法**

替换现有的 `publish_batch` 方法（约第 206-213 行）：

```python
async def publish_batch(
    self,
    messages: list[dict],
    format: str = "msgpack",
    compression: str = "none",
    retry: int = 0,
    retry_delay: float = 0.1,
) -> None:
    """批量发布消息。

    Args:
        messages: 消息列表，每个元素包含 topic(必填) + data(必填)
                  + 可选的 format/compression 覆盖
        format: 全局默认序列化格式
        compression: 全局默认压缩算法
        retry: 重试次数
        retry_delay: 重试间隔（秒）
    """
    for msg in messages:
        await self.publish(
            topic=msg["topic"],
            data=msg["data"],
            format=msg.get("format", format),
            compression=msg.get("compression", compression),
            retry=retry,
            retry_delay=retry_delay,
        )
```

- [ ] **Step 6: 更新 subscribe/query/ping 中的硬编码**

将 `subscribe` 方法中的 `self._serializer`/`self._compressor` 替换为 `self._DEFAULT_SER`/`self._DEFAULT_COMP`。涉及方法：

- `subscribe` 中的 `_decode_message`：`self._serializer` → `self._DEFAULT_SER`，`self._compressor` → `self._DEFAULT_COMP`
- `query`：同上
- `ping`：同上

具体替换位置（在 `_decode_message`、`query`、`ping` 三个方法中）：

```python
# 所有 self._serializer → self._DEFAULT_SER
# 所有 self._compressor → self._DEFAULT_COMP
```

- [ ] **Step 7: 验证语法正确**

Run: `cd D:/workflow/pulse-mq && uv run python -c "from pulsemq.client.async_client import PulseClient; print('OK')"`

Expected: `OK`

- [ ] **Step 8: 提交**

```bash
git add src/pulsemq/client/async_client.py
git commit -m "feat: 统一 publish API，支持 format/compression/retry，自动推断 record_count"
```

---

### Task 3: 更新测试

**Files:**
- Modify: `tests/unit/test_client.py`

- [ ] **Step 1: 更新 `TestPulseClientInit` 测试**

构造函数不再有 `serializer`/`compressor` 参数，更新测试：

```python
class TestPulseClientInit:
    def test_default_config(self):
        client = PulseClient("tcp://localhost:5555")
        assert client._address == "tcp://localhost:5555"
        assert client._auto_reconnect is True
        assert client._identity is not None

    def test_custom_config(self):
        client = PulseClient(
            "tcp://localhost:5555",
            api_key="pulse_sk_test",
            recv_timeout=10.0,
        )
        assert client._api_key == "pulse_sk_test"
        assert client._recv_timeout == 10.0
```

- [ ] **Step 2: 添加 `_infer_record_count` 测试类**

```python
class TestInferRecordCount:
    def test_dict_returns_1(self):
        assert PulseClient._infer_record_count({"price": 15.8}) == 1

    def test_bytes_returns_1(self):
        assert PulseClient._infer_record_count(b"hello") == 1

    def test_str_returns_1(self):
        assert PulseClient._infer_record_count("hello") == 1

    def test_list_dict_returns_1(self):
        assert PulseClient._infer_record_count([{"a": 1}, {"b": 2}]) == 1

    def test_dataframe_returns_row_count(self):
        import pandas as pd
        df = pd.DataFrame({"price": [1.0, 2.0, 3.0]})
        assert PulseClient._infer_record_count(df) == 3

    def test_empty_dataframe_returns_0(self):
        import pandas as pd
        df = pd.DataFrame({"price": []})
        assert PulseClient._infer_record_count(df) == 0
```

- [ ] **Step 3: 添加 `_prepare_data` 测试类**

```python
class TestPrepareData:
    def test_str_with_none_format_encodes_to_bytes(self):
        result = PulseClient._prepare_data("hello", "none")
        assert result == b"hello"

    def test_bytes_with_none_format_passthrough(self):
        result = PulseClient._prepare_data(b"\x01\x02", "none")
        assert result == b"\x01\x02"

    def test_dict_with_none_format_raises_type_error(self):
        with pytest.raises(TypeError, match="format='none'"):
            PulseClient._prepare_data({"key": 1}, "none")

    def test_dict_with_msgpack_passthrough(self):
        result = PulseClient._prepare_data({"key": 1}, "msgpack")
        assert result == {"key": 1}
```

- [ ] **Step 4: 运行测试验证通过**

Run: `cd D:/workflow/pulse-mq && uv run pytest tests/unit/test_client.py -v`

Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add tests/unit/test_client.py
git commit -m "test: 更新客户端测试，覆盖 infer_record_count 和 prepare_data"
```

---

### Task 4: 全量回归测试

- [ ] **Step 1: 运行全部单元测试**

Run: `cd D:/workflow/pulse-mq && uv run pytest tests/unit/ -v`

Expected: 全部 PASS

- [ ] **Step 2: 运行集成测试**

Run: `cd D:/workflow/pulse-mq && uv run pytest tests/integration/ -v`

Expected: 全部 PASS

- [ ] **Step 3: 最终提交（如有遗漏修复）**

```bash
git add -A
git commit -m "fix: 修复测试回归问题"
```
