# PulseMQ API 使用文档

## 目录

- [1. 快速开始](#1-快速开始)
- [2. 客户端 SDK — PulseClient](#2-客户端-sdk--pulseclient)
  - [2.1 创建客户端](#21-创建客户端)
  - [2.2 连接管理](#22-连接管理)
  - [2.3 发布消息](#23-发布消息)
  - [2.4 订阅消息](#24-订阅消息)
  - [2.5 取消订阅](#25-取消订阅)
  - [2.6 管理查询](#26-管理查询)
  - [2.7 心跳检测](#27-心跳检测)
  - [2.8 错误类型](#28-错误类型)
  - [2.9 消息对象 — PulseMessage](#29-消息对象--pulsemessage)
- [3. 服务端 — PulseServer](#3-服务端--pulseserver)
  - [3.1 启动服务](#31-启动服务)
  - [3.2 优雅停止](#32-优雅停止)
  - [3.3 运行时属性](#33-运行时属性)
  - [3.4 CLI 入口](#34-cli-入口)
- [4. 配置 — ServerConfig](#4-配置--服务端config)
  - [4.1 配置项一览](#41-配置项一览)
  - [4.2 环境变量覆盖](#42-环境变量覆盖)
  - [4.3 加载配置](#43-加载配置)
- [5. 事件循环](#5-事件循环)
- [6. 序列化注册表](#6-序列化注册表)
  - [6.1 内置序列化器](#61-内置序列化器)
  - [6.2 SerializationRegistry API](#62-serializationregistry-api)
  - [6.3 自定义序列化器](#63-自定义序列化器)
- [7. 压缩注册表](#7-压缩注册表)
  - [7.1 内置压缩算法](#71-内置压缩算法)
  - [7.2 CompressionRegistry API](#72-compressionregistry-api)
  - [7.3 自定义压缩器](#73-自定义压缩器)
- [8. 协议层](#8-协议层)
  - [8.1 帧格式](#81-帧格式)
  - [8.2 消息类型 — MsgType](#82-消息类型--msgtype)
  - [8.3 帧编解码 — FrameCodec](#83-帧编解码--framecodec)
  - [8.4 Flags 位域 — FrameFlags](#84-flags-位域--frameflags)
- [9. 认证与权限](#9-认证与权限)
  - [9.1 用户管理 — UserRepository](#91-用户管理--userrepository)
  - [9.2 权限组管理 — PermissionGroupRepo](#92-权限组管理--permissiongrouprepo)
  - [9.3 权限服务 — PermissionService](#93-权限服务--permissionservice)
  - [9.4 Topic 通配符匹配](#94-topic-通配符匹配)
- [10. 监控 API](#10-监控-api)
  - [10.1 HTTP 端点](#101-http-端点)
  - [10.2 实时指标 — RealtimeMetrics](#102-实时指标--realtimemetrics)
- [11. 数据模型](#11-数据模型)
- [12. 过载保护 — DualBuffer](#12-过载保护--dualbuffer)
- [13. 消息路由 — MessageRouter](#13-消息路由--messagerouter)

---

## 1. 快速开始

### 安装

```bash
# 基础安装
pip install pulsemq

# 带 PyArrow 支持（DataFrame 高效传输）
pip install pulsemq[pyarrow]
```

### 启动 服务端

```python
import asyncio
from pulsemq import PulseServer, load_config

async def main():
    config = load_config()
    server = PulseServer(config)
    await server.start()

asyncio.run(main())
```

或使用 CLI 命令：

```bash
pulse-mq
```

### 客户端使用

```python
import asyncio
from pulsemq import PulseClient

async def main():
    async with PulseClient("tcp://localhost:5555", api_key="pulse_sk_admin_default") as client:
        # 发布
        await client.publish("team-a.mkt.sh.600000", {"price": 10.5, "volume": 1000})

        # 订阅
        async for msg in client.subscribe("team-a.mkt.*"):
            print(f"收到: {msg.topic} → {msg.payload}")

asyncio.run(main())
```

---

## 2. 客户端 SDK — PulseClient

> 模块路径: `pulsemq.client.async_client.PulseClient`
> 顶层导出: `from pulsemq import PulseClient`

### 2.1 创建客户端

```python
PulseClient(
    address: str,                          # 服务端 ROUTER 地址
    api_key: str | None = None,            # API Key（认证用）
    xpub_address: str | None = None,       # 服务端 XPUB 地址（默认自动推导）
    auto_reconnect: bool = True,           # 自动重连
    reconnect_initial_delay: float = 1.0,  # 重连初始延迟（秒）
    reconnect_max_delay: float = 30.0,     # 重连最大延迟（秒）
    reconnect_backoff: float = 2.0,        # 指数退避倍数
    heartbeat_interval: float = 10.0,      # 心跳间隔（秒）
    recv_timeout: float = 5.0,             # 接收超时（秒）
    connect_timeout: float = 5.0,          # 连接超时（秒）
    identity: bytes | None = None,         # ZMQ identity（默认自动生成）
)
```

**参数说明:**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `address` | `str` | 必填 | 服务端 ROUTER socket 地址，如 `"tcp://localhost:5555"` |
| `api_key` | `str \| None` | `None` | 用户 API Key，用于 ZAP 认证 |
| `xpub_address` | `str \| None` | `None` | XPUB 广播地址，默认将 ROUTER 端口 5555 替换为 5556 |
| `auto_reconnect` | `bool` | `True` | 连接断开时自动重连（指数退避） |

### 2.2 连接管理

#### `connect() → None`

建立到 服务端 的连接，创建 DEALER + SUB socket。

```python
client = PulseClient("tcp://localhost:5555")
await client.connect()
# ... 使用完毕后
await client.disconnect()
```

#### `disconnect() → None`

断开连接，关闭所有 socket 和 context。

#### 上下文管理器（推荐用法）

```python
async with PulseClient("tcp://localhost:5555", api_key="sk_xxx") as client:
    # 自动调用 connect()
    await client.publish("topic", {"data": 1})
    # 退出时自动调用 disconnect()
```

#### 自动重连

当 `auto_reconnect=True` 时，连接断开后自动按指数退避重连：

```
delay = min(initial_delay * (backoff ^ n), max_delay)
```

- 第 1 次重连: 1.0s
- 第 2 次重连: 2.0s
- 第 3 次重连: 4.0s
- ...
- 最大延迟: 30.0s

### 2.3 发布消息

#### `publish(topic, data, format="msgpack", compression="none", retry=0, retry_delay=0.1) → None`

发布一条消息（fire-and-forget，不等待响应）。`record_count` 根据 `data` 类型自动推断，无需手动指定。

```python
# 发布 dict 数据
await client.publish("team-a.mkt.sh.600000", {"price": 10.5, "volume": 1000})

# 透传 bytes
await client.publish("topic", b"\x01\x02\x03", format="none")

# 透传 string
await client.publish("topic", "hello world", format="none")

# 指定序列化格式和压缩
await client.publish("topic", data, format="pyarrow", compression="lz4")

# 带重试（最多 3 次，指数退避）
await client.publish("topic", data, retry=3, retry_delay=0.5)

# DataFrame 发布（record_count 自动推断为行数）
import pandas as pd
df = pd.DataFrame({"price": [10.5, 11.0], "volume": [100, 200]})
await client.publish("topic", df, format="pyarrow")
```

**参数:**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `topic` | `str` | 必填 | Topic 路径，如 `"team-a.mkt.sh.600000"` |
| `data` | `bytes \| str \| dict \| list[dict] \| DataFrame` | 必填 | 消息数据 |
| `format` | `str` | `"msgpack"` | 序列化格式: `"none"` / `"msgpack"` / `"pyarrow"` |
| `compression` | `str` | `"none"` | 压缩算法: `"none"` / `"lz4"` / `"zstd"` / `"snappy"` |
| `retry` | `int` | `0` | 发送失败重试次数 |
| `retry_delay` | `float` | `0.1` | 重试间隔（指数退避基数，秒） |

**`record_count` 自动推断规则:**

| `data` 类型 | `record_count` | 说明 |
|-------------|----------------|------|
| `DataFrame` | `len(df)` | 按实际行数 |
| `dict` | `1` | |
| `list[dict]` | `1` | 整体作为一条消息 |
| `str` | `1` | |
| `bytes` | `1` | |

**`format` 格式说明:**

| format | 说明 |
|--------|------|
| `"msgpack"` | 默认格式，支持 dict / list[dict] / str |
| `"pyarrow"` | Arrow IPC 格式，支持 DataFrame / dict（自动转 1 行表） |
| `"none"` | 直接透传 bytes，`data` 必须是 `bytes` 或 `str`（自动 encode） |

#### `publish_batch(messages, format="msgpack", compression="none", retry=0, retry_delay=0.1) → None`

> ⚠️ **已废弃**: 客户端 Batcher 策略已在 v1.0 中撤销, `publish_batch` 不再可用。
> 改用多次 `await client.publish(topic, data)` 调用实现。

_(原批量发布文档已随 batcher 策略一并移除)_

**参数:**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `messages` | `list[dict]` | 必填 | 消息列表，每个元素包含 `topic`(必填) + `data`(必填) + 可选的 `format`/`compression` 覆盖 |
| `format` | `str` | `"msgpack"` | 全局默认序列化格式 |
| `compression` | `str` | `"none"` | 全局默认压缩算法 |
| `retry` | `int` | `0` | 重试次数 |
| `retry_delay` | `float` | `0.1` | 重试间隔（秒） |

### 2.4 订阅消息

#### `subscribe(*topics) → AsyncIterator[PulseMessage]`

订阅一个或多个 topic，返回异步迭代器。

```python
# 订阅单个 topic
async for msg in client.subscribe("team-a.mkt.sh.600000"):
    print(msg.topic, msg.payload)

# 订阅多个 topic
async for msg in client.subscribe("topic-a", "topic-b"):
    print(msg.topic, msg.payload)

# 通配符订阅
async for msg in client.subscribe("team-a.mkt.*"):
    print(msg.topic, msg.payload)

# 多层通配符
async for msg in client.subscribe("team-a.>"):
    print(msg.topic, msg.payload)
```

**通配符规则:**

| 通配符 | 含义 | 示例 |
|--------|------|------|
| `*` (中间) | 匹配恰好一个段 | `a.*.c` → `a.b.c` |
| `*` (末尾) | 匹配一个或多个段 | `team-a.*` → `team-a.mkt.sh.600000` |
| `>` | 匹配一个或多个段 | `team-a.>` → `team-a.mkt.sh.600000` |

**注意:** 订阅循环内置心跳保活机制，超时未收到消息会自动发送 PING。

### 2.5 取消订阅

#### `unsubscribe(topic) → None`

取消订阅指定 topic。

```python
await client.unsubscribe("team-a.mkt.sh.600000")
```

### 2.6 管理查询

#### `query(params) → dict`

发送管理查询请求并等待响应。

```python
# 查询 topic 列表
result = await client.query({"action": "list_topics"})

# 查询订阅状态
result = await client.query({"action": "subscriptions"})
print(result)
```

### 2.7 心跳检测

#### `ping() → dict`

发送 PING 请求，返回延迟信息。

```python
result = await client.ping()
print(result)  # {"client_ts": 1717600000.0, "server_ts": 1717600000.1, ...}
```

### 2.8 错误类型

所有客户端异常均继承自 `PulseError`。

```python
from pulsemq import PulseError, PulseConnectionError, PulseAuthError
from pulsemq import PulsePermissionError, PulseTimeoutError, PulseServerError
```

| 错误类 | 说明 |
|--------|------|
| `PulseError` | 所有错误的基类 |
| `PulseConnectionError` | 连接失败/断开 |
| `PulseAuthError` | 认证失败 |
| `PulsePermissionError` | 权限不足 |
| `PulseTimeoutError` | 操作超时 |
| `PulseServerError` | 服务端返回错误（含 `code` 和 `message` 属性） |

```python
try:
    await client.publish("topic", data)
except PulseConnectionError:
    print("连接断开")
except PulseServerError as e:
    print(f"服务端错误: [{e.code}] {e.message}")
except PulseError as e:
    print(f"其他错误: {e}")
```

### 2.9 消息对象 — PulseMessage

订阅收到的消息对象。

```python
@dataclass
class PulseMessage:
    topic: str           # Topic 路径
    msg_type: int        # 消息类型（MsgType 常量）
    payload: Any         # 反序列化后的数据
    raw_payload: bytes   # 原始 payload 字节
    meta_flags: int      # Frame flags
    timestamp: float     # 接收时间戳
```

```python
async for msg in client.subscribe("topic"):
    print(f"Topic: {msg.topic}")
    print(f"Payload: {msg.payload}")
    print(f"Raw size: {len(msg.raw_payload)} bytes")
```

---

## 3. 服务端 — PulseServer

> 模块路径: `pulsemq.server.PulseServer`
> 顶层导出: `from pulsemq import PulseServer`

### 3.1 启动服务

```python
server = PulseServer(config)  # config 为 ServerConfig 实例
await server.start()
```

`start()` 会同时启动以下协程:
- ZMQ Transport（ROUTER + XPUB）
- Engine 主循环（消息处理）
- 监控指标同步
- Topic 空闲清理（每 5 分钟）
- ZMQ 连接/断开事件监听

### 3.2 优雅停止

```python
await server.stop()
```

停止顺序:
1. 停止 Engine（取消后台任务）
2. Drain 缓冲区残余消息
3. 停止监控聚合器和 HTTP 服务
4. 关闭 ZMQ Transport（等待 linger 时间）
5. 关闭数据库连接

### 3.3 运行时属性

| 属性 | 类型 | 说明 |
|------|------|------|
| `engine` | `Engine` | 消息引擎实例 |
| `router` | `MessageRouter` | 消息路由器 |
| `monitor` | `MonitorInterceptor` | 监控拦截器 |
| `realtime_metrics` | `RealtimeMetrics` | 实时指标 |

### 3.4 CLI 入口

```python
from pulsemq.server import main
main()  # 或命令行 pulse-mq
```

支持信号处理（SIGINT/SIGTERM），优雅关闭。

---

## 4. 配置 — ServerConfig

> 模块路径: `pulsemq.config.ServerConfig`
> 顶层导出: `from pulsemq import ServerConfig, load_config`

### 4.1 配置项一览

```python
@dataclass
class ServerConfig:
    # 传输层
    transport: str = "zmq"                    # 传输协议（当前仅支持 zmq）
    bind: str = "tcp://*:5555"                # ROUTER 绑定地址
    xpub_bind: str = "tcp://*:5556"           # XPUB 绑定地址

    # 存储层
    db_url: str = "sqlite://./pulse_mq.db"    # 主数据库 URL
    stats_db_url: str = "sqlite://./stats.sqlite"  # 统计数据库 URL
    stats_retention_days: int = 7             # 统计数据保留天数

    # 引擎层
    max_concurrency: int = 100                # 最大并发处理任务数
    drain_timeout_ms: int = 1                 # 缓冲区 drain 超时（毫秒）
    use_uvloop: bool = True                   # 是否使用 uvloop（Linux/macOS）
    object_pool_size: int = 4096              # 对象池大小

    # ZMQ socket 参数
    zmq_rcvhwm: int = 10000                   # 接收高水位
    zmq_sndhwm: int = 10000                   # 发送高水位
    zmq_heartbeat_ivl: int = 2000             # 心跳间隔（毫秒）
    zmq_heartbeat_timeout: int = 5000         # 心跳超时（毫秒）
    zmq_heartbeat_ttl: int = 8000             # 心跳 TTL（毫秒）

    # 过载保护
    data_buffer_size: int = 9000              # 数据缓冲区大小
    ctrl_buffer_size: int = 1000              # 控制缓冲区大小
    backpressure_threshold: float = 0.8       # 背压阈值（0.0-1.0）

    # 序列化/压缩
    default_serializer: str = "msgpack"       # 默认序列化格式
    default_compressor: str = "none"          # 默认压缩算法

    # 认证
    auth_enabled: bool = True                 # 是否启用认证
    default_admin_key: str = "pulse_sk_admin_default"  # 默认管理员密钥

    # 监控
    metrics_enabled: bool = True              # 是否启用监控
    metrics_bind: str = "0.0.0.0:9090"        # 监控 HTTP 绑定地址
```

### 4.2 环境变量覆盖

所有配置项均可通过环境变量覆盖，优先级: **环境变量 > 默认值**。

| 环境变量 | 配置字段 | 类型 |
|----------|----------|------|
| `PULSEMQ_TRANSPORT` | `transport` | str |
| `PULSEMQ_BIND` | `bind` | str |
| `PULSEMQ_XPUB_BIND` | `xpub_bind` | str |
| `PULSEMQ_DB_URL` | `db_url` | str |
| `PULSEMQ_STATS_DB_URL` | `stats_db_url` | str |
| `PULSEMQ_STATS_RETENTION` | `stats_retention_days` | int |
| `PULSEMQ_CONCURRENCY` | `max_concurrency` | int |
| `PULSEMQ_USE_UVLOOP` | `use_uvloop` | bool |
| `PULSEMQ_POOL_SIZE` | `object_pool_size` | int |
| `PULSEMQ_ZMQ_RCVHWM` | `zmq_rcvhwm` | int |
| `PULSEMQ_ZMQ_SNDHWM` | `zmq_sndhwm` | int |
| `PULSEMQ_HEARTBEAT_IVL` | `zmq_heartbeat_ivl` | int |
| `PULSEMQ_HEARTBEAT_TIMEOUT` | `zmq_heartbeat_timeout` | int |
| `PULSEMQ_HEARTBEAT_TTL` | `zmq_heartbeat_ttl` | int |
| `PULSEMQ_DATA_BUFFER` | `data_buffer_size` | int |
| `PULSEMQ_CTRL_BUFFER` | `ctrl_buffer_size` | int |
| `PULSEMQ_BP_THRESHOLD` | `backpressure_threshold` | float |
| `PULSEMQ_SERIALIZER` | `default_serializer` | str |
| `PULSEMQ_COMPRESSOR` | `default_compressor` | str |
| `PULSEMQ_AUTH_ENABLED` | `auth_enabled` | bool |
| `PULSEMQ_ADMIN_KEY` | `default_admin_key` | str |

布尔类型环境变量接受: `"true"`, `"1"`, `"yes"`。

使用示例:

```bash
# 自定义绑定端口
export PULSEMQ_BIND="tcp://*:6000"
export PULSEMQ_XPUB_BIND="tcp://*:6001"

# 启用 LZ4 压缩
export PULSEMQ_COMPRESSOR="lz4"

# 调大并发
export PULSEMQ_CONCURRENCY=200

# 禁用认证（开发环境）
export PULSEMQ_AUTH_ENABLED=false

pulse-mq
```

### 4.3 加载配置

```python
from pulsemq.config import load_config, ServerConfig

# 使用默认值 + 环境变量覆盖
config = load_config()

# 使用自定义字典覆盖（优先级最高）
config = load_config({"bind": "tcp://*:6000", "max_concurrency": 200})

# 直接构造
config = ServerConfig(bind="tcp://*:6000", auth_enabled=False)
```

---

## 5. 事件循环

> 模块路径: `pulsemq.event_loop`

### `install_event_loop(use_uvloop=True) → str`

安装高性能事件循环。**必须在 `asyncio.run()` 之前调用。**

- **Windows**: 强制使用 `WindowsSelectorEventLoopPolicy`（pyzmq 要求）
- **Linux/macOS + uvloop 已安装**: 使用 uvloop
- **Linux/macOS + uvloop 未安装**: 使用标准 asyncio（打印警告）

返回实际使用的事件循环名称: `"uvloop"` 或 `"asyncio"`。

### `get_event_loop_info() → dict`

返回当前事件循环诊断信息。

```python
from pulsemq.event_loop import get_event_loop_info

info = get_event_loop_info()
# {
#     "platform": "win32",
#     "loop_type": "SelectorEventLoop",
#     "uvloop_available": False
# }
```

---

## 6. 序列化注册表

> 模块路径: `pulsemq.serialization.registry`

### 6.1 内置序列化器

| 名称 | 类 | 输入类型 | 说明 |
|------|-----|----------|------|
| `"msgpack"` | `MsgpackSerializer` | dict, list, str, int, float, bytes | 二进制 JSON 格式，通用性最佳 |
| `"bytes"` | `BytesSerializer` | bytes | 纯字节透传，不做任何序列化 |
| `"none"` | `BytesSerializer` | bytes | `"bytes"` 的别名，用于 `format="none"` 透传 |
| `"pyarrow"` | `PyArrowSerializer` | pa.Table, pd.DataFrame, dict | Arrow IPC 流格式，DataFrame 高效传输 |

### 6.2 SerializationRegistry API

```python
from pulsemq.serialization.registry import SerializationRegistry

# 查询已注册的序列化器
names = SerializationRegistry.list()       # ["msgpack", "bytes", "pyarrow"]
has = SerializationRegistry.has("msgpack") # True

# 获取序列化器实例
ser = SerializationRegistry.get("msgpack")
data = ser.serialize({"key": "value"})     # bytes
obj = ser.deserialize(data)                # {"key": "value"}

# 注册自定义序列化器
SerializationRegistry.register("custom", MySerializer())
```

### 6.3 自定义序列化器

继承 `Serializer` 抽象类：

```python
from pulsemq.serialization.registry import Serializer, SerializationRegistry

class JsonSerializer(Serializer):
    def serialize(self, obj) -> bytes:
        import json
        return json.dumps(obj).encode("utf-8")

    def deserialize(self, data: bytes):
        import json
        return json.loads(data.decode("utf-8"))

# 注册
SerializationRegistry.register("json", JsonSerializer())
```

---

## 7. 压缩注册表

> 模块路径: `pulsemq.serialization.registry`

### 7.1 内置压缩算法

| 名称 | 类 | 特点 |
|------|-----|------|
| `"none"` | `NoneCompressor` | 不压缩，直接透传 |
| `"snappy"` | `SnappyCompressor` | Google Snappy，极速压缩/解压 |
| `"lz4"` | `Lz4Compressor` | LZ4 Frame 格式，极速压缩/解压 |
| `"zstd"` | `ZstdCompressor` | Zstandard，高压缩比 |

**额外依赖:**
- snappy: `pip install python-snappy`
- lz4: `pip install lz4`
- zstd: `pip install zstandard`

### 7.2 CompressionRegistry API

```python
from pulsemq.serialization.registry import CompressionRegistry

# 查询已注册的压缩器
names = CompressionRegistry.list()       # ["none", "snappy", "lz4", "zstd"]
has = CompressionRegistry.has("snappy")  # True

# 获取压缩器实例
comp = CompressionRegistry.get("lz4")
compressed = comp.compress(b"hello")     # bytes
original = comp.decompress(compressed)   # b"hello"

# 注册自定义压缩器
CompressionRegistry.register("custom", MyCompressor())
```

### 7.3 自定义压缩器

继承 `Compressor` 抽象类：

```python
from pulsemq.serialization.registry import Compressor, CompressionRegistry

class GzipCompressor(Compressor):
    def compress(self, data: bytes) -> bytes:
        import gzip
        return gzip.compress(data)

    def decompress(self, data: bytes) -> bytes:
        import gzip
        return gzip.decompress(data)

# 注册
CompressionRegistry.register("gzip", GzipCompressor())
```

---

## 8. 协议层

> 模块路径: `pulsemq.protocol`

### 8.1 帧格式

PulseMQ 使用固定帧格式:

**客户端发送（4 帧）:**

| 帧 | 内容 | 大小 |
|----|------|------|
| Frame 1 | topic（UTF-8） | 可变 |
| Frame 2 | meta（2 字节） | 2B |
| Frame 3 | record_count（big-endian uint32） | 4B |
| Frame 4 | payload（序列化 + 压缩后的数据） | 可变 |

**服务端 ROUTER 收到（5-6 帧，ZMQ 自动附加 identity）:**

| 帧 | 内容 |
|----|------|
| Frame 1 | identity（ZMQ 自动附加） |
| Frame 2 | delimiter（空帧，可选） |
| Frame 3-6 | 同客户端 4 帧 |

**服务端广播（4 帧）:**

| 帧 | 内容 |
|----|------|
| Frame 1 | topic（UTF-8） |
| Frame 2 | meta（2 字节） |
| Frame 3 | record_count（4B） |
| Frame 4 | payload |

### 8.2 消息类型 — MsgType

> 模块路径: `pulsemq.protocol.msg_type.MsgType`

| 常量 | 值 | 方向 | 说明 |
|------|-----|------|------|
| `AUTH` | `0x01` | 服务端→客户端 | 认证结果推送 |
| `PUB` | `0x02` | 客户端→服务端 | 发布消息 |
| `SUB` | `0x03` | 客户端→服务端 | 订阅 topic |
| `UNSUB` | `0x04` | 客户端→服务端 | 取消订阅 |
| `QUERY` | `0x05` | 双向 | 管理查询 |
| `PING` | `0x06` | 客户端→服务端 | 心跳 |
| `PONG` | `0x07` | 服务端→客户端 | 心跳响应 |
| `STATUS` | `0x08` | 服务端→客户端 | 状态推送 |
| `ERROR` | `0x09` | 服务端→客户端 | 错误响应 |
| `BROADCAST` | `0x0A` | 服务端→客户端 | 广播消息 |
| `HISTORY_REPLAY` | `0x0B` | 服务端→客户端 | 历史回放 |

**控制消息集合**（进入 ctrl_buffer）: `AUTH, SUB, UNSUB, QUERY, PING`

```python
from pulsemq.protocol.msg_type import MsgType

MsgType.is_control(MsgType.SUB)    # True
MsgType.is_control(MsgType.PUB)    # False
MsgType.from_byte(0x02)            # 2 (MsgType.PUB)
MsgType.from_byte(0xFF)            # None
```

### 8.3 帧编解码 — FrameCodec

> 模块路径: `pulsemq.protocol.frames.FrameCodec`

#### `FrameCodec.encode(msg_type, topic, record_count, payload, ser_fmt="msgpack", comp="none") → list[bytes]`

编码为 4 帧格式。

```python
from pulsemq.protocol.frames import FrameCodec
from pulsemq.protocol.msg_type import MsgType

frames = FrameCodec.encode(
    msg_type=MsgType.PUB,
    topic="team-a.mkt.sh.600000",
    record_count=1,
    payload=b"\x81\xa5price\xcb$FFFFFF",
    ser_fmt="msgpack",
    comp="none",
)
# [b"team-a.mkt.sh.600000", b"\x02\x20", b"\x00\x00\x00\x01", b"\x81\xa5price..."]
```

#### `FrameCodec.decode_server(frames) → DecodedFrame`

解码服务端 ROUTER 收到的帧（5 或 6 帧）。

```python
decoded = FrameCodec.decode_server(frames)
print(decoded.identity)       # bytes
print(decoded.topic)          # str
print(decoded.msg_type)       # int
print(decoded.flags)          # FrameFlags
print(decoded.record_count)   # int
print(decoded.payload)        # bytes
print(decoded.ser_fmt)        # "msgpack"
print(decoded.comp)           # "none"
```

#### `FrameCodec.encode_payload(obj, ser_fmt="msgpack", comp="none") → bytes`

序列化 + 压缩 payload。

#### `FrameCodec.decode_payload(data, ser_fmt="msgpack", comp="none") → Any`

解压 + 反序列化 payload。

### 8.4 Flags 位域 — FrameFlags

> 模块路径: `pulsemq.protocol.flags.FrameFlags`

Frame 2 的第 2 字节（meta[1]）编码:

```
bit[0:2] = 序列化格式 (000=msgpack, 001=raw, 010=pyarrow, 011=protobuf)
bit[3:4] = 压缩算法   (00=none, 01=snappy, 10=lz4, 11=zstd)
bit[5]   = has_topic  (0=无topic, 1=有topic)
bit[6:7] = reserved
```

```python
from pulsemq.protocol.flags import FrameFlags

# 编码
flags = FrameFlags(ser_fmt="msgpack", comp="lz4", has_topic=True)
byte_val = flags.encode()  # int

# 解码
decoded = FrameFlags.decode(byte_val)
print(decoded.ser_fmt)    # "msgpack"
print(decoded.comp)       # "lz4"
print(decoded.has_topic)  # True
```

---

## 9. 认证与权限

### 9.1 用户管理 — UserRepository

> 模块路径: `pulsemq.storage.interfaces.UserRepository`

用户数据模型:

```python
@dataclass
class User:
    id: int | None = None
    username: str = ""
    api_key: str = ""
    role: str = "user"          # "admin" | "user"
    namespace: str = ""
    disabled: bool = False
    max_connections: int = 10
    created_at: float = 0.0
    updated_at: float = 0.0
```

接口方法:

| 方法 | 签名 | 说明 |
|------|------|------|
| `get_by_id` | `(user_id: int) → User \| None` | 按 ID 查询用户 |
| `get_by_api_key` | `(api_key: str) → User \| None` | 按 API Key 查询用户 |
| `create` | `(user: User) → User` | 创建用户 |
| `update` | `(user: User) → User` | 更新用户 |
| `delete` | `(user_id: int) → None` | 删除用户 |
| `list_all` | `() → list[User]` | 列出所有用户 |

### 9.2 权限组管理 — PermissionGroupRepo

> 模块路径: `pulsemq.storage.interfaces.PermissionGroupRepo`

权限组数据模型:

```python
@dataclass
class PermissionGroup:
    id: int | None = None
    name: str = ""
    created_at: float = 0.0

@dataclass
class GroupPermission:
    id: int | None = None
    group_id: int = 0
    topic_pattern: str = ""     # "*.mkt.*"
    action: str = ""            # "pub" | "sub" | "query"
```

接口方法:

| 方法 | 说明 |
|------|------|
| `create_group(name) → PermissionGroup` | 创建权限组 |
| `delete_group(group_id) → None` | 删除权限组 |
| `get_group(group_id) → PermissionGroup \| None` | 查询权限组 |
| `list_groups() → list[PermissionGroup]` | 列出所有权限组 |
| `add_permission(group_id, topic_pattern, action) → None` | 添加权限规则 |
| `remove_permission(group_id, topic_pattern, action) → None` | 移除权限规则 |
| `get_permissions(group_id) → list[GroupPermission]` | 获取组权限 |
| `add_member(group_id, user_id) → None` | 添加组成员 |
| `remove_member(group_id, user_id) → None` | 移除组成员 |
| `get_members(group_id) → list[User]` | 获取组成员列表 |
| `get_user_groups(user_id) → list[PermissionGroup]` | 获取用户所属组 |
| `get_user_expanded_permissions(user_id) → dict[str, list[str]]` | 展开用户权限 |
| `get_group_all_members(group_id) → list[int]` | 获取组成员 ID 列表 |

### 9.3 权限服务 — PermissionService

> 模块路径: `pulsemq.auth.permission.PermissionService`

```python
service = PermissionService(perm_repo, ttl=60.0)

# 检查权限（admin 直接通过）
has_perm = await service.check_permission(user, "pub", "team-a.mkt.sh.600000")

# 缓存管理
service.invalidate_user(user_id)                  # 失效单用户缓存
service.invalidate_group_members([1, 2, 3])       # 失效多用户缓存
service.clear_cache()                              # 清空全部缓存
```

### 9.4 Topic 通配符匹配

> 模块路径: `pulsemq.auth.permission.topic_match`

```python
from pulsemq.auth.permission import topic_match

topic_match("a.*.c", "a.b.c")              # True（中间 * 匹配一段）
topic_match("a.*.c", "a.b.x.c")            # False
topic_match("team-a.mkt.*", "team-a.mkt.sh.600000")  # True（末尾 * 匹配多段）
topic_match("team-a.>", "team-a.mkt.sh.600000")       # True（> 匹配多段）
topic_match("a.>.c", "a.b.x.c")            # True（> 匹配多段）
```

---

## 10. 监控 API

### 10.1 HTTP 端点

PulseMQ 内置轻量级监控 HTTP 服务（标准库实现，无外部依赖）。

| 端点 | 方法 | 说明 |
|------|------|------|
| `/api/v1/metrics/realtime` | GET | 实时指标快照 |
| `/healthz` | GET | 健康检查 |

```bash
# 实时指标
curl http://localhost:9090/api/v1/metrics/realtime

# 健康检查
curl http://localhost:9090/healthz
```

### 10.2 实时指标 — RealtimeMetrics

> 模块路径: `pulsemq.monitoring.realtime.RealtimeMetrics`

`/api/v1/metrics/realtime` 返回的 JSON 结构:

```json
{
  "timestamp": 1717600000.0,
  "msg_rate": 52300.5,
  "record_rate": 52300.5,
  "bytes_rate": 4096000.0,
  "latency_p50_ms": 0.19,
  "latency_p99_ms": 0.47,
  "active_connections": 5,
  "active_subscriptions": 12,
  "error_rate": 0.0,
  "dropped_total": 0,
  "backpressure": false,
  "engine_pending_tasks": 8,
  "engine_concurrency_usage": 0.08
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `timestamp` | float | 快照时间戳 |
| `msg_rate` | float | 消息速率（EWMA） |
| `record_rate` | float | 记录速率（EWMA） |
| `bytes_rate` | float | 字节速率（EWMA） |
| `latency_p50_ms` | float | P50 延迟（毫秒，60s 滑动窗口） |
| `latency_p99_ms` | float | P99 延迟（毫秒，60s 滑动窗口） |
| `active_connections` | int | 活跃连接数 |
| `active_subscriptions` | int | 活跃订阅数 |
| `error_rate` | float | 错误速率（EWMA） |
| `dropped_total` | int | 累计丢弃消息数 |
| `backpressure` | bool | 是否触发背压 |
| `engine_pending_tasks` | int | 引擎待处理任务数 |
| `engine_concurrency_usage` | float | 并发使用率 |

#### EWMA — 指数加权移动平均

```python
from pulsemq.monitoring.realtime import EWMA

ewma = EWMA(alpha=0.3)  # alpha 越大越敏感
ewma.update(100.0)
print(ewma.value)  # 100.0
```

#### SlidingWindow — 滑动窗口

```python
from pulsemq.monitoring.realtime import SlidingWindow

window = SlidingWindow(window_seconds=60.0, max_samples=4096)
window.add(0.25)                    # 添加数据点
p50 = window.percentile(50)         # P50
p99 = window.percentile(99)         # P99
print(window.count)                 # 数据点数量
```

---

## 11. 数据模型

> 模块路径: `pulsemq.models`

### AuthUser

```python
@dataclass
class AuthUser:
    user_id: int
    role: str              # "admin" | "user"
    groups: list[str]      # 权限组名称列表
    api_key: str
    namespace: str = ""    # home_namespace

    @property
    def is_admin(self) -> bool
```

### TopicInfo

```python
@dataclass
class TopicInfo:
    full_name: str         # "team-a.mkt.sh.600000"
    namespace: str         # "team-a"
    topic_path: str        # "mkt.sh.600000"
    is_wildcard: bool = False
    subscriber_count: int = 0
    created_at: float = ...

    @classmethod
    def from_name(cls, full_name: str) -> TopicInfo
```

### BufferedMessage

```python
@dataclass(slots=True)
class BufferedMessage:
    topic: str
    seq: int               # topic 内单调递增序号
    record_count: int      # 数据行数
    meta: bytes            # Frame 3 的 2 字节
    payload: bytes         # 序列化+压缩后的 payload
    timestamp: float       # 服务端 接收时间
```

### ExpandedPermissions

```python
@dataclass
class ExpandedPermissions:
    pub: list[str] = []     # 可发布的 topic pattern
    sub: list[str] = []     # 可订阅的 topic pattern
    query: list[str] = []   # 可查询的 topic pattern

    @classmethod
    def from_dict(cls, d: dict[str, list[str]]) -> ExpandedPermissions
```

---

## 12. 过载保护 — DualBuffer

> 模块路径: `pulsemq.engine.overload.DualBuffer`

双缓冲过载保护机制：控制消息和数据消息分池存储，控制路径永不饿死。

```python
from pulsemq.engine.overload import DualBuffer

buf = DualBuffer(data_buffer_size=9000, ctrl_buffer_size=1000)

# 入队（自动根据 msg_type 分流到 data/ctrl buffer）
buf.enqueue(frames)

# 消费（ctrl 优先）
ctrl_msgs = buf.drain_ctrl(limit=10)   # 消费最多 10 条控制消息
data_msgs = buf.drain_data(limit=64)   # 消费最多 64 条数据消息

# 统计
stats = buf.stats
print(stats.dropped_total)        # 累计丢弃数
print(stats.data_buffer_usage)    # 数据缓冲区使用率 (0.0-1.0)
print(stats.ctrl_buffer_usage)    # 控制缓冲区使用率 (0.0-1.0)
```

**丢弃策略:**
- **数据消息** (PUB): 缓冲区满时丢弃新消息
- **控制消息** (AUTH/SUB/QUERY/PING): 缓冲区满时丢弃最旧消息

---

## 13. 消息路由 — MessageRouter

> 模块路径: `pulsemq.engine.router.MessageRouter`

纯内存消息路由器，管理 topic 注册、订阅关系、通配符匹配和消息缓冲。

### Topic 管理

```python
router = MessageRouter()

# 注册 topic
info = router.register_topic("team-a.mkt.sh.600000")

# 查询 topic
info = router.get_topic("team-a.mkt.sh.600000")

# 无订阅者时移除
router.remove_topic_if_empty("team-a.mkt.sh.600000")
```

### 订阅管理

```python
# 精确订阅
router.subscribe(identity=b"client-1", topic="team-a.mkt.sh.600000")

# 通配符订阅
matched = router.subscribe_wildcard(identity=b"client-1", pattern="team-a.mkt.*")
# 返回匹配到的精确 topic 列表

# 取消订阅
router.unsubscribe(identity=b"client-1", topic="team-a.mkt.sh.600000")

# 获取订阅者（含通配符展开，带缓存）
subscribers = router.get_subscribers("team-a.mkt.sh.600000")

# 快速检查是否有订阅者（不拷贝 set，热路径优化）
has_subs = router.has_subscribers("team-a.mkt.sh.600000")

# 获取 identity 的所有订阅
subs = router.get_subscriptions(b"client-1")

# 连接断开时清理所有订阅
router.remove_identity(b"client-1")
```

### 消息缓冲

```python
# 追加消息
msg = router.append_message(
    topic="team-a.mkt.sh.600000",
    meta=b"\x02\x20",
    record_count=1,
    payload=b"\x81\xa5price...",
)

# 回放消息
messages = router.replay_messages("team-a.mkt.sh.600000", from_seq=10, limit=100)

# 最新序列号
seq = router.latest_seq("team-a.mkt.sh.600000")

# 移除 topic 缓冲
router.remove_topic_buffer("team-a.mkt.sh.600000")
```

### 统计

```python
router.topic_count()          # 注册 topic 数
router.subscription_count()   # 总订阅数
```
