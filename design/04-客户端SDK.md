# Client SDK 接口设计

## 定位

用户最终使用的 API，决定系统体验。仅提供异步客户端。

Sync 场景：用户自行用 `asyncio.run()` 包装异步调用，无需单独维护同步客户端。

---

## Async Client（主）

### 基本用法

```python
client = PulseClient("tcp://localhost:5555")

await client.connect()
await client.auth(api_key="pulse_sk_xxx")

# 发布（单条）
await client.publish("team-a.mkt.sh.600000", {"price": 15.8, "volume": 1000})

# 发布批量 DataFrame（自动设置 record_count）
import pandas as pd
df = pd.DataFrame({"price": [15.8, 15.9], "volume": [1000, 2000]})
await client.publish("team-a.mkt.sh.600000", df, format="pyarrow")  # record_count=2 自动设置

# 指定序列化格式
await client.publish("topic", {"data": 1}, format="pyarrow")

# 订阅（异步迭代器）
async for msg in client.subscribe("team-a.mkt.*"):
    print(msg.topic, msg.payload)

# 取消订阅
await client.unsubscribe("team-a.mkt.*")

# 管理查询
status = await client.query({"action": "system_status"})

# 断开
await client.disconnect()
```

### Context Manager（推荐）

```python
async with PulseClient("tcp://localhost:5555", api_key="xxx") as client:
    await client.publish("topic", {"data": 1})
    # 自动 auth + 自动 disconnect
```

### 批量发布

```python
await client.publish_batch([
    ("topic1", {"data": 1}),
    ("topic2", {"data": 2}),
])

# 指定格式
await client.publish_batch(messages, format="pyarrow")
```

### 带重试的发布

```python
await client.publish("topic", data, retry=3, retry_delay=0.1)

# 重试策略: 指数退避
# 第1次失败: 等 0.1s → 重试
# 第2次失败: 等 0.2s → 重试
# 第3次失败: 等 0.4s → 重试
# 全部失败: 抛出 ConnectionError
```

### 运行中更改 topic 过滤

```python
async for msg in client.subscribe("team-a.mkt.*"):
    print(msg)
    if some_condition:
        await client.unsubscribe("team-a.mkt.*")
        await client.subscribe("team-b.mkt.*")  # 切换过滤
```

---
## 配置

```python
PulseClient(
    address: str,                     # 服务端地址 "tcp://host:port"
    api_key: str = None,              # API Key，也可以 connect 后调用 auth()
    auto_reconnect: bool = True,      # 自动重连
    reconnect_initial_delay: float = 1.0,     # 初始重连间隔
    reconnect_max_delay: float = 30.0,        # 最大重连间隔
    reconnect_backoff: float = 2.0,           # 退避因子
    heartbeat_interval: float = 10.0,         # 应用层 PING 间隔（延迟采样用）
    recv_timeout: float = 5.0,               # 单次接收超时
    connect_timeout: float = 5.0,             # 连接超时
    serializer: str = "msgpack",              # 默认序列化
    compressor: str = "none",                 # 默认压缩
)
```

### 重连策略

```
第1次重连: 等 1.0s
第2次重连: 等 2.0s
第3次重连: 等 4.0s
...
第N次重连: 等 min(1.0 * 2^N, 30.0)s

连接成功后: 重置计数器
```

---

## 消息对象

```python
class PulseMessage:
    topic: str          # "team-a.mkt.sh.600000"
    msg_type: int       # msg_type 枚举 (0x0A = BROADCAST)
    payload: Any        # 自动反序列化
    raw_payload: bytes  # 原始字节（不反序列化时用）
    meta_flags: int     # flags 字节（序列化/压缩信息）
    timestamp: float    # 本地接收时间
```

---

## 错误处理

```python
class PulseError(Exception): pass
class ConnectionError(PulseError): pass      # 连接失败
class AuthError(PulseError): pass            # 认证失败 (code 100x)
class PermissionError(PulseError): pass      # 权限不足 (code 200x)
class TimeoutError(PulseError): pass         # 超时
class ServerError(PulseError):               # 服务端返回的 ERROR 消息
    code: int
    message: str

# 使用
try:
    await client.publish("topic", data)
except PermissionError:
    print("no permission")
except ConnectionError:
    print("lost connection, will retry")
```

---

## 注意事项

### 订阅和发布的分离

```
推荐: 一个 client 实例只做 publish 或只做 subscribe
      不要混合使用同一个连接做 pub+sub
原因: ZMQ DEALER 是请求/响应模式，PUB 是 fire-and-forget
      混合会干扰 ZMQ 的接收循环

发布专用: PulseClient(...) + publish()
订阅专用: PulseClient(...) + subscribe() 异步迭代器
管理查询: 独立 client 或复用发布连接
```

### 序列化格式

```
默认 msgpack（速度快，紧凑）
支持 pyarrow（行情结构化数据场景）
支持 raw（纯字节流，不做任何序列化）
暂不支持 JSON（性能不足，不推荐用于行情数据）
```
