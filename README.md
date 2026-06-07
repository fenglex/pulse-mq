# PulseMQ

高性能金融行情消息中间件，基于 ZeroMQ 构建。

## 特性

- **高性能** — 基于 ZeroMQ 异步 I/O
- **Pub/Sub 模式** — 支持 topic 通配符匹配（`*` 单段、`>` 多段）
- **三层消息模型** — `str`（文本/JSON）、`bytes`（二进制透传）、`DataFrame`（结构化数据）
- **多种序列化** — String、Msgpack、PyArrow IPC、Bytes
- **可选压缩** — Snappy、LZ4、Zstandard
- **认证与权限** — ZAP 认证 + 基于 topic 的细粒度权限控制
- **过载保护** — 双缓冲 + 优先级丢弃，控制消息永不饿死
- **实时监控** — EWMA 速率统计、P50/P99 延迟追踪、HTTP 指标 API
- **自动重连** — 指数退避重连策略
- **优雅关闭** — drain 缓冲区后再关闭连接

## 安装

> 要求 Python >= 3.13

```bash
pip install pulsemq
```

包含全部依赖：压缩（Snappy / LZ4 / Zstandard）、序列化（Msgpack / PyArrow / Pandas）均开箱即用。

## 快速开始

### 启动服务端

```bash
# 使用 CLI 命令
pulse-mq

# 或在 Python 中
from pulsemq import PulseServer
server = PulseServer()
await server.start()
```

### 客户端使用

```python
from pulsemq import PulseClient

async with PulseClient("tcp://localhost:5555", api_key="your_key") as client:
    # 发布文本消息
    await client.publish("market.sh.600000", '{"price": 10.5}')

    # 发布二进制数据
    await client.publish("raw.feed", b'\x00\x01\x02')

    # 发布 DataFrame
    import pandas as pd
    df = pd.DataFrame({"price": [10.5, 10.6], "volume": [100, 200]})
    await client.publish("market.data", df, format="msgpack")
    await client.publish("market.data", df, format="pyarrow")

    # 订阅消息（支持通配符）
    async for msg in client.subscribe("market.sh.*"):
        print(msg.topic, msg.payload)

    # 多 topic 订阅
    async for msg in client.subscribe("topic-a", "topic-b", "team-a.>"):
        print(msg.topic, msg.payload)
```

### 使用压缩

```python
await client.publish("topic", data, compression="snappy")
await client.publish("topic", data, compression="lz4")
await client.publish("topic", data, compression="zstd")
```

## 配置

### 环境变量

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `PULSEMQ_BIND` | ROUTER 绑定地址 | `tcp://*:5555` |
| `PULSEMQ_XPUB_BIND` | XPUB 绑定地址 | `tcp://*:5556` |
| `PULSEMQ_DB_URL` | 数据库路径 | `sqlite://./pulse_mq.db` |
| `PULSEMQ_AUTH_ENABLED` | 启用认证 | `true` |
| `PULSEMQ_ADMIN_KEY` | 默认管理员 API Key | `pulse_sk_admin_default` |
| `PULSEMQ_CONCURRENCY` | 最大并发数 | `100` |
| `PULSEMQ_SERIALIZER` | 默认序列化格式 | `msgpack` |
| `PULSEMQ_COMPRESSOR` | 默认压缩算法 | `none` |
| `PULSEMQ_USE_UVLOOP` | 使用 uvloop | `true` |
| `PULSEMQ_ZMQ_RCVHWM` | ZMQ 接收高水位 | `10000` |
| `PULSEMQ_ZMQ_SNDHWM` | ZMQ 发送高水位 | `10000` |

### Python 配置

```python
from pulsemq import ServerConfig, PulseServer

config = ServerConfig(
    bind="tcp://*:5555",
    xpub_bind="tcp://*:5556",
    auth_enabled=True,
    max_concurrency=200,
    default_serializer="msgpack",
)
server = PulseServer(config)
```

## 监控

服务端启动后默认在 `0.0.0.0:9090` 暴露 HTTP 指标接口：

```bash
curl http://localhost:9090/metrics
```

返回 JSON 格式的实时指标：

```json
{
  "timestamp": 1717660800.0,
  "msg_rate": 1250.3,
  "record_rate": 5000.0,
  "bytes_rate": 1048576.0,
  "latency_p50_ms": 0.125,
  "latency_p99_ms": 2.340,
  "active_connections": 42,
  "active_subscriptions": 128,
  "error_rate": 0.0,
  "dropped_total": 0,
  "backpressure": false,
  "engine_pending_tasks": 5,
  "engine_concurrency_usage": 0.05
}
```

## 错误处理

客户端提供完整的异常层级：

```python
from pulsemq import (
    PulseError,            # 基类
    PulseConnectionError,  # 连接失败
    PulseAuthError,        # 认证失败
    PulsePermissionError,  # 权限不足
    PulseTimeoutError,     # 超时
    PulseServerError,      # 服务端错误（含错误码）
)
```

## Topic 通配符

| 通配符 | 说明 | 示例 |
|--------|------|------|
| `*` | 中间位置匹配恰好一个段；末尾匹配一个或多个段 | `market.sh.*` 匹配 `market.sh.600000` |
| `>` | 匹配一个或多个段 | `team-a.>` 匹配 `team-a.mkt.sh.600000` |

## 许可证

[MIT](LICENSE)
