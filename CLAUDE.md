# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test

```bash
# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_frames_v2.py

# Run a single test
uv run pytest tests/test_frames_v2.py::test_decode -v

# Install dev dependencies
uv sync --dev

# Build package
uv run python -m build

# Publish to PyPI
uv run python -m twine upload dist/*

# Syntax check only
uv run python -c "import ast; ast.parse(open('src/pulsemq/server.py').read()); print('OK')"
```

## Architecture Overview

Client/Server 消息系统，基于 ZeroMQ ROUTER/DEALER + ZAP PLAIN(bcrypt) 认证。

### 模块结构

```
src/pulsemq/
├── server.py          # 服务端：ROUTER 数据面+控制面 + 路由 + 统计 + admin
├── client.py          # 客户端：ProducerClient / ConsumerClient（DEALER）
├── transport/
│   └── router.py      # ZMQ ROUTER/DEALER 封装 + ZAP 认证 handler + monitor
├── protocol/
│   ├── frames.py      # 单 bytes 帧编解码：encode/decode/decode_header
│   ├── serialization.py  # msgpack/json/pyarrow/str/bytes
│   ├── compression.py    # none/snappy/lz4/zstd
│   ├── flags.py         # 帧标志位编解码
│   └── msg_type.py      # MsgType / DataType 常量
├── routing.py         # SubscriptionTable：topic 前缀匹配 → identity
├── control.py         # OnlineRegistry + ControlCmd 常量
├── security.py        # CredentialStore：bcrypt 哈希 + TOML 持久化
├── auth.py            # PlainAuth 认证决策器
├── stats/
│   ├── traffic.py     # TrafficStats：分钟粒度 topic 流量（内存 8h 窗口）
│   ├── latency.py     # LatencyStats：固定桶直方图 P50/P95/P99
│   ├── connections.py # ConnectionStats：事件环 + 在线客户端快照
│   └── storage.py     # StatsStorage(SQLite) + AsyncArchiveWriter
├── admin/
│   ├── server.py      # HTTP REST + SSE 服务（独立线程）
│   ├── web_ui.py      # 单文件 Web UI（内嵌 ECharts）
│   └── auth.py        # TokenAuth
├── producers/
│   ├── manager.py     # ProducerManager：定时/burst 回调调度
│   └── types.py       # PubData 白名单 + 回调签名
├── config.py          # ServerConfig：环境变量 + TOML 配置
├── cli/
│   ├── server.py      # `pulsemq server` 入口
│   └── users.py       # `pulsemq users` 子命令
└── logging_setup.py   # loguru 初始化 + 每日滚动文件
```

### 端口

| 端口 | 协议 | 用途 |
|------|------|------|
| 5555 | ROUTER | 数据面（消息收发） |
| 5556 | ROUTER | 控制面（REGISTER/HEARTBEAT/SUBSCRIBE/DISCONNECT） |
| 9090 | HTTP | 监控 Web UI + REST + SSE |

### 数据流

```
DEALER → [ROUTER] → decode_header → TrafficStats.record → LatencyStats.sample
  → SubscriptionTable.match → [ROUTER] → DEALER
```

服务端不解压/不反序列化 payload（`decode_header` 仅提取头部），转发后由消费者完整 `decode` 还原。

### 客户端生命周期

```
start()
  ├─ 数据面 DEALER + PLAIN + monitor → 等待认证裁定
  │   ├─ handshake_ok → 继续
  │   ├─ auth_failed → AuthenticationError (exit 3)
  │   └─ 超时 → ClientStartupError (exit 4)
  ├─ 控制面 DEALER（无 monitor）
  ├─ REGISTER → ALREADY_ONLINE 退避 or OK
  ├─ 订阅恢复
  └─ recv_loop + heartbeat_loop(1s)

断线重连（指数退避 1s→2s→4s→...→30s 封顶）：
  disconnected → cancel bg tasks → 新 Transport → PLAIN 认证 → REGISTER
    ├─ ALREADY_ONLINE → 退避重试（等心跳 6s 超时释放）
    ├─ auth_failed → _reconnect_fatal → exit 3
    └─ OK → 恢复订阅 → 重启 recv/heartbeat
```

### 协议帧格式

单 bytes 帧（通过 DEALER/ROUTER 传输）：

```
magic(2) ver(1) msg_type(1) flags(1) data_type(1) topic_len(2 BE)
topic(N) ts(8 BE ns) record_count(4 BE) payload [CRC32?(4)]
```

- `data_type`: UNKNOWN(0), DICT(1), DATAFRAME(2), STR(3), BYTES(4)
- `_restore_type` 在 decode 时根据 data_type 还原 Python 原始类型（DataFrame→DataFrame，dict→dict）

### 服务端内置 producer

```python
srv = Server(...)

@srv.producer("market.tick", interval=2.0, serializer="msgpack")
async def gen():
    return {"symbol": "AAPL", "price": 180.5}
```

自动编码为协议帧并通过 `_routing.match(topic)` 路由到所有匹配订阅者。

### 认证

- **bcrypt CredentialStore**：TOML 文件持久化，`pulsemq users add/list/disable/enable/passwd/reload`
- **ZAP PLAIN**：`AsyncZAPHandler` 在 `run_in_executor` 中调用 `bcrypt.checkpw`，避免阻塞事件循环
- **Admin token**：32 字节随机 base64url，通过 `?token=` 或 `Authorization: Bearer` 传递

### 关键配置

```bash
# 凭据文件路径
PULSEMQ_CREDENTIALS_FILE=./pulsemq_users.toml
# 监控 token
PULSEMQ_ADMIN_TOKEN=xxx
# 心跳超时（秒）
PULSEMQ_HEARTBEAT_TIMEOUT=6.0
# 延迟采样率（默认 1%）
PULSEMQ_LATENCY_SAMPLE_RATE=0.01
# bcrypt 代价因子
PULSEMQ_BCRYPT_COST=12
```
