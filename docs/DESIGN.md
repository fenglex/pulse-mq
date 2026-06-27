# PulseMQ 模块设计文档

> 版本：v2 (Client/Server) ｜ 最后更新：2026-06-27
>
> 本文档描述 PulseMQ v2 的模块职责、核心类与接口、关键数据结构、以及模块间的依赖关系。面向项目维护者。

---

## 目录

1. [总体架构](#1-总体架构)
2. [服务端核心 `server.py`](#2-服务端核心-serverpy)
3. [客户端 `client.py`](#3-客户端-clientpy)
4. [传输层 `transport/router.py`](#4-传输层-transportrouterpy)
5. [协议模块 `protocol/`](#5-协议模块-protocol)
   - 5.1 帧编解码 `protocol/frames.py`
   - 5.2 序列化 `protocol/serialization.py`
   - 5.3 压缩 `protocol/compression.py`
   - 5.4 flags 位域 `protocol/flags.py`
   - 5.5 消息类型常量 `protocol/msg_type.py`
6. [路由模块 `routing.py`](#6-路由模块-routingpy)
7. [控制面 `control.py`](#7-控制面-controlpy)
8. [认证模块 `auth.py` 和 `security.py`](#8-认证模块-authpy-和-securitypy)
9. [统计模块 `stats/`](#9-统计模块-stats)
   - 9.1 流量统计 `stats/traffic.py`
   - 9.2 延迟统计 `stats/latency.py`
   - 9.3 连接事件 `stats/connections.py`
   - 9.4 持久化 `stats/storage.py`
10. [管理后台 `admin/`](#10-管理后台-admin)
    - 10.1 HTTP 服务 `admin/server.py`
    - 10.2 Web UI `admin/web_ui.py`
    - 10.3 Token 认证 `admin/auth.py`
11. [Producer 管线 `producers/`](#11-producer-管线-producers)
12. [包入口与 CLI](#12-包入口与-cli)

---

## 1. 总体架构

PulseMQ v2 是一个 **Client/Server 消息中间件**，基于 ZeroMQ ROUTER/DEALER + ZAP PLAIN 认证架构。不含中间 broker，服务端为无状态消息路由器。

### 核心设计原则

- **数据面/控制面分离**：两个 ROUTER socket（数据面 `:5555`，控制面 `:5556`）独立运行，互不阻塞
- **服务端不解压/不反序列化**：`decode_header` 仅提取 topic/record_count/timestamp_ns，payload 盲转发
- **ROUTER identity 即 routing key**：DEALER 侧设置 `ZMQ_IDENTITY`，ROUTER 以其为路由目标
- **单用户单在线**：OnlineRegistry 以 username 为唯一键，重复 REGISTER 返回 ALREADY_ONLINE
- **无 topic 持久化缓冲**：不缓存消息，纯转发

### 模块依赖关系

```
                    ┌─────────────┐
                    │  config.py  │ （环境变量加载，纯数据）
                    └──────┬──────┘
                           │
        ┌──────────────────┼─────────────────────┐
        ▼                  ▼                     ▼
  ┌──────────┐      ┌────────────┐        ┌───────────┐
  │ protocol │◄─────│  server    │───────►│ transport │
  │(帧/编解码) │     │  (核心)     │  ZAP   │(ROUTER套接字)│
  └────┬─────┘      └────┬───────┘        └───────────┘
       │                  │
       │            ┌─────┼──────┬──────────┬──────────┐
       │            ▼     ▼      ▼          ▼          ▼
       │      ┌──────┐┌──────┐┌──────┐┌──────────┐┌──────────┐
       │      │ client│routing│control│ security │  stats   │
       │      │(DEALER)│      │      │(bcrypt)  │  (监控)   │
       │      └──────┘└──────┘└──────┘└──────────┘└────┬─────┘
       │                                                 │
       │                                          ┌──────┴──────┐
       │                                          │  admin/     │
       │                                          │ (HTTP/SSE)  │
       └──────────────────────────────────────────┘──────────────┘
```

**依赖方向（严格单向）**：
- `protocol/`：最底层，不依赖任何业务模块
- `transport/`：仅依赖 `zmq`，不依赖 `protocol/`
- `server.py`：编排中心，依赖 `transport/`、`routing/`、`control/`、`stats/`、`admin/`
- `client.py`：独立客户端，依赖 `protocol/` 和 `transport/`
- `admin/`：依赖 `stats/`（读取数据）、`cache/`（topic buffer 尺寸）
- `security/`、`auth/`：独立，仅被 server 引用

---

## 2. 服务端核心 `server.py`

`Server` 类是服务端入口，组装传输层、路由、控制面、统计、admin 各模块。

### 生命周期

```python
srv = Server(data_endpoint, control_endpoint, admin_endpoint,
             credentials={"user": "pass"}, admin_token="...")
await srv.start()   # bind 数据面 + 控制面 + admin HTTP，启动 4 个后台任务
await srv.stop()    # 取消任务 → 归档统计 → 关闭 admin → drain archive → 关 transport → 关存储
```

### 后台任务

| 任务 | 方法 | 作用 |
|------|------|------|
| 数据面循环 | `_data_loop` | recv → decode_header → stats → match → send |
| 控制面循环 | `_control_loop` | recv → decode_control → dispatch |
| 心跳扫描 | `_heartbeat_sweep_loop` | 每秒检查超时 client（heartbeat_timeout=6s） |
| 分钟归档 | `_minute_roll_loop` | 每分钟滚动 TrafficStats → AsyncArchiveWriter → SQLite |

### 数据流

```
DEALER → [ROUTER] → decode_header → TrafficStats.record → LatencyStats.sample
  → SubscriptionTable.match → [ROUTER] → DEALER
```

核心路由调用链：
```python
hdr = frames.decode_header(frame_bytes)          # 只读头部
self._stats.record(hdr.topic, hdr.record_count, len(hdr.raw_payload))
for target in self._routing.match(hdr.topic):    # 前缀匹配 → set[bytes identity]
    await self._transport.send(target, frame_bytes, role="server_ingress")
```

### 停止顺序

1. `_running = False`, `_stop.set()`
2. 取消 4 个后台任务
3. `roll_minute()` 最后一次归档
4. `_admin.stop()` — 停 SSE + HTTP
5. `_archive_writer.stop()` — drain 剩余 SQLite 写入
6. `_transport.close()` — 关 ZMQ 套接字
7. `_storage.close()` — 关 SQLite 连接

---

## 3. 客户端 `client.py`

`Client` 类（及其子类 `ProducerClient` / `ConsumerClient`）是客户端核心。

### 子类

| 类 | 角色 | 特有能力 |
|----|------|---------|
| `Client` | 通用 | 发布 + 订阅 |
| `ProducerClient` | publisher | `producer()` / `burst_producer()` 装饰器 + `run_forever()` |
| `ConsumerClient` | subscriber | 仅订阅，屏蔽 publish |

### 启动流程

```
start()
  ├─ 数据面 DEALER + PLAIN + monitor → 等待认证裁定
  │   ├─ handshake_ok → 继续
  │   ├─ auth_failed → AuthenticationError (exit 3)
  │   └─ 超时 → ClientStartupError (exit 4)
  ├─ 控制面 DEALER + PLAIN（无 monitor）
  ├─ REGISTER → 等待 reply
  ├─ 恢复订阅（重连场景）
  ├─ 启动 recv_loop + heartbeat_loop
  └─ 切换到运行期 monitor 回调
```

### 重连机制

`_on_runtime_monitor("disconnected")` → `_reconnect_loop()`：

```
指数退避初始 1s，×2 封顶 30s
  └─ 新 Transport → PLAIN 认证 → REGISTER
      ├─ auth_failed → _reconnect_fatal → exit 3
      ├─ ALREADY_ONLINE → 退避重试（等心跳超时释放）
      └─ OK → 恢复订阅 → 重启 recv/heartbeat
```

### 监控角色

`_roles` 列表用于 REGISTER payload，影响 Web UI 统计：
- `Client`: `["publisher", "subscriber"]`
- `ProducerClient`: `["publisher"]`
- `ConsumerClient`: `["subscriber"]`

---

## 4. 传输层 `transport/router.py`

包装 ZMQ ROUTER/DEALER + ZAP PLAIN + monitor。**唯一直接 import zmq 的模块**。

### 核心类

| 类 | 用途 |
|----|------|
| `Transport` | ROUTER bind / DEALER connect / send / recv / monitor |
| `AsyncZAPHandler` | inproc ZAP REP socket，异步处理 PLAIN 认证 |
| `PlainAuthDict` |（兼容层）明文 dict 白名单认证 |

### ZAP 认证流程

```
DEALER connect (PLAIN)
  → libzmq 发 ZAP 请求到 inproc://zeromq.zap.01
  → AsyncZAPHandler._loop recv
  → _handle: run_in_executor(self._auth.verify, username, password)
    └─ CredentialStore.verify → bcrypt.checkpw（线程池）
  → _reply (200/400) → on_auth 回调
```

`verify` 在 `run_in_executor` 中执行，避免 bcrypt.checkpw（~200ms）阻塞事件循环。

### Transport 关键特性

- `ROUTER_MANDATORY=1`：发送到已断开 identity 时抛异常（_control_loop 需 try/except 保护）
- `LINGER=1000`：关闭时等待 1 秒完成待发消息
- monitor 事件： `connected` / `disconnected` / `handshake_ok` / `auth_failed`
- 双 DEALER 共享同一 `ZMQ_IDENTITY`：数据面/控制面呈现相同 bytes identity

---

## 5. 协议模块 `protocol/`

### 5.1 帧编解码 `protocol/frames.py`

**单 bytes 帧格式**：

```
magic(2) ver(1) msg_type(1) flags(1) data_type(1) topic_len(2 BE)
topic(N) ts(8 BE ns) record_count(4 BE) payload(变长) [CRC32?(4)]
```

核心函数：

| 函数 | 用途 | 数据路径 |
|------|------|---------|
| `encode()` | 编码：序列化 + 压缩 → bytes | 发送端 |
| `decode()` | 解码：解压 + 反序列化 + `_restore_type` | 接收端 |
| `decode_header()` | 仅读头部（不解压 payload） | 服务端 `_data_loop` |
| `encode_control()` | 控制面帧编码 | 控制面 |
| `decode_control()` | 控制面帧解码 | 服务端 `_control_loop` |

**`_restore_type(data, data_type, serializer)`**

根据 `data_type` 还原原始 Python 类型：

| 原始类型 | serializer | 存储格式 | 还原方式 |
|----------|-----------|---------|---------|
| DataFrame | msgpack/json | list[dict] | `pd.DataFrame(data)` |
| DataFrame | pyarrow | pa.Table | `data.to_pandas()` |
| dict | pyarrow | pa.Table | `data.to_pylist()[0]` |
| str/bytes | 透传 | 原始类型 | 透传 |

### 5.2 序列化 `protocol/serialization.py`

注册机制：`register(name, serializer)` / `get(name) → Serializer`

| 格式 | 类 | 后端 |
|------|----|------|
| msgpack | MsgpackSerializer | msgspec.msgpack |
| json | JsonSerializer | msgspec.json |
| pyarrow | PyArrowSerializer | pyarrow.ipc |
| str | StrSerializer | UTF-8 |
| bytes | BytesSerializer | 透传 |

### 5.3 压缩 `protocol/compression.py`

| 算法 | 后端 |
|------|------|
| none | 透传 |
| snappy | python-snappy |
| lz4 | lz4.frame |
| zstd | zstandard |

### 5.4 flags 位域 `protocol/flags.py`

编码/解码序列化器（bit 0-2）和压缩算法（bit 3-4）和 CRC（bit 5）。

### 5.5 消息类型 `protocol/msg_type.py`

```python
class MsgType:   DATA=1, CONTROL=2
class DataType:  UNKNOWN=0, DICT=1, DATAFRAME=2, STR=3, BYTES=4
```

---

## 6. 路由模块 `routing.py`

`SubscriptionTable` — topic 前缀匹配路由表。

```
{ROUTER_identity_bytes → set[topic_pattern]}
match(topic) → set[identity_bytes]  (遍历所有 pattern，前缀匹配)
```

匹配规则：
- `foo.*` → 匹配 `foo` 和 `foo.<anything>`
- `foo` → 精确匹配 `foo`

订阅/取消订阅由控制面 `SUBSCRIBE`/`UNSUBSCRIBE` 驱动，数据面 `match()` 只读。

---

## 7. 控制面 `control.py`

| 类 | 用途 |
|----|------|
| `ControlCmd` | 命令常量：REGISTER/HEARTBEAT/SUBSCRIBE/UNSUBSCRIBE/DISCONNECT/KICK |
| `ControlMessage` | 控制消息：cmd(str) + payload(dict) |
| `OnlineRegistry` | 在线用户表：key=username，sweep_timeout() 清理超时 client |
| `RegisterResult` | OK / ALREADY_ONLINE / REJECTED |

`OnlineRegistry` 数据结构：
```
_by_client: {client_id → ClientInfo}
_by_user:   {username → client_id}
```

`heartbeat_timeout=6.0s`：`sweep_timeout()` 每分钟扫描，清理 `last_seen` 超过阈值的记录。

---

## 8. 认证模块 `auth.py` 和 `security.py`

### `auth.py`

`PlainAuth` — ZAP 认证决策器，委托 `CredentialStore`。

```python
class PlainAuth:
    def verify(username, password) → tuple[bool, reason]
```

### `security.py`

`CredentialStore` — bcrypt 哈希凭据持久化。

| 方法 | 作用 |
|------|------|
| `load()` | 加载 TOML 文件；不存在则生成默认 admin |
| `save()` | 原子写 TOML（tmp + rename） |
| `verify(u, p)` | bcrypt.checkpw → `AuthResult` |
| `add_user()` | 新增用户（验证名称合法性） |
| `set_password()` | 原地改密码 |
| `set_enabled()` | 启用/禁用 |
| `reload()` | 热更新（SIGHUP） |
| `list_users()` | 列出全部用户 |

> bcrypt cost 默认 12（约 200ms/次），通过 `run_in_executor` 避免阻塞。

---

## 9. 统计模块 `stats/`

### 9.1 流量统计 `stats/traffic.py`

`TrafficStats` — 分钟粒度 topic 流量，内存 8 小时窗口。

`record(topic, record_count, payload_size)` 数据路径：
```
单次 dict.get → 增 count → 每 1024 条检查分钟滚动
```

`roll_minute()` → 归档当前分钟 → 追加到 slots deque（maxlen=retention_minutes）。

`all_topics_snapshot()` 计算 60 秒滚动均值：当前分钟实测 + 上一分钟按比例外推。

### 9.2 延迟统计 `stats/latency.py`

`LatencyStats` — 固定桶直方图（采样率 1%），P50/P95/P99 线性插值。

桶边界（ns）：50_000 / 100_000 / 500_000 / 1_000_000 / 5_000_000 / 10_000_000 / 50_000_000

`should_sample()` 用 `random.random() < rate` 控制采样。

### 9.3 连接事件 `stats/connections.py`

`ConnectionStats` — 事件环（deque maxlen=200）+ 在线客户端快照。

| 方法 | 触发 |
|------|------|
| `on_connect()` | REGISTER 成功 |
| `on_disconnect()` | DISCONNECT / heartbeat_timeout |
| `on_auth()` | ZAP 认证回调 |

### 9.4 持久化 `stats/storage.py`

`StatsStorage` — SQLite (WAL) 分钟统计持久化。

`AsyncArchiveWriter` — asyncio.Queue 批量写入（batch_size=50），不阻塞数据路径。

---

## 10. 管理后台 `admin/`

### 10.1 HTTP 服务 `admin/server.py`

stdlib asyncio HTTP server，手写请求解析（无框架）。

| 端点 | 用途 |
|------|------|
| `GET /` | Web UI 首页 |
| `GET /static/{path}` | 静态资源（ECharts） |
| `GET /api/v1/stats/realtime` | 实时指标 JSON |
| `GET /api/v1/stats/stream` | SSE 实时推送（1s/帧） |
| `GET /api/v1/clients` | 在线客户端明细 |
| `GET /api/v1/events` | 生命周期事件 |
| `GET /api/v1/topics` | 主题列表 |
| `GET /api/v1/topics/{t}/history` | 分钟级历史 |
| `GET /api/v1/system/status` | 系统状态 |
| `GET /healthz` | 健康检查 |

**独立线程模式**（默认 `admin_thread=True`）：HTTP server 运行在独立 daemon 线程 + 独立 asyncio loop，不阻塞 ZMQ 数据线程。

**Token 认证**：除 `/healthz` 外所有端点需 `?token=` 或 `Authorization: Bearer`。

**SSE**：`sse_events` 字段携带最近 10 条生命周期事件，JS 全量替换 state（无重复）。

### 10.2 Web UI `admin/web_ui.py`

单文件 HTML，内嵌 CSS + JS + ECharts。

组件：
- 指标卡片（4 个）：活跃主题、消息量/s、流量/s、运行时间
- 客户端卡片（4 个）：在线用户、生产者、消费者、订阅数
- 流量趋势图：ECharts line，分钟级精确值，1H/6H 切换，最多 5 topic 叠加
- 延迟柱状图：P50/P95/P99 实时
- 事件流：最近事件，自动滚动
- 主题卡片网格
- Client 详情弹窗（`/api/v1/clients`）

### 10.3 Token 认证 `admin/auth.py`

`TokenAuth` — 校验 `query.token` 或 `Authorization: Bearer` header。

---

## 11. Producer 管线 `producers/`

### `producers/types.py`

回调签名与 `PublisherSender`（v3 遗留，v2 通过 `ProducerClient.producer()` 注册）。

### `producers/manager.py`

`ProducerManager` — 定时/连续 producer 调度。

```python
register(fn, name=topic, interval=5.0, inject_sender=False)
register_burst(fn, name=topic, ...)
start_all(on_produce, sender_factory=None)
stop_all()
```

---

## 12. 包入口与 CLI

### `__init__.py`

版本号 `__version__`（从 `_version.py` 读取）。
Windows 自动设置 `WindowsSelectorEventLoopPolicy`。

### CLI 入口

```bash
pulsemq server          # 启动服务
pulsemq users add       # 添加用户
pulsemq users list      # 列出用户
pulsemq users disable   # 禁用
pulsemq users enable    # 启用
pulsemq users passwd    # 改密码
pulsemq users reload    # 热更新
```

---

## 附录：关键端口

| 端口 | 协议 | 用途 |
|------|------|------|
| 5555 | ROUTER | 数据面（消息收发） |
| 5556 | ROUTER | 控制面（注册/心跳/订阅/断开） |
| 9090 | HTTP | 监控管理界面 |
