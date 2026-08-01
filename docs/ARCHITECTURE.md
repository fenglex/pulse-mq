# PulseMQ 架构文档

本文档描述 PulseMQ v9 的系统设计与底层实现，分两部分：**架构概览**（快速建立全局认知）与**底层设计**（逐模块深入）。

---

## 目录

**第一部分：架构概览**

1. [总体架构](#1-总体架构)
2. [模块依赖与职责](#2-模块依赖与职责)
3. [线程模型](#3-线程模型)

**第二部分：底层设计**

4. [传输层 `transport/router.py`](#4-传输层-transportrouterpy)
5. [协议模块 `protocol/`](#5-协议模块-protocol)
6. [路由 `routing.py`](#6-路由-routingpy)
7. [控制面 `control.py`](#7-控制面-controlpy)
8. [服务端 `server.py`](#8-服务端-serverpy)
9. [客户端 `client.py`](#9-客户端-clientpy)
10. [流控与丢弃监控](#10-流控与丢弃监控)
11. [认证与安全](#11-认证与安全)
12. [统计模块 `stats/`](#12-统计模块-stats)
13. [管理后台 `admin/`](#13-管理后台-admin)
14. [Producer 管线 `producers/`](#14-producer-管线-producers)
15. [性能优化清单](#15-性能优化清单)
16. [配置、异常与生命周期](#16-配置异常与生命周期)

---

# 第一部分：架构概览

## 1. 总体架构

PulseMQ 是 Client/Server 模型的消息中间件，基于 ZeroMQ ROUTER/DEALER + ZAP PLAIN 认证。服务端内存转发、不持久化消息（仅统计落 SQLite）。

```
                      ┌─────────────────────────────────────┐
                      │               Server                │
   生产者 DEALER ────→ │  数据面  ROUTER :5555  (同步线程)    │ ────→ 消费者 DEALER
                      │                                     │
   所有客户端 DEALER ⇄ │  控制面  ROUTER :5556  (异步)        │
                      │                                     │
                      │  Admin   HTTP :9090   (独立线程)     │ ──── REST / SSE / Web UI
                      │     ZAP PLAIN (bcrypt)              │
                      └─────────────────────────────────────┘
```

### 核心设计原则

- **数据面/控制面分离** — 两个独立 ROUTER socket：数据面专做高吞吐转发，控制面处理注册/心跳/订阅。互不阻塞。
- **零反序列化路由** — 服务端转发时只调 `decode_header`（提取 topic/计数/时间戳），绝不解压或反序列化 payload。完整 `decode` 由消费者做。
- **路由键 = ROUTER bytes identity** — 不是 client_id 字符串。Transport 发送 `send_multipart([identity_bytes, frame])`。两层面 DEALER 共用同一 bytes identity，使控制面注册的 identity 可直接用于数据面转发。
- **内存转发** — 消息不落盘；统计（分钟级流量）异步写入 SQLite，不阻塞转发路径。

### 端口

| 端口 | 协议 | socket | 线程 | 用途 |
|------|------|--------|------|------|
| 5555 | ROUTER | 数据面 | 同步数据线程 | 消息收发 |
| 5556 | ROUTER | 控制面 | 主事件循环（异步） | REGISTER/HEARTBEAT/SUBSCRIBE/... |
| 9090 | HTTP | asyncio TCP | 独立 admin 线程 | Web UI + REST + SSE |

### 数据流

```
生产者 DEALER
   │ encode() → 单 bytes 帧
   ▼
数据面 ROUTER (同步线程)
   │ decode_header()        仅提取头部，不解 payload
   ├─→ TrafficStats.record(topic, record_count, payload_size)
   ├─→ LatencyStats.sample  按 sample_rate 采样半程延迟
   ├─→ SubscriptionTable.match(topic)  COW 无锁读 + 缓存
   └─→ Transport.broadcast_sync(matched, frame, credits)  DONTWAIT
              │ credit=0 跳过（信用流控）；发送失败计入 DropStats
              ▼
         消费者 DEALER
              │ decode()  完整还原 → _restore_type → PulseMessage
              ▼
         订阅回调
```

---

## 2. 模块依赖与职责

```
                 ┌──────────┐
                 │  cli/    │  pulsemq (server:main) / pulsemq-users (users:main)
                 └────┬─────┘
                      │
                 ┌────▼─────┐
        ┌────────┤ server   ├────────┐
        │        └────┬─────┘        │
        │             │              │
   ┌────▼───┐   ┌─────▼──────┐  ┌───▼────┐
   │ client │   │ transport  │  │ admin  │
   └─┬──┬───┘   │  /router   │  └───┬────┘
     │  │       └─────┬──────┘      │
     │  │             │             │
     │  │   ┌─────────┴─────────┐   │
     │  │   │       protocol     │  │
     │  │   │ frames/serialization│  │
     │  │   │ compression/flags  │  │
     │  │   └────────────────────┘  │
     │  │                           │
  ┌──▼──┴──┐  ┌────────┐  ┌────────▼────────┐
  │routing │  │control │  │     stats/      │
  └────────┘  └────────┘  │ traffic/latency  │
              ┌────────┐  │ connections/drops│
              │  auth  │  │ storage(SQLite)  │
              └───┬────┘  └──────────────────┘
                  │
             ┌────▼────┐
             │security │  CredentialStore (bcrypt + TOML)
             └─────────┘
```

| 模块 | 职责 |
|------|------|
| `server.py` | 组装 transport + routing + control + stats + admin，运行后台任务 |
| `client.py` | `Client`/`ProducerClient`/`ConsumerClient`，认证检测 + 重连 + 收发 |
| `transport/router.py` | **唯一 import zmq 的模块**。ROUTER/DEALER 封装 + 双 ZAP + monitor + 同步数据线程 |
| `protocol/frames.py` | 单 bytes 帧编解码：`encode`/`decode`/`decode_header` |
| `protocol/serialization.py` | 序列化注册表：msgpack/json/pyarrow/str/bytes |
| `protocol/compression.py` | 压缩注册表：none/snappy/lz4/zstd |
| `protocol/flags.py` | flags 位域编解码 |
| `routing.py` | `SubscriptionTable`：topic 前缀匹配，COW 无锁读 + 结果缓存 |
| `control.py` | 控制命令集 + `OnlineRegistry`（在线用户表） |
| `security.py` | `CredentialStore`：bcrypt 哈希 + TOML 持久化 + 默认生成 + 热更新 |
| `auth.py` | `PlainAuth`：ZAP 认证决策器，委托 CredentialStore |
| `stats/traffic.py` | `TrafficStats`：分钟粒度 topic 流量，内存 8h 窗口 |
| `stats/latency.py` | `LatencyStatsRegistry`：固定桶直方图 P50/P95/P99 + 分钟窗口 |
| `stats/connections.py` | `ConnectionStats`：事件环 + 在线客户端快照 |
| `stats/drops.py` | `DropStats`：消费端丢弃按 topic 聚合，分钟桶 + 1h 窗口 |
| `stats/storage.py` | `StatsStorage`(SQLite WAL) + `AsyncArchiveWriter`（异步批量归档） |
| `admin/server.py` | stdlib asyncio HTTP：REST + SSE + Web UI |
| `admin/web_ui.py` | 单文件 Web UI（内嵌 ECharts） |
| `admin/auth.py` | `TokenAuth`：admin token 中间件 |
| `producers/manager.py` | `ProducerManager`：定时/burst 回调调度 |
| `config.py` | `ServerConfig`/`ClientConfig`：TOML + 环境变量 |
| `lifecycle.py` | 统一启动顺序 + SIGINT/SIGTERM 优雅关闭 |
| `errors.py` | 异常体系 + 退出码 |

---

## 3. 线程模型

PulseMQ 运行时涉及 **四个并发执行域**，职责严格隔离：

| 执行域 | 运行内容 | 上下文 |
|--------|---------|--------|
| **主事件循环**（asyncio） | 控制面 ROUTER 收发、心跳扫描、分钟滚动、归档、内置 producer 调度 | 主线程，异步 ctx |
| **同步数据线程** | 数据面 ROUTER `recv → on_message → broadcast`，全程同步 | 独立 daemon 线程，**独立同步 zmq.Context** |
| **Admin 线程** | HTTP/SSE 服务 | 独立 daemon 线程 + 独立 asyncio loop |
| **（可选）decode worker 线程** | 消费端完整 decode + 回调 | 客户端侧 daemon 线程 |

为什么数据面用独立同步线程？

- asyncio 事件循环在 `await` 间存在调度抖动，对追求亚毫秒转发的行情数据是瓶颈。
- 同步线程内 `recv → decode_header → match → broadcast` 全程无 `await`，消除调度延迟。
- 独立 `zmq.Context` 使数据面与异步面的 ZAP 各自独立，互不干扰。
- 主线程通过 PUSH→PULL（`send_sync_data`）向数据线程投递发送请求（如内置 producer），线程安全。

跨线程协作点：

- 数据线程写 `TrafficStats.record` / `LatencyStatsRegistry.record`（线程安全，见 [§12](#12-统计模块-stats)）。
- 数据线程的 ZAP 认证事件通过 `run_coroutine_threadsafe` 回调到主循环的 `ConnectionStats`。
- 主线程内置 producer 通过 `send_sync_data`（PUSH）投递到数据线程转发。

---

# 第二部分：底层设计

## 4. 传输层 `transport/router.py`

**唯一 import zmq 的模块**，封装所有 socket 操作。

### 4.1 核心类

| 类 | 角色 | 说明 |
|----|------|------|
| `Transport` | 服务端/客户端统一入口 | 管理 socket 字典（按 role 索引）、ZAP、monitor |
| `SyncDataThread` | 服务端数据面 | 独立同步 ctx + 独立线程，ROUTER recv→回调→send |
| `AsyncZAPHandler` | 异步控制面 ZAP | inproc REP，`run_in_executor` 跑 bcrypt |
| `SyncZAPHandler` | 同步数据面 ZAP | 独立线程内直接 `bcrypt.checkpw`，认证事件 `run_coroutine_threadsafe` 回主循环 |
| `PlainAuthDict` | 凭据接口 | `verify(username, password) → (ok, reason)`，由 `PlainAuth`/`CredentialStore` 实现 |

### 4.2 ZAP 认证流程

ZeroMQ 的 ZAP（ZeroMQ Authentication Protocol）是 ctx 级单例：同一 `Context` 上所有 `plain_server=True` 的 socket 共享 `inproc://zeromq.zap.01` 的 REP socket，**只能 bind 一次**。

```
DEALER connect(PLAIN) ──→ ROUTER 触发握手
    ROUTER 所在 ctx 的 ZAP REP socket 收到认证请求
    ZAP handler 调 auth.verify(username, password)
        └─ CredentialStore.verify → bcrypt.checkpw（~200ms 阻塞）
    返回 200/400 → libzmq 据此允许/拒绝连接
    握手结果通过 monitor socket 通知（handshake_ok / auth_failed）
```

关键点：

- **两个独立 ctx**：数据面用同步 `zmq.Context`（`SyncZAPHandler`），控制面用 `zmq.asyncio.Context`（`AsyncZAPHandler`）。各自 bind 自己的 inproc ZAP，互不冲突。
- **bcrypt 不阻塞事件循环**：异步 ZAP 用 `loop.run_in_executor` 把 `checkpw` 丢线程池；同步 ZAP 在自己线程内直接调用。
- **ZAP 是 ctx 单例**：`Transport` 用 `_zap_started` 标志确保异步 ctx 只首次 `bind(auth=)` 时创建 handler；控制面 bind 复用数据面已启动的 ZAP。因此 `on_auth` 回调必须在**首次**（数据面）bind 时提供。

### 4.3 SyncDataThread 关键特性

```python
# 用 zmq.Poller 同时监听 ROUTER(recv) 和 PULL(来自主线程的发送请求)
poller = zmq.Poller()
poller.register(self._socket, zmq.POLLIN)   # ROUTER
poller.register(self._pull, zmq.POLLIN)     # PULL（主线程 PUSH 投递）

while running:
    events = poller.poll(timeout=100)
    if ROUTER 可读:
        # 批量 drain：一次 poll 唤醒后连续 NOBLOCK 取完所有消息
        while True:
            parts = socket.recv_multipart(NOBLOCK)  # 直到 Again
            on_message(parts[0], parts[-1])
    if PULL 可读:
        # 主线程投递的发送请求
        msg = pull.recv_multipart(NOBLOCK)
        socket.send_multipart(msg)
```

- **批量 drain**：一次 poll 唤醒后连续 `NOBLOCK` 取完所有可用消息，摊薄 poll 开销。
- **100ms poll 超时**：以便及时检查 `_running` 标志退出。
- **PUSH/PULL 跨线程**：主线程 `send_from_main()` 通过 PUSH→PULL 投递发送请求（如内置 producer），所有 socket 操作都在数据线程内完成，线程安全。
- **不 close socket**（Windows 兼容）：Windows 上 bundled libzmq close 同步 ctx 的 socket 会触发 signaler `Assertion failed`，因此停机时仅置 `_running=False` + join 线程，由 ctx 析构清理。

### 4.4 monitor 机制

客户端 `connect` 时开启 ZMQ monitor，监听握手期事件：

```python
mon = sock.get_monitor_socket(
    EVENT_CONNECTED | EVENT_DISCONNECTED
    | EVENT_HANDSHAKE_FAILED_AUTH | EVENT_HANDSHAKE_SUCCEEDED
)
```

`_monitor_loop` 解析事件为 `connected`/`disconnected`/`auth_failed`/`handshake_ok`，回调 `Transport._on_monitor`。客户端据此实现**启动认证检测**与**运行期断线重连**（见 [§9](#9-客户端-clientpy)）。

### 4.5 Transport 的 `send` / `recv`

- `send(identity, frame, role)`：`identity` 非空时 `send_multipart([identity, frame])`（ROUTER 模式），为空时 `send(frame)`（DEALER 模式）。
- `recv(role)`：返回 `(identity_bytes, frame_bytes)`；DEALER 收到单帧时 identity 为 `b""`。
- `_socket_for(role)`：按 role 取 socket；仅一个 socket 时自动回退，使客户端无需每次传 role。

---

## 5. 协议模块 `protocol/`

### 5.1 帧编解码 `protocol/frames.py`

**单 bytes 帧**（非 ZMQ 多帧）。定长头 + 变长 topic + payload：

```
偏移  字段             长度    编码
0     magic            2       b"PM"
2     version          1       0x01
3     msg_type         1       DATA=0x01 / CONTROL=0x02
4     flags            1       位域（见 5.6）
5     data_type        1       UNKNOWN/DICT/DATAFRAME/STR/BYTES
6     topic_len        2       大端 uint16
8     topic            N       UTF-8
8+N   timestamp_ns     8       大端 int64（纳秒）
16+N  record_count     4       大端 uint32（上限 1,000,000）
20+N  payload          变长     序列化+压缩后的 bytes
      [CRC32]          4?      大端 uint32（flags CRC 位置 1 时追加）
```

定长头用预编译 `struct.Struct`（`_HEAD_BEFORE_TOPIC` / `_HEAD_AFTER_TOPIC`）打包，热路径无格式串解析开销。

核心函数：

| 函数 | 用途 | 反序列化 payload？ |
|------|------|-------------------|
| `encode(topic, data, ...)` | 编码为单 bytes 帧 | — |
| `decode(frame)` → `PulseMessage` | 完整解码（含 payload 还原） | ✅ |
| `decode_header(frame)` → `FrameHeader` | 仅提取头部 | ❌（服务端转发用） |
| `encode_control(cmd, payload)` | 控制帧（cmd 作为 topic） | — |
| `decode_control(frame)` → `ControlMessage` | 解码控制帧 | ✅ |

`FrameHeader`（`@dataclass(slots=True)`）只含 `topic`/`record_count`/`timestamp_ns`/`msg_type`/`raw_payload`，供服务端路由使用。

### 5.2 `encode` 自动推断

`encode` 无需手动指定类型，自动推断：

1. **推断 `data_type`**：`_infer_data_type(data)` 按 Python 类型映射（DataFrame→DATAFRAME，dict→DICT，str→STR，bytes→BYTES）。非白名单类型（list/int/None/set）→ `UNKNOWN` → `TypeError`。
2. **选择默认序列化器**：按 `_SERIALIZER_RULES`（DICT→msgpack，DATAFRAME→pyarrow，STR→str，BYTES→bytes）。显式传 `serializer` 时校验兼容性。
3. **DataFrame + msgpack/json 预处理**：`to_dict(orient="records")` 转 `list[dict]`。
4. **推断 `record_count`**：list→`len`，DataFrame→行数，标量/dict→1。显式传值则覆盖。

### 5.3 `_restore_type(data, data_type, serializer)`

解码后据 `data_type` 还原原始 Python 类型，实现端到端类型保真：

- **DATAFRAME**：pyarrow → `table.to_pandas()`；msgpack/json → `list[dict]` → `pd.DataFrame`。
- **DICT**：pyarrow → `to_pylist()[0]`；其他原样。
- **STR**：bytes → UTF-8 decode。
- **BYTES**：str → UTF-8 encode。

### 5.4 序列化 `protocol/serialization.py`

注册表模式（`Serializer` ABC + `_REGISTRY` dict）。内置：

| 名称 | 类 | 后端 | 说明 |
|------|----|----|------|
| `str` | `StringSerializer` | UTF-8 | str↔bytes |
| `msgpack` | `MsgpackSerializer` | msgspec | 二进制，通用 |
| `json` | `JsonSerializer` | msgspec | 拒绝 bytes（防 base64 变形） |
| `pyarrow` | `PyArrowSerializer` | pyarrow IPC | Table/DataFrame/dict/list[dict]→Table |
| `bytes` | `BytesSerializer` | 透传 | — |

后端 import 缓存到模块级（`_msgspec`/`_pa`/`_pd`），避免热路径重复 import 查找。`pyarrow` 缺失时静默跳过注册。

### 5.5 压缩 `protocol/compression.py`

注册表模式（`Compressor` ABC）。内置 none/snappy/lz4/zstd。

**`ZstdCompressor` 线程安全**（v9 修复）：`ZstdCompressor`/`ZstdDecompressor` 非线程安全，用 `threading.local()` 为每个线程维护独立 context 实例并复用（避免每条消息重建 context，+44% 吞吐）。

### 5.6 flags 位域 `protocol/flags.py`

单字节 flags：

```
bit 7    : CRC 标志（1=追加 CRC32）
bit 6-5  : reserved
bit 4-3  : 压缩算法（00=none, 01=snappy, 10=lz4, 11=zstd）
bit 2-0  : 序列化器（000=msgpack, 001=bytes, 010=pyarrow, 100=str, 101=json）
```

`encode_flags(ser, comp, crc)` 打包，`decode_flags(byte)` 拆包，`has_crc(byte)` 检测 bit 7。

---

## 6. 路由 `routing.py`

`SubscriptionTable`：topic 前缀匹配 → identity 集合。只由控制面驱动写入，数据面只读 `match()`。

### 6.1 COW 无锁读

```python
@dataclass(frozen=True)
class _Index:
    exact: dict[str, frozenset[bytes]]      # 精确 topic → identities
    wild: dict[str, frozenset[bytes]]       # 通配前缀 → identities
    by_identity: dict[bytes, frozenset[str]] # identity → 订阅模式
```

- **写路径**（`subscribe`/`unsubscribe`/`remove`）：持 `_write_lock`，拷贝并更新 `_Index`，原子替换 `self._read_index` 引用，递增 `_version` 并清空缓存。
- **读路径**（`match`，数据面热路径）：**无锁**。直接读 `_read_index` 引用。GIL 保证引用赋值原子，数据面见到的快照永远一致。写频率极低（仅订阅变更），拷贝成本可忽略。

### 6.2 match 结果缓存

```python
self._match_cache: dict[str, tuple[int, frozenset[bytes]]]  # {topic: (version, result)}
```

同一 topic 反复 match（典型发布场景）命中缓存时跳过 `split`/`join` 分配与多次 dict 查找，直接返回缓存的 `frozenset`。写操作递增 `_version` 使缓存失效——`match` 命中时校验 `version` 一致才返回，避免写后读到陈旧结果（v9 优化，12x 提速）。

### 6.3 前缀匹配语义

`foo.*` 匹配 `foo` 和 `foo.<anything>`；否则精确匹配。`match` 遍历 topic 的各级前缀（`a.b.c` 查 `a`、`a.b`）合并通配结果。

---

## 7. 控制面 `control.py`

不依赖 transport，纯逻辑。

### 7.1 命令集

```python
class ControlCmd:
    REGISTER = "REGISTER"
    HEARTBEAT = "HEARTBEAT"
    SUBSCRIBE = "SUBSCRIBE"
    UNSUBSCRIBE = "UNSUBSCRIBE"
    DISCONNECT = "DISCONNECT"
    LATENCY_REPORT = "LATENCY_REPORT"
```

### 7.2 OnlineRegistry

在线用户表，**key = username（单用户单在线）**。

- `register(info)`：username 已存在 → `ALREADY_ONLINE`；否则 `OK`。
- `heartbeat(client_id)`：刷新 `last_seen`。
- `sweep_timeout()`：返回 `now - last_seen > heartbeat_timeout` 的客户端并注销。
- `subscribe`/`unsubscribe`：回写 `ClientInfo.topics`，使在线快照/订阅计数与实际订阅一致。
- `snapshot()`：返回所有在线 client 明细（供监控）。

### 7.3 关键区分：client_id vs identity

- `client_id`：REGISTER payload 里的 app 字符串（uuid4 或用户指定）。
- `identity`：ROUTER 的 **bytes identity**。

Server 维护 `_ident_by_client_id: dict[str, bytes]` 映射。心跳超时清理 routing 时，registry 只知道 client_id 字符串，需经此映射反查 bytes identity 才能清 `SubscriptionTable`。

---

## 8. 服务端 `server.py`

`Server` 组装 transport + routing + control + stats + admin，运行后台任务。

### 8.1 构造

构造顺序有依赖：`ConnectionStats` 持有 `registry.snapshot` 引用，`AsyncArchiveWriter` 持有 `storage` 引用，须在 registry/storage 之后构造。延迟统计分**半程**（`_lat_half`，producer→server）和**全程**（`_lat_e2e`，producer→consumer，consumer 采样回传）两个 registry。

### 8.2 后台任务

| 任务 | 周期 | 职责 |
|------|------|------|
| `_control_loop` | 事件驱动 | 控制面 ROUTER recv → `_dispatch_control` |
| `_heartbeat_sweep_loop` | 每 1s | 扫描超时客户端，清 routing + credits + 触发断开事件 |
| `_minute_roll_loop` | 每 60s | 归档流量/延迟/丢弃统计，入队 AsyncArchiveWriter |

### 8.3 数据面回调 `_on_data_message`

在**数据面线程**中调用，全程同步：

```python
def _on_data_message(ident, frame_bytes):
    hdr = decode_header(frame_bytes)           # 仅头部
    stats.record(hdr.topic, hdr.record_count, len(hdr.raw_payload))
    if lat_half.should_sample():               # 计数器采样
        lat_half.record(hdr.topic, time.time_ns() - hdr.timestamp_ns)
    matched = routing.match(hdr.topic)         # COW 无锁 + 缓存
    if matched:
        dropped = transport.broadcast_sync(matched, frame_bytes, credits)
        if dropped: drop_stats.record(hdr.topic, dropped)
```

### 8.4 控制命令分发 `_dispatch_control`

- **REGISTER**：`registry.register` → OK 时写 routing + `_ident_by_client_id` + `on_connect` 事件；返回 result（关联 request_id）。
- **HEARTBEAT**：刷新 last_seen；提取 `drops`（消费端丢弃，向后兼容老客户端）记入 DropStats；提取 `credit`（剩余 decode queue 容量）存入 `_credits`。
- **SUBSCRIBE/UNSUBSCRIBE**：写 routing + 回写 registry.topics + 订阅事件。
- **DISCONNECT**：清 routing + ident + credits + registry + 断开事件。回执可能因 peer 已离开失败（`ROUTER_MANDATORY` Host unreachable），属预期竞态，降为 debug。
- **LATENCY_REPORT**：consumer 回传端到端延迟，记入 `_lat_e2e`，fire-and-forget 无 ack。

### 8.5 admin token 解析

`_resolve_admin_token` 优先级：显式参数（含空串=禁用）> config > 环境变量 > 随机生成（写文件 + stderr 输出）。POSIX 检查 0600 权限位并告警；Windows 提示目录 ACL 受控。

### 8.6 内置 producer 调度

`@srv.producer(topic, ...)` / `@srv.burst_producer(topic, ...)` 注册到 `ProducerManager`。`start()` 末尾 `start_all`。回调 `_on_server_produce` 复用 `_on_data_message` 的统计口径（`TrafficStats.record` + 采样延迟），使服务端 producer 推送的 topic 在监控中与客户端消息一样可见；通过 `send_sync_data`（PUSH→PULL）投递到数据线程转发。

### 8.7 停止顺序

```
stop():
  _running=False, _stop.set → 取消 3 个后台任务
  → 最后归档一次当前分钟（入队 archive_writer）
  → producer_mgr.stop_all
  → admin.stop（独立线程：call_soon_threadsafe 关闭）
  → archive_writer.stop（drain 剩余落库）
  → transport.close（同步数据线程先关）
  → storage.close（必须在 archive_writer.stop 之后，否则 drain 写已关闭连接）
```

---

## 9. 客户端 `client.py`

### 9.1 启动流程（monitor-based 认证检测）

```
start()
  ├─ 数据面 DEALER connect(PLAIN + monitor) → 创建 startup_event future
  ├─ 等待 monitor 裁定（超时 5s）:
  │   ├─ handshake_ok → _connected=True, _authenticated=True
  │   ├─ auth_failed → close + AuthenticationError（exit 3）
  │   └─ 超时/其他 → close + ClientStartupError（exit 4）
  ├─ 控制面 DEALER connect（复用认证态，不开 monitor）
  ├─ _register() → 发 REGISTER，按 request_id 匹配回复（超时 3s → exit 4）
  ├─ 恢复既有订阅（重连场景；首次为空）
  ├─ （可选）创建 _DropQueue + decode worker 线程
  ├─ recv_loop + heartbeat_loop
  └─ 切换 monitor 回调到运行期（接管断线重连）
```

启动期 `_on_startup_monitor` 仅在 auth-outcome 事件（`handshake_ok`/`auth_failed`）resolve future，`connected`/`disconnected` 忽略（服务器宕机表现为超时）。

### 9.2 重连状态机 `_reconnect_loop`

指数退避（初始 1s，×2，封顶 30s）：

```
disconnected → _reconnecting=True → 创建重连任务
  cancel recv/heartbeat → close 旧 transport
  loop (直到 _stop):
    new_transport → connect(PLAIN + monitor) → 等待认证裁定(5s)
      ├─ auth_failed → 存 _reconnect_fatal + _stop.set + return（不在后台 raise，防 GC 吞掉）
      ├─ handshake_ok → connect 控制面 → _register → 恢复订阅
      │     ├─ 成功 → 重启 recv/heartbeat + 切运行期 monitor + return
      │     └─ 失败 → close + 退避重试
      └─ 超时/其他 → close + 退避重试
```

**关键设计：致命错误不在后台任务内 raise**。asyncio 后台任务的异常会被 GC 吞掉，进程不会 exit 3。因此 `_reconnect_loop` 把 `AuthenticationError` 存到 `self._reconnect_fatal` 并 set `_stop`，由 `run_forever`/`stop` 在主任务上下文重新抛出，使 CLI 经 `exit_code_for` 拿到 exit 3。

**`in_flight` transport 生命周期**：本次重连尚未完整成功的新 transport，pre-handshake 与 post-handshake 取消（`CancelledError` 属 `BaseException`，绕过 `except Exception`）都在 `except BaseException` 中关闭，防 socket/monitor 任务泄漏。

### 9.3 ALREADY_ONLINE 偏差

Spec 规定重连 REGISTER 收到 `ALREADY_ONLINE` 应 exit 4。但服务端 stale 记录要等心跳超时扫描（`heartbeat_timeout=6s`）才释放。若一遇到 ALREADY_ONLINE 立即退出 4，网络闪断后的任何重连都会被旧条目击落，自动重连形同虚设。因此当前实现把 ALREADY_ONLINE 视为暂态失败，退避重试，待心跳扫描释放后自然成功。

### 9.4 两线程消费模型 + `_DropQueue`

```
recv_loop (asyncio):
  recv → decode_header → 延迟采样回传 → 路由匹配
    ├─ decode_queue 存在 → put(frame, hdr, matched) 入队
    └─ 否则 → _inline_decode_and_dispatch（同步 decode + 回调）

worker 线程 (_decode_worker_loop):
  get_batch(64) 批量出队 → 完整 decode → 回调分发
    同步回调在 worker 直接调用；异步回调 run_coroutine_threadsafe 回事件循环
```

`_DropQueue`（`deque(maxlen=N)`）：满时丢弃最老消息，按 topic 计数。`drain_drops()` 取走并清零（供心跳上报），`remaining()` 报告剩余容量（供 credit 流控）。

### 9.5 控制面回复匹配 `_recv_control_reply`

循环 recv 控制面回复直到 `request_id` 匹配，丢弃不匹配的帧（防多订阅 ack 串扰）。兼容旧 server（reply 无 request_id）时退化为直接返回。

---

## 10. 流控与丢弃监控

v9 引入的端到端流控与可观测性。

### 10.1 信用窗口流控

```
消费端 _DropQueue.remaining() ──(心跳 credit 字段)──→ 服务端 _credits[ident]
                                                          │
数据面 broadcast_sync(matched, frame, credits):            │
  for target in matched:                                   ▼
    if credits.get(target, -1) == 0:   ← 信用耗尽，跳过
        drops += 1; continue
    sock.send_multipart([target, frame], DONTWAIT)
```

消费端解码队列满时 `credit=0`，服务端跳过该订阅者的发送，避免向慢消费者堆积消息。

### 10.2 DONTWAIT 非阻塞发送

`broadcast` 用 `zmq.DONTWAIT`：订阅者队列满（HWM）时立即跳过而非阻塞，防 head-of-line blocking（一个慢消费者卡住所有订阅者的转发）。

### 10.3 丢弃统计 `DropStats`

两个丢弃来源聚合到服务端 `DropStats`：

| 来源 | 触发 | 上报路径 |
|------|------|---------|
| 服务端 DONTWAIT 发送失败 / credit 耗尽 | `broadcast_sync` 返回丢弃数 | 直接 `drop_stats.record` |
| 消费端 `_DropQueue` 满 | recv 丢弃最老消息 | 心跳 `drops` 字段（向后兼容老客户端） |

`DropStats`：分钟桶 + 1h 滚动窗口，提供三级粒度：`drops_current`（当前分钟）/ `drops_last_min`（上一完整分钟）/ `drops_1h_total`（近 1h 累计）。

---

## 11. 认证与安全

### 11.1 CredentialStore（`security.py`）

- **bcrypt 哈希**：`_hash_password`（`bcrypt.gensalt(cost)`），`_check_password`（`bcrypt.checkpw`）。默认 cost=12。
- **TOML 持久化**：`save()` 用 f-string 生成 TOML，**原子写**（临时文件 + `os.replace`）。
- **默认生成**：文件不存在时自动生成 `admin` 用户，密码取 `PULSEMQ_ADMIN_PASSWORD` 或随机 16 位（含大小写+数字+符号），明文仅启动时 stderr 输出一次。
- **热更新**：`reload()` 重新读文件，原子替换内存白名单。SIGHUP 触发（`_install_sighup_reload`，仅 POSIX）。
- **内存态**：`from_dict(creds)` 构造无文件 store，save/reload 为 no-op（测试/显式明文 dict）。
- **准入校验**：`_validate_name` 限制用户名/角色仅 `[A-Za-z0-9_-]{1,64}`，阻断 `.`/`"`/`]`/换行等危险字符流入 save 的 f-string，防 TOML 损坏/注入。
- `hash_algo` 非 bcrypt 时告警并回退 bcrypt（argon2 等为预留）。

### 11.2 PlainAuth（`auth.py`）

ZAP handler 兼容接口，委托 CredentialStore。`verify(username, password) → (ok, reason)`，reason 为 `user_not_found`/`invalid_password`/`user_disabled`。

### 11.3 admin token（`admin/auth.py`）

`TokenAuth`：除 `/healthz` 外所有 admin 路由需携带有效 token。token 经 `?token=` query 或 `Authorization: Bearer` header 携带，用 `hmac.compare_digest` 常量时间比较（防时序攻击）。空 token → 禁用（放行，向后兼容）。

---

## 12. 统计模块 `stats/`

### 12.1 流量统计 `stats/traffic.py`

`TrafficStats`：分钟粒度 topic 流量，内存 8h 窗口（`deque(maxlen=480)`）。

**无锁 record（v9 热路径优化）**：数据面线程是 `_current` 的**唯一写者**。常规路径（topic 已存在、未跨分钟）**不加锁**，直接累加 `msg_count`/`record_count`/`bytes_total`。仅在新 topic 首次出现或分钟切换时加锁（每分钟最多触发一次）。每 1024 条（`msg_count & 0x3FF == 0`）检查一次分钟滚动，防全热 topic 永不觉滚动。

`all_topics_snapshot()`：近 60s 滚动均值——当前分钟累积 + 上一分钟按 `(60-elapsed)/60` 比例补齐。

### 12.2 延迟统计 `stats/latency.py`

`LatencyStats`：固定桶直方图 + P50/P95/P99。

桶上界（ns）：0.05 / 0.1 / 0.5 / 1 / 5 / 10 / 50ms，末桶 `[50ms, +∞)`。分位数采用**桶内线性插值**——目标分位落入某桶时，按桶内位置在 `[下界, 上界]` 间插值，比固定代表值更准确。

`LatencyStatsRegistry`：按 topic + 分钟窗口。**计数器采样**（v9 优化）：`should_sample` 用递减计数器（每 `1/rate` 条采样 1 条）替代 `random.random()`，消除热路径 RNG 开销，确定性采样方差更低。两个实例：

- **半程**（`_lat_half`）：数据面 `time.time_ns() - hdr.timestamp_ns`，producer→server。
- **全程**（`_lat_e2e`）：consumer 采样回传（`LATENCY_REPORT`），producer→consumer。

分钟滚动归档为 `MinuteLatency`（p50/p95/p99/count）存入 history deque，供延迟趋势曲线。

### 12.3 连接事件 `stats/connections.py`

`ConnectionStats`：事件环（`deque(maxlen=200)`）+ 在线客户端快照。

事件埋点（`on_connect`/`on_disconnect`/`on_subscribe`/`on_unsubscribe`/`on_auth`）**无锁**——单写者（Server 数据/控制线程）+ GIL，`deque.append` 原子。事件 `type` 统一小写（connect/disconnect/subscribe/unsubscribe/auth），与前端颜色分类对齐。

`online_clients()` 读 `registry.snapshot()`（跨线程只读快照），算在线时长。`counters()` 统计 online_users/producers/consumers/total_subscriptions。

### 12.4 持久化 `stats/storage.py`

`StatsStorage`：SQLite（WAL 模式），分钟统计表 `(topic, timestamp, msg_count, record_count, bytes_total)`。

- **跨线程**：`check_same_thread=False` 打开 + `threading.Lock` 串行化所有连接操作。锁只在「主线程归档任务」与「admin 线程」间共享，zmq 数据接收循环从不触碰 SQLite，DB 读写不阻塞 zmq。
- `load_history`：进程重启后恢复图表（内存 + SQLite 合并去重）。
- `cleanup(retention_days)`：清理过期数据。

`AsyncArchiveWriter`：分钟归档异步批量写。`enqueue` 进 `asyncio.Queue`，consumer 任务批量 `save_minutes_batch`。`stop()` 先取消 consumer 再在主上下文 drain 剩余项（可靠，不依赖被取消任务执行）。

### 12.5 丢弃统计 `stats/drops.py`

见 [§10.3](#103-丢弃统计-dropstats)。

---

## 13. 管理后台 `admin/`

### 13.1 HTTP 服务 `admin/server.py`

**stdlib asyncio HTTP**，手写请求解析（`asyncio.start_server` + `StreamReader`），不引入框架。

**独立线程模式**（`admin_thread=True`，默认）：在独立 daemon 线程 + 独立 asyncio loop 上运行，HTTP 请求不阻塞 ZMQ 数据线程。`start()` 阻塞至线程内 server 就绪（`_thread_started` Event）后返回。

**端点**：

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | Web UI 首页（内嵌 ECharts 单文件 HTML） |
| GET | `/static/{path}` | 静态资源（echarts.min.js，路径穿越防护） |
| GET | `/api/v1/stats/realtime` | 实时指标（topics/latency/drops/计数/事件/start_time） |
| GET | `/api/v1/stats/stream` | SSE 实时推送（1s 一帧） |
| GET | `/api/v1/clients` | 在线客户端明细 |
| GET | `/api/v1/events?limit=` | 生命周期事件 |
| GET | `/api/v1/topics` | topic 列表 + 当前指标 |
| GET | `/api/v1/topics/{topic}/history?minutes=` | 分钟级历史（内存+SQLite 合并去重） |
| GET | `/api/v1/latency/topics/{topic}/history?minutes=&kind=half\|e2e` | 延迟历史 |
| GET | `/api/v1/system/status` | 版本/uptime |
| GET | `/healthz` | 健康检查（无需 token） |

**SSE**：`_sse_broadcast_loop` 每 1s 广播 `_realtime_snapshot`。客户端队列满（64）时主动取消该连接，避免死客户端残留造成内存泄漏。

**停机**（独立线程模式）：`call_soon_threadsafe` 在 admin loop 上同步关闭 server + 取消 SSE 任务（不创建协程，避免 loop 关闭前协程未执行的 RuntimeWarning）。

### 13.2 Web UI `admin/web_ui.py`

单文件 HTML（`INDEX_HTML` 字符串），内嵌 ECharts（`/static/echarts.min.js`，1MB，由 `scripts/fetch_echarts.py` 一次性下载）。深色主题，展示指标卡片、流量趋势折线图、延迟趋势曲线、sparkline、事件流、在线客户端详情。

### 13.3 token 认证 `admin/auth.py`

见 [§11.3](#113-admin-token-adminauthpy)。

---

## 14. Producer 管线 `producers/`

### 14.1 `producers/types.py`

数据白名单类型 `PubData = DataFrame | dict | bytes | str`，与运行时 `encode` 的类型校验一一对应。回调签名 `ProducerCallback = Callable[[], Awaitable[PubData | None]]`。

### 14.2 `producers/manager.py`

`ProducerManager`：回调注册 + asyncio Task 并发调度。两种模式：

- **普通 producer**（`_run_loop`）：固定延迟调度——执行 → `sleep(interval - elapsed)` → 执行。`elapsed ≥ interval` 时 `sleep(0)` 不积压。异常不崩溃，warning 后继续。
- **burst producer**（`_run_burst_loop`）：无间隔连续发送，回调返回 `None` 停止。异常后 0.1s 冷却防空转。

### 14.3 两种接入方式

- **服务端内置**：`@srv.producer` / `@srv.burst_producer` → `_on_server_produce`（encode → 统计 → 路由 → `send_sync_data` 投递数据线程）。
- **客户端**：`ProducerClient.producer` / `burst_producer` → `_on_produce`（`publish` 经网络发送）。

---

## 15. 性能优化清单

v9 对服务端热路径的优化（`_on_data_message` 每条消息执行）：

| 优化 | 位置 | 效果 |
|------|------|------|
| routing.match() 结果缓存 + version 校验 | `routing.py` | 同 topic 反复 match 跳过 split/join，12x 提速 |
| topic interning（有界缓存 10000） | `frames.py:_intern_topic` | 消除每消息 UTF-8 decode 分配 |
| TrafficStats 无锁 record（单写者） | `traffic.py` | 常规路径不加锁，仅新 topic/分钟切换加锁 |
| 计数器采样替代 random.random() | `latency.py` | O(1) 无 RNG 开销，确定性采样 |
| 零拷贝广播（≥1KB `zmq.Frame` + `copy=False`） | `router.py:broadcast` | 大 payload 广播免拷贝 |
| DONTWAIT 非阻塞发送 | `router.py:broadcast` | 防 head-of-line blocking |
| 批量 drain（一次 poll 连续 NOBLOCK 取完） | `router.py:_loop` | 摊薄 poll 开销 |
| FrameHeader `@dataclass(slots=True)` | `frames.py` | 减少 per-instance 内存开销 |
| Zstd context 线程安全复用（`threading.local`） | `compression.py` | 免每消息重建 context，+44% |
| 模块级缓存后端 import（`_pd`/`_msgspec`/`_pa`） | `frames.py`/`serialization.py` | 免热路径重复 import 查找 |
| 预编译 `struct.Struct` 打包定长头 | `frames.py` | 免格式串解析 |

---

## 16. 配置、异常与生命周期

### 16.1 配置 `config.py`

TOML + 环境变量，全默认值，零配置可启动。环境变量覆盖 TOML，显式构造参数覆盖一切。

- `ServerConfig`（`config.py:17`）：data/control/admin endpoint、credentials_file、heartbeat_timeout、stats_db、stats_retention_minutes、bcrypt_cost、admin_token/file、sse_interval、latency_sample_rate、event_ring_size、stats_archive_batch_size、admin_thread、ui_enabled、retention_days、sndhwm/rcvhwm。
- `ClientConfig`（`config.py:46`）：endpoint、username/password、client_id、heartbeat_interval、reconnect_*（initial/max/backoff）、sndhwm/rcvhwm、decode_queue_size。
- `__post_init__`：确保 `data/` 目录存在。

### 16.2 异常与退出码 `errors.py`

统一异常体系，每个异常类绑定 `exit_code`。`exit_code_for(exc)` 返回退出码。

| exit code | 异常 | 含义 |
|-----------|------|------|
| 1 | `PulseMQError` | 通用 |
| 2 | `TransportError` / `ConnectionError` | 传输/连接 |
| 3 | `AuthenticationError` | 认证失败 |
| 4 | `ClientStartupError` | 客户端启动失败 |
| 5 | `FrameError` / `SerializationError` | 帧/序列化 |
| 6 | `ConfigurationError` / `SecurityError` | 配置/安全 |
| 7 | `ResourceExhaustedError` | 资源耗尽 |

### 16.3 生命周期 `lifecycle.py`

`run_server(server)`：启动 Server，监听 SIGINT/SIGTERM 触发优雅关闭，返回退出码。`server.stop()` 完成所有清理（取消任务、关闭 transport 等），10s 超时强制退出（防某步骤卡死致进程无法终止）。Windows 不支持 `add_signal_handler`，静默跳过。

### 16.4 日志 `logging_setup.py`

loguru 初始化：stderr sink + 文件 sink（`data/logs/pulsemq_{time:YYYY-MM-DD}.log`，每日滚动，保留 30 天）。`log_event(level, event_type, **fields)` 输出结构化生命周期事件。Windows 强制 `WindowsSelectorEventLoopPolicy`（`__init__.py`）。
