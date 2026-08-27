# 01 整体架构

> 来源：对 `src/pulsemq/` 全量源码的反向分析。

## 1. 系统定位

PulseMQ 是一个 **Client/Server 模型的内存消息中间件**（pub/sub），面向高频小消息场景（如行情推送）：

- **内存转发，不持久化消息本体**——`server.py` 模块 docstring 明确 "内存转发，不持久化消息"；持久化的只有监控统计（SQLite 分钟粒度）。
- **投递语义为 at-most-once**：订阅者队列满/离线即丢弃，但丢弃量全程可观测（`DropStats` + 心跳上报 + 信用流控）。
- Python ≥ 3.13，异步 asyncio + ZeroMQ（pyzmq），无 Web 框架依赖（Admin HTTP 用 stdlib 手写）。

## 2. 总体拓扑

```
┌─────────────────────── ProducerClient / Client ───────────────────────┐
│  DEALER(data, PLAIN, monitor)      DEALER(control, PLAIN)             │
│  └─ 同一 bytes identity（= client_id utf-8）──┘                        │
└───────────┬──────────────────────────────────┬────────────────────────┘
            │ tcp://host:5555                  │ tcp://host:5556
            ▼                                  ▼
┌────────────────────────────── Server 进程 ─────────────────────────────┐
│                                                                        │
│  ┌── 数据面线程（独立 zmq.Context，SyncZAPHandler 线程认证）──┐         │
│  │  ROUTER :5555 (ROUTER_MANDATORY)                            │        │
│  │   recv → decode_header → TrafficStats.record                │        │
│  │        → 延迟采样 → SubscriptionTable.match                  │        │
│  │        → broadcast(DONTWAIT + 信用流控) → DropStats          │        │
│  └──────────────▲──────────────────────────────────────────────┘        │
│                 │ PUSH→PULL inproc（内置 producer 从主线程投递）         │
│  ┌── 主线程 asyncio loop ────────────────────────────────────────┐     │
│  │  ROUTER :5556 控制面（AsyncZAPHandler）                        │     │
│  │   REGISTER/HEARTBEAT/SUBSCRIBE/UNSUBSCRIBE/DISCONNECT/        │     │
│  │   LATENCY_REPORT 分发                                         │     │
│  │  后台任务：_control_loop / _heartbeat_sweep_loop(1s) /         │     │
│  │            _minute_roll_loop(60s) / AsyncArchiveWriter        │     │
│  │  ProducerManager 调度（@srv.producer）                         │     │
│  └───────────────────────────────────────────────────────────────┘     │
│  ┌── Admin 线程（独立 loop，admin_thread=True）───┐                     │
│  │  HTTP :9090 — REST + SSE + Web UI + /healthz  │                     │
│  └───────────────────────────────────────────────┘                     │
│  SQLite：data/pulsemq_stats.sqlite（minute_stats 表，WAL）              │
└────────────────────────────────────────────────────────────────────────┘
```

## 3. 端口与端点

| 端点 | 默认值 | Socket | 用途 |
|------|--------|--------|------|
| data_endpoint | `tcp://0.0.0.0:5555` | ROUTER（服务端同步线程）/ DEALER（客户端） | 消息帧收发 |
| control_endpoint | `tcp://0.0.0.0:5556` | ROUTER（异步）/ DEALER（客户端） | 控制命令 + 心跳 + 延迟回传 |
| admin_endpoint | `0.0.0.0:9090` | TCP（stdlib asyncio） | 监控 REST/SSE/Web UI |

## 4. 核心架构决策

| # | 决策 | 动机（源码注释/结构推断） | 关键实现 |
|---|------|--------------------------|----------|
| D1 | **数据面/控制面分离**（两个 ROUTER socket） | 消息洪峰不阻塞控制命令；控制面保持异步低复杂度 | `Transport.bind_sync_data` vs `Transport.bind("control")` |
| D2 | **数据面走同步独立线程**（独立 `zmq.Context`） | 消除 asyncio 事件循环调度延迟，recv→forward 全同步 | `SyncDataThread` |
| D3 | **服务端零反序列化**：只 `decode_header`，payload 透传 | 完整 decode 约占 80% 时间（注释）；路由只需 topic | `frames.decode_header`；服务端不调用 `frames.decode` |
| D4 | **路由键 = ROUTER bytes identity**，非 client_id 字符串 | ROUTER 发送必须用 bytes identity；server 另维护 `client_id→ident` 映射用于心跳超时清理 | `Server._ident_by_client_id` |
| D5 | **客户端数据/控制两个 DEALER 共用同一 identity** | 服务端以控制面 ident 写路由表，可直接向数据面 DEALER 转发 | `Transport.connect(identity=...)` |
| D6 | **ZAP PLAIN + bcrypt 认证**，verify 抛线程池/独立线程 | `bcrypt.checkpw` ~200ms 同步阻塞，不能卡事件循环 | `AsyncZAPHandler`（`run_in_executor`）/ `SyncZAPHandler`（自有线程） |
| D7 | **类型保真协议**：帧内携带 DataType，消费端还原 Python 原始类型 | DataFrame→list[dict]→DataFrame 等往返还原 | `frames._restore_type` |
| D8 | **COW 无锁路由读** | 数据面线程高频 `match()` 不加锁 | `routing.SubscriptionTable` |
| D9 | **背压即丢弃 + 可观测**：DONTWAIT 发送、消费者信用流控、丢弃计数 | 行情场景宁可丢旧不留队尾（head-of-line blocking 防护） | `SyncDataThread.broadcast`、`Server._credits`、`DropStats` |
| D10 | **监控全旁路**：统计在热路径内联记录，HTTP 服务独立线程 | DB 读写不阻塞 zmq 数据循环 | `stats/storage.py` 线程模型注释 |
| D11 | **Admin HTTP 无框架**（stdlib asyncio 手写解析） | 减少依赖面；单文件内嵌 Web UI | `admin/server.py` |
| D12 | **单 bytes 帧协议**（非多段消息） | DEALER/ROUTER 间一个 multipart 即一条消息，编解码边界清晰 | `protocol/frames.py` |

## 5. 进程与线程模型

### 5.1 Server（1 主线程 + 3 辅助线程 + 若干 asyncio 任务）

| 执行体 | 运行内容 | 通信方式 |
|--------|----------|----------|
| 主线程 asyncio loop | 控制面 recv 分发、心跳扫描(1s)、分钟滚动(60s)、`AsyncArchiveWriter` consumer、`ProducerManager` 调度、`AsyncZAPHandler`（异步 ctx 的 ZAP REP） | — |
| 数据面线程（daemon） | `SyncDataThread._loop`：Poller 同时监听 ROUTER 与 PULL；批量 drain；`on_message` 回调内同步转发 | 主线程经 PUSH→PULL inproc 投递发送请求（`send_sync_data`） |
| 数据面 ZAP 线程（daemon） | `SyncZAPHandler._loop`：同步 ctx 的 ZAP REP，Poller 100ms 轮询 | `on_auth` 经 `run_coroutine_threadsafe` 回主 loop |
| Admin 线程（daemon，可配 `admin_thread=false` 关闭） | 独立 event loop 跑 HTTP/SSE | 只读调用各统计对象快照方法（跨线程靠对象内部锁/只读设计） |

跨线程共享对象与保护方式：

| 对象 | 写者 | 读者 | 保护 |
|------|------|------|------|
| `SubscriptionTable` | 主线程（控制面） | 数据面线程 `match()` | COW 原子引用替换，读无锁 |
| `TrafficStats` | 数据面线程 `record()` | 主线程 `roll_minute` / Admin 线程 `snapshot` | 单写者无锁快路径 + `RLock` 慢路径 |
| `LatencyStatsRegistry` ×2 | 数据面线程（半程）/ 控制面（回传） | Admin 线程 | `threading.Lock` |
| `DropStats` | 控制面（心跳） | Admin 线程 | `threading.Lock` |
| `ConnectionStats` 事件环 | 主线程（控制分发/ZAP 回调） | Admin 线程 | 单写者 append + GIL（deque maxlen） |
| `Server._credits` | 控制面写 / 数据面读 | — | GIL（dict 读写原子） |
| SQLite 连接 | 主线程（ArchiveWriter 写） | Admin 线程（读历史） | `check_same_thread=False` + `threading.Lock` 串行 |

### 5.2 Client（1 事件循环 + 可选 1 worker 线程）

| 执行体 | 运行内容 |
|--------|----------|
| asyncio loop | 数据面 recv loop（头部解码 + 延迟采样 + 本地路由匹配 + 入队）、心跳 loop(1s)、重连状态机 |
| decode worker 线程（`decode_queue_size>0` 时） | `_DropQueue` 批量出队 → 完整 decode → 回调分发（同步回调零调度开销，异步回调 `run_coroutine_threadsafe` 回 loop） |

## 6. 数据流

### 6.1 发布路径（热路径）

```
业务回调/ProducerClient.publish
  → frames.encode(topic, data, serializer, compression)     # 单 bytes 帧
  → DEALER(data).send
  → [网络]
  → 数据面线程 ROUTER recv（批量 drain）
  → frames.decode_header(frame)                              # 仅 20B+topic 头部
  → TrafficStats.record(topic, rc, payload_len)              # 无锁快路径
  → should_sample()? → LatencyStatsRegistry(半程).record     # 计数器采样
  → SubscriptionTable.match(topic)                           # COW + 缓存，无锁
  → SyncDataThread.broadcast(targets, frame, credits)        # DONTWAIT + 信用
       ├─ credit==0 → 跳过并计丢弃
       └─ send 失败(EAGAIN) → 计丢弃
  → DropStats.record(topic, dropped)
```

### 6.2 控制路径

```
DEALER(control) → ROUTER(5556) → _control_loop → decode_control
  → _dispatch_control:
     REGISTER    → OnlineRegistry.register → 写路由表 + ident 映射 + 连接事件 → 回 {result, request_id}
     HEARTBEAT   → registry.heartbeat + DropStats.record(drops) + credits[ident]=credit → 回 OK
     SUBSCRIBE   → SubscriptionTable.subscribe + registry 回写 topics + 订阅事件 → 回 OK
     UNSUBSCRIBE → 对称移除 → 回 OK
     DISCONNECT  → 全量清理（路由/映射/信用/registry）+ 断开事件 → 回执失败仅 debug
     LATENCY_REPORT → LatencyStatsRegistry(全程).record（无回执）
```

### 6.3 监控路径

```
数据面/控制面内联统计（内存）
  → 每分钟 _minute_roll_loop：
       TrafficStats.roll_minute() → AsyncArchiveWriter.enqueue → 批量写 SQLite
       LatencyStats(半程/全程).roll_minute()、DropStats.roll_minute()（仅内存滚动）
  → Admin 线程 HTTP：
       /api/v1/stats/realtime 拉取快照
       /api/v1/stats/stream   SSE 每 1s 推快照
```

## 7. 可靠性与流控语义

1. **服务端 → 消费者**：`DONTWAIT` 发送，订阅者队列满（SNDHWM 默认 10000 帧）立即跳过该订阅者，不阻塞其他订阅者。
2. **信用流控**：消费者心跳携带 `credit`（解码队列剩余容量）；为 0 时服务端跳过发送并计丢弃——在 ZMQ 水位之前更早感知消费端积压。
3. **消费端丢弃**：`_DropQueue`（deque maxlen）满时丢最老消息并按 topic 计数，经心跳 `drops` 字段汇聚到服务端 `DropStats`（当前分钟/上一分钟/近 1 小时三级粒度）。
4. **断线**：客户端 monitor 捕获 `disconnected` → 指数退避重连（1s→2s→…→30s 封顶）→ 重新认证/REGISTER/恢复订阅；服务端心跳超时（默认 6s）清除路由。未送达期间消息不补发（at-most-once）。

## 8. 性能设计要点清单（热路径优化）

| 优化点 | 位置 | 手段 |
|--------|------|------|
| 头部解码代替完整解码 | `frames.decode_header` | 只 unpack 20B 定长 + topic；payload 不动 |
| topic 字符串驻留 | `frames._TOPIC_INTERN` | bytes→str 缓存（上限 10000），消除重复 UTF-8 decode |
| 路由匹配缓存 | `SubscriptionTable._match_cache` | {topic: (version, frozenset)}，写操作 version 失效 |
| COW 无锁读 | `SubscriptionTable` | 不可变 `_Index` 快照 + 原子引用替换 |
| 流量统计无锁快路径 | `TrafficStats.record` | 单写者直加；仅新 topic/跨分钟加锁；每 1024 条检查分钟滚动 |
| 延迟采样去 RNG | `LatencyStatsRegistry.should_sample` | 递减计数器（每 1/rate 条采 1 条），O(1) |
| 批量 drain | `SyncDataThread._loop` | 一次 poll 唤醒后 NOBLOCK 连续取完 |
| 零拷贝广播 | `SyncDataThread.broadcast` | ≥1KB payload 用 `zmq.Frame(copy=False)` 复用同一帧 |
| 客户端解码卸载 | `Client._DropQueue` + worker 线程 | recv 线程只做头部解码/匹配/入队 |
| 批量出队 | `_DropQueue.get_batch` | 一次锁获取取 ≤64 条 |
| 后端模块级缓存 import | `frames._pd` / `serialization._msgspec` 等 | 避免热路径重复 import 查找 |
| zstd 线程本地 context | `compression.ZstdCompressor` | threading.local 持有 cctx/dctx |

## 9. 模块依赖层次（自底向上）

```
L0  errors / logging_setup / _version
L1  protocol/（frames, serialization, compression, flags, msg_type）   ← 仅依赖 L0
L2  routing / control / security / producers.types                     ← 领域对象，不依赖传输
L3  transport/router.py                                                ← 全项目唯一 import zmq
L4  stats/（traffic, latency, connections, drops, storage）
L5  server.py（组装 L1–L4 + admin） / client.py（组装 L1–L3）
L6  admin/（server, web_ui, auth）                                      ← 依赖 L4
L7  cli/（server, users）→ 进程入口；lifecycle.py 服务端启停编排
```

循环依赖打破手法：`frames.decode_control` 在函数体内延迟 `from pulsemq.control import ControlMessage`。

## 10. 技术栈

| 依赖 | 用途 |
|------|------|
| pyzmq ≥26 | ROUTER/DEALER/ZAP/monitor/Poller |
| msgspec ≥0.18 | msgpack + json 序列化后端 |
| pyarrow ≥14 / pandas ≥2 | DataFrame IPC 序列化与类型还原 |
| python-snappy / lz4 / zstandard | 压缩算法 |
| bcrypt ≥4 | 凭据哈希（默认 cost 12） |
| loguru ≥0.7 | 结构化日志 |
| SQLite（stdlib） | 分钟统计持久化（WAL） |

打包：hatchling；wheel 额外收录 `src/pulsemq/admin/static/*.js`（本地化 ECharts）。

## 11. 平台适配（Windows 相关）

- 包导入即设置 `WindowsSelectorEventLoopPolicy`（`__init__.py`），规避 Proactor 与 pyzmq fd 的兼容问题。
- 数据面/ZAP 线程退出时**不 close 同步 socket**：Windows bundled libzmq 的 close 会触发 signaler 断言（源码注释），改用 Poller 100ms 轮询让线程自然退出。
- SIGHUP 凭据热更新、`add_signal_handler` 在 Windows 静默跳过；admin token 文件 chmod 在非 POSIX 上降级为提示。
