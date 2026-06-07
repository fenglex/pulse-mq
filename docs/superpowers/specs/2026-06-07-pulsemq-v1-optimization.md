# PulseMQ v1.0 优化设计 — 性能 / 后台管理 / 监控 / 权限 / 批量

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** PulseMQ v0.6.0 → v1.0 的全面优化: 跨平台性能、后台管理 (Web UI + API)、完整监控 (每分钟粒度, 7 天 TTL)、用户/权限/订阅管控、可配置批量发布 (count + time, 低延迟)。

**Architecture:**
- **跨平台**: Linux 走 uvloop + epoll; Windows 走 SelectorEventLoop + IOCP (zmq.asyncio 兼容)
- **后台管理**: 复用现有 `MetricsHTTPServer`, 扩展为 `AdminServer` (REST + 静态文件 + WebSocket 实时推送)
- **监控分层**: 内存实时 (SlidingWindow 1-min) → 分钟级聚合 → SQLite 7 天 TTL (后台 task 自动清理)
- **权限**: 扩展现有 `PermissionService`, 加 `batch_config` 字段
- **批量**: 客户端 `Batcher` (count + time 双触发, first-of-both), 服务端 `Batcher` (按 topic 聚合多 sub 广播)

**Tech Stack:** Python 3.13+, pyzmq 27.x, msgpack, pyarrow, pandas, aiosqlite, vanilla HTML/JS (no React/Vue), Server-Sent Events (SSE) for 实时推送

**Spec:** 当前文件

---

## 一、跨平台性能优化

### 1.1 事件循环策略

| 平台 | 事件循环 | 原因 |
|------|----------|------|
| Linux | uvloop | 高性能 epoll 封装, 5-10% 提速 |
| Windows | SelectorEventLoop | pyzmq 不兼容 Proactor (会抛 NotImplementedError) |
| macOS | SelectorEventLoop + uvloop | uvloop 在 macOS 上工作 |

`pulsemq/event_loop.py` 已有 `install_event_loop()`, 验证它按平台正确选择。

### 1.2 客户端批量 (Client-side Batching)

**核心 API**:
```python
client = PulseClient(
    address=...,
    batch_size=10,           # 攒够 10 条 flush
    batch_interval_ms=10,    # 或每 10ms flush (取先到)
    batch_max_wait_ms=50,    # 硬上限: 即使没攒够 10 条, 50ms 也必须发
)
```

**Batcher 行为 (first-of-both)**:
- 单条 `publish()` 调用不立即发, 入队
- 满足任一条件即 flush:
  1. 队列长度 ≥ `batch_size`
  2. 距离首次入队 ≥ `batch_interval_ms` (低延迟)
  3. 距离上次 flush ≥ `batch_max_wait_ms` (硬上限)
- 同一批共享 ser/comp (必要时内部按 key 分组)
- flush 时把 N 条 payload 合并为单帧 (格式: `[N, payload1, payload2, ...]`)
- 旧 API `publish()` 单条直发, 默认 batch 关闭 (`batch_size=1`)

**性能影响 (预期)**:
- 单条 publish 在金融行情下 30k msg/s → 批量后 100k+ msg/s (3x)
- 延迟 p50: 0.01ms → 0.5ms (微增, 可接受)
- 延迟 p99: 0.1ms → 1ms (可控)
- 批量大小 = 1 时无影响 (向后兼容)

**格式**: 用 msgpack 编码 N 条 payload 数组:
```python
batch_payload = msgpack.packb([payload1, payload2, ..., payloadN])
```

接收端 (server / sub) 解包后逐条 yield 出去, 现有 `msg.payload` 仍为单条 (不破坏 API)。

### 1.3 服务端批量广播 (Server-side Batching)

**场景**: 同一 topic 有 N 个订阅者, 收到 1 条 PUB 时, server 要广播 N 次。
**优化**: 同 topic 聚合 N 个 sub 的广播, 单次 XPUB 推送 N 帧 (ZMQ 原生支持)。

**限制**: 单帧 sub 端只收到 1 条, 现有 server 已是这种行为, 无需修改。
**优化点**: batch 推送时的 latency 改进 (XPUB 单次 send 比 N 次快)。

### 1.4 性能优化清单

| 优化项 | 预计收益 | 实施位置 |
|--------|----------|----------|
| uvloop (Linux) | +10% 吞吐 | `event_loop.py` |
| 客户端批量 (count+time) | +200% 吞吐, +0.5ms p50 | 新增 `client/batcher.py` |
| 服务端批量广播 (聚合) | +20% 高 fan-out 场景 | `engine/handlers.py` |
| 内存池 (FrameCodec 复用) | -30% GC 压力 | `engine/pool.py` |
| 零拷贝 (msgpack view) | -20% CPU 大 payload | `serialization/registry.py` |
| 心跳优化 (延迟 + 自适应) | -50% 控制帧 | `engine/handlers.py` |

---

## 二、后台管理系统

### 2.1 系统架构

```
┌──────────────────────────┐
│   浏览器 (Web UI)         │
│   - 实时指标 (SSE 推送)   │
│   - 用户/权限管理         │
│   - 主题/订阅管理         │
└──────────┬───────────────┘
           │ HTTP + SSE
┌──────────▼───────────────┐
│   AdminServer            │
│   (扩展 MetricsHTTPServer)│
│   - REST API             │
│   - 静态文件 (HTML/JS)    │
│   - SSE 推送             │
└──────────┬───────────────┘
           │ 共享内存 (无 IPC, 同进程)
┌──────────▼───────────────┐
│   AdminBackend           │
│   - RealtimeMetrics      │
│   - MinuteAggregator     │
│   - SQLiteStatsRepo       │
│   - UserStore            │
│   - PermissionService    │
│   - ClientTracker        │
└──────────────────────────┘
```

**关键设计**: AdminServer 和 Engine 在**同进程**, 通过共享内存对象 (Python 引用) 通信, 零 IPC 开销。Web UI 通过 SSE (Server-Sent Events) 接收实时推送, 避免 WebSocket 复杂度。

### 2.2 REST API 端点

```
GET  /                                    # Web UI 入口 (HTML)
GET  /static/*                            # 静态资源

# 实时指标 (SSE)
GET  /api/v1/metrics/stream               # SSE: 实时指标流
GET  /api/v1/metrics/realtime             # JSON: 当前 1 分钟滑动窗口
GET  /api/v1/metrics/snapshot             # JSON: 全量快照

# Topic 监控
GET  /api/v1/topics                        # JSON: 所有 topic + 1min msg/s + p50/p99 + 背压
GET  /api/v1/topics/{topic}                # JSON: 单 topic 详情
GET  /api/v1/topics/{topic}/history        # JSON: 最近 N 分钟历史 (从 SQLite)

# 客户端
GET  /api/v1/clients                       # JSON: 在线客户端列表
GET  /api/v1/clients/{identity}            # JSON: 单客户端详情 (订阅 topic, 最近活动)
GET  /api/v1/clients/{identity}/sub_list   # JSON: 该 client 订阅的 topic

# 用户管理
GET  /api/v1/users                         # JSON: 用户列表
POST /api/v1/users                         # JSON: 创建用户
GET  /api/v1/users/{user_id}              # JSON: 用户详情
PUT  /api/v1/users/{user_id}               # JSON: 更新用户
DELETE /api/v1/users/{user_id}             # JSON: 删除用户
POST /api/v1/users/{user_id}/api_keys     # JSON: 生成新 API key

# 权限管理
GET  /api/v1/permissions                   # JSON: 所有权限规则
POST /api/v1/permissions                   # JSON: 授予权限 (user, topic_pattern, action)
DELETE /api/v1/permissions/{perm_id}      # JSON: 撤销权限

# 批量配置
GET  /api/v1/users/{user_id}/batch_config  # JSON: 该用户的 batch 配置
PUT  /api/v1/users/{user_id}/batch_config  # JSON: 设置 batch_size / batch_interval_ms / batch_max_wait_ms

# 系统状态
GET  /api/v1/system/status                 # JSON: 启动时间, 运行时长, 版本
GET  /api/v1/system/resources              # JSON: CPU, 内存, fd, 连接数
```

### 2.3 监控指标 (1 分钟粒度)

**每分钟指标** (1 分钟滑动窗口, 内存 O(1) 更新):

| 指标 | 存储 | 含义 |
|------|------|------|
| msg/s | EWMA | 每秒消息数 (指数加权) |
| msg_total | counter | 累计消息数 (60s 重置) |
| p50/p99/p999 latency | SlidingWindow | 消息端到端延迟分位数 (60s 滑动) |
| in_flight | gauge | 当前处理中消息数 |
| backpressure | bool | 队列占用 > 80% |
| active_connections | gauge | 在线客户端数 |
| subs_per_topic | dict | 每个 topic 的订阅数 |

**持久化** (7 天 TTL):

- 每分钟聚合后写入 `topic_stats` 表: (topic, minute_ts, msg_count, p50, p99, max_latency, peak_in_flight)
- 每 5 分钟清理过期数据 (DELETE WHERE ts < now - 7 days)
- 自动后台 task, 不影响主循环

### 2.4 客户端追踪 (ClientTracker)

**不破坏隐私**: 追踪 identity (ZMQ 自带), 不记录 payload。

**数据**: 在内存 dict[identity → ClientInfo] 维护, ClientInfo 字段:
- `connected_at: float` (连接时间)
- `last_heartbeat: float` (心跳时间, 60s 内视为在线)
- `subscribed_topics: set[str]` (订阅的 topic)
- `user_id: int | None` (认证后的 user)
- `msg_in_count: int` (收到的 PUB 计数, 1 分钟窗口)
- `msg_out_count: int` (发出的 PUB 计数, 1 分钟窗口)

**集成点**:
- `MessageHandlers._handle_sub` 记录订阅
- `MessageHandlers._handle_pub` 计数
- `MessageHandlers._dispatch_internal` 更新心跳

### 2.5 Web UI (Vanilla HTML/JS)

**单页应用 (SPA)**, 5 个 tab:

1. **概览** (Overview): 系统状态卡片, 总消息数, 在线客户端, 内存/CPU
2. **主题监控** (Topics): 所有 topic 列表, 点击查看详情 (1min msg/s, 延迟分位数, 历史曲线)
3. **客户端** (Clients): 在线客户端表格 (identity, 连接时长, msg/s, 订阅数)
4. **用户/权限** (Users & Permissions): 用户列表 + 权限规则 CRUD
5. **批量配置** (Batch Config): 用户批量参数配置

**实时推送**: 用 SSE 接收服务端推送的指标变化, 局部刷新表格 (不重载整个页面)。

**资源**: 静态文件 (HTML/CSS/JS) 约 50KB, 嵌入在 Python 代码字符串中 (避免外部资源依赖)。

---

## 三、用户/订阅权限管控

### 3.1 用户表 (SQLite)

```sql
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    api_key_hash TEXT NOT NULL,       -- 密码哈希
    batch_size INTEGER DEFAULT 1,     -- 批量大小
    batch_interval_ms INTEGER DEFAULT 10,
    batch_max_wait_ms INTEGER DEFAULT 50,
    is_admin INTEGER DEFAULT 0,       -- 0/1
    is_active INTEGER DEFAULT 1,
    created_at REAL NOT NULL
);
```

### 3.2 权限表 (SQLite)

```sql
CREATE TABLE permissions (
    perm_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    topic_pattern TEXT NOT NULL,        -- 支持 * 和 >
    action TEXT NOT NULL,              -- 'pub' 或 'sub'
    created_at REAL NOT NULL,
    UNIQUE(user_id, topic_pattern, action),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
```

### 3.3 校验流程 (服务端)

**PUB 时** (`_handle_pub`):
1. 通过 ZAP 拿到 `user_id` (从 connection metadata)
2. 查询 user 的 pub 权限
3. topic 命中任一 pub 权限 → 允许, 否则拒绝 (返回 ERROR 3001)

**SUB 时** (`_handle_sub`):
1. 拿到 `user_id`
2. 查询 user 的 sub 权限
3. topic 命中任一 sub 权限 → 允许, 否则拒绝 (返回 ERROR 3002)

**性能优化**: 权限结果缓存在内存 (`PermissionService._cache`), TTL 30s。

### 3.4 批量配置

**用户级配置** (在 `users` 表):
- `batch_size`: 攒够 N 条 flush (默认 1, 关闭批量)
- `batch_interval_ms`: 时间触发 (默认 10ms)
- `batch_max_wait_ms`: 硬上限 (默认 50ms)

**Client 行为**:
- 客户端连上后, server 把 batch 配置发给 client (作为 connect ack)
- client 用配置初始化 Batcher
- 用户在 Web UI 修改后, client 需重连 (或 server 主动推送配置变更)

---

## 四、消息格式变更 (不兼容旧版, 用户已同意)

### 4.1 单条消息格式 (不变)

保持现有 4 帧格式: `[topic, meta(2B), record_count(4B), payload]`

### 4.2 批量消息格式 (新)

新增 msg_type = `BATCH` (0x0C):

```
frame 0: topic (str)
frame 1: meta(2B): [msg_type=BATCH, flags]
frame 2: record_count(4B): N (批量条数)
frame 3: payload (msgpack 编码的 list[原始 payload])
```

**兼容**: 旧 client 收到 BATCH 帧时按 ERROR 3003 处理。
**新 client 收到单条 PUB 帧** (非 BATCH) 仍按单条处理 (向后兼容可选, 允许同集群混跑)。

### 4.3 server 端解包

- 收到 PUB (msg_type=0x02): 单条, 原有路径
- 收到 BATCH (msg_type=0x0C): 拆 N 条, 每条都过 handlers + broadcast
- 收到 SUB/UNSUB/PING/QUERY: 不变

---

## 五、性能目标 (与 100k 基线对比)

| 组合 | 100k 基线 (msg/s) | 目标 (msg/s) | 提升 |
|------|------------------|--------------|------|
| str/none | 23,446 | **100,000+** (batch=10) | +325% |
| str/snappy | 19,628 | **80,000+** (batch=10) | +308% |
| df-msgpack/none | 3,132 | **15,000+** (batch=20) | +379% |
| df-pyarrow/zstd | 2,584 | **12,000+** (batch=20) | +364% |

**延迟目标**:
- str/bytes: p50 < 1ms (含 batch 等待), p99 < 5ms
- DataFrame: p50 < 2ms, p99 < 10ms

---

## 六、文件结构

| 文件 | 状态 | 职责 |
|------|------|------|
| `src/pulsemq/client/batcher.py` | 新增 | 客户端批量器 (count + time + max_wait) |
| `src/pulsemq/client/async_client.py` | 修改 | 集成 Batcher, `publish()` 自动批 |
| `src/pulsemq/monitoring/realtime.py` | 扩展 | 加 1-min 滑动窗口的 topic 维度 |
| `src/pulsemq/monitoring/client_tracker.py` | 新增 | 客户端追踪 (identity, sub, msg_rate) |
| `src/pulsemq/monitoring/admin_server.py` | 新增 | 完整 REST + SSE + 静态文件 |
| `src/pulsemq/monitoring/admin_backend.py` | 新增 | AdminServer 的后端业务逻辑 |
| `src/pulsemq/monitoring/web_ui.py` | 新增 | Web UI 静态资源 (HTML/CSS/JS 字符串) |
| `src/pulsemq/storage/sqlite_stats.py` | 新增 | topic_stats 表 + 7 天 TTL |
| `src/pulsemq/storage/sqlite_batch.py` | 新增 | batch_config 字段在 users 表 |
| `src/pulsemq/auth/permission.py` | 修改 | 扩展: pub/sub 校验 + batch 配置下发 |
| `src/pulsemq/protocol/msg_type.py` | 修改 | 新增 BATCH msg_type |
| `src/pulsemq/engine/handlers.py` | 修改 | 集成 ClientTracker, 解 BATCH |
| `tests/integration/test_admin.py` | 新增 | admin API + 权限 + 监控 e2e |
| `tests/performance/test_batching.py` | 新增 | 批量 vs 单条性能对比 |
| `docs/admin-ui-preview.md` | 新增 | UI 设计文档 (含 ASCII mockup) |

---

## 七、验收清单

- [ ] 跨平台: Linux 启 uvloop, Windows 启 Selector, 单元测试覆盖
- [ ] 客户端批量: `batch_size=10, batch_interval_ms=10` 在 100k 压测下 str/none > 80k msg/s
- [ ] Web UI: 能查看实时指标, 修改用户权限, 看到所有 topic 流量
- [ ] 监控: 1 分钟滑动窗口 + 7 天 SQLite 历史 + 自动清理
- [ ] 权限: pub/sub 校验生效, 未授权返回 ERROR 3001/3002
- [ ] 后台不影响主循环: SSE 推送走单独 task, 主循环延迟增加 < 0.1ms
- [ ] 内存: AdminServer 启动后 RSS 增加 < 30MB
- [ ] 434+ 单元测试 + 集成测试 全绿
- [ ] 16/16 e2e + 100k perf baseline 仍过

---

## 八、不在范围

- 多租户隔离 (单租户足够)
- 分布式 (单 server)
- 协议加密 (CURVE 留 v1.1)
- 客户端 SDK 重构
- 第三方集成 (Kafka/Pulsar 桥接)
