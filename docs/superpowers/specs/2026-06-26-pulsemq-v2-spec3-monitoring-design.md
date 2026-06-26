# PulseMQ v2 重构 · Spec 3：监控扩展设计

> 版本：v1.0 ｜ 日期：2026-06-26
> 范围：在 Spec 1 沿用的监控体系上扩展在线 Client、端到端延迟、事件流，并补齐 admin 独立线程与反压保护
> 关联文档：`docs/PulseMQ_重构架构设计_Client_Server.md`（§3.1.5 monitoring、§6 监控模型、§6.4 Web 界面、§6.5 性能设计）
> 前置：Spec 1 核心架构骨架、Spec 2 安全（admin token 认证）
> 执行策略：原地大改、不兼容旧版协议

---

## 1. 目标与非目标

### 1.1 目标

在 Spec 1 沿用的 `stats/traffic`、`stats/storage`、`admin/server`、`admin/web_ui` 基础上，补齐总体设计 §6.4 / §6.5 要求：

- **在线 Client 概览**：在线用户数、生产者数、消费者数、总订阅 topic 数。
- **端到端延迟分位**：P50 / P95 / P99，采样采集（默认 1%）。
- **最近事件流**：最近 50 条连接 / 认证 / 断线事件，固定长度环形缓冲。
- **admin HTTP 独立线程**：AdminServer 运行在独立事件循环，不抢占 ZeroMQ 数据线程。
- **数据路径零阻塞**：无锁统计、有界事件队列、异步批量落盘、SSE 反压。
- **admin token 认证**：复用 Spec 2 的 token 机制保护所有新路由。
- **web_ui 新区块**：在线 Client 卡片、延迟分位图、事件流、在线 Client 详情弹窗。

### 1.2 非目标

- Prometheus 指标导出 → 阶段 5。
- 拖拽配置 / 多面板自定义等复杂交互 → 明确不做（保持界面精简）。
- 消息级延迟拆分（仅做端到端）→ 不做。
- 历史事件持久化（事件流仅内存环形）→ 不做（分钟统计仍持久化）。

### 1.3 与前置 spec 的衔接

- Spec 1 §6.3 `routing.SubscriptionTable.snapshot()`、§7.2 `OnlineRegistry.snapshot()` 已预留供监控读取，本 spec 接入。
- Spec 1 §9.2 Server 运行任务留了 `monitoring 占位` 与 admin 服务，本 spec 填充。
- Spec 2 的 admin token 中间件本 spec 直接复用，新路由全部需 token。
- 沿用模块不动接口：`TrafficStats`、`StatsStorage`、`AdminServer` 路由骨架、`web_ui` 视觉风格。

---

## 2. 模块清单

| 模块 | 状态 | 职责 |
|------|------|------|
| `stats/traffic` | 沿用 | 内存分钟级聚合，8h 窗口，lock-free |
| `stats/storage` | 改造 | SQLite 写入改异步批量（Spec 1 沿用同步，本 spec 异步化） |
| `stats/connections` | 新增 | `ConnectionStats`：在线 Client 快照、认证事件、连接/断线事件 |
| `stats/latency` | 新增 | `LatencyStats`：端到端延迟直方图与 P50/P95/P99 |
| `monitoring` | 新增 | 事件总线 + 处理器（日志 / 存储 / HTTP / SSE） |
| `admin/server` | 改造 | 独立线程事件循环 + token 中间件 + 新路由 |
| `admin/web_ui` | 扩展 | 新增在线 Client / 延迟 / 事件流区块 |

依赖方向：`stats/*` 独立无业务依赖；`monitoring` 依赖 `stats/*`；`admin` 依赖 `monitoring`、`routing.snapshot`、`OnlineRegistry.snapshot`、`security`（token，Spec 2）。

---

## 3. 新增统计模块

### 3.1 ConnectionStats（在线 Client 与事件）

```python
@dataclass
class ClientSnapshot:
    client_id: str
    username: str
    role: str                 # producer / consumer / both
    endpoint: str
    topics: list[str]
    connected_at: float
    duration_seconds: float

@dataclass
class LifecycleEvent:
    ts: float
    level: str                # INFO / WARNING / ERROR
    type: str                 # AUTH / CLIENT
    message: str

class ConnectionStats:
    def on_connect(self, client_id, username, endpoint, role) -> None
    def on_disconnect(self, client_id, reason: str) -> None
    def on_auth(self, username, endpoint, success: bool, reason: str | None) -> None
    def online_clients(self) -> list[ClientSnapshot]      # 供 /api/v1/clients
    def recent_events(self, limit=50) -> list[LifecycleEvent]  # 供 /api/v1/events
    def counters(self) -> dict                             # online_users/producers/consumers/total_subscriptions
```

- 在线 Client 数据来源：订阅 Spec 1 `OnlineRegistry.snapshot()`，`ConnectionStats` 负责聚合计数与快照。
- 事件流用 `collections.deque(maxlen=200)`（配置 `event_ring_size`），溢出自动丢旧，内存有界。
- `on_*` 方法只做 `deque.append` + 计数累加，无 I/O、无锁（单写者 Server 数据线程 + GIL）。

### 3.2 LatencyStats（端到端延迟）

```python
class LatencyStats:
    def __init__(self, sample_rate: float = 0.01):   # 默认 1% 采样
    def record(self, latency_ns: int) -> None        # 采样命中才记
    def percentiles(self) -> dict:                    # {p50_ms, p95_ms, p99_ms}
    def should_sample(self) -> bool                   # 采样判定
```

- 延迟采集点（照总体设计 §6.5.2）：Client 发布时在 frame 的 `ts` 字段写入 `time.time_ns()`；Server 收到后 `now_ns - frame.ts` 得端到端延迟。
- 采样：`should_sample()` 按概率命中（`sample_rate=1.0` 全量，默认 `0.01`）。未命中不调 `record`，避免 `time_ns()` 开销累积。
- 直方图：固定桶（如 0.05ms / 0.1 / 0.5 / 1 / 5 / 10 / 50ms），无锁更新（GIL）；`percentiles` 用桶估算分位。
- 仅在 Server 接收线程记录，单写者，无锁。

### 3.3 StatsStorage 异步化改造

Spec 1 沿用现有 `StatsStorage`（同步 `execute`，分钟归档时短暂阻塞）。本 spec 改造为异步批量：

- 分钟归档数据进入 `asyncio.Queue`，由独立 consumer 任务批量 `executemany` 写入（`stats_archive_batch_size=50`）。
- 不在 Server 数据接收线程直接写 SQLite，磁盘 I/O 不阻塞消息路径。
- 表结构不变（`minute_stats`），向后兼容 Spec 1 已有数据。
- 新增 `connection_events` 表？**不加**——事件流仅内存环形，不持久化（§1.2）。

---

## 4. monitoring 事件总线

### 4.1 事件来源与统一入口

```
transport 连接事件   ──┐
transport 认证事件   ──┤
control 注册/心跳事件 ──┼──→ monitoring 事件总线 ──→ 处理器
数据面 send/recv      ──┤                          ├─ 日志输出
业务埋点（延迟采样）  ──┘                          ├─ SQLite（分钟统计，异步批量）
                                                  ├─ HTTP/SSE 推送
                                                  └─ 内存事件环（ConnectionStats）
```

### 4.2 事件队列模型

- 事件先进入 `collections.deque(maxlen=N)` 或 `asyncio.Queue(maxsize=N)`（有界）。
- 溢出丢弃旧事件，不反压数据路径。
- 处理器（日志 / 存储 / SSE）从队列消费，与数据线程解耦。

### 4.3 与现有日志的关系

Spec 1 `logging` 已按总体设计 §3.2.9 表输出 Client 生命周期事件。本 spec 的 `ConnectionStats` 事件环与日志**并行**：同一事件既走 `logging`（人读），又进事件环（UI 展示），两者都从 `monitoring` 事件总线派生，不重复埋点。

---

## 5. admin/server 改造

### 5.1 独立线程事件循环

- AdminServer 运行在独立线程的独立 asyncio 事件循环（`monitoring.admin_thread=true`）。
- ZeroMQ 数据线程不被 HTTP 请求打断。
- AdminServer 通过线程安全的方式读取 `TrafficStats` / `ConnectionStats` / `LatencyStats` / `routing.snapshot` / `OnlineRegistry.snapshot` 快照（都是 GIL 安全的只读快照）。
- SSE 广播循环每 `sse_interval`（默认 1s）推一帧 diff。

### 5.2 token 中间件

复用 Spec 2 `admin/auth.TokenAuth`：除 `/healthz` 外所有路由需 token。新增路由同样受保护。

### 5.3 路由表（扩展后）

| 方法 | 路径 | 功能 | 认证 |
|------|------|------|------|
| GET | `/` | 监控面板首页 | token |
| GET | `/static/{path}` | 静态资源 | token |
| GET | `/api/v1/stats/realtime` | 实时指标 JSON（扩展，见 §6.1） | token |
| GET | `/api/v1/stats/stream` | SSE 实时推送（1s/帧） | token |
| GET | `/api/v1/topics` | topic 列表 + 指标 | token |
| GET | `/api/v1/topics/{topic}/history` | 分钟级历史（1H/6H） | token |
| GET | `/api/v1/clients` | **新增** 当前在线 Client 列表 | token |
| GET | `/api/v1/events` | **新增** 最近连接/认证事件流 | token |
| GET | `/api/v1/system/status` | 系统状态 | token |
| GET | `/healthz` | 健康检查 | 无需 token |

### 5.4 新增接口数据格式

```
GET /api/v1/stats/realtime   # 扩展字段
{
  "topics": { "market.stock.600000": { "record_rate_1min": 1204.3, ... } },
  "online_users": 3,
  "online_producers": 1,
  "online_consumers": 2,
  "total_subscriptions": 5,
  "latency_p50_ms": 0.12,
  "latency_p95_ms": 0.45,
  "latency_p99_ms": 0.82,
  "server_time": 1700000006.0
}

GET /api/v1/clients
{
  "clients": [
    {"client_id":"uuid-1234","username":"alice","role":"consumer",
     "endpoint":"192.168.1.10:54321","topics":["market.stock.*"],
     "connected_at":"2026-06-26T08:00:01Z","duration_seconds":725}
  ]
}

GET /api/v1/events?limit=50
{
  "events": [
    {"ts":"2026-06-26T08:12:01Z","level":"WARNING","type":"AUTH","message":"bob 认证失败: invalid_password"},
    {"ts":"2026-06-26T08:12:00Z","level":"INFO","type":"CLIENT","message":"alice 上线"}
  ]
}
```

### 5.5 SSE 反压保护

- 每个 SSE 客户端 `asyncio.Queue(maxsize=64)`。
- 队列满（客户端断开 / 消费过慢）→ 主动取消该连接，从 `_sse_clients` 移除（沿用现有实现策略）。
- 慢消费者丢帧而非阻塞 Server。

---

## 6. web_ui 扩展

### 6.1 界面布局（照总体设计 §6.4.1）

```
┌────────────────────────────────────────────────────────────┐
│  PulseMQ 监控面板                [在线]  [版本号]          │
├────────────────────────────────────────────────────────────┤
│  活跃主题  │  消息量/秒  │  流量/秒  │  运行时间             │
│  在线用户  │  在线生产者 │ 在线消费者│ 总订阅 topic          │
├────────────────────────────────────────────────────────────┤
│  流量趋势（记录数/秒）        [1H] [6H]                    │
│  ┌──────────────────────────────────────────────────────┐ │
│  │              ECharts 折线图                          │ │
│  └──────────────────────────────────────────────────────┘ │
├────────────────────────────────────────────────────────────┤
│  端到端延迟（P50 / P95 / P99）                             │
│  ┌──────────────────────────────────────────────────────┐ │
│  │              ECharts 柱状/折线图                     │ │
│  └──────────────────────────────────────────────────────┘ │
├────────────────────────────────────────────────────────────┤
│  最近事件流                          [自动滚动]            │
│  08:12:01  [AUTH]    bob 认证失败: invalid_password        │
│  08:12:00  [CLIENT]  alice 上线, topics=[...]              │
│  08:11:58  [CLIENT]  carol 心跳超时 offline                │
├────────────────────────────────────────────────────────────┤
│  Topic 列表（卡片网格，点击叠加到流量趋势图）              │
└────────────────────────────────────────────────────────────┘
```

### 6.2 沿用不变

- 深色玻璃态视觉风格（CSS 渐变 + `backdrop-filter`）。
- 4 个顶部指标卡片（活跃主题 / 消息量/秒 / 流量/秒 / 运行时间）。
- ECharts 流量趋势图（1H/6H，最多 5 topic 叠加，LRU 淘汰）。
- Topic 卡片网格。
- SSE 实时推送（1s/帧）。

### 6.3 新增区块

| 区块 | 说明 |
|------|------|
| 在线 Client 概览卡片 | 在线用户数 / 生产者数 / 消费者数 / 总订阅 topic 数 |
| 延迟分位图 | ECharts 柱状/折线，P50/P95/P99，SSE 实时更新 |
| 最近事件流 | 最近 50 条事件，自动滚动，SSE 实时追加 |
| 在线 Client 详情弹窗 | 点击在线用户卡片弹出 `/api/v1/clients` 列表（username / client_id / 角色 / 订阅 topic / 连接时长） |

### 6.4 token 携带

前端从 URL `?token=xxx` 取 token，后续 fetch 与 SSE 请求放入 `Authorization: Bearer xxx` header。token 失效时 UI 提示并跳回带 token 的入口。

---

## 7. 监控性能设计（数据路径零阻塞）

照总体设计 §6.5，本 spec 必须遵循：

### 7.1 数据路径隔离

| 设计 | 落地 |
|------|------|
| 统计采集无锁 | `TrafficStats.record()` / `LatencyStats.record()` / `ConnectionStats.on_*` 单写者 + GIL，不引入互斥锁 |
| 事件队列有界 | 事件流 `deque(maxlen=event_ring_size)`，溢出丢旧 |
| 异步批量落盘 | 分钟归档经 `asyncio.Queue` + 独立任务 `executemany`（§3.3） |
| Admin HTTP 独立运行 | AdminServer 独立线程事件循环（§5.1） |
| SSE 反压保护 | 客户端队列 `maxsize=64`，满则丢帧取消连接（§5.5） |

### 7.2 采集点最小化

```
ProducerClient.publish(data)
  → frame.encode(...) 内写入 ts = time.time_ns()      # 仅 1 次 time_ns()
Server.transport.recv()
  → TrafficStats.record(topic, count, bytes)           # 内存累加，无锁
  → if LatencyStats.should_sample(): record(now_ns - frame.ts)  # 采样命中才记
  → routing.match → 转发
```

### 7.3 禁止的性能陷阱

| 禁止 | 替代 |
|------|------|
| 数据线程直接写 SQLite | 异步批量（§3.3） |
| 数据线程格式化日志字符串 | 结构化日志 + 延迟格式化 |
| 无限缓存历史事件 | 固定长度环形缓冲 |
| Admin 请求触发全量扫描 | 返回预聚合快照 |
| 每条消息采集延迟 | 采样（默认 1%） |

---

## 8. 配置扩展

`[monitoring]` 块完整化（照总体设计 §6.5.4）：

```toml
[monitoring]
ui_enabled = true
storage = "sqlite://./pulsemq_stats.sqlite"
retention_days = 7
sse_interval = 1.0
latency_sample_rate = 0.01          # 延迟采样率，1=全量
event_ring_size = 200               # 内存事件环大小
stats_archive_batch_size = 50       # SQLite 批量写入条数
admin_thread = true                 # Admin HTTP 独立线程
# admin_token / admin_token_file 见 Spec 2
```

---

## 9. 测试策略

### 9.1 新增测试

- `test_connections_stats.py`：在线 Client 计数（生产者/消费者/订阅数）、事件环 maxlen 丢弃、`recent_events(limit)` 截断。
- `test_latency_stats.py`：采样命中/未命中、P50/P95/P99 计算、`sample_rate=1.0` 全量。
- `test_storage_async.py`：异步批量写入（进 queue → executemany）、不阻塞数据线程、重启后数据可读。
- `test_admin_v2.py`：
  - `/api/v1/clients` 返回在线列表（与 OnlineRegistry 一致）。
  - `/api/v1/events?limit=50` 返回事件。
  - 新路由无 token → 401（复用 Spec 2 中间件测试）。
  - SSE 队列满丢帧不崩 Server。
  - Admin 独立线程：HTTP 请求不影响数据线程（用 mock 长请求验证数据线程继续收发）。
- `test_monitoring_perf.py`（基线）：高吞吐下 `TrafficStats`/`LatencyStats` 无锁路径不抛错、内存有界。

### 9.2 沿用测试

`test_stats.py`（traffic/storage 单测）继续通过；`test_e2e_*`（Spec 1）继续通过并验证 SSE 实时推送。

### 9.3 web_ui 验证

UI 为单文件 HTML，主要靠人工/截图验证（沿用现有做法）；数据接口由 `test_admin_v2.py` 覆盖。

---

## 10. 关键设计决策

| 决策 | 说明 |
|------|------|
| 沿用 ECharts 深色风格 | 保持视觉一致，不重写 UI |
| 延迟采样默认 1% | 避免 high-frequency 下 time_ns() 累积开销，可配全量 |
| 事件流仅内存环形 | 不持久化，内存有界；分钟统计仍持久化 |
| Admin 独立线程 | HTTP 请求不抢占 ZMQ 数据线程 CPU |
| SQLite 异步批量 | 磁盘 I/O 不阻塞消息路径 |
| SSE maxsize=64 反压 | 慢消费者丢帧不拖垮 Server |
| 快照只读 GIL 安全 | Admin 线程读快照、数据线程写，无锁 |
| 复用 Spec 2 token | 新路由统一受 token 保护，不另立认证 |

---

## 11. 边界与注意事项

1. **采样率权衡**：`latency_sample_rate` 默认 1%，P99 在低采样下可能有偏差；高吞吐场景 1% 足够，低吞吐可调高。
2. **事件环不持久化**：Server 重启后事件流清空，历史事件不可追溯（仅分钟统计可追溯）。
3. **Admin 独立线程与数据线程的数据共享**：只传不可变快照（dict/list 拷贝或 GIL 安全的只读视图），避免跨线程可变状态。
4. **SSE 连接泄漏**：沿用 Spec 1 现有策略——队列满/连接断立即从 `_sse_clients` 移除，防止死客户端残留。
5. **延迟直方图桶选择**：固定桶在极端延迟（>50ms）下分辨率下降；金融行情正常 <1ms，桶设计偏小值高分辨率，超大值归入末桶。
6. **事件去重**：同一事件既走 logging 又进事件环，但不重复入环（`on_*` 只 append 一次）。
7. **Admin 线程关闭顺序**：Server 关闭时先停 Admin（停接收新请求 → 等 SSE writer 结束 → 关 server），再停数据线程（沿用 Spec 1 lifecycle）。
8. **跨平台事件循环**：Windows 上 Admin 独立线程用 `ProactorEventLoop`（无 pyzmq 限制，纯 HTTP），与数据线程的 `WindowsSelectorEventLoop` 隔离，互不干扰。

---

## 12. 完成后的整体形态

Spec 1 + 2 + 3 全部落地后，PulseMQ v2 达成总体设计 §11 总结的目标：

- Client/Server 模型，强制 PLAIN 认证，默认凭据自动生成，admin token 保护。
- 内置 Web 监控（在线 Client / 延迟分位 / 事件流），数据路径零阻塞。
- Client 启动硬失败 + 运行期重连重新认证，Server 单用户单在线。
- 模块化解耦协议 / 安全 / 路由 / 控制 / 监控。

剩余阶段 5（多 Server、topic ACL、Prometheus、CURVE/TLS）作为后续独立 spec 推进。
