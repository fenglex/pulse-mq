# PulseMQ 延迟监控 + 客户端 API 优化设计

> 创建日期：2026-07-31 ｜ 状态：待审阅
>
> 范围：① 新增按 topic 维度的端到端延迟监控（半程 + 全程 + 8h 折线图）；
> ② 客户端生命周期 API 修复（消除 `asyncio.sleep` 模式 + 致命错误吞没问题）；
> ③ 性能与易用性优化清单（热路径无锁化、配置接入、命名修正、死代码清理等）。

---

## 1. 背景与目标

### 1.1 起因

- 消费端被迫写 `await asyncio.sleep(3600)` 维持运行，暴露 `ConsumerClient` 缺少 `run_forever`。
- 该缺口进一步导致**重连致命错误被静默吞没**（`_reconnect_fatal` 仅在 `ProducerClient.run_forever` 检查），消费者进程"看起来活着、实际已死"。
- 现有延迟监控只有一个**全局累计直方图**（`LatencyStats`），测的是 producer→server 半程，不分 topic、不重置，无法定位"哪个行情源慢"、也看不出实时变化。
- 多处热路径每条消息获取 RLock、序列化器每次 `import`、socket role 命名混乱、`ClientConfig` 形同虚设等遗留问题。

### 1.2 目标

1. 消费者/生产者都有对称的 `run_forever`，消除 sleep 模式，修复致命错误吞没。
2. admin UI 实时展示**按 topic** 的半程（producer→server）与全程（producer→consumer）延迟，保留近 8 小时分钟级历史折线图。
3. 数据面热路径**零新增开销**（采样保护，99% 消息不进统计分支）。
4. 清理热路径锁、死代码，修正命名，接入配置。

---

## 2. 关键设计决策（已与用户确认）

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 延迟测量语义 | **C：半程 + 全程都展示** | 全程是金融场景真正关心的；半程几乎免费，便于定位瓶颈段 |
| 聚合维度 | **A：按 topic** | 延迟是消息流的属性，topic 稳定、无需随 client 上下线管理直方图 |
| 时间窗口 | **C：分钟窗口** | 随现有 `roll_minute` 滚动，与 `TrafficStats` 架构一致，"实时"语义清晰 |
| 全程回传频率 | **A：复用 `latency_sample_rate`（1%）** | 一个配置项管两侧，回传量可控（1 万 msg/s → 100 回传/s） |
| 延迟历史 | 内存 8h（480 分钟），不落 SQLite | 用户需求即"近 8 小时"，等于内存窗口；落库为 YAGNI |
| `cache/` 模块 | 删除 | 死代码（未接入主流程），需要时基于新需求重写 |
| 回传告警 | 不做 | 丢几条采样无影响（YAGNI） |

---

## 3. 延迟监控功能

### 3.1 数据模型

新增 `LatencyStatsRegistry`（`stats/latency.py`），组合多个 `LatencyStats`（复用其桶/分位插值逻辑），外层加 topic 维度 + 分钟滚动 + 8h 历史窗口。存储模型与 `TrafficStats` 完全对齐。

```python
@dataclass
class MinuteLatency:
    """一个 topic 一分钟的延迟快照。"""
    timestamp: int       # 整分钟秒
    p50_ms: float
    p95_ms: float
    p99_ms: float
    count: int           # 本分钟采样命中数


class LatencyStatsRegistry:
    """按 topic + 分钟窗口的延迟统计（线程安全）。

    线程模型：数据面线程写 record()（半程），控制面协程写 record()（全程回传），
    主线程协程写 roll_minute()，admin 线程读 snapshot()/get_history()。
    用 threading.Lock 保护（record 仅在采样命中时执行，lock 开销可接受）。
    """

    def __init__(self, sample_rate: float = 0.01, retention_minutes: int = 480) -> None: ...
        # self._current: dict[str, LatencyStats]   # 当前分钟进行中
        # self._history: dict[str, deque[MinuteLatency]]  # maxlen=retention
        # self._lock = threading.Lock()

    def should_sample(self) -> bool: ...           # 复用现有逻辑
    def record(self, topic: str, latency_ns: int) -> None: ...   # 写 _current[topic]
    def roll_minute(self) -> None: ...             # _current 各 topic 算分位 → MinuteLatency 追加 _history；清空 _current
    def snapshot(self) -> dict[str, dict]: ...     # 各 topic 当前进行值 {p50,p95,p99,count}
    def get_history(self, topic: str, minutes: int = 60) -> list[dict]: ...  # 近 N 分钟序列（给折线图）
```

Server 持有两个实例：
- `self._lat_half = LatencyStatsRegistry(sample_rate, retention)` — 半程（producer→server）
- `self._lat_e2e = LatencyStatsRegistry(sample_rate, retention)` — 全程（producer→consumer 回传）

> 现有全局 `self._latency: LatencyStats` 替换为上述两个 registry。现有 `LatencyStats` 类保留作为 per-topic 桶逻辑复用单元。

### 3.2 数据流

**半程**（复用现有测量点，仅改维度与 registry）：

```
_on_data_message (server.py:334, 数据面线程, 已有)
  hdr = frames.decode_header(frame)                                    # 已有
  self._stats.record(hdr.topic, hdr.record_count, len(hdr.raw_payload))  # 已有
  if self._lat_half.should_sample():                                   # 已有 should_sample
      self._lat_half.record(hdr.topic, time.time_ns() - hdr.timestamp_ns)  # 改：按 topic 进 registry
  for target in self._routing.match(hdr.topic): ...                    # 已有
```

`_on_server_produce`（server.py:312，内置 producer）同步改：`self._lat_half.record(...)`。

**全程**（consumer 回传，新增）：

```
consumer _recv_loop (client.py:516, 已有)
  hdr = frames.decode_header(frame)                                    # 已有，不需 decode payload
  ... 订阅匹配与回调分发（已有）...
  if self._should_sample_e2e():                                        # 新增：复用 sample_rate
      latency_ns = time.time_ns() - hdr.timestamp_ns
      frame = frames.encode_control(ControlCmd.LATENCY_REPORT,
                                    {"topic": hdr.topic, "latency_ns": latency_ns})
      await self._transport.send(b"", frame, role="control")           # fire-and-forget

server _dispatch_control (server.py:377)
  elif cmd == ControlCmd.LATENCY_REPORT:                               # 新增分支
      topic = cmd_msg.payload.get("topic", "")
      latency_ns = int(cmd_msg.payload.get("latency_ns", 0))
      if topic and latency_ns > 0:
          self._lat_e2e.record(topic, latency_ns)
      # 无 ack（fire-and-forget，不触发 C3 的 ack 串扰问题）
```

**分钟滚动**（复用现有循环 `_minute_roll_loop`, server.py:487）：

```
self._stats.roll_minute()          # 已有（TrafficStats）
self._lat_half.roll_minute()       # 新增
self._lat_e2e.roll_minute()        # 新增
```

### 3.3 控制命令

`control.py` 新增：`ControlCmd.LATENCY_REPORT = "LATENCY_REPORT"`。

- consumer → server，fire-and-forget（不 recv、不等 ack）。
- payload：`{"topic": str, "latency_ns": int}`，msgpack 编码，体积极小。
- 不与 REGISTER/SUBSCRIBE 的 recv 串扰（C3 修复的 ack 关联机制不影响本命令，因为本命令不 recv）。

### 3.4 Admin API

扩展 `/api/v1/stats/realtime`（SSE 已每秒推送、前端已订阅），注入当前进行值：

```json
"latency": {
  "half": {"market.tick": {"p50_ms":0.3,"p95_ms":0.8,"p99_ms":2.1,"count":120}},
  "e2e":  {"market.tick": {"p50_ms":0.6,"p95_ms":1.5,"p99_ms":3.4,"count":98}}
}
```

新增历史端点（对齐流量 history，`server.py` 现有 `/api/v1/topics/{topic}/history`）：

```
GET /api/v1/latency/topics/{topic}/history?minutes=60&kind=half|e2e
→ [{timestamp, p50_ms, p95_ms, p99_ms, count}, ...]
```

### 3.5 Web UI（`admin/web_ui.py`）

1. **延迟折线图**（替换现有全局延迟柱状图）：ECharts line，横轴时间（分钟级），纵轴延迟(ms)。
   - 交互复用现有流量趋势图：1H/8H 切换、多 topic 叠加（LRU 淘汰最多 5 个）、30s 自动刷新历史。
   - 默认展示 P50/P95/P99 三条线；可切换 half/e2e。
2. **底部端到端延迟列表**（页面底部新增）：表格，每行一个 topic，列：topic、half(P50/P95/P99)、e2e(P50/P95/P99)、采样数。点击行在上方折线图定位该 topic。
3. **在线客户端延迟**：client 详情弹窗，producer 展示其发送 topic 的 half，consumer 展示其订阅 topic 的 e2e（从 topic registry 按 `client.topics` 派生）。
4. **流量趋势图时间范围统一**：现有流量趋势图最大历史从 6H 提升到 8H，与延迟折线图时间范围一致（`TrafficStats` 内存窗口本就是 8h/480 分钟，仅 UI 切换选项需从 `1H/6H` 改为 `1H/8H`）。

### 3.6 性能分析（核心约束）

| 调用点 | 执行频率 | 开销 | 措施 |
|--------|---------|------|------|
| `_lat_half.should_sample()` | 每条消息 | `random()` 比较 | 已存在，99% 在此返回，**零新增** |
| `_lat_half.record()` | 仅 1% 采样命中 | `dict.get` + `bisect` + `Lock` | 采样保护，开销分摊到 1% 消息，可接受 |
| consumer `_recv_loop` 的 `should_sample()` | consumer 侧每条消息（**新增**） | `random()` 比较（~50ns） | server 侧早有同量级开销；仅 1% 命中时才触发回传 |
| consumer 回传 encode + control send | 仅 1% | msgpack 小 dict + DEALER send | 走控制面，server 数据面不动 |
| `roll_minute` | 每分钟一次 | 遍历 `_current` 算分位 + deque append | 无逐条剔除 |

**结论**：**server 数据面热路径每条消息新增开销 = 零**（`should_sample` 已存在，未命中直接返回）；**consumer 数据面**每条消息新增一次 `random()` 比较（~50ns，与 server 既有 `should_sample` 同量级，远小于 `decode_header`），仅 1% 命中时触发回传。这与 B1（`SubscriptionTable` COW 无锁化）不冲突——B1 优化的是每条消息都走的 `traffic.record`/`routing.match`；延迟 record 走采样分支，保留 `Lock` 即可。

---

## 4. 客户端生命周期 API（A1–A4）

| # | 改动 | 细节 |
|---|------|------|
| **A1** | `Client` 基类新增 `run_forever()` | `await self.start()` → `await self._stop.wait()` → finally `stop()`；包含 `_reconnect_fatal` 重抛逻辑。`ProducerClient.run_forever` 改为 `super().run_forever()` 框架内插入 producer 调度的 `start_all/stop_all` |
| **A2** | 致命错误重抛 | A1 自然解决：所有 client 走基类 `run_forever`，在主任务上下文检查并 raise `_reconnect_fatal`，CLI 经 `exit_code_for` 拿到 exit 3 |
| **A3** | `subscribe` 支持 start 前注册 | 未连接时（`self._transport` 未就绪）只写 `_subscriptions`/`_sub_header_only`，不调 `_send_subscribe`；`start()` 末尾已有 flush 逻辑（`client.py:192`）自动补发 |
| **A4** | 信号处理 | `run_forever` 内注册 SIGINT/SIGTERM → `stop()`；Windows 不支持 `add_signal_handler` 则静默跳过（复用 `lifecycle.py:22` 模式） |

改造后消费者用法：

```python
cons = ConsumerClient(...)
await cons.subscribe("market.*", on_msg)   # 可在 start 前注册（A3）
await cons.run_forever()                    # 替代 asyncio.sleep（A1），致命错误自动抛（A2）
```

---

## 5. 性能与易用性优化（B/C/D）

### 5.1 性能（B）

| # | 改动 | 文件 |
|---|------|------|
| **B1** | `SubscriptionTable` 改 COW：写时构建不可变查找结构、原子替换引用；`match()` 完全无锁读。GIL 保证引用赋值原子，数据面见到的快照永远一致。写频率极低（仅订阅变更），拷贝成本可忽略 | `routing.py` |
| **B2** | `Client.__init__` 接受 `heartbeat_interval`/`reconnect_initial_delay`/`reconnect_max_delay`/`reconnect_backoff_multiplier`/`startup_timeout`/`register_reply_timeout` 参数（默认=现有模块常量 `client.py:51-61`）；接入 `ClientConfig` | `client.py`、`config.py` |
| **B3** | socket role `"consumer"` → `"data"`（数据面 DEALER），全文替换；`"control"` 保持 | `client.py` |

### 5.2 易用性（C）

| # | 改动 | 文件 |
|---|------|------|
| **C1** | 序列化器 `import msgspec/pyarrow/pandas` 提模块级缓存，与 `frames.py` 的 `_pd` 模式对齐 | `serialization.py` |
| **C2** | `ServerConfig` 补环境变量覆盖：`PULSEMQ_HEARTBEAT_TIMEOUT`/`PULSEMQ_LATENCY_SAMPLE_RATE`/`PULSEMQ_RETENTION_DAYS`/`PULSEMQ_BCRYPT_COST`/`PULSEMQ_SSE_INTERVAL` 等 | `config.py` |
| **C3** | 控制面 reply 加 `request_id`；client 发送时生成 id，`recv("control")` 按 id 匹配回执，避免心跳 ack 串扰 REGISTER/SUBSCRIBE | `client.py`、`control.py` |

### 5.3 清理（D）

| # | 改动 | 文件 |
|---|------|------|
| **D1** | 删除：`cache/` 整个模块、`ProducerSpec.cache_size`、`inject_sender`/`PublisherSender`/`PulsePublisher` 前向引用、`ControlCmd.KICK`、`MsgType.HEARTBEAT`/`ADMIN`、`PlainAuthDict`（保留测试需迁移）、`StatsStorage.save_minute`（单条）、`ClientConfig` 旧定义（B2 重写） | 多文件 |
| **D2** | `SNDHWM`/`RCVHWM` 默认 1000→10000；`ServerConfig`/`Client` 暴露可配 | `router.py`、`config.py` |
| **D3** | client `_recv_loop` 订阅匹配改前缀索引（精确 + 通配双索引，复用 `SubscriptionTable` 思路），消除 O(订阅数) 遍历 | `client.py` |

---

## 6. 实施顺序（按依赖分组，每组独立可验证）

每组完成后跑 `uv run pytest`，组间保持可提交状态。

1. **组一·基础清理**：D1（删死代码）+ C1（import 模块级）+ B3（role 命名）。低风险先清场，减少后续干扰。
2. **组二·客户端 API**：A1 + A2 + A3 + A4 + B2。解决 sleep(3600) 与致命错误吞没。
3. **组三·性能与配置**：B1（COW）+ D2（HWM）+ C2（环境变量）+ C3（ack request_id）。
4. **组四·延迟功能**：`LatencyStatsRegistry` + `LATENCY_REPORT` 控制命令 + consumer 回传 + admin API + UI 折线图/列表。
5. **组五·D3**：客户端订阅索引（独立小优化）。

---

## 7. 验证策略

- **现有测试**：每组完成后 `uv run pytest` 全量通过，不回归。
- **延迟功能新增测试**：
  - `LatencyStatsRegistry`：record/roll_minute/get_history/snapshot 单测。
  - `LATENCY_REPORT`：control 帧编解码 + dispatch 单测。
  - e2e：producer→server→consumer→回传→registry 记录 的集成测试。
- **性能验证**：延迟功能接入后，数据面 `_on_data_message` 的 `should_sample` 未命中路径行为不变（可通过基准对比消息吞吐确认无回归）。

---

## 8. 风险与回退

| 风险 | 应对 |
|------|------|
| B1 COW 引入数据面/控制面竞态 | COW 快照不可变，数据面只读引用；写时深拷贝索引。充分单测 + 并发场景测试 |
| C3 request_id 改动影响现有 ack 协议 | `request_id` 为可选字段：服务端回复原样回带，client 按 id 匹配 `recv`，无 id 回执退化为"最近未匹配"兜底。同包同版本协同升级；混合版本下旧 client 回执无 id，走兜底不阻断 |
| consumer 回传增加控制面负载 | 1% 采样封顶；若极端高消息量场景仍偏高，可调低 `latency_sample_rate`（C2 补环境变量后可运行时调） |
| D1 删除 `cache/` 影响未知引用 | 删除前全局 grep 确认引用方（`admin/server.py:22` 类型引用需同步清理） |
