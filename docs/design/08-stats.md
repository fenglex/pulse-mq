# 08 统计子系统设计

> 源码：`src/pulsemq/stats/`（traffic / latency / connections / drops / storage）

## 1. 总体结构

五个组件覆盖四类观测维度 + 一个持久化通道，全部由 `Server` 组装、`AdminServer` 消费：

```
数据面线程 ──┬─ TrafficStats.record（每条消息）
             └─ LatencyStatsRegistry(半程).record（采样命中时）
控制面协程 ──┬─ LatencyStatsRegistry(全程).record（LATENCY_REPORT）
             └─ DropStats.record（心跳 drops + 广播丢弃）
主线程任务 ──┬─ minute_roll_loop：traffic.roll → AsyncArchiveWriter → StatsStorage(SQLite)
             └─ latency/drops.roll_minute（仅内存）
Admin 线程 ─── 只读 snapshot()/get_history()/load_history()
```

统一线程模型约定：**zmq 数据接收循环从不触碰 SQLite**；跨线程共享靠「单写者无锁」或 `threading.Lock`。

## 2. TrafficStats（分钟流量，内存 8h 窗口）

### 2.1 数据结构

- `MinuteSlot(timestamp, msg_count, record_count, bytes_total)`——一个 topic 一分钟的快照。
- `_current: {topic: MinuteSlot}` 当前分钟累积器；`_slots: {topic: deque[MinuteSlot(maxlen=480)]}` 滚动窗口。

### 2.2 单写者无锁快路径（`record`）

数据面线程是 `_current` 的唯一写者：

- 常规路径（topic 已存在且未跨分钟）：**无锁**直加三计数。
- 每 1024 条（`msg_count & 0x3FF == 0`）检查一次分钟切换，跨分钟才加锁调 `roll_minute`——避免全热 topic 路径永不觉察滚动。
- 慢路径（新 topic 首现/分钟切换）加 `RLock` 双检后建 slot。

### 2.3 `roll_minute()`（持锁）

归档 `_current` 中 `msg_count>0` 的 slot（拷贝值，防后续累加污染归档）→ 追加 `_slots` → 清空 `_current` 切新分钟 → 清理空 topic。返回归档 dict 供 SQLite 落库；**同一分钟内重复调用返回空**（幂等）。

### 2.4 近 60 秒速率估算（`all_topics_snapshot`）

分钟粒度下「当前分钟尚未满」不能直接除 60，采用**比例外推**：

```
elapsed   = now - 当前分钟起点
window_x  = 当前分钟实测_x + 上一分钟_x × (60 - elapsed) / 60
rate_1min = window_x / 60
```

输出每 topic：`msg_count_current / record_count_current / bytes_total_current / msg_rate_1min / record_rate_1min / bytes_rate_1min / history_minutes`。读路径对 key 集合先做快照、逐条 `.get()`，避免与 `roll_minute` 的 `clear()` 并发迭代崩溃。

`bytes_total` 统计的是**压缩后的 payload 字节数**（不含帧头），`msg_count` 是帧数、`record_count` 是逻辑记录数（批量帧 >1）。

## 3. LatencyStats / LatencyStatsRegistry（延迟直方图）

### 3.1 固定桶直方图（`LatencyStats`）

桶上界（ns）：50µs / 100µs / 500µs / 1ms / 5ms / 10ms / 50ms，共 7 界 8 桶，末桶 `[50ms, +∞)`（插值用有限上界取末界×2）。

- `record`：`bisect_left` 定位桶 +1。
- 分位数：目标序号 `pct × total` 落入第 i 桶时按桶内偏移在 `[下界, 上界]` **线性插值**，比固定代表值更准。
- 输出 `p50_ms / p95_ms / p99_ms / count`。

### 3.2 Registry（按 topic × 分钟窗口）

- 线程模型（源码注释）：数据面线程写半程、控制面协程写回传、主线程 roll、admin 线程读，`threading.Lock` 保护（采样命中时才加锁，开销可接受）。
- **计数器采样替代 RNG**：`should_sample` 用递减计数器（阈值 `round(1/rate)`），每 1/rate 条采 1 条，O(1) 无随机数开销、方差更低。注意：两个 Registry 实例各有独立计数器，`_lat_half` 由服务端数据面调用，e2e 采样在客户端侧（`random.random()`）。
- `roll_minute`：各 topic 现算分位 → `MinuteLatency(timestamp, p50, p95, p99, count)` 追加 `deque(maxlen=480)` → 清 `_current`。
- `get_history(topic, minutes)`：近 N 分钟序列（延迟趋势折线）。

两套实例的语义：
| 实例 | 写入源 | 含义 |
|------|--------|------|
| `_lat_half` | 服务端数据面（now − 帧内 ts） | producer→server 传输延迟 |
| `_lat_e2e` | 消费端 LATENCY_REPORT 回传 | producer→consumer 端到端 |

## 4. ConnectionStats（在线状态 + 生命周期事件环）

### 4.1 事件环

`deque[LifecycleEvent(maxlen=ring_size=200)]`，单写者（主线程：控制分发 + ZAP on_auth 回调）append，无锁（GIL）；admin 线程 `recent_events(limit)` 拷贝读。

事件类型词表（小写，与 Web UI 颜色分类对齐）：`connect / disconnect / subscribe / unsubscribe / auth`，另有 level（INFO/WARNING）与 message 文本。

| 埋点 | 触发 | 内容 |
|------|------|------|
| on_connect | REGISTER 成功 | `user 上线 role=... endpoint=...` |
| on_disconnect | DISCONNECT / 心跳超时 | `client_id 离线 reason=disconnect|heartbeat_timeout` |
| on_subscribe / on_unsubscribe | 对应命令 | `user 订阅/取消订阅 pattern` |
| on_auth | ZAP 裁定后 | 成功 INFO / 失败 WARNING + reason |

### 4.2 在线快照（委托模式）

不自己存在线表，构造时注入 `registry_snapshot_fn`（`OnlineRegistry.snapshot`），`online_clients()` 调用后组装 `ClientSnapshot`（含 `duration_seconds = now − connected_at`）。`_role_of(roles)`：含 pub 且含 sub → both；仅 pub → producer；否则 consumer。

`counters()`：`online_users / online_producers / online_consumers / total_subscriptions`。

## 5. DropStats（消费端丢弃聚合，1h 窗口）

- 双写源，汇聚到同一实例：
  1. 服务端数据面：`broadcast` 返回的丢弃数（订阅者队列满 / 信用耗尽）；
  2. 控制面心跳：`drops` 字段（客户端解码队列满丢最老的按 topic 计数，`drain_drops` 取走自上次心跳的增量）。
- 结构：`_current: {topic: count}` + `_history: {topic: deque[MinuteDrop(maxlen=60)]}`，`threading.Lock`。
- `snapshot()` 三级粒度：`drops_current`（进行中分钟）/ `drops_last_min`（上一完整分钟）/ `drops_1h_total`（近 1h 累计 + 当前）。

## 6. StatsStorage（SQLite 持久化）

### 6.1 Schema 与连接策略

```sql
CREATE TABLE IF NOT EXISTS minute_stats (
    topic TEXT NOT NULL, timestamp INTEGER NOT NULL,
    msg_count INTEGER, record_count INTEGER, bytes_total INTEGER,
    PRIMARY KEY (topic, timestamp)
);
PRAGMA journal_mode=WAL;
```

- 连接 `check_same_thread=False` + `threading.Lock` 串行化全部操作——因写方在主线程（ArchiveWriter consumer）、读方在 admin 线程（`load_history`），SQLite 默认禁止跨线程。
- `db_path` 兼容 `sqlite://` 前缀。
- `INSERT OR REPLACE`（同分钟重归档覆盖）。

### 6.2 接口

| 方法 | 用途 |
|------|------|
| `save_minutes_batch(dict)` | 批量写（异常仅 debug，不抛） |
| `load_history(topic, since_ts)` | 进程重启后恢复图表历史 |
| `cleanup(retention_days=7)` | 删除过期行。**当前无调用方接线**（配置项 `retention_days` 亦未使用），属预留能力 |

## 7. AsyncArchiveWriter（异步批量归档）

```
minute_roll_loop ──enqueue──▶ asyncio.Queue ──▶ consumer 任务
                                                  │ 攒批：阻塞取第 1 个 + get_nowait ≤ batch_size-1
                                                  ▼
                                          storage.save_minutes_batch(merged)
stop()：cancel consumer → 主上下文 _drain() 清空队列落库 → 清引用
```

- 同一分钟多 topic 的归档以 `{topic: MinuteSlot}` 为一个队列元素，merge 时 dict.update 天然按 topic 去重（迟到批次覆盖）。
- stop 的 drain **不依赖被取消任务内执行**（可靠收尾），这是 `Server.stop` 顺序中 `archive_writer.stop()` 必须先于 `storage.close()` 的原因。
