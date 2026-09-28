# PulseMQ 系统设计文档（v9.2.8）

> 本文描述 PulseMQ **当前实现的真实状态**（以代码为准，非愿景规划）。
> 涵盖：消息帧格式、进程/线程模型、线路协议、流控与缓冲、多进程消费池、
> 丢弃语义全景、配置参考、监控接口、实测性能摘要与已知问题。
>
> 更新日期：2026-09-28 · 对应版本：9.2.8 · 代码位置：`src/pulsemq/`

---

## 1. 系统定位与总览

PulseMQ 是一个基于 ZeroMQ（ROUTER/DEALER + PLAIN 认证）的轻量级 Python 消息总线，
面向**行情数据分发**场景：发布/订阅、主题通配匹配、低延迟转发、慢消费者治理
（服务端缓冲 + 丢弃策略 + 全程可对账）、可选确认发布与序号对账。内存转发，不持久化。

**三个角色：**

| 角色 | 说明 |
|---|---|
| Server | 中心节点：数据面转发 + 控制面管理 + 订阅者缓冲 + 监控 HTTP |
| ProducerClient | 发布端（fire-and-forget 默认 / confirm 确认可选） |
| ConsumerClient | 订阅端：唯一消费形态——多进程池（默认 workers=1） |

**默认端口**：数据 `5555`、控制 `5556`、监控 HTTP `9090`（均可配置）。

---

## 2. 进程与线程模型

```
┌─ Server 进程 ──────────────────────────────────────────────┐
│ asyncio 事件循环（控制面）                                  │
│   ├ _control_loop        控制命令分发（REGISTER/心跳/订阅…）│
│   ├ _heartbeat_sweep_loop 心跳超时扫描（踢线下线）          │
│   └ _minute_roll_loop    统计分钟归档                       │
│ SyncDataThread（同步数据面线程，独立 zmq.Context）          │
│   poll(10ms) → 收生产者帧 → 统计/序号/ACK → 广播/缓冲入队  │
│   → drain_all（credit 窗口内放行缓冲积压）→ maybe_gc       │
│ AdminServer 线程（HTTP + SSE 监控，独立 loop）              │
│ AsyncArchiveWriter（SQLite 统计归档）                       │
└────────────────────────────────────────────────────────────┘

┌─ ConsumerClient 进程（9.2.4 起唯一消费形态：多进程池）──────┐
│ 主进程：批量排空 recv→轻量头部解析→缺口检测→key 路由→写入  │
│         SM 环（零 payload 解码/零逐帧 await，9.2.7）        │
│ worker 进程×N：出环→完整解码→回调（共享内存字节环，真并行） │
│ 默认 workers=1：一个主进程接收 + 一个 worker 进程处理       │
│ start() 时已有订阅即建池；运行期动态 subscribe 懒建池       │
└────────────────────────────────────────────────────────────┘

ProducerClient 进程：publish = 编码 → DEALER fire-and-forget
                     （confirm=True 时等 PUBLISH_ACK）
```

**关键取舍**：数据面走独立同步线程（转发路径无 asyncio 调度延迟）；
消费端唯一消费路径为 worker 进程池（绕开 GIL，实测 4 进程 6.7~23 倍于旧单进程多线程，
见《性能报告》第七章）；线程间/进程间交接分别为对象引用（零拷贝）与共享内存字节环。

---

## 3. 消息帧设计

### 3.1 v3 帧格式（9.2.5 起唯一版本，单 bytes 帧，大端，不考虑向后兼容）

```
偏移   大小  字段                说明
0      2    magic               固定 b"PM"
2      1    version             0x03
3      1    msg_type            0x01=DATA  0x02=CONTROL
4      1    flags               位域，见 3.2
5      1    data_type           0x00 UNKNOWN / 0x01 DICT / 0x02 DATAFRAME
                                / 0x03 STR / 0x04 BYTES（类型保真标记）
6      1    topic_len           BE uint8（≤255 字节）
7      8    seq                 BE uint64，服务端 per-topic 单调序号
                                （上行帧占位 0，服务端按固定偏移 7 改写）
15     8    timestamp_ns        BE int64（生产端打点，端到端延迟基准）
23     4    record_count        BE uint32（帧内记录条数；DataFrame=len(df)，
                                dict/list=推断值，上限 1,000,000）
27     4    ack_token           可选（flags bit6）：BE uint32 确认发布回执编号
27/31  N    topic               UTF-8（≤255B）
...    ...  payload             序列化+压缩后的数据（帧边界 = ZMQ 消息边界，
                                头部无 payload 长度字段）
末尾   4    CRC32               可选（flags bit7），对前面全部字节校验
```

- **seq 恒在帧内**：v3 删除了 v1/v2 双版本与 ext 扩展段——扩展帧并入头部，
  所有帧都携带服务端 seq，消费端缺口检测无条件生效；服务端广播单一路径
  （`rewrite_seq` 固定偏移改写一次，同一帧发全部订阅者，无版本分流副本）。
- **ack_token**（9.2.5）：确认发布回执编号由 uuid 字符串改为 4B uint32
  per-publisher 计数器（`itertools.count`，客户端循环取值），结构开销从
  ~44B 降到 4B；服务端据此回 PUBLISH_ACK（payload 键 `ack_token`）。
- **topic 上限 255 字节**：topic_len 2B→1B，结构开销有界（最坏
  27+4+255+4 = 290B；v1/v2 因 2B topic_len 最坏 ~64KB）。
- **单帧上限保护**：`encode()` 拒绝超过 `MAX_FRAME_BYTES`（256MB）的帧。
  真正的运行时瓶颈在消费端共享内存环（默认 64MB，超限帧静默丢弃）与
  服务端订阅者缓冲（默认 64MB/50 万帧，慢消费者下单帧超限即被淘汰）。

### 3.2 flags 位域（1 字节）

```
bit[0:2] 序列化格式   000=msgpack  001=bytes  010=pyarrow  100=str  101=json
bit[3:4] 压缩算法     00=none      01=snappy  10=lz4       11=zstd
bit5     reserved
bit6     ack_token（头部带 4B 回执编号）
bit7     CRC 使能
```

- 序列化器按 data_type 有兼容白名单：DATAFRAME→{pyarrow(默认), msgpack, json}；
  DICT→{msgpack(默认), json}；STR→str；BYTES→bytes。
- 压缩注册表：none / snappy / lz4 / zstd（snappy、zstd 依赖可选安装）。

### 3.3 帧级变换（服务端）

- `rewrite_seq(frame, seq)`：seq 固定偏移（7..15）改写，无需重组帧；
  CRC 帧重算 CRC。服务端为每个被接受的数据帧调用一次，输出帧广播给全部
  订阅者。实测单核 ~400 万帧/s，不在数据面热路径瓶颈上。

### 3.4 控制帧

`encode_control(cmd, payload)`：msg_type=CONTROL（同为 v3 帧，seq 占位 0），
cmd 作为 topic，msgpack 载荷，无压缩。控制帧走**控制面 socket**；一个例外
走数据面：`PUBLISH_ACK`（服务端→生产者，确认回执）。
服务端数据面**不产生**CONTROL 帧给消费者；消费端数据面 socket 收到 CONTROL 帧
（即 PUBLISH_ACK）由 recv 循环拦截处理，不进入订阅分发。

**控制命令集**（payload 均含 `client_id`，请求-回复用 `request_id` 关联）：

| 命令 | 方向 | 关键 payload | 回复 |
|---|---|---|---|
| REGISTER | C→S | username / roles / topics / buffer{policy,max_messages,max_bytes,max_age_s} | result（请求 id 回填）|
| HEARTBEAT | C→S | drops{topic:n} / credit / gaps{topic:n} / proc{topic:{...}} | result=OK |
| SUBSCRIBE / UNSUBSCRIBE | C→S | topic（模式，如 `mkt.*`） | OK |
| DISCONNECT | C→S | — | OK（尽力而为） |
| LATENCY_REPORT | C→S | topic / latency_ns | 无回复 |
| PUBLISH_ACK | S→C（数据面 socket） | ack_token / topic / seq / record_count | — |

### 3.5 帧大小与压缩

- 压缩策略 `auto`：序列化后 <256B 用 none，≥256B 用 lz4。
- 实测（行情快照 749B/条 × 200 条/帧）：msgpack+lz4 帧约 60~90KB（JSON 148KB）。
- CRC 默认关闭（opt-in），开启后 `rewrite_seq` 自动重算。

---

## 4. 线路与通道

| 通道 | 端点 | socket | 认证 | 承载 |
|---|---|---|---|---|
| 数据面 | 5555/5557 | ROUTER(S) / DEALER(C)，SNDHWM=RCVHWM=10000 | PLAIN | 数据帧 + PUBLISH_ACK |
| 控制面 | 5556/5558 | ROUTER(S) / DEALER(C) | PLAIN | 全部控制命令 |
| 监控 | 9090/9091 | HTTP + SSE | Bearer token | 管理面板与 API |

- 数据面/控制面**分离**：控制命令洪峰不阻塞转发；同一客户端两个 DEALER
  使用相同 bytes identity，服务端路由表（以控制面 ident 为 key）可直接用于数据面转发。
- 服务端数据面为**独立同步线程 + 独立 zmq.Context**；控制面命令在 asyncio 循环处理。
- 控制面回复一律 **DONTWAIT 直发**（`_safe_send`）：对端队列满/僵死立即丢弃并告警，
  控制循环永不阻塞（9.2.1 修复的 head-of-line blocking）。
- 客户端控制面发送同样带超时保护（`_CONTROL_SEND_TIMEOUT=2s`）。

**生命周期**：connect（ZAP PLAIN 握手，monitor 裁定）→ REGISTER（注册/订阅/缓冲协商）→
运行期（数据 + 1s 心跳 + 随时 SUBSCRIBE / UNSUBSCRIBE）→ DISCONNECT 或心跳超时
（默认 6s，可配置）→ 服务端清扫（路由/credits/缓冲）。

**部署约束（9.2.6 起显式化）**：同一进程只能启动一个 Server 实例——ZAP 认证
端点 `inproc://zeromq.zap.01` 是 ZeroMQ 的进程级全局端点，第二个实例启动时
会得到明确的 PulseMQError（而非隐晦的 ZMQError）。单机多实例分进程部署
（测试实例 5557/5558/9091 与生产实例 5555/5556/9090 即此形态）。

---

## 5. 核心机制

### 5.1 流控：credit 窗口守恒

**目的**：慢消费者保护——服务端只发消费端上报"装得下"的量，杜绝 zmq 管道满
导致的静默丢弃。

```
消费端（每 1s 心跳）：credit = 队列/环剩余容量
服务端（心跳到达）：set_credit(ident, credit) 开新窗口
数据面 drain（每轮 poll，有积压时 10ms）：
    budget = 窗口快照 − 本窗口已发送
    budget ≤ 0 → 不发，帧留服务端缓冲
    否则发送 min(512, budget) 帧（_DRAIN_BUDGET=512/轮）
    对端 EAGAIN → 本轮停，帧保留
```

多进程消费池（唯一消费形态）的 credit 来源与限流值：

| 形态 | credit 来源 | 供给节流上限 |
|---|---|---|
| 多进程池（workers=N，9.2.4 起唯一模式） | Σ 环空闲字节 ÷ 观测最大帧长（64MB 环 ≈ 65,536） | ~65k 帧/s |

已知边界：窗口 1s 粒度（快 worker 被钉在快照值、突发响应慢一拍）、按帧计数
（池模式按最大帧长折算字节，混合帧长预算保守）、服务端出流硬上限 ~51k 帧/s
（512×100 轮/s）。细化方案见第 9 节路线。

### 5.2 订阅者缓冲（服务端，慢消费者治理）

每个声明了缓冲策略的订阅者一个 `SubscriberBuffer`：

| 策略 | 结构 | 丢弃语义 |
|---|---|---|
| drop_old（消费端默认） | `deque` + 运行字节数 | 三上限任一触发→淘汰最旧；drain 时超龄过期 |
| conflate | `dict[topic→最新帧]` | 新帧覆盖旧帧（仅字节上限触发淘汰） |

三上限（服务端全局封顶，客户端 REGISTER 只能请求更小值，`_clamp`）：
`buffer_max_messages=500,000 帧`、`buffer_max_bytes=64MB`（9.2.8 起；
9.2.2~9.2.7 曾为 10 万条/10MB，按帧计数下批量负载几乎总是字节上限先触发，
故调大）、`buffer_max_age_s=0`（不限龄；消费端默认协商 30s）。
**计数按帧不按记录**——批量负载下字节上限是实际生效的闸门。

缓冲满时的行为：drop_old 持续驱逐最旧（新数据优先），驱逐/过期全部计入
DropStats 与 `/api/v1/stats/buffers`，与客户端缺口精确对账（实测分毫不差）。

### 5.3 确认发布（confirm=True）

```
生产者：publish(data, confirm=True, ack_timeout=5.0)
  → v3 帧（头部 4B ack_token，seq 占位 0）
服务端数据面：收到 → 分配 per-topic seq → 回 PUBLISH_ACK{ack_token, seq, record_count}
           （数据面 socket 同线程直发）→ 路由广播照常
生产者：await ack → 返回服务端 seq；超时抛 PublishAckTimeout
```

语义：ack = **服务端已接受**（含路由），不代表订阅者已送达。代价：每批 1 RTT
（实测 p50 33.6ms）。

### 5.4 序号与缺口对账（三方账本）

- 服务端：`_topic_seq[topic]` 单调递增；缓冲驱逐/过期计入 DropStats 与
  buffers 快照（evicted/expired）。
- 消费端：v3 帧头 seq 逐帧跟踪（`_track_gap`，无条件生效），缺口 = seq 跳变量；
  随心跳上报 `gaps` 增量；服务端聚合到 `_gap_stats`（admin 实时快照 `gaps` 字段）。
- **语义**：缺口 = 观测窗口内的缺失（首帧之前的不可见）——与 Iggy/Kafka
  offset 语义一致。环满丢弃的帧通过"seq 基线回退"计入后续缺口（9.2.3 修复漏报）。
- **对账恒等式**（实测验证）：`发送 = 处理 + 驱逐 + 过期 + 环丢弃 + 在途`，
  三方账本（服务端 evicted/expired、客户端 missing、环计数）互为印证。

### 5.5 多进程消费池（WorkerPool）

```
ConsumerClient(workers=N, key=..., worker_ring_mb=..., worker_init=...)
  本进程（ingestor）：批量排空 recv → 轻量头部解析 → 缺口检测 → key 路由
                    → SM 环写入（9.2.7：一次唤醒连收，批内零 await）
  worker 进程 ×N   ：出环 → 完整解码 → 用户回调（真并行，绕开 GIL）
```

- **ShmRing**：共享内存变长字节环（每 worker 独立，SPSC）。
  布局：`[tail:8][head:8][diag读:8][diag处理:8][数据区 cap]`；
  记录 `[len:4 LE][payload]`，`len=0xFFFFFFFF` 为回绕填充标记；
  头尾为单调字节计数（物理偏移取模），x86 TSO 下无锁安全（ARM 需栅栏，暂不支持）。
  满语义 **drop-new**（丢最新帧并计数；credit 守恒下正常运行不会满，仅 worker
  停滞兜底）。诊断计数由 worker 写、主进程直读（强对账通道）。
- **key 路由**（9.2.7）：`None`（**默认**）= **最短队列路由**：每帧选积压字节
  数最少的环、并列时轮询——worker 等速时即完美轮询，慢 worker 自动降载
  （不保序）。`"topic"` / payload 字段名 /
  `callable(payload, topic)->str`（运行在主进程，lambda 可用）
  → **jump consistent hash**（over crc32；确定性，跨进程/重启一致；worker 数
  变化时仅 ~1/n 换位；同 key 恒定同 worker、保序）。
- **key 拆分载荷契约**（9.2.9）：字段名/callable 路由**仅支持 dict / str
  载荷**——dict 按字段值提取（缺失视为不可拆分）、callable 收到真实
  payload（dict/str 本身）、str 载荷消息内容本身即 key；且订阅必须是
  **精确 topic**（通配符在 `subscribe()` 即报错——key 提取假设该 topic
  载荷结构已知）。路由粒度为整帧，**DataFrame 不支持 key 拆分**（帧内
  不按列拆分）：多股票 DataFrame 在发布端按 key 拆分（groupby 逐组
  发布 dict/str），或 `key=None` + worker 回调内自行 groupby。不支持
  载荷/字段缺失/callable 异常 → **显式 WARNING（每原因一次）+ 回退
  topic 路由**（确定性、同 topic 保序），回退帧数经心跳 `key_fallback`
  delta 上报，服务端累计进 `/clients` 的 `key_fallback` 字段——
  修复 9.2.8 前非 dict 载荷一律得 `{}`、key="None" 全帧静默落单 worker
  的缺陷。
- **回调契约**：模块级可导入函数（跨进程按引用 pickle），**同步或异步均可**
  （9.2.4：异步回调在 worker 进程自有事件循环上执行，同一 worker 内串行）；
  lambda/闭包在订阅时即报错——worker 内不共享主进程内存，闭包捕获是拷贝。
  `worker_init(worker_index)` 每进程初始化一次。
- **动态订阅/退订**：start 后调用 subscribe 经每 worker 的 sub_q 下发（9.2.6 起
  `unsubscribe()` 同通道下发 remove，并向服务端发 UNSUBSCRIBE，幂等）；
  首次动态订阅会懒创建池（阻塞 ~0.3s 等 worker attach）。
- **统计**：worker 每 1s 经 mp.Queue 上报 processed/rows/proc 耗时（9.2.4：
  空闲路径同样定期 flush，避免最后一批统计滞留）；SM 头部 diag 计数为主对账通道。
- 顺带：credit 照常生效（free_bytes ÷ max_frame 折算），环在守恒下不会满。

### 5.6 心跳与生命周期

- 消费端心跳 1s：client_id + drops + credit + gaps + proc。
- 服务端 `heartbeat_timeout`（默认 6s，`PULSEMQ_HEARTBEAT_TIMEOUT` 可配）
  扫描踢线：删路由/credits/缓冲/池，发 on_disconnect 事件。
- 控制面回复 DONTWAIT 化（9.2.1）：僵死 peer 不再阻塞控制循环。

---

## 6. 丢弃点全景（哪里会丢、什么策略、怎么记账）

| # | 层 | 触发条件 | 策略 | 记账 |
|---|---|---|---|---|
| 1 | 服务端订阅者缓冲 | 三上限任一触发 | drop_old / conflate（可配） | DropStats + buffers 快照（evicted/expired），客户端缺口印证 |
| 2 | 服务端直发路径 | 无缓冲订阅者 + 对端 EAGAIN（zmq 管道满） | 丢弃并计数 | DropStats |
| 3 | 消费端 zmq 管道 | RCVHWM 10000 满 | libzmq 丢弃（上游体现为 #2/#4） | — |
| 4 | 池模式 SM 环 | 环满（worker 停滞兜底；credit 守恒下不应发生） | drop-new | route_drops → 心跳 → DropStats + 缺口回退计数 |

**丢失语义总结**（9.2.4 两线程队列与内联模式删除后）：缓冲订阅者的静默丢失已
结构性消除——超载转化为"缓冲积压 → 可观测的驱逐/过期"；缺口检测提供客户端
独立账本；confirm 提供生产者侧感知。唯一保留的"可丢"路径：直发路径（未协商
缓冲）的 zmq 背压丢弃与池环兜底（均为"尽力而为"语义的显式选择）。

---

## 7. 配置参考

### ServerConfig（TOML `[server]` / 环境变量 `PULSEMQ_*`）

| 字段 | 默认 | 说明 |
|---|---|---|
| data/control/admin_endpoint | 5555/5556/9090 | 三通道监听 |
| heartbeat_timeout | 6.0s | 心跳超时踢线 |
| sndhwm / rcvhwm | 10000 / 10000 | zmq 管道高水位 |
| buffer_max_messages | 500,000 帧 | 订阅者缓冲条数上限（按帧，9.2.8 起） |
| buffer_max_bytes | 64 MB | 订阅者缓冲字节上限（压缩后帧大小，9.2.8 起） |
| buffer_max_age_s | 0（不限） | 缓冲存活时间 |
| stats_db / retention / admin_token / latency_sample_rate … | — | 统计与监控 |
| alert_webhook | ""（仅日志） | 告警 webhook URL；空=降级 log_event WARNING |
| alert_cooldown_s / alert_drop_per_min / alert_gap_per_min / alert_buffer_age_s / alert_starved_per_s / alert_loop_stall_ms | 60 / 1000 / 1000 / 5.0 / 100 / 1000 | 告警阈值（0=关闭规则），环境变量 `PULSEMQ_ALERT_*` 可覆盖 |

### ConsumerClient 关键参数

| 参数 | 默认 | 说明 |
|---|---|---|
| buffer_policy / buffer_cfg | drop_old / {max_age_s:30} | 服务端缓冲协商（None=直发不缓冲） |
| workers | 1 | worker 进程数（唯一消费模式；1=主进程+1 worker） |
| key | None | 路由键：None（默认，最短队列，等速即轮询）/ topic / payload 字段名 / callable。字段名/callable（key 拆分）仅 dict/str 载荷 + 精确 topic 订阅，DataFrame 不支持（9.2.9，详见 §5.5） |
| worker_ring_mb | 64 | 每 worker 共享内存环大小 |
| worker_init | None | 每 worker 初始化函数 |
| latency_sample_rate | 0.01 | 端到端延迟采样率 |

**回调约束（9.2.4）**：所有订阅回调必须为模块级可导入函数（pickle 按引用
传递），同步或异步均可；worker 进程不与主进程共享内存，lambda/闭包在
subscribe() 时即报错。基类 `Client` 订阅同样走池（workers 等参数已上移）。

---

## 8. 监控与观测

Admin API（Bearer token）：`/api/v1/stats/realtime`（主题速率/延迟/丢弃/缺口/
缓冲/在线客户端 + **数据面健康 dataplane + 心跳质量 heartbeat + 对账
reconciliation**，9.2.8）、`/api/v1/stats/buffers`（每订阅者缓冲：策略/深度/
字节/最老年龄/淘汰/过期/合并/已发 + **credit 余量/窗口/拦截轮次**）、
`/api/v1/stats/drops/history` 与 `/api/v1/stats/gaps/history`（**分钟历史，
内存 + SQLite 合并**，`?topic=` 可选）、`/api/v1/stats/stream`（SSE）、
`/api/v1/clients`（含 **per-worker 细分 workers、confirm 统计、credit 视图**）、
`/api/v1/events`、`/api/v1/topics/{t}/history`、
`/api/v1/latency/topics/{t}/history`、`/api/v1/system/status`（uptime/version/
pid/线程数/RSS）、`/healthz`、**`/metrics`（Prometheus 文本 exposition）**。
管理面板（INDEX_HTML）含缓冲区与缺口汇总、**对账、服务端健康（数据面
chips + 心跳质量）页签**。

**9.2.8 新增观测面**：

| 观测点 | 载体 | 回答的问题 |
|---|---|---|
| credit 余量/窗口剩余/拦截轮次 | buffers snapshot、/clients、/metrics | 消费端是否被流控掐住、掐了多少 |
| 数据面循环 avg/max、收/发速率、EAGAIN、信用拦截 | DataPlaneStats → realtime.dataplane、/metrics | 转发线程是否饱和/停顿、背压强度 |
| 心跳到达间隔 avg/max、踢线计数 | HeartbeatMonitor → realtime.heartbeat、/metrics | 踢线前的"心跳迟到"前置预警 |
| 处理时 e2e 延迟（avg/max） | worker 出环时刻 − 帧内生产 ts，心跳 proc.e2e → /clients、processing_by_topic | 消费端真实积压延迟（含环内排队） |
| per-worker 细分（速率/CPU/环积压/累计） | 心跳 workers 字段 → /clients | 最短队列路由下 worker 均衡度 |
| 发布端 confirm 统计（ack 均值/最大/超时累计） | 心跳 confirm 字段 → /clients | 确认发布延迟与超时率 |
| per-topic 对账（发布/处理/缺失/丢弃累计 + 在途估算） | realtime.reconciliation | 三方账本一屏对齐 |
| 丢弃/缺口分钟归档 | minute_drops/minute_gaps 表 + history API | 事后回溯"何时开始丢" |

**告警（AlertManager）**：固定规则集 + 阈值可配（`PULSEMQ_ALERT_*` 环境变量
或 TOML `[monitoring]`）：drops_burst / gap_burst / buffer_backlog /
credit_starved / dataplane_stall / client_kicked。每规则独立冷却（默认 60s），
配置 `alert_webhook` 时 POST JSON，未配置降级为日志 WARNING 事件。阈值设 0
关闭对应规则。

**压测方法学**（可复现脚本在性能测试目录）：阶梯增速 + 每档排空对账，
对账恒等式 `发送 = worker处理 + 驱逐 + 过期 + 环丢弃 + 在途`，
三方账本（服务端计数 / 客户端缺口 / SM 环头 diag 计数）互证。

---

## 9. 实测性能摘要（WiFi 环境，客户端 Windows ↔ 172.16.1.84）

| 场景 | 结果 |
|---|---|
| 单条延迟（300 条，fire-and-forget） | e2e p50 ≈ 35~45ms，min ≈ 31ms（RTT 主导） |
| 批量 200 行/帧 发送 | 24~31 万行/s，零丢失 |
| 确认发布（confirm） | ack p50 33.6ms（≈1 RTT）；9.2.5 v3 帧 p50 27.2ms / p99 35.2ms，seq 单调 |
| 池模式 workers=1 无丢失吞吐 | 9.2.4：轻 ≈ 1.64 万帧/s（零丢失至 18000 档）；重回调 ≈ 1.03 万帧/s（处理边界）。9.2.5 v3（发布端独立进程工装，`perf_test.py`）：轻零丢失至 1.74 万帧/s（发布端饱和，系统下限）；重回调 1.07 万帧/s（处理边界，与 9.2.4 持平） |
| 行情快照 200 条/帧（749B/条） | 持续 ≤ 5 万条/s 零丢失；超载 ~30s 后开始可观测驱逐 |
| 多进程池隔离基准 | P4 较单线程 ×6.7~23（GIL 密集/批量场景） |
| 9.2.7 批量接收（4 核远程测试机） | 旧接收循环 ~43k 帧/s 单点上限消除：workers=2 峰值 7.6 万帧/s；workers=4 全程总吞吐 86.5 万帧（topic 粘性模式 78.5 万）。同机跨次方差 ±20%，峰值与 4-worker 总量为主要信号 |
| 僵尸 peer 攻击 | 洪灌 11.4 万心跳后新客户端 0.74s 完成注册（9.1.1 同场景卡死） |

完整数据与图表见《性能报告》（perf_921_final.html）。

**压测工装注意**：发布端与消费端主进程共用一个 asyncio 事件循环时，
每条消息消耗 2 次 await（publish + recv），工装自身在 ~1.1 万帧/s 饱和，
测的是工装不是系统。标尺脚本 `perf_test.py` 的 e2e 阶段把发布端放到
独立进程（每档结束后经 mp.Queue 上报，queue.get 必须经线程池执行，
直接阻塞 get 会冻结心跳 → 消费者被 6s 心跳扫描下线 → 全部帧静默丢失）。

---

## 10. 已知问题与技术债路线

| 优先级 | 事项 | 说明 |
|---|---|---|
| ~~9.2.7~~ 已完成 | 客户端接收循环批量化 | 9.2.7 落地：DONTWAIT 批量排空 + peek_topic_seq 轻量头部 + 批尾聚合统计 |
| 9.2.8 | credit 粒度细化 | 1s 窗口 → 数据通道按需捎带 + 字节预算（混合帧长失真、突发响应慢一拍）|
| 9.2.8 | worker 统计通路加固 | mp.Queue 在 Windows feeder 延迟不可靠；SM diag 为权威，Queue 通道需重做 |
| 9.2.8 | 缓冲"条数"改记录数语义 | max_messages 按帧计数，批量负载下形同虚设（§5.2） |
| 9.3.0 | **消费端拉模式（FETCH 协议）** | 删除 credit 机制整体（背压天然成立），抬高供给上限；API 不变 |
| 9.3.0 | 消费端丢弃策略统一参数化 | 池模式 drop-new 与行情语义相反（worker 停滞丢最新），需 Vyukov 序列号环改 drop-oldest |
| 9.3.0 | 服务端数据面多线程/分片 | 单线程出流 ~51k 帧/s 上限（fetch 批量回发可先缓解） |
| backlog | 通配符语义文档化 | `foo.*` 为层级前缀匹配（不匹配 `foo.c1` 类同级名），易误解 |
| backlog | HEARTBEAT 对未注册 client_id 回 OK | 协议卫生 |
| backlog | ShmRing ARM 支持 | 当前依赖 x86 TSO，ARM 需内存栅栏 |

---

## 11. 版本演进

| 版本 | 关键变更 |
|---|---|
| 9.1.1 | 基线：push 转发、直发满即丢、控制面阻塞 bug 在场 |
| 9.2.0 | 订阅者缓冲（drop_old/conflate）+ 协商、admin 缓冲端点；发现控制面阻塞与信用超发两个缺陷 |
| 9.2.1 | 控制面 DONTWAIT 修复；v2 扩展帧（seq/ack_id）+ proto 协商；确认发布；缺口对账；信用窗口守恒（修复超发）；ConsumerClient 缓冲默认开启；GC 防护；缓冲面板 |
| 9.2.2 | 缓冲默认上限调低为 10 万条/10MB；记录文件分片等压测工具修复 |
| 9.2.3 | 多进程消费池（ShmRing + workers/key 参数 + 动态订阅 + worker_pool 开关）；环满缺口回退计数；帧级对账工具 |
| 9.2.4 | **消费端架构收敛为唯一模式：多进程池**（删除两线程解码队列与内联分发，默认 workers=1 = 主进程+1 worker）；池下沉到基类（任意订阅走池，运行期懒建）；异步回调 worker 内事件循环支持；worker 空闲统计 flush 修复；回调订阅时校验 |
| 9.2.5 | **v3 帧格式（唯一版本，无兼容层）**：扩展帧并入头部（seq 8B 恒在帧内，偏移 7），ack_id uuid → 4B ack_token 计数器（flags bit6），topic 上限 65535→255 字节（topic_len 2B→1B），头 20B→27B；删除 v1/v2 分流、proto 协商、ext 变换（stamp/strip）；新增 MAX_FRAME_BYTES(256MB) 编码防护 |
| 9.2.6 | 客户端动态退订 `unsubscribe()`（本地订阅表 + worker 池 + 服务端 UNSUBSCRIBE 三层同步，幂等）；`pulsemq` CLI 接入 argparse（--help/--version 立即返回，不再启动服务器）；同进程双 Server 报错清晰化（ZAP inproc 端点独占约束文档化） |
| 9.2.7 | **消费端吞吐与路由三连改**：客户端接收循环批量化（一次唤醒 DONTWAIT 连收 ≤256 帧 + `peek_topic_seq` 轻量头部 + 批尾聚合统计，消除逐帧 await）；`key=None` 改最短队列路由并将其设为**默认**（积压最少优先、并列轮询——等速时即轮询、慢 worker 自动降载）；key 路由换 jump consistent hash（worker 数变化仅 ~1/n 换位）。附带修复心跳处理统计丢失：`_ProcStats.drain` 只遍历有新接收计数的 topic，worker 统计晚到一个窗口时被静默吞掉——改为接收∪处理并集，服务端按 topic latch 延迟字段 |
| 9.2.8 | **监控可观测性大补**：数据面健康统计（DataPlaneStats：循环 avg/max、收/发速率、EAGAIN、信用拦截）；credit 可见性（buffers/clients 含余量/窗口剩余/拦截轮次）；心跳质量监控（HeartbeatMonitor：per-client 到达间隔 + 踢线计数）；worker 处理时 e2e 延迟（出环时刻−生产 ts，avg/max）；per-worker 细分与发布端 confirm 统计随心跳上报；per-topic 对账视图（发布/处理/缺失/丢弃累计+在途估算，TrafficStats 累计计数 + proc count_cum 服务端滚动累加）；drops/gaps 分钟归档到 SQLite（minute_drops/minute_gaps 表 + history API）；Prometheus `/metrics` exposition；AlertManager 告警规则（阈值可配，webhook 可选，0=关闭）；Web UI 新增服务端健康/对账面板与客户端弹窗扩展（e2e/credit/workers/confirm），表格斑马纹+粘性表头。**订阅者缓冲默认上限调大为 50 万帧/64MB**（10 万条/10MB 按帧计数在批量负载下形同虚设，几乎总是字节上限先触发） |
| 9.2.9 | **key 拆分载荷契约收紧**：字段名/callable 路由仅支持 dict/str 载荷（str 消息内容本身即 key）且订阅必须精确 topic（通配符 `subscribe()` 即报错）；DataFrame 明确不支持 key 拆分（路由粒度为整帧，帧内不按列拆分——多股票 DataFrame 请发布端 groupby 拆分或 key=None+worker 内拆）。不支持载荷/字段缺失/callable 异常 → 显式 WARNING + 回退 topic 路由（同 topic 保序），帧数心跳上报累计进 `/clients` 的 `key_fallback`。**修复**：`_peek_payload` 对非 dict 载荷返回 `{}` 使 key="None" 全帧静默落单 worker 的缺陷 |

---

## 12. 代码地图

```
src/pulsemq/
├── protocol/        帧：frames.py（v3 编解码 + rewrite_seq 固定偏移改写）、
│                    flags.py、msg_type.py、compression.py、serialization.py
├── transport/       router.py：Transport（asyncio）+ SyncDataThread（数据面线程）
│                    + ZAP 认证 + broadcast（credit/缓冲入队）
├── server.py        组装：控制循环/心跳扫描/数据面回调（seq 改写/ACK/广播）
├── buffering.py     SubscriberBuffer（drop_old/conflate）+ BufferManager（窗口守恒/GC 防护）
├── worker_pool.py   ShmRing（共享内存变长字节环）+ WorkerPool + worker 进程主循环
├── client.py        Client/ProducerClient/ConsumerClient（唯一池消费、confirm、缺口、动态退订）
├── control.py       控制命令集 + OnlineRegistry
├── config.py        ServerConfig/ClientConfig（TOML + 环境变量）
├── stats/           traffic/latency/drops/throughput/connections/storage
│                    + dataplane（数据面健康）/health（心跳质量）/gaps（缺口归档）
├── alerts.py        AlertManager（阈值规则 → webhook / 日志事件）
├── admin/           AdminServer（HTTP/SSE）+ web_ui.py（监控面板）
└── security.py      CredentialStore（凭据文件/哈希）
tests/               292 项（协议/生命周期/缓冲/池/CLI/路由/监控/压测辅助，mp 回调录制器）
```
