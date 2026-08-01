# PulseMQ

面向金融行情的高性能消息中间件，基于 ZeroMQ ROUTER/DEALER 架构。

- **Client/Server 架构** — 服务端（ROUTER）集中路由 + 控制面；客户端（DEALER）发布/订阅
- **PLAIN + bcrypt 认证** — ZAP 认证链：bcrypt 哈希凭据存储，admin token 保护监控接口
- **类型保真** — DataFrame / dict / str / bytes 端到端保真（`_restore_type` 自动还原）
- **高性能转发** — 路由结果缓存（12x）、topic interning、无锁统计、零拷贝广播、DONTWAIT 防阻塞
- **流控与丢弃监控** — 信用窗口流控 + DONTWAIT 非阻塞发送 + per-topic 丢弃统计（消费端 + 服务端）
- **完整监控** — Web UI（ECharts）+ REST API + SSE 实时推送，延迟趋势曲线、sparkline、丢弃指标
- **运行期重连** — 断线自动指数退避重连（1s → 2s → 4s ... → 30s 封顶）

---

## 安装

> Python >= 3.13

```bash
pip install pulse-mq
```

依赖项：ZeroMQ、msgspec、python-snappy、lz4、zstandard、pyarrow、pandas、bcrypt、loguru。

```python
import pulsemq            # Python 模块名（无连字符）
from pulsemq import Server
```

> PyPI 分发名是 `pulse-mq`（`pip install` 用），import 名是 `pulsemq`（因 Python 标识符不允许连字符）。与 `python-dateutil` → `import dateutil` 模式一致。

---

## 快速开始

### 启动服务端

最简单的方式是直接用 CLI（零配置，首次启动自动生成默认 `admin` 用户，密码输出到 stderr）：

```bash
pulsemq          # 或： pulsemq-server
```

也可以在代码中启动（注意 `start()` 是协程，需要 `asyncio.run` 包裹）：

```python
import asyncio
from pulsemq import Server

async def main():
    srv = Server(
        data_endpoint="tcp://0.0.0.0:5555",
        control_endpoint="tcp://0.0.0.0:5556",
        admin_endpoint="0.0.0.0:9090",
        credentials={"user1": "pass1"},  # 或省略，用 credentials_file（默认 ./data/pulsemq_users.toml）
    )
    await srv.start()
    await srv.wait_for_shutdown()  # Ctrl+C / srv.stop() 后返回

asyncio.run(main())
```

用户管理是独立的 CLI（`pulsemq-users`，不连 Server，直接读写凭据文件）：

```bash
pulsemq-users add user1 --password pass1 --roles publisher,subscriber
pulsemq-users list
```

### 服务端内置定时推送

无需外部生产者客户端，直接在 Server 上注册定时回调：

```python
import asyncio
from pulsemq import Server

async def main():
    srv = Server(credentials={"u": "p"})

    @srv.producer("market.tick", interval=2.0, serializer="msgpack")
    async def gen_tick():
        return {"symbol": "AAPL", "price": 180.5, "volume": 1000}

    @srv.producer("market.quote", interval=0.5, serializer="pyarrow", compression="lz4")
    async def gen_quote():
        import pandas as pd
        return pd.DataFrame({"price": [10, 20], "vol": [100, 200]})

    # burst_producer：无间隔连续推送，回调返回 None 即停止本 producer
    seq = 0
    @srv.burst_producer("bench", serializer="msgpack")
    async def bench():
        nonlocal seq
        if seq >= 1000:   # 发满 1000 条后停止
            return None
        seq += 1
        return {"seq": seq}

    await srv.start()       # 注册的 producer 自动开始调度
    await srv.wait_for_shutdown()

asyncio.run(main())
```

| 方法 | 参数 | 说明 |
|------|------|------|
| `srv.producer(topic, interval, serializer, compression)` | `interval` 秒 | 固定间隔定时推送 |
| `srv.burst_producer(topic, serializer, compression)` | 无间隔 | 连续推送，回调返回 `None` 停止 |

回调返回值支持 DataFrame / dict / str / bytes，自动编码为协议帧并路由到所有匹配的消费者。

### 生产者

先创建用户（一次性），再运行下面任一示例（Server 必须已在运行）：

```bash
pulsemq-users add publisher --password pass1 --roles publisher
pulsemq-users add subscriber --password pass2 --roles subscriber
```

```python
import asyncio
from pulsemq.client import ProducerClient

async def main():
    prod = ProducerClient(
        "tcp://127.0.0.1:5555", "tcp://127.0.0.1:5556",
        username="publisher", password="pass1",
    )
    await prod.start()
    await prod.publish("market.stock.AAPL", {"price": 180.5, "volume": 1000})
    await prod.stop()

asyncio.run(main())
```

### 消费者

```python
import asyncio
from pulsemq.client import ConsumerClient

async def main():
    cons = ConsumerClient(
        "tcp://127.0.0.1:5555", "tcp://127.0.0.1:5556",
        username="subscriber", password="pass2",
    )
    await cons.start()

    def on_msg(msg):
        print(msg.topic, msg.payload, msg.timestamp_ns)

    await cons.subscribe("market.*", on_msg)  # 可在 start 前预注册（A3）
    await cons.run_forever()  # 运行直到 Ctrl+C，重连致命错误自动抛出

asyncio.run(main())
```

### header_only 模式（跳过完整解码）

仅需 topic / record_count / timestamp 时，跳过反序列化以降低延迟：

```python
await cons.subscribe("market.*", on_header, header_only=True)
# on_header 收到 FrameHeader 而非 PulseMessage
```

### 消费端解码队列（opt-in 两线程）

默认单线程（最高吞吐）。对于慢回调场景，启用 worker 线程 + 有界丢弃队列：

```python
cons = ConsumerClient(
    "tcp://127.0.0.1:5555", "tcp://127.0.0.1:5556",
    username="sub", password="p",
    decode_queue_size=10000,  # 0=单线程（默认），>0=启用 worker 线程 + 丢弃队列
)
```

启用后：
- **recv 线程**仅做 header 解码 + 入队，不阻塞
- **worker 线程**批量出队 → 完整 decode → 回调
- 队列满时**丢弃最老消息**，按 topic 统计丢弃量
- 丢弃量 + 剩余容量通过心跳上报服务端，在 Web UI 可见

> **何时启用**：回调处理耗时 > 1ms（DB 写入、HTTP 调用、复杂计算）时建议启用。快回调（计数、转发）保持默认（单线程）以获得最高吞吐。

---

## 架构

```
                      ┌──────────────────────────────┐
                      │           Server              │
                      │  ┌─ 数据面（独立线程）───────┐│──── DONTWAIT ──→ 消费者 DEALER
   生产者 DEALER ────→│  │ ROUTER :5555              ││     ↑ 信用流控
                      │  │ 批量 drain → 路由缓存匹配  ││     ↑ 满队列跳过+丢弃计数
                      │  │ → 零拷贝广播              ││
                      │  └───────────────────────────┘│
                      │  ┌─ 控制面（asyncio）───────┐│
                      │  │ ROUTER :5556              ││←── DEALER ← 生产者/消费者
                      │  │ REGISTER/HEARTBEAT/       ││
                      │  │ SUBSCRIBE/DISCONNECT      ││
                      │  │ +drops +credit 心跳扩展   ││
                      │  └───────────────────────────┘│
                      │  ┌─ Admin（独立线程）────────┐│
                      │  │ HTTP :9090                ││─── REST / SSE / Web UI
                      │  └───────────────────────────┘│
                      │  ZAP PLAIN (bcrypt)           │
                      └──────────────────────────────┘
```

| 端口 | 协议 | 用途 |
|------|------|------|
| `5555` | ROUTER (数据面) | 消息发布/接收 |
| `5556` | ROUTER (控制面) | REGISTER / HEARTBEAT / SUBSCRIBE / DISCONNECT |
| `9090` | HTTP | 监控 Web UI + REST API + SSE |

### 数据流

```
生产者 DEALER ──encode→  ROUTER  decode_header  match topic ──DONTWAIT──→ 消费者 DEALER
                              ↓                                        ↓
                         TrafficStats.record                    frames.decode
                         LatencyStats.sample                     → callback
                         (无锁，topic intern)
```

服务端**不解压/不反序列化** payload（`decode_header` 仅提取头部），转发后由消费者完整 `decode` 还原。

### 控制面

| 命令 | 方向 | 作用 |
|------|------|------|
| `REGISTER` | Client → Server | 注册上线（含用户名、角色、订阅列表） |
| `HEARTBEAT` | Client → Server | 保活 + 消费端丢弃量 + 剩余信用（每秒 1 次，6 秒超时自动下线） |
| `SUBSCRIBE` | Client → Server | 订阅 topic 模式 |
| `UNSUBSCRIBE` | Client → Server | 取消订阅 |
| `DISCONNECT` | Client → Server | 优雅下线 |

### 流控与丢弃追踪

三层背压机制，互不阻塞：

| 层级 | 机制 | 行为 |
|------|------|------|
| **服务端→消费者** | DONTWAIT 非阻塞发送 | 消费者 HWM 满 → 跳过发送 → DropStats 按 topic 计数 |
| **服务端→消费者** | 信用窗口（心跳报告剩余容量） | 消费者 decode queue 满 → 信用=0 → 服务端跳过 |
| **消费端（opt-in）** | 有界丢弃队列 | worker 线程处理慢 → 队列满 → 丢弃最老消息 → 按 topic 计数 |

所有丢弃量统一汇聚到服务端 `DropStats`，在 Web UI 按 topic 展示（当前分钟 / 上一分钟 / 1 小时累计）。

---

## 数据类型与序列化

### 支持的数据类型

| Python 类型 | 可用序列化器 | record_count |
|-------------|-------------|-------------|
| `pd.DataFrame` | msgpack, json, **pyarrow** | 行数 |
| `dict` | msgpack, json | 1 |
| `str` | **str**（仅此一种） | 1 |
| `bytes` | **bytes**（仅此一种） | 1 |

> pyarrow 对 DataFrame 可直接序列化；json/msgpack 下 DataFrame 先转 `list[dict]`
> 再以 `data_type=DATAFRAME` 标记，接收端 `_restore_type` 还原回 DataFrame。

### 序列化格式

| 格式 | 后端 | 适合 | 批处理场景 |
|------|------|------|-----------|
| `msgpack` | msgspec | 结构化小消息 ✅ | 批量 DataFrame 需先 to_dict |
| `json` | msgspec | 人类可读、跨语言 | 同上 |
| `pyarrow` | pyarrow IPC | 列存/分析 ✅ | **直接序列化 DataFrame，最快** |
| `str` | UTF-8 | 纯文本 | ❌ |
| `bytes` | 透传 | 二进制 | ❌ |

### 压缩算法

| 算法 | 适用场景 |
|------|---------|
| `none` | 小消息，极速 |
| `snappy` | 速度优先 |
| `lz4` | 批数据，平衡 |
| `zstd` | 压缩比优先（带宽受限），context 复用提速 |
| `auto` | 自适应：<256B 用 none，>=256B 用 lz4 |

小消息场景压缩是**负收益**（计算开销 > 传输节省）；批量 DataFrame 场景 `lz4`/`zstd` 有明显效果。

---

## 客户端生命周期

### 首次启动

| 场景 | 异常 | exit code |
|------|------|-----------|
| 密码错误 | `AuthenticationError` | 3 |
| 服务器不可达 | `ClientStartupError` | 4 |

### 运行期重连

断线后 `Client._reconnect_loop` 按指数退避自动重连：

```
断线 → disconnected → cancel bg tasks → 新 Transport → PLAIN 认证
  → REGISTER（同 client_id）
    ├─ ALREADY_ONLINE → 退避重试（等待心跳超时释放）
    ├─ auth_failed → _reconnect_fatal → exit 3
    └─ OK → 恢复订阅 → 重启 recv/heartbeat
```

**业务无感**：订阅自动恢复，消息继续接收。

---

## 监控

### Web UI

浏览器打开 `http://localhost:9090/?token=<admin_token>` 查看实时面板：

- 4 个指标卡片（含 sparkline 迷你趋势线）：活跃主题、消息量/秒、流量/秒、运行时间
- 4 个客户端卡片：在线用户、生产者数、消费者数、订阅数
- ECharts 流量趋势折线图（分钟级，1H/8H 切换，最多 5 topic 叠加）
- **延迟趋势曲线**（P50/P95/P99 时间序列，半程/全程切换）
- 延迟对比柱状图（按 topic 半程/全程 P50）+ 底部端到端延迟列表（P50/P95/P99）
- 实时事件流（认证/连接/断线）
- **topic 卡片丢弃指示**（红色高亮，显示当前分钟 + 1 小时累计丢弃量）
- 在线 Client 详情弹窗

### REST API

```bash
# 实时指标（含 drops / latency / topics）
curl 'http://localhost:9090/api/v1/stats/realtime?token=<token>'

# 主题列表
curl 'http://localhost:9090/api/v1/topics?token=<token>'

# 主题分钟级历史
curl 'http://localhost:9090/api/v1/topics/market.tick/history?minutes=60&token=<token>'

# 延迟历史（半程/全程 P50/P95/P99 时间序列）
curl 'http://localhost:9090/api/v1/latency/topics/market.tick/history?minutes=60&kind=half&token=<token>'

# 在线客户端明细
curl 'http://localhost:9090/api/v1/clients?token=<token>'

# 生命周期事件
curl 'http://localhost:9090/api/v1/events?token=<token>'

# 健康检查（无需 token）
curl http://localhost:9090/healthz
```

### SSE 实时流

```bash
curl -N 'http://localhost:9090/api/v1/stats/stream?token=<token>'
```

每 1 秒推送一帧 JSON，包含 topics / latency_half / latency_e2e / drops / online_users / sse_events 等。

---

## 协议帧格式

**单 bytes 帧**（非 ZMQ 多帧，通过 DEALER/ROUTER 传输）：

```
magic(2) ver(1) msg_type(1) flags(1) data_type(1) topic_len(2 BE)
topic(N) ts(8 BE ns) record_count(4 BE) payload(变长) [CRC32?(4)]
```

- `magic` = `"PM"`
- `msg_type` = DATA(0x01) / CONTROL(0x02)
- `flags` = 编码序列化器(3bit) + 压缩算法(2bit) + CRC(1bit)
- `data_type` = UNKNOWN(0x00) / DICT(0x01) / DATAFRAME(0x02) / STR(0x03) / BYTES(0x04)
- `record_count` 上限 **1,000,000**
- CRC 可选（由 flags 指示）

---

## 凭据管理

### CLI

用户管理是独立入口 `pulsemq-users`（不连 Server，直接读写凭据文件，密码自动 bcrypt 哈希）：

```bash
# 添加用户（密码自动 bcrypt 哈希；--password 可省略，改为交互输入）
pulsemq-users add trader1 --password secret123 --roles publisher,subscriber

# 列出所有用户
pulsemq-users list

# 禁用 / 启用用户
pulsemq-users disable trader1
pulsemq-users enable  trader1

# 修改密码
pulsemq-users passwd trader1 --password new_secret

# 热加载凭据（向 Server 进程发 SIGHUP；需 PULSEMQ_PID 环境变量，仅 POSIX）
pulsemq-users reload
```

启动服务端用另一个入口 `pulsemq`（等价于 `pulsemq-server`）：

```bash
pulsemq            # 启动 Server，Ctrl+C 优雅关闭
```

> 也可以指定凭据文件：把 `--file <path>` 放在子命令**之后**（如 `pulsemq-users list --file /etc/pulsemq/users.toml`）。

### 文件格式 (`pulsemq_users.toml`)

```toml
[users.admin]
hashed_password = "$2b$12$..."
roles = ["admin"]
enabled = true
created_at = "2026-06-27T00:00:00Z"
```

### Admin Token

首次启动自动生成 32 字节随机 base64url token，写入 `./data/pulsemq_admin.token`（0600 权限）。
Web UI 和 REST API 通过 `?token=...` 或 `Authorization: Bearer ...` 传递。

可通过环境变量 `PULSEMQ_ADMIN_TOKEN` 或配置文件覆盖。

---

## 日志

日志输出到 `data/logs/` 目录，每日滚动，保留 30 天：

```
data/logs/
├── pulsemq_2026-06-27.log
├── pulsemq_2026-06-28.log
└── ...
```

stderr 同步输出（容器/交互可见）。

---

## 性能优化（v9.0.0）

### 服务端热路径优化

| 优化项 | 效果 |
|--------|------|
| 路由 match() 结果缓存 | 同一 topic 跳过 split/join 分配，**12x 提速**（0.79µs → 0.07µs/call） |
| topic 字节→str interning | 消除每消息 UTF-8 decode 分配 |
| TrafficStats 无锁 record | 数据面单写者，常规路径不加锁（仅新 topic/分钟切换加锁） |
| 延迟采样计数器替代 RNG | 消除每消息 random.random() 调用 |
| 零拷贝广播 broadcast() | 大 payload(≥1KB) 用 zmq.Frame + copy=False，避免 N 次内存拷贝 |
| DONTWAIT 非阻塞发送 | 慢消费者不阻塞数据面线程（防 head-of-line blocking） |
| 批量 drain | 一次 poll 唤醒后连续取完所有消息，摊薄 poll 开销 |
| FrameHeader slots | 消除 __dict__ 分配 |
| Zstd 压缩 context 复用 | 每线程独立 context，避免重复初始化（dict/zstd 吞吐 +44%） |

### 流控与丢弃监控

- **信用流控**：消费者心跳报告剩余 decode queue 容量，服务端据此跳过即将被丢弃的发送
- **DONTWAIT**：订阅者 HWM 满时立即跳过，按 topic 计入 DropStats
- **DropStats**：分钟桶 + 1 小时窗口，提供 drops_current / drops_last_min / drops_1h_total

### 性能基准

仓库自带跨机器基准脚本：

```bash
# 跨机器基准（Server 在远程，Producer/Consumer 在本地）
python scripts/bench_dist.py --remote <host> --ssh root@<host>

# 单进程快速基准
python scripts/bench_simple.py --duration 5

# 多进程基准（28 组合全覆盖）
python scripts/bench_multiprocess.py
```

---

## 配置

### 环境变量

| 变量 | 说明 | 默认 |
|------|------|------|
| `PULSEMQ_DATA_ENDPOINT` | 数据面绑定地址 | `tcp://0.0.0.0:5555` |
| `PULSEMQ_CONTROL_ENDPOINT` | 控制面绑定地址 | `tcp://0.0.0.0:5556` |
| `PULSEMQ_ADMIN_BIND` | 管理 HTTP 绑定地址 | `0.0.0.0:9090` |
| `PULSEMQ_CREDENTIALS_FILE` | 凭据 TOML 路径 | `./data/pulsemq_users.toml` |
| `PULSEMQ_ADMIN_TOKEN` | 监控接口 token（覆盖随机生成） | 自动生成 |
| `PULSEMQ_SNDHWM` | ZMQ 发送高水位（帧数） | `10000` |
| `PULSEMQ_RCVHWM` | ZMQ 接收高水位（帧数） | `10000` |
| `PULSEMQ_HEARTBEAT_TIMEOUT` | 心跳超时（秒） | `6.0` |
| `PULSEMQ_LATENCY_SAMPLE_RATE` | 延迟采样率（0-1） | `0.01` |
| `PULSEMQ_RETENTION_DAYS` | SQLite 统计保留天数 | `7` |
| `PULSEMQ_BCRYPT_COST` | bcrypt 代价因子 | `12` |
| `PULSEMQ_SSE_INTERVAL` | SSE 推送间隔（秒） | `1.0` |
| `PULSEMQ_STATS_RETENTION_MINUTES` | 内存统计窗口（分钟） | `480` |

### 配置文件（TOML）

更多参数（`stats_db`、`heartbeat_timeout`、`latency_sample_rate`、`stats_retention_minutes`、`bcrypt_cost`、`admin_token_file`、`sse_interval`、`event_ring_size` 等）需通过 TOML 配置文件设置。在 `Server(config=...)` 中传入自定义 `ServerConfig` 即可生效。完整字段见 [`ServerConfig`](src/pulsemq/config.py)。

### 消费端配置

| 参数 | 说明 | 默认 |
|------|------|------|
| `decode_queue_size` | 解码队列长度（0=单线程，>0=worker线程+丢弃队列） | `0` |
| `latency_sample_rate` | 端到端延迟采样回传率 | `0.01` |
| `sndhwm` / `rcvhwm` | ZMQ 高水位 | `10000` |

---

## 更新日志

### v9.0.0

- **性能优化** — 路由 match() 结果缓存（12x）；topic interning；TrafficStats 无锁 record；计数器采样替代 RNG；FrameHeader slots；Zstd context 复用（+44%）；批量 drain
- **零拷贝广播** — 大 payload(≥1KB) zmq.Frame + copy=False；小 payload 直接 send
- **DONTWAIT 防阻塞** — 慢消费者 HWM 满时跳过发送，防 head-of-line blocking
- **信用流控** — 消费者心跳报告剩余 decode queue 容量，服务端据此跳过
- **丢弃监控** — DropStats 分钟桶+1h 窗口；消费端 _DropQueue（opt-in）丢弃最老消息+per-topic 计数；服务端 DONTWAIT 丢弃计数；Web UI topic 卡片丢弃指示
- **消费端两线程（opt-in）** — `decode_queue_size>0` 启用 recv→queue→worker 模式；默认单线程（最高吞吐）
- **监控增强** — 延迟趋势曲线（P50/P95/P99 时间序列，半程/全程切换）；msg/s + bytes/s sparkline
- **Bug 修复** — ZstdCompressor 线程安全（thread-local context）；topic intern 缓存有界（max 10000）

### v8.0.0

- **延迟监控** - 按 topic 的半程(producer->server)+全程(producer->consumer)延迟，分钟窗口+8h 历史，consumer 采样回传，Web UI 对比图+底部列表
- **客户端生命周期** - `Client.run_forever()` 上提到基类；subscribe 支持 start 前预注册；SIGINT/SIGTERM 优雅退出
- **性能优化** - `SubscriptionTable` COW 无锁读；客户端订阅匹配改前缀索引；序列化器 import 提模块级
- **控制面** - reply 关联 request_id，解决多订阅 ack 串扰
- **配置** - ClientConfig 接入 Client；HWM 默认 10000；ServerConfig 常用字段支持环境变量覆盖
- **清理** - 删除 cache/、inject_sender、MsgType.HEARTBEAT/ADMIN、ControlCmd.KICK、save_minute 等死代码
- **Web UI** - 延迟对比图+底部列表；流量趋势图 6H->8H

### v7.2.x

- **同步数据面线程** - SyncDataThread 独立线程 + 独立 ctx，端到端 p50 < 1ms
- **压缩算法自适应** - `compression="auto"` 根据 payload 大小自动选择 none/lz4
- **Consumer decode_header 快速过滤** - 跳过不匹配 topic 的完整 decode
- **ZMQ HWM 可配置化** - `sndhwm`/`rcvhwm` 配置项 + 环境变量
- **encode 自动推断** - serializer/data_type 自动推断
- **多进程基准测试脚本** - 28 组合全覆盖

### v2

完整重构：PUB/SUB → Client/Server (ROUTER/DEALER)

---

## 许可证

[MIT](LICENSE)
