# PulseMQ

面向金融行情与实时数据流的高性能消息中间件，基于 ZeroMQ ROUTER/DEALER 架构。

- **Client/Server 架构** — 服务端双 ROUTER（数据面 + 控制面）集中路由；客户端 DEALER 发布/订阅
- **PLAIN + bcrypt 认证** — ZAP 认证链：bcrypt 哈希凭据存储，随机 admin token 保护监控接口
- **类型保真** — DataFrame / dict / str / bytes 端到端保真（`_restore_type` 自动还原原始 Python 类型）
- **低延迟数据面** — 独立同步线程转发，端到端 p50 可低至亚毫秒级
- **完整监控** — Web UI（ECharts）+ REST API + SSE 实时推送，流量趋势、延迟分位（P50/P95/P99）、丢弃统计、在线客户端、事件流
- **流控与丢弃监控** — 信用窗口流控 + DONTWAIT 非阻塞发送，消费端有界队列满时按 topic 统计丢弃
- **运行期重连** — 断线自动指数退避重连（1s → 2s → 4s … → 30s 封顶），订阅自动恢复

---

## 安装

> Python >= 3.13

```bash
pip install pulse-mq
```

依赖项：pyzmq、msgspec、python-snappy、lz4、zstandard、pyarrow、pandas、bcrypt、loguru。

```python
import pulsemq            # Python 模块名（无连字符）
from pulsemq import Server
```

> PyPI 分发名是 `pulse-mq`（`pip install` 用），import 名是 `pulsemq`（Python 标识符不允许连字符）。与 `python-dateutil` → `import dateutil` 同一模式。

---

## 快速开始

### 启动服务端

最简单的方式是直接用 CLI（零配置，首次启动自动生成默认 `admin` 用户，密码输出到 stderr）：

```bash
pulsemq          # 或： pulsemq-server
```

也可以在代码中启动（`start()` 是协程，需要 `asyncio.run` 包裹）：

```python
import asyncio
from pulsemq import Server

async def main():
    srv = Server(
        data_endpoint="tcp://0.0.0.0:5555",
        control_endpoint="tcp://0.0.0.0:5556",
        admin_endpoint="0.0.0.0:9090",
        credentials={"user1": "pass1"},  # 或省略，用 credentials_file
    )
    await srv.start()
    await srv.wait_for_shutdown()  # Ctrl+C / srv.stop() 后返回

asyncio.run(main())
```

`Server` 构造参数：

| 参数 | 说明 |
|------|------|
| `data_endpoint` | 数据面绑定地址（默认 `tcp://0.0.0.0:5555`） |
| `control_endpoint` | 控制面绑定地址（默认 `tcp://0.0.0.0:5556`） |
| `admin_endpoint` | 管理 HTTP 绑定地址（默认 `0.0.0.0:9090`） |
| `credentials` | 显式明文 dict（内存态，哈希落值） |
| `credentials_file` | 凭据 TOML 路径（默认 `./data/pulsemq_users.toml`） |
| `config` | 自定义 `ServerConfig`（覆盖各项默认） |
| `admin_token` | 监控 token；传 `""` 禁用校验 |
| `latency_sample_rate` | 延迟采样率（0-1） |

用户管理是独立 CLI（`pulsemq-users`，不连 Server，直接读写凭据文件）：

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

先创建用户（一次性），再运行示例（Server 必须已在运行）：

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

    await cons.subscribe("market.*", on_msg)  # 可在 start 前预注册
    await cons.run_forever()  # 运行直到 Ctrl+C，重连致命错误自动抛出

asyncio.run(main())
```

`subscribe` 支持 `header_only=True`——回调只接收 `FrameHeader`（topic / record_count / timestamp_ns），跳过完整反序列化，适合只需头部信息的低延迟场景：

```python
await cons.subscribe("market.*", on_header, header_only=True)
```

`ProducerClient` / `ConsumerClient` 分别屏蔽订阅 / 发布能力；通用 `Client` 同时支持两者。

---

## 架构速览

```
                      ┌──────────────────────────┐
                      │         Server           │
   生产者 DEALER ────→ │  ┌─ 数据面 ────────────┐ │ ────→ 消费者 DEALER
                      │  │ ROUTER :5555 (同步线程)│ │
                      │  └──────────────────────┘ │
                      │  ┌─ 控制面 ────────────┐ │ ←──── DEALER（REGISTER/HEARTBEAT/…）
                      │  │ ROUTER :5556 (异步) │ │
                      │  └──────────────────────┘ │
                      │  ┌─ Admin ─────────────┐ │ ──── REST / SSE / Web UI
                      │  │ HTTP :9090 (独立线程)│ │
                      │  └──────────────────────┘ │
                      │     ZAP PLAIN (bcrypt)    │
                      └──────────────────────────┘
```

| 端口 | 协议 | 用途 |
|------|------|------|
| `5555` | ROUTER（数据面） | 消息发布/接收 |
| `5556` | ROUTER（控制面） | REGISTER / HEARTBEAT / SUBSCRIBE / UNSUBSCRIBE / DISCONNECT / LATENCY_REPORT |
| `9090` | HTTP | 监控 Web UI + REST API + SSE |

### 数据流

```
生产者 DEALER ──encode→  数据面 ROUTER  decode_header  match topic ──→ 消费者 DEALER
                              ↓
                    TrafficStats.record  LatencyStats.sample（半程）
```

服务端**不解压、不反序列化** payload（`decode_header` 仅提取头部），转发后由消费者完整 `decode` 还原。

### 控制面命令

| 命令 | 方向 | 作用 |
|------|------|------|
| `REGISTER` | Client → Server | 注册上线（含用户名、角色、订阅列表），返回 OK / ALREADY_ONLINE |
| `HEARTBEAT` | Client → Server | 保活（每秒 1 次，6 秒超时自动下线）；携带消费端丢弃量与剩余信用 |
| `SUBSCRIBE` | Client → Server | 订阅 topic 模式 |
| `UNSUBSCRIBE` | Client → Server | 取消订阅 |
| `DISCONNECT` | Client → Server | 优雅下线 |
| `LATENCY_REPORT` | Client → Server | consumer 回传端到端延迟（采样），fire-and-forget |

详细设计见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)。

---

## 数据类型与序列化

### 支持的数据类型

| Python 类型 | 可用序列化器 | 默认 | record_count |
|-------------|-------------|------|-------------|
| `pd.DataFrame` | msgpack, json, **pyarrow** | pyarrow | 行数 |
| `dict` | msgpack, json | msgpack | 1 |
| `str` | **str**（仅此一种） | str | 1 |
| `bytes` | **bytes**（仅此一种） | bytes | 1 |

> `encode` 会自动推断 `data_type` 并选择默认序列化器，无需手动指定。DataFrame 在 msgpack/json 下先转 `list[dict]`（`data_type=DATAFRAME`），接收端 `_restore_type` 还原回 DataFrame；pyarrow 直接序列化 IPC 流。

### 序列化格式

| 格式 | 后端 | 适合 |
|------|------|------|
| `msgpack` | msgspec | 结构化小消息 ✅ |
| `json` | msgspec | 人类可读、跨语言 |
| `pyarrow` | pyarrow IPC | 列存 / DataFrame ✅（最快） |
| `str` | UTF-8 | 纯文本 |
| `bytes` | 透传 | 二进制 |

### 压缩算法

| 算法 | 适用场景 |
|------|---------|
| `none` | 小消息，极速 |
| `snappy` | 速度优先 |
| `lz4` | 批数据，平衡 |
| `zstd` | 压缩比优先（带宽受限） |
| `auto` | 自适应：< 256B 用 none，≥ 256B 用 lz4 |

小消息场景压缩通常是**负收益**（计算开销 > 传输节省）；批量 DataFrame 场景 `lz4`/`zstd` 有明显效果。

---

## 客户端生命周期

### 首次启动

启动认证检测采用 ZMQ monitor 设计：

```
start()
  ├─ 数据面 DEALER + PLAIN + monitor → 等待认证裁定（超时 5s）
  │   ├─ handshake_ok → 继续
  │   ├─ auth_failed → AuthenticationError（exit 3）
  │   └─ 超时 / 其他 → ClientStartupError（exit 4）
  ├─ 控制面 DEALER（复用数据面认证态，不开 monitor）
  ├─ REGISTER → OK / 超时（exit 4） / 被拒（exit 4）
  ├─ 恢复既有订阅（重连场景）
  ├─ recv_loop + heartbeat_loop（每秒）
  └─ 切换到运行期 monitor（接管断线重连）
```

| 场景 | 异常 | exit code |
|------|------|-----------|
| 密码错误 | `AuthenticationError` | 3 |
| 服务器不可达 / 握手超时 | `ClientStartupError` | 4 |
| REGISTER 被拒 / 超时 | `ClientStartupError` | 4 |

### 运行期重连

断线后按指数退避自动重连（初始 1s，×2，封顶 30s）：

```
disconnected → cancel bg tasks → 新 Transport → PLAIN 认证 → REGISTER（同 client_id）
  ├─ ALREADY_ONLINE → 退避重试（等心跳超时释放旧记录）
  ├─ auth_failed → 致命错误，exit 3
  └─ OK → 恢复订阅 → 重启 recv/heartbeat
```

**业务无感**：订阅自动恢复，消息继续接收，业务层无需重新 `subscribe()`。

### 两线程消费模型（可选）

默认单线程：recv 线程同时负责解码与回调。当解码成为瓶颈时，传入 `decode_queue_size > 0` 启用两线程模式：

```
recv 线程：header 解码 + 延迟采样 + 路由匹配 → 入队 _DropQueue
worker 线程：批量出队 → 完整 decode + 回调分发
```

队列满时丢弃最老消息并按 topic 计数，通过心跳上报给服务端，在 Web UI 的 topic 卡片上可见。

---

## 监控

### Web UI

浏览器打开 `http://localhost:9090/?token=<admin_token>` 查看实时面板：

- 指标卡片：活跃主题、消息量/秒、流量/秒、运行时间
- 客户端卡片：在线用户、生产者数、消费者数、订阅数
- ECharts 流量趋势折线图（分钟级，1H/8H 切换，最多 5 topic 叠加）+ msg/s、bytes/s sparkline
- 延迟趋势曲线（P50/P95/P99 时间序列，半程/全程可切换）+ 端到端延迟列表
- 实时事件流（认证 / 连接 / 断线 / 订阅）
- topic 卡片含丢弃指示；在线 Client 详情

### REST API

```bash
# 实时指标（topics / latency / drops / 在线计数 / 最近事件）
curl 'http://localhost:9090/api/v1/stats/realtime?token=<token>'

# 主题列表
curl 'http://localhost:9090/api/v1/topics?token=<token>'

# 主题分钟级历史（内存 + SQLite 合并）
curl 'http://localhost:9090/api/v1/topics/market.tick/history?minutes=60&token=<token>'

# 延迟历史（kind=half 半程 / e2e 全程）
curl 'http://localhost:9090/api/v1/latency/topics/market.tick/history?minutes=60&kind=e2e&token=<token>'

# 在线客户端明细
curl 'http://localhost:9090/api/v1/clients?token=<token>'

# 生命周期事件
curl 'http://localhost:9090/api/v1/events?limit=50&token=<token>'

# 系统状态
curl 'http://localhost:9090/api/v1/system/status?token=<token>'

# 健康检查（无需 token）
curl http://localhost:9090/healthz
```

token 通过 `?token=...` 或 `Authorization: Bearer ...` 传递。

### SSE 实时流

```bash
curl -N 'http://localhost:9090/api/v1/stats/stream?token=<token>'
```

每 1 秒推送一帧 JSON，包含 topics / latency_half / latency_e2e / drops / online_users / sse_events / server_time / start_time 等。

---

## 协议帧格式

**单 bytes 帧**（非 ZMQ 多帧，通过 DEALER/ROUTER 传输）：

```
magic(2) ver(1) msg_type(1) flags(1) data_type(1) topic_len(2 BE)
topic(N) ts(8 BE ns) record_count(4 BE) payload(变长) [CRC32?(4)]
```

- `magic` = `"PM"`，`ver` = `0x01`
- `msg_type` = DATA(`0x01`) / CONTROL(`0x02`)
- `flags` 位域：序列化器（bit 0-2）+ 压缩算法（bit 3-4）+ CRC（bit 7）+ reserved（bit 5-6）
- `data_type` = UNKNOWN(`0x00`) / DICT(`0x01`) / DATAFRAME(`0x02`) / STR(`0x03`) / BYTES(`0x04`)
- `record_count` 上限 **1,000,000**
- CRC 可选（由 flags bit 7 指示，默认关闭）

位级布局与 flags 编码细节见 [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#5-协议模块-protocol)。

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

> 指定凭据文件：把 `--file <path>` 放在子命令前后均可（如 `pulsemq-users list --file /etc/pulsemq/users.toml`）。

### 文件格式（`pulsemq_users.toml`）

```toml
[users.admin]
hashed_password = "$2b$12$..."
roles = ["admin"]
enabled = true
created_at = "2026-06-27T00:00:00Z"
```

文件采用原子写（临时文件 + rename），用户名/角色做准入校验（仅字母数字 `_` `-`），阻断危险字符流入 TOML 防注入。

### Admin Token

优先级：显式 `admin_token=` 参数 > 配置文件 > 环境变量 `PULSEMQ_ADMIN_TOKEN` > 随机生成。

首次启动自动生成 32 字节随机 base64url token，写入 `./data/pulsemq_admin.token`（POSIX 下 0600 权限），并在 stderr 输出一次。Web UI 和 REST API 通过 `?token=...` 或 `Authorization: Bearer ...` 传递。

---

## 日志

日志输出到 `data/logs/` 目录，每日滚动，保留 30 天：

```
data/logs/
├── pulsemq_2026-08-01.log
├── pulsemq_2026-08-02.log
└── ...
```

stderr 同步输出（容器/交互可见）。

---

## 性能基准

仓库自带多套基准脚本：

```bash
# 1. 单进程快速基准（Server + Producer + Consumer 同进程）
python scripts/bench_simple.py --duration 5
python scripts/bench_simple.py --duration 5 --records-per-frame 1000 --serializer pyarrow --compression lz4
#   可选：--serializer {msgpack,json,pyarrow,str,bytes}
#         --compression {none,snappy,lz4,zstd}

# 2. 全面基准（协议层微基准 + 端到端矩阵 + 扇出，单进程）
python scripts/bench_full.py                  # 跑全部
python scripts/bench_full.py --duration 10 --part 2   # 只跑 Part 2（端到端矩阵）

# 3. 多进程基准（生产端/服务端/消费端独立进程，28 组合全覆盖）
python scripts/bench_multiprocess.py          # 跑全部 28 组合
python scripts/bench_multiprocess.py --count 3000 --data-type dict

# 4. 跨机器基准（远程 Linux 跑 Server，本地跑 Producer/Consumer）
python scripts/bench_dist.py --remote <ip> --ssh root@<ip>
python scripts/bench_dist.py --part a         # 只跑 Part A
```

脚本输出帧/记录吞吐量与 p50/p90/p99/max 帧延迟。

**性能特征**（可从实现推断，实际数值请自行运行脚本获取）：

- **DataFrame + msgpack/json**：encode 限速、无积压，端到端延迟最低（p50 常在亚毫秒级）
- **小消息（dict/str/bytes）**：吞吐最高（可达 10 万+ f/s），但 burst 发送易致队列积压，p50 较高
- **pyarrow**：encode 快，但 consumer 端 `to_pandas()` 转换是延迟瓶颈
- **zstd**：对大 payload 压缩比最优，对 < 200B 小消息通常为负收益
- **数据面同步线程**：转发路径无 asyncio 调度，路由匹配有结果缓存

---

## 配置

### 环境变量

| 变量 | 说明 | 默认 |
|------|------|------|
| `PULSEMQ_DATA_ENDPOINT` | 数据面绑定地址 | `tcp://0.0.0.0:5555` |
| `PULSEMQ_CONTROL_ENDPOINT` | 控制面绑定地址 | `tcp://0.0.0.0:5556` |
| `PULSEMQ_ADMIN_BIND` | 管理 HTTP 绑定地址 | `0.0.0.0:9090` |
| `PULSEMQ_CREDENTIALS_FILE` | 凭据 TOML 路径 | `./data/pulsemq_users.toml` |
| `PULSEMQ_ADMIN_TOKEN` | 监控 token（覆盖随机生成） | 自动生成 |
| `PULSEMQ_ADMIN_PASSWORD` | 首次生成默认 admin 的密码 | 随机 |
| `PULSEMQ_SNDHWM` | ZMQ 发送高水位（帧数） | `10000` |
| `PULSEMQ_RCVHWM` | ZMQ 接收高水位（帧数） | `10000` |
| `PULSEMQ_HEARTBEAT_TIMEOUT` | 心跳超时（秒） | `6.0` |
| `PULSEMQ_LATENCY_SAMPLE_RATE` | 延迟采样率（0-1） | `0.01` |
| `PULSEMQ_RETENTION_DAYS` | SQLite 统计保留天数 | `7` |
| `PULSEMQ_BCRYPT_COST` | bcrypt 代价因子 | `12` |
| `PULSEMQ_SSE_INTERVAL` | SSE 推送间隔（秒） | `1.0` |
| `PULSEMQ_STATS_RETENTION_MINUTES` | 内存统计窗口（分钟） | `480` |
| `PULSEMQ_USERNAME` | 客户端用户名 | — |
| `PULSEMQ_PASSWORD` | 客户端密码 | — |
| `PULSEMQ_PID` | Server PID（供 `pulsemq-users reload` 发 SIGHUP） | — |

### 配置文件（TOML）

更多参数（`stats_db`、`event_ring_size`、`stats_archive_batch_size`、`admin_thread`、`ui_enabled`、`decode_queue_size` 等）通过 TOML 配置文件设置。在 `Server(config=...)` 传入自定义 `ServerConfig`，或调用 `load_server_config(path)` / `load_client_config(path)` 加载。完整字段见 [`ServerConfig` / `ClientConfig`](src/pulsemq/config.py)。

`ServerConfig` 默认值：零配置即可启动。

---

## 许可证

[MIT](LICENSE)
