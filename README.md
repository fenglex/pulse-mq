# PulseMQ

面向金融行情的高性能消息中间件，基于 ZeroMQ ROUTER/DEALER 架构。

- **Client/Server 架构** — 服务端（ROUTER）集中路由 + 控制面；客户端（DEALER）发布/订阅
- **PLAIN + bcrypt 认证** — ZAP 认证链：bcrypt 哈希凭据存储，admin token 保护监控接口
- **类型保真** — DataFrame / dict / str / bytes 端到端保真（`_restore_type` 自动还原）
- **完整监控** — Web UI（ECharts）+ REST API + SSE 实时推送，延迟分位、事件流、在线客户端
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

    await cons.subscribe("market.*", on_msg)
    await asyncio.sleep(3600)  # 持续接收

asyncio.run(main())
```

---

## 架构

```
                      ┌──────────────────┐
                      │    Server        │
                      │  ┌─ 数据面 ─────┐│──── DEALER → 消费者
                      │  │ ROUTER :5555 ││
                      │  └──────────────┘│
                      │  ┌─ 控制面 ─────┐│
                      │  │ ROUTER :5556 ││←── DEALER  ← 生产者
                      │  └──────────────┘│
                      │  ┌─ Admin ──────┐│
                      │  │ HTTP :9090   ││─── REST / SSE / Web UI
                      │  └──────────────┘│
                      │  ZAP (bcrypt)    │
                      └──────────────────┘
```

| 端口 | 协议 | 用途 |
|------|------|------|
| `5555` | ROUTER (数据面) | 消息发布/接收 |
| `5556` | ROUTER (控制面) | REGISTER / HEARTBEAT / SUBSCRIBE / DISCONNECT |
| `9090` | HTTP | 监控 Web UI + REST API + SSE |

### 数据流

```
生产者 DEALER ──encode→  ROUTER  decode_header  match topic ──→ 消费者 DEALER
                              ↓
                         TrafficStats.record  LatencyStats.sample
```

服务端**不解压/不反序列化** payload（`decode_header` 仅提取头部），转发后由消费者完整 `decode` 还原。

### 控制面

| 命令 | 方向 | 作用 |
|------|------|------|
| `REGISTER` | Client → Server | 注册上线（含用户名、角色、订阅列表） |
| `HEARTBEAT` | Client → Server | 保活（每秒 1 次，6 秒超时自动下线） |
| `SUBSCRIBE` | Client → Server | 订阅 topic 模式 |
| `UNSUBSCRIBE` | Client → Server | 取消订阅 |
| `DISCONNECT` | Client → Server | 优雅下线 |

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
| `zstd` | 压缩比优先（带宽受限） |
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

- 4 个指标卡片：活跃主题、消息量/秒、流量/秒、运行时间
- 4 个客户端卡片：在线用户、生产者数、消费者数、订阅数
- ECharts 流量趋势折线图（分钟级，1H/6H 切换，最多 5 topic 叠加）
- 延迟 P50/P95/P99 柱状图
- 实时事件流（认证/连接/断线）
- 在线 Client 详情弹窗

### REST API

```bash
# 实时指标
curl 'http://localhost:9090/api/v1/stats/realtime?token=<token>'

# 主题列表
curl 'http://localhost:9090/api/v1/topics?token=<token>'

# 主题分钟级历史
curl 'http://localhost:9090/api/v1/topics/market.tick/history?minutes=60&token=<token>'

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

每 1 秒推送一帧 JSON，包含 topics / latency / online_users / sse_events 等。

---

## 协议帧格式

**单 bytes 帧**（非 ZMQ 多帧，通过 DEALER/ROUTER 传输）：

```
magic(2) ver(1) msg_type(1) flags(1) data_type(1) topic_len(2 BE)
topic(N) ts(8 BE ns) record_count(4 BE) payload(变长) [CRC32?(4)]
```

- `magic` = `"PM"`
- `msg_type` = DATA(0x01) / CONTROL(0x02) / HEARTBEAT(0x03) / ADMIN(0x04)
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

## 性能基准

### 运行基准

仓库自带多套基准脚本：

```bash
# 1. 单进程快速基准（Server + Producer + Consumer 同进程）
python scripts/bench_simple.py --duration 5
python scripts/bench_simple.py --duration 5 --records-per-frame 1000 --serializer pyarrow --compression lz4
# 可选参数：--serializer {msgpack,json,pyarrow,str,bytes}
#           --compression {none,snappy,lz4,zstd}

# 2. 全面基准（协议层微基准 + 端到端矩阵 + 扇出，单进程）
python scripts/bench_full.py                  # 跑全部，结果写 bench_results.md
python scripts/bench_full.py --duration 10    # 端到端/扇出每场景秒数

# 3. 多进程基准（生产端/服务端/消费端独立进程，28 组合全覆盖）
python scripts/bench_multiprocess.py          # 跑全部 28 组合
python scripts/bench_multiprocess.py --count 3000     # 每组合发送帧数
python scripts/bench_multiprocess.py --data-type dict  # 只测指定类型
```

脚本输出帧/记录吞吐量与 p50/p90/p99/max 帧延迟。

### 参考数据

下列数值来自上述脚本在本机（Windows 11, Python 3.13, 单机 localhost）的一次运行，**仅作量级参考**，实际表现因机器、负载、序列化/压缩组合而异：

**单条消息（dict，每帧 1 条）**

| 序列化 | 压缩 | 量级 |
|--------|------|------|
| msgpack | none | ~1e4 frames/s |
| json | none | ~1e4 frames/s |

> 小消息场景 pyarrow 单帧开销过大（序列化 + schema），不推荐用于单条 dict。
> 压缩对 <200B payload 通常为负收益。

**批量 DataFrame（1000 行/帧）**

| 序列化 | 压缩 | 量级 |
|--------|------|------|
| pyarrow | lz4/zstd | ~1e6 records/s |
| pyarrow | none | 略高于带压缩（取决于数据可压缩性） |

> 批量场景 pyarrow 是**最优选择**（直接序列化 DataFrame，无需 dict 转换）。
> 帧延迟为压测下 ZMQ 缓冲区排队所致，真实场景以固定间隔发送时远低于此。

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
| `PULSEMQ_SNDHWM` | ZMQ 发送高水位（帧数） | `1000` |
| `PULSEMQ_RCVHWM` | ZMQ 接收高水位（帧数） | `1000` |

### 配置文件（TOML）

更多参数（`stats_db`、`heartbeat_timeout`、`latency_sample_rate`、`stats_retention_minutes`、`bcrypt_cost`、`admin_token_file`、`sse_interval`、`event_ring_size` 等）需通过 TOML 配置文件设置。在 `Server(config=...)` 中传入自定义 `ServerConfig` 即可生效。完整字段见 [`ServerConfig`](src/pulsemq/config.py)。

---

## 更新日志

### v7.2.3 (current)

- **修复 AdminServer 关闭时 RuntimeWarning** - 线程 loop 关闭前取消未完成任务
- **修复 Ctrl+C 优雅关闭超时** - `run_server` 等待 `server.stop()` 完成，10s 超时强制退出
- **requires-python 恢复 >=3.13**

### v7.2.2

- **降低 requires-python 到 3.11**（后续恢复 3.13）
- **修复 Linux Ctrl+C 无法终止** - `lifecycle.py` 等待 stop task 完成

### v7.2.1

- **修正 README 文档与代码不一致**（7 处）

### v7.2.0

- **同步数据面线程** - SyncDataThread 独立线程 + 独立 ctx，端到端 p50 < 1ms
- **压缩算法自适应** - `compression="auto"` 根据 payload 大小自动选择 none/lz4
- **Consumer decode_header 快速过滤** - 跳过不匹配 topic 的完整 decode
- **ZMQ HWM 可配置化** - `sndhwm`/`rcvhwm` 配置项 + `PULSEMQ_SNDHWM`/`PULSEMQ_RCVHWM` 环境变量
- **多进程基准测试脚本** - `scripts/bench_multiprocess.py`，三进程独立运行，28 组合全覆盖

### v7.1.0

- **encode 自动推断** - serializer/data_type 自动推断，DataFrame 兼容 msgpack/json + STR/BYTES 支持

### v7.0.x

- **record_count 自动推断** - list/DataFrame 行数自动推断

### v2

完整重构：PUB/SUB → Client/Server (ROUTER/DEALER)

- **架构变更**：单 PUB socket → 双 ROUTER（数据面 + 控制面）+ HTTP admin
- **认证升级**：api_key 明文 → bcrypt CredentialStore + ZAP PLAIN + 用户 CLI
- **类型保真**：`_restore_type` 确保 DataFrame/dict str/bytes 端到端还原
- **监控增强**：延迟 P50/P95/P99、在线客户端、事件流、SSE 实时推送、独立 Admin 线程
- **性能优化**：`decode_header` 服务端零反序列化路由、`TrafficStats` 单 `dict.get`
- **自动重连**：运行期指数退避重连（1s→2s→4s→...→30s）
- **日志系统**：loguru 统一，每日滚动写入 `logs/`，30 天保留
- **安全性**：密码 bcrypt 哈希、admin token 随机生成（0600）、凭据文件原子写入
- **批次处理**：DataFrame 批量 1000 行/帧，pyarrow 直序列化（见上文「性能基准」）

---

## 许可证

[MIT](LICENSE)
