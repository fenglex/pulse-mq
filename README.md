# PulseMQ

面向金融行情的高性能 pub → sub 消息中间件，基于 ZeroMQ 构建。采用**单进程 pub → sub 无 broker**架构，publisher 进程同时承担数据生产、权限控制、流量统计和后台管理界面。

## 特性

- **单进程架构** — publisher 即服务，无独立 broker，部署极简
- **高性能** — 基于 ZeroMQ PUB，SNDHWM=0 无丢消息；burst 模式可压榨到硬件极限
- **多数据格式** — `str` / `bytes` / `DataFrame` / `list[dict]` 等类型，发布端零配置自动推断 record_count
- **多种序列化** — `str`、`msgpack`（默认）、`json`、`pyarrow` IPC、`bytes` 透传
- **可选压缩** — `none`（默认）、`snappy`、`lz4`、`zstd`
- **PLAIN 认证** — ZeroMQ PLAIN 协议 + ZAP handler，api_key 白名单机制
- **实时监控** — 分钟粒度流量统计，内存 8 小时窗口 + SQLite 持久化
- **可视化后台** — 内置深色 Web UI（ECharts 折线图 + SSE 实时推送），支持 1H/6H 时间范围切换，60 秒滚动均值
- **优雅关闭** — Producer 任务 drain、Admin 停止、PUB socket linger 后退出
- **纳秒时间戳** — 帧级时间戳独立成帧，端到端延迟可精确测量

## 安装

> 要求 Python >= 3.13

```bash
pip install pulsemq
```

依赖项：ZeroMQ、msgspec、python-snappy、lz4、zstandard、pyarrow、pandas 全部开箱即用。

## 快速开始

### 启动 Publisher

```bash
# CLI 零配置启动
pulse-mq
```

更常见的用法是在 Python 中注册自己的 producer：

```python
from pulsemq import PulsePublisher

pub = PulsePublisher()

@pub.producer(name="sh_market", interval=2.0)
async def sh_market():
    # 任意可序列化对象
    return {"symbol": "600000", "price": 10.5, "volume": 12345}

@pub.producer(name="deep_quote", interval=0.5, compression="lz4")
async def deep_quote():
    import pandas as pd
    return pd.DataFrame({
        "price": [10.5, 10.6, 10.7],
        "volume": [100, 200, 300],
    })

pub.start()  # 阻塞运行
```

`PulsePublisher` 也提供 `start_async()` 方便嵌入其他 asyncio 程序。

### 订阅消息

```python
import asyncio
from pulsemq import PulseSubscriber

async def main():
    # 关闭认证时 username/password 可省略
    async with PulseSubscriber("tcp://localhost:5555") as sub:
        async for msg in sub.subscribe("sh_market"):
            print(msg.topic, msg.payload, msg.timestamp_ns)

    # 开启认证时必须传入凭证
    async with PulseSubscriber(
        "tcp://localhost:5555",
        username="user1",
        password="pulse_sk_xxx",
    ) as sub:
        async for msg in sub.subscribe("sh_market", "deep_quote"):
            print(msg.topic, msg.payload)

asyncio.run(main())
```

`PulseMessage` 字段：

| 字段 | 类型 | 说明 |
|------|------|------|
| `topic` | `str` | topic 名称 |
| `payload` | `Any` | 解码后的数据 |
| `raw_payload` | `bytes` | 解码前的原始字节 |
| `record_count` | `int` | 本帧包含的记录条数 |
| `timestamp_ns` | `int` | publisher 发送时的纳秒时间戳 |
| `serializer` | `str` | 使用的序列化格式名 |
| `compression` | `str` | 使用的压缩算法名 |

## 数据类型与序列化

### Producer 回调返回值

```python
# 单条数据
return "hello"             # str → 1 record
return b"\x00\x01\x02"     # bytes → 1 record
return df                  # DataFrame → N records (行数)

# 批量数据
return ["a", "b", "c"]                # list[str] → N records
return [b"\x01", b"\x02"]            # list[bytes] → N records
return [df1, df2]                    # list[DataFrame] → sum(行数) records
return [{"a": 1}, {"a": 2}]          # list[dict] → N records
```

`record_count` 由发布端自动推断并写入帧头，单帧上限 **1,000,000** 条。

### 序列化与压缩

通过 producer 装饰器参数声明：

```python
@pub.producer(name="market", serializer="msgpack", compression="none")
async def market():
    return {...}

@pub.producer(name="ticks", serializer="pyarrow", compression="zstd")
async def ticks():
    return pd.DataFrame(...)
```

| 序列化 | 适用场景 |
|--------|----------|
| `msgpack`（默认） | 通用结构化数据 |
| `json` | 人类可读、跨语言 |
| `pyarrow` | DataFrame、列存 |
| `str` | 纯文本 / UTF-8 字符串 |
| `bytes` | 二进制透传 |

| 压缩 | 适用场景 |
|------|----------|
| `none`（默认） | 调试 / 极小数据 |
| `snappy` | 速度优先 |
| `lz4` | 速度优先，压缩比略高 |
| `zstd` | 压缩比优先 |

### Burst 模式

极限性能测试场景可用 `burst_producer` 装饰器，无间隔连续发送（回调返回 `None` 时停止）：

```python
@pub.burst_producer(name="bench", cache_size=200_000)
async def bench():
    if not has_more():
        return None
    return [generate_record() for _ in range(1000)]
```

## 配置

### 环境变量

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `PULSEMQ_BIND` | ZMQ PUB 绑定地址 | `tcp://*:5555` |
| `PULSEMQ_ADMIN_BIND` | Admin 后台绑定地址 | `0.0.0.0:9090` |
| `PULSEMQ_STATS_DB` | 统计 SQLite 路径 | `sqlite://./stats.sqlite` |
| `PULSEMQ_API_KEYS` | API Key 列表 `user1:pass1,user2:pass2`，空=关闭认证 | `""` |

### Python 配置

```python
from pulsemq import PublisherConfig, PulsePublisher

config = PublisherConfig(
    bind="tcp://*:5555",
    admin_bind="0.0.0.0:9090",
    stats_db="sqlite://./stats.sqlite",
    stats_retention_minutes=480,   # 内存窗口，默认 8 小时
    api_keys_str="alice:pulse_sk_alice,bob:pulse_sk_bob",
)

pub = PulsePublisher(config)

# 或运行时追加 key
pub.add_api_key("carol", "pulse_sk_carol")
```

`PulsePublisher` 构造参数 `bind` / `admin_bind` / `api_keys` 可在启动前覆盖配置。

## 监控与 Admin 后台

Publisher 启动后，Admin 后台默认监听 `0.0.0.0:9090`，提供深色 Web UI 和 REST/SSE 接口。

### Web UI

浏览器打开 `http://localhost:9090/` 即可看到实时监控面板：

- **顶部指标卡片**：Topics 数量、Messages/s（60 秒滚动均值）、Data/s（60 秒滚动均值）、Uptime
- **ECharts 流量折线图**：点击 topic 卡片叠加折线（最多 5 个，LRU 淘汰），支持 **1H / 6H** 时间范围切换，30 秒自动刷新历史数据
- **Topic 列表**：实时显示每个 topic 的速率和缓存用量

### REST API

```bash
# 实时指标快照（含 60 秒滚动均值）
curl http://localhost:9090/api/v1/stats/realtime

# 所有 topic 列表
curl http://localhost:9090/api/v1/topics

# 单个 topic 分钟级历史（支持 minutes 参数）
curl http://localhost:9090/api/v1/topics/sh_market/history?minutes=60
curl http://localhost:9090/api/v1/topics/sh_market/history?minutes=360

# 系统状态
curl http://localhost:9090/api/v1/system/status

# 健康检查
curl http://localhost:9090/healthz
```

### SSE 实时推送

```bash
curl -N http://localhost:9090/api/v1/stats/stream
```

每 1 秒一帧 JSON，结构与 `/api/v1/stats/realtime` 一致。Web UI 与外部看板可直接订阅。

## 协议帧格式

每条 ZMQ 消息由 4 帧组成：

| 帧序号 | 内容 | 说明 |
|--------|------|------|
| 1 | topic | UTF-8 字节串 |
| 2 | meta | 6 字节：`[msg_type(1)][flags(1)][record_count(4, big-endian uint32)]` |
| 3 | timestamp | 8 字节 big-endian int64，纳秒 |
| 4 | payload | 序列化 + 压缩后的字节 |

- `msg_type`：`0x01` = DATA，`0x02` = PING
- `flags`：`bit[0:2]` 序列化格式编码，`bit[3:4]` 压缩算法编码
- 单帧 `record_count` 上限 **1,000,000**

## 性能基准

### Burst 极限测试

`scripts/bench_burst.py` 提供单场景 burst 极限性能测试：

```bash
python scripts/bench_burst.py
```

### 全矩阵 Benchmark

`scripts/bench_pubsub_matrix.py` 对所有合法的 (序列化 × 压缩 × 数据形态) 组合做全面测试：

```bash
python scripts/bench_pubsub_matrix.py
```

覆盖 48 个合法组合，同时测试：
- 纯编解码性能（序列化 + 压缩，不经过网络）
- 端到端 pub→sub 性能（吞吐量、延迟 p50/p90/p99、压缩率）
- 正确性验证（pub 端发送数据在 sub 端完整还原）

### v2.1.0 典型测试结果

**纯编解码性能**（200 次迭代平均）：

| 组合 | 编码 ops/s | 解码 ops/s | 编码 μs | 压缩率 |
|------|-----------|-----------|---------|--------|
| bytes+none | 14.6M | 29.9M | 0.07 | 1.00x |
| msgpack+none | 5.6M | 9.3M | 0.18 | 1.00x |
| msgpack+lz4+list_dict | 172K | 96K | 5.8 | 0.12x |
| msgpack+zstd+large_dict | 27K | 209K | 37.6 | 0.00x |

**端到端 pub→sub**（经过 ZMQ 网络，单 subscriber，50 条消息/组合）：

| 组合 | 记录吞吐/s | 延迟 p50 | 延迟 p99 |
|------|-----------|---------|---------|
| json+none+list_dict | 880,514 | 2.68ms | 3.51ms |
| msgpack+none+list_dict | 825,900 | 2.74ms | 3.05ms |
| msgpack+none+dataframe | 135,096 | 17.8ms | 34.2ms |
| pyarrow+none+dataframe | 86,663 | 27.6ms | 53.7ms |

> 测试环境：Windows 11，Python 3.13，单机 localhost

## 更新日志

### v2.1.0

- **监控 UI 全面升级**：深色渐变主题，ECharts 折线图支持 1H/6H 时间范围切换
- **60 秒滚动均值**：Messages/s 和 Data/s 改为近 60 秒的加权均值，不再每分钟重置
- **折线图交互优化**：首次进入自动选中第一个 topic，30 秒自动刷新历史，hover tooltip 不再闪烁
- **后端去重**：history API 合并内存 + SQLite 数据，按 timestamp 去重
- **全矩阵 Benchmark**：新增 `scripts/bench_pubsub_matrix.py`，覆盖 48 种组合的性能与正确性测试

### v2.0.2

- 协议帧 record_count 从 uint16 扩展到 uint32，单帧上限 1,000,000 条
- 重写 README 对齐 v2 架构

## 许可证

[MIT](LICENSE)
