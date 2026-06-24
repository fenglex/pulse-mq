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
- **跨平台** — Windows / macOS / Linux 开箱即用；`import pulsemq` 自动修正 Windows 事件循环策略，SUB 端无需任何额外配置即可正常收消息
- **连接可观测** — SUB 连接时 publisher 端打印 `[SUB 上线] user=xxx addr=1.2.3.4 auth=OK`，认证失败打印 `[SUB 认证失败] ... reason=...`，便于排查谁连进来、为什么连不上

## 安装

> 要求 Python >= 3.13

```bash
pip install pulse-mq
```

依赖项：ZeroMQ、msgspec、python-snappy、lz4、zstandard、pyarrow、pandas 全部开箱即用。

> **关于包名**：PyPI 分发名是 `pulse-mq`（`pip install` 用），而 Python import 名是 `pulsemq`（`import` 用，无连字符，因 Python 标识符不允许连字符）。这是 Python 生态的常见双名模式，与 `pip install python-dateutil` → `import dateutil`、`pip install scikit-learn` → `import sklearn` 一致。
>
> ```bash
> pip install pulse-mq      # 安装
> ```
> ```python
> import pulsemq            # 使用
> from pulsemq import PulsePublisher, PulseSubscriber
> ```

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

### 支持的返回类型（白名单）

Producer 回调**只接受以下 7 种返回类型**，其余一律抛 `TypeError`：

`pd.DataFrame` / `list[pd.DataFrame]` / `list[dict]` / `list[str]` / `dict` / `str` / `bytes`

### 数据类型 × 序列化器 强绑定对照表

PulseMQ 采用**强类型绑定**（方案 A）：数据类型与序列化器一一对应，不匹配会在发布时抛 `TypeError`。单元格 = record_count 值（合法）或 ❌（不匹配，报错）：

| 返回类型 | `msgpack` | `json` | `pyarrow` | `str` | `bytes` | record_count |
|----------|:---------:|:------:|:---------:|:-----:|:-------:|:------------:|
| `pd.DataFrame`（N 行） | ✅ | ✅ | ✅ | ❌ | ❌ | N（行数）|
| `list[pd.DataFrame]` | ✅ | ✅ | ✅ | ❌ | ❌ | **行数和** |
| `list[dict]`（N 个） | ✅ | ✅ | ✅ | ❌ | ❌ | N |
| `list[str]`（N 个） | ✅ | ✅ | ❌ | ❌ | ❌ | N |
| `dict` | ✅ | ✅ | ✅ | ❌ | ❌ | 1 |
| `str` | ❌ | ❌ | ❌ | **✅** | ❌ | 1 |
| `bytes` | ❌ | ❌ | ❌ | ❌ | **✅** | 1 |

**绑定规则**：
- `str` 数据 → **只能用 `str` 序列化器**（纯 UTF-8，最快）
- `bytes` 数据 → **只能用 `bytes` 序列化器**（零拷贝透传，最快）
- `pd.DataFrame` / `list[pd.DataFrame]` / `list[dict]` / `dict` → 可选 `msgpack` / `json` / `pyarrow`
- `list[str]` → 可选 `msgpack` / `json`

```python
return "hello"                              # str            → 1 record,  用 str
return b"\x00\x01"                          # bytes          → 1 record,  用 bytes
return {"a": 1}                             # dict           → 1 record,  用 msgpack/json/pyarrow
return [{"a": 1}, {"a": 2}]                 # list[dict]     → 2 records, 用 msgpack/json/pyarrow
return ["a", "b", "c"]                      # list[str]      → 3 records, 用 msgpack/json
return pd.DataFrame({"a": [1, 2]})          # DataFrame      → 2 records, 用 msgpack/json/pyarrow
return [df1, df2]                           # list[DataFrame]→ 行数和,    用 msgpack/json/pyarrow
```

> **record_count 推断**：DataFrame/Table 按行数；`list[dict]`/`list[str]` 按 list 长度；`list[DataFrame]` 按**各 DataFrame 行数之和**；`dict`/`str`/`bytes` 按 1。单帧上限 **1,000,000** 条。
>
> **list 元素必须类型一致**：`list[dict]` 要求所有元素都是 dict，`list[str]` 要求都是 str。混合类型（如 `[{"a":1}, "hello"]`）会抛 `TypeError`。
>
> **白名单外类型全部报错**：标量（int/float/bool）、`pa.Table`、`list[bytes]`、`list[int]`、`set`、`tuple` 等均不支持。

### 序列化格式（5 种）

通过 producer 装饰器的 `serializer` 参数声明。**序列化器会根据数据类型自动校验**，无需手动匹配（配错会报错提示）：

```python
@pub.producer(name="market", serializer="msgpack", compression="none")
async def market():
    return {"symbol": "600000", "price": 10.5}

@pub.producer(name="ticks", serializer="pyarrow", compression="zstd")
async def ticks():
    return pd.DataFrame(...)

@pub.producer(name="log", serializer="str")       # str 数据必须用 str
async def log():
    return "some log line"

@pub.producer(name="raw", serializer="bytes")     # bytes 数据必须用 bytes
async def raw():
    return b"\x01\x02\x03"
```

| 格式 | 后端 | 适用数据类型 | 特点 |
|------|------|--------------|------|
| `msgpack` | `msgspec.msgpack` | dict / list[dict] / list[str] / DataFrame / list[DataFrame] | 通用结构化，二进制紧凑 |
| `json` | `msgspec.json` | 同 msgpack（不含 bytes） | 人类可读、跨语言 |
| `pyarrow` | `pyarrow` IPC | dict / list[dict] / DataFrame / list[DataFrame] | 列存 IPC，分析场景（可选依赖）|
| `str` | UTF-8 | **仅 str** | 纯文本透传，最快 |
| `bytes` | 透传 | **仅 bytes** | 二进制透传，最快 |

> **`pyarrow` 为可选依赖**：未安装时该格式不注册，使用会抛 `KeyError`。其余 4 种为硬依赖，始终可用。
>
> **`pyarrow` 类型严格**：返回 `list[str]` 或标量时会抛 `TypeError`，提示改用 `msgpack`/`json`。

### 压缩算法（4 种）

通过 `compression` 参数声明，默认 `none`：

| 算法 | 后端 | 压缩比 | 速度 | 适用场景 |
|------|------|--------|------|----------|
| `none`（默认） | — | 1.00x | 最快 | 调试 / 极小数据 |
| `snappy` | `python-snappy` | 低 | 极快 | 速度优先 |
| `lz4` | `lz4.frame` | 中 | 极快 | 速度与压缩比平衡，金融行情常用 |
| `zstd` | `zstandard` | 高 | 中 | 压缩比优先，带宽受限场景 |

4 种压缩算法可与任意序列化格式自由组合（5×4 = 20 种合法组合）。

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

- **顶部指标卡片**：活跃主题数、消息量/秒（记录数口径，60 秒滚动均值）、流量/秒（压缩后字节，60 秒滚动均值）、运行时间
- **ECharts 流量折线图**：点击 topic 卡片叠加折线（最多 5 个，LRU 淘汰），支持 **1H / 6H** 时间范围切换，30 秒自动刷新历史数据，玻璃态美化 + 渐变填充
- **Topic 列表**：实时显示每个 topic 的记录速率、当前分钟记录数和缓存用量

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
| 2 | meta | 7 字节：`[msg_type(1)][flags(1)][data_type(1)][record_count(4, big-endian uint32)]` |
| 3 | timestamp | 8 字节 big-endian int64，纳秒 |
| 4 | payload | 序列化 + 压缩后的字节 |

- `msg_type`：`0x01` = DATA，`0x02` = PING
- `flags`：`bit[0:2]` 序列化格式编码，`bit[3:4]` 压缩算法编码
- `data_type`（v3 新增）：原始数据类型标记，让 sub 端还原 pub 端原始 Python 类型（DataFrame → DataFrame，而非 list[dict]）。取值：`0x00`=UNKNOWN, `0x01`=dict, `0x02`=list[dict], `0x03`=list[str], `0x04`=DataFrame, `0x05`=list[DataFrame], `0x06`=str, `0x07`=bytes
- 单帧 `record_count` 上限 **1,000,000**

> **v3 Breaking**：meta 帧从 6 字节扩展到 7 字节（新增 data_type 字节），record_count 位置从 `[2:6]` 后移到 `[3:7]`。v3 与 v2.x 不兼容（record_count 读取错位）。

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

### v3.0.0

⚠️ **Breaking Change**：meta 帧从 6 字节扩展到 7 字节，实现 pub→sub 全链路类型保真。**v3 与 v2.x 不兼容，pub/sub 两端必须同时升级。**

- **🔴 修复类型变形（致命）**：此前 pub 端发送 `DataFrame` / `list[DataFrame]` 时，sub 端收到的类型被降级——msgpack/json 路径变成 `list[dict]`，pyarrow 路径变成 `pa.Table`，且 `dict` / `list[dict]` 经 pyarrow 也统一变成 `pa.Table`。实测 16 个合法组合中有 **8 个类型变形**。现在协议层记录原始数据类型，sub 端自动还原：

  | pub 端发送 | sub 端收到（v2.x） | sub 端收到（v3.0） |
  |---|---|---|
  | `DataFrame` | `list[dict]` / `Table` | **`DataFrame`** ✅ |
  | `list[DataFrame]` | `list[dict]` / `Table` | **`list[DataFrame]`** ✅ |
  | `dict`（pyarrow）| `Table` | **`dict`** ✅ |
  | `list[dict]`（pyarrow）| `Table` | **`list[dict]`** ✅ |

- **meta 帧扩展（Breaking）**：新增 Byte 2 = `data_type`（原始数据类型标记），record_count 位置从 `[2:6]` 后移到 `[3:7]`。`DataType` 常量：`0x00`=UNKNOWN, `0x01`=dict, `0x02`=list[dict], `0x03`=list[str], `0x04`=DataFrame, `0x05`=list[DataFrame], `0x06`=str, `0x07`=bytes
- **类型还原机制**：pub 端 `_infer_data_type()` 推断原始类型写入 meta；sub 端 `decode()` 据此把反序列化结果（list[dict] / pa.Table）还原为原始 Python 类型。`PulseMessage` 新增 `data_type` 字段
- **测试强化**：`assert_message_roundtrip` 从"两侧降级为 list[dict] 比较值"升级为"类型保真 + 值相等"双断言（DataFrame 用 `assert_frame_equal`）
- **诊断脚本**：新增 `scripts/_diag_type_fidelity.py`，覆盖 16 个合法组合的类型+值保真验证

> **升级注意**：
> - v3 与 v2.x **协议不兼容**，pub/sub 两端必须同时升级到 v3.0.0
> - `list[DataFrame]` 因多个分片在序列化时展平，sub 端还原为 `[单个合并后的 DataFrame]`（行数据完全一致，仅丢失原分片个数信息）
> - 用户代码无需改动（`PulseMessage.payload` 现在直接是原始类型，如 DataFrame 而非 list[dict]）

### v2.4.1

🔧 **bugfix**：修复 SUB 端 PLAIN 认证失败时卡死不退出的问题。

- **🔴 修复 SUB 认证失败卡死（致命）**：pyzmq 的 SUB socket 在 PLAIN 认证被服务端 ZAP 拒绝时，`recv()` 不会抛错，而是在后台无限重连，导致用户的 `await sub.recv_multipart()` **永久阻塞**，程序卡死无任何提示。现在 `PulseSubscriber` 通过 ZMQ monitor 检测 `EVENT_HANDSHAKE_FAILED_AUTH` 事件，一旦发生就**自行打 error 日志并静默结束迭代**，`async for` 自然退出，**用户无需 try/except**：

  ```python
  async with PulseSubscriber(addr, username="alice", password="wrong") as sub:
      async for msg in sub.subscribe("topic"):
          print(msg.payload)
      # 认证失败时：async for 自动结束，无需异常处理
  # 日志会显示：[SUB 认证失败] PLAIN 握手被服务端拒绝，已停止订阅 (user='alice', ...)。
  ```

- **零 API 负担**：库自行处理认证失败，不抛异常、不需要用户捕获，看日志即可定位是凭证问题
- **仅认证场景启用**：无认证（`username=""`）时完全不启用 monitor，零开销零误报
- **测试强化**：认证失败测试从弱断言（"2秒内无消息"）改为强断言（收到 0 条 + 有 error 日志），加 timeout 防回归

> **升级建议**：所有开启 PLAIN 认证的用户强烈建议立即升级（避免错误凭证导致程序卡死）。

### v2.4.0

🔧 **关键 bugfix + 可观测性增强**：修复 Windows 平台 SUB 端收不到消息的致命问题，并新增 SUB 连接日志。

- **🔴 修复 Windows SUB 收不到消息（致命）**：Windows 上 Python 默认使用 `ProactorEventLoop`，但 pyzmq 的 asyncio 集成只支持 `SelectorEventLoop`，导致 SUB 端 `recv` 永远不返回（或抛 `RuntimeError: Proactor event loop does not implement add_reader family`）。现在 `import pulsemq` 时会在 win32 上自动 `set_event_loop_policy(WindowsSelectorEventLoopPolicy)`，用户**零配置**即可正常收消息（与 pyzmq / aiohttp / tornado 等库的通用做法一致）
- **SUB 连接可观测性增强**：ZAP handler 此前只在认证失败时打 `warning`，认证成功完全静默。现在三种情况都打印结构化日志：
  - 认证成功：`[SUB 上线] user=alice addr=192.168.1.5 auth=OK`
  - 凭证错误：`[SUB 认证失败] user=alice addr=192.168.1.5 auth=FAIL reason=invalid-credentials`
  - 非 PLAIN 机制：`[SUB 认证失败] user=... addr=... auth=FAIL reason=not-PLAIN mechanism=...`
  - 含 **用户名 + 客户端 IP + 认证结果 + 失败原因**，便于排查谁连进来、为什么连不上
- **SUB 端连接日志完善**：`PulseSubscriber.connect()` 现在打印自身用户名，开启认证时为 `Subscriber 连接到 xxx (auth=on, user=alice)`，便于 sub 端自己确认连上的是谁
- **回归测试**：新增 `test_event_loop_policy_on_windows`，防止 Windows 事件循环策略回归
- **诊断脚本**：新增 `scripts/_diag_sub_problem.py`（启动真实 pub 服务 + sub 收消息端到端验证）和 `scripts/_diag_auth_fail.py`（认证失败场景验证）

> **升级建议**：Windows 用户强烈建议立即升级；Linux/macOS 用户无影响但可享受新增的连接日志。

### v2.3.0

⚠️ **Breaking Change**：数据类型收紧为白名单，序列化器改为强类型绑定。

- **数据类型白名单**：producer 回调只接受 7 种类型——`pd.DataFrame` / `list[pd.DataFrame]` / `list[dict]` / `list[str]` / `dict` / `str` / `bytes`。其余（标量、`pa.Table`、`list[bytes]`/`list[int]`、混合 list、set/tuple 等）一律抛 `TypeError`
- **序列化器强绑定**（方案 A）：`str` 数据只能用 `str` 序列化器，`bytes` 数据只能用 `bytes` 序列化器，结构化数据（DataFrame/dict/list）用 msgpack/json/pyarrow。配错会在发布时报错
- **修复 `list[pd.DataFrame]` 的 record_count bug**：原按 list 长度（DataFrame 个数）计，现按各 DataFrame 行数之和（与 payload 展平后的实际记录数一致）
- **`bytes × json` 报错**：json 序列化器明确拒绝 bytes（避免 base64 编码后解码类型变形为 str 的语义不一致）
- **缓存按记录数淘汰**：`TopicBuffer` 从"按帧数（deque maxlen）"改为"按累计记录数"淘汰，DataFrame 一批 N 条占 N 配额。监控显示 `N / 上限（满）` 格式
- **pyarrow 序列化器严格化**：遇到不支持的类型（list[str]、标量等）抛 `TypeError`，而非静默退回 msgpack 导致编解码不一致
- **监控 UI 文案精确化**：消息量/流量卡片副标题标注"近60秒估算"，tooltip 说明算法口径；主题卡片去掉 record_count_current，缓存显示 `N/M(满)`
- **文档**：新增「数据类型 × 序列化器 强绑定对照表」；更新序列化器/压缩算法表格
- **测试**：新增 `tests/test_data_types.py`（59 个专项用例）；e2e 矩阵扩展至 7 种数据形态；修复 burst 测试跨分钟 flaky

### v2.2.2

- **文档修正**：README 安装命令包名 `pulsemq` → `pulse-mq`（PyPI 实际包名）
- **文档同步**：监控卡片描述对齐 v2.2.0 的记录数口径与玻璃态 UI；补全 v2.2.0/v2.2.1 更新日志

### v2.2.1

- **启动修复**：`publisher.py` 补 `if __name__ == "__main__"` 守卫，修复 `python -m pulsemq.publisher` 无法启动的问题
- **版本号统一**：新增 `pulsemq/_version.py` 作为版本号单一来源，`publisher.__version__` 与 `/api/v1/system/status` 的 `SERVER_VERSION` 动态读取一致（修复后者写死 2.0.0）
- **健壮性增强**：
  - `subscriber` 遵守 asyncio 取消协议，`CancelledError` 时清理 socket 后重新抛出
  - admin 路由异常补 `logger.debug` 日志（不再静默吞掉）
  - `_respond_html` / `_respond_json` 复用 `_STATUS_TEXT` 状态文本映射
  - `TrafficStats` 读路径快照迭代，规避并发 `clear()` 的 `RuntimeError`
  - SSE 队列满时主动断开死客户端，避免内存泄漏
  - `_topic_history` off-by-one 修正（`>= minutes` → `>= minutes - 1`）

### v2.2.0

- **消息量口径变更**：监控指标从"帧数"（发送次数）切换为"记录数"（record_count）。卡片速率、折线图、topic 列表全部改用 `record_rate_1min`，一条带 N 条记录的批量消息现在如实显示为 N 条/秒
- **监控 UI 美化与中文化**：玻璃态卡片 + 渐变发光 + ECharts 渐变填充 + 全面中文文案 + emoji 图标

### v2.1.0

- **监控 UI 全面升级**：深色渐变主题，ECharts 折线图支持 1H/6H 时间范围切换
- **60 秒滚动均值**：Messages/s 和 Data/s 改为近 60 秒的加权均值，不再每分钟重置（注：v2.2.0 起 Messages/s 改用记录数口径）
- **折线图交互优化**：首次进入自动选中第一个 topic，30 秒自动刷新历史，hover tooltip 不再闪烁
- **后端去重**：history API 合并内存 + SQLite 数据，按 timestamp 去重
- **全矩阵 Benchmark**：新增 `scripts/bench_pubsub_matrix.py`，覆盖 48 种组合的性能与正确性测试

### v2.0.2

- 协议帧 record_count 从 uint16 扩展到 uint32，单帧上限 1,000,000 条
- 重写 README 对齐 v2 架构

## 许可证

[MIT](LICENSE)
