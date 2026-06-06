# PulseMQ 全参数 PUB 基准测试设计

> 替代现有 `scripts/bench_1m.py`，使用 PulseClient 高级 API，覆盖所有 PUB 参数组合。

## 概述

- **目标**：100 万条记录为基准，测量 PulseMQ 在所有序列化×压缩×数据形态组合下的 PUB 吞吐性能
- **方式**：自包含脚本，自动启动/关闭独立 服务端
- **接口**：使用 PulseClient 高级 API（publish/subscribe）
- **输出**：终端矩阵表格

## 参数矩阵

20 个测试单元（5 data_shape × 4 compression）：

| # | data_shape | 序列化 | 批大小 | 压缩 |
|---|-----------|--------|--------|------|
| 1-4 | single_msgpack | msgpack | 1 | none / snappy / lz4 / zstd |
| 5-8 | batch_msgpack | msgpack | 2000 | none / snappy / lz4 / zstd |
| 9-12 | single_pyarrow | pyarrow | 1 | none / snappy / lz4 / zstd |
| 13-16 | batch_pyarrow | pyarrow | 2000 | none / snappy / lz4 / zstd |
| 17-20 | single_raw | raw(none) | 1 | none / snappy / lz4 / zstd |

每组 100 万条记录：
- 单条模式：1,000,000 次 `publish()` 调用，每次 1 条
- 批量模式：500 次 `publish()` 调用，每次 DataFrame 2000 行
- raw 模式：1,000,000 次 `publish()` 调用，每次 ~250 bytes

## 测试单元流程

每个测试单元独立运行，拥有独立的 服务端 实例：

```
1. start_server(port)                           — 启动独立 服务端
2. SUB 客户端连接 → subscribe(topic)            — 异步接收循环启动
3. sleep(0.5)                                   — 等待订阅生效
4. PUB 客户端连接                               — 连接同一 服务端
5. 发送 1 条诊断消息                             — 确认 SUB 能收到
6. ─── 计时开始 ───
7. PUB 循环发送 100 万记录                       — 每 1000 条采样 _send_ts
8. ─── 计时结束，记录 send_elapsed ───
9. 等待 SUB 接收完毕（最多 60s）                  — 或收齐全部消息
10. 记录 recv_count, recv_elapsed               — SUB 端统计
11. 关闭 PUB/SUB 客户端
12. stop_server()                               — 关闭 服务端
```

### 数据模型 — A 股行情快照

```python
# 单条 dict（msgpack / pyarrow 单条模式）
{
    "seq": int, "code": str, "name": str,
    "open": float, "high": float, "low": float, "close": float,
    "volume": int, "amount": float, "ts": float,
}

# 批量 DataFrame（msgpack / pyarrow 批量模式）
# 同样字段，2000 行

# raw bytes（raw 模式）
# 固定 ~250 bytes 二进制数据，模拟行情二进制协议
```

### PulseClient 调用方式

```python
# single_msgpack
await client.publish(topic, snap_dict, format="msgpack", compression=comp)

# batch_msgpack
await client.publish(topic, df_2000, format="msgpack", compression=comp)

# single_pyarrow
await client.publish(topic, snap_dict, format="pyarrow", compression=comp)

# batch_pyarrow
await client.publish(topic, df_2000, format="pyarrow", compression=comp)

# single_raw
await client.publish(topic, bytes_data, format="none", compression=comp)
```

### SUB 端解码说明

PulseClient 的 `_decode_message` 固定使用 `msgpack+none` 解码（`_DEFAULT_SER / _DEFAULT_COMP`），不从帧 meta 中提取实际 ser/comp。因此：

| 发布组合 | SUB 解码结果 |
|----------|-------------|
| msgpack + none | ✅ 成功，payload 为 dict |
| msgpack + snappy/lz4/zstd | ❌ 失败，未先解压就用 msgpack 反序列化 |
| pyarrow + * | ❌ 失败，用 msgpack 反序列化 pyarrow IPC |
| raw + * | ❌ 失败，用 msgpack 反序列化原始 bytes |

- 解码失败不影响吞吐量统计（ZMQ 帧仍然收到，计入接收数）
- **记录解码失败次数**，作为后续修复依据（PulseClient 应从 frame meta 提取 ser/comp）

## 服务端 配置

```python
ServerConfig(
    bind=f"tcp://*:{port}",
    xpub_bind=f"tcp://*:{port + 1}",
    auth_enabled=False,          # 关闭认证，减少干扰
    max_concurrency=200,
    max_batch_size=256,
    zmq_rcvhwm=0,                # 关闭 ZMQ 高水位限制
    zmq_sndhwm=0,
    data_buffer_size=100_000,    # 适配 100 万记录
    ctrl_buffer_size=5_000,
    metrics_enabled=False,       # 关闭监控
    default_compressor="none",
)
```

端口分配：从 `--port` 基础端口开始，每组递增 2。

## PulseClient 配置

```python
PulseClient(
    address=f"tcp://localhost:{port}",
    xpub_address=f"tcp://localhost:{port + 1}",
    heartbeat_interval=30.0,     # 长间隔，避免干扰
    recv_timeout=5.0,
    auto_reconnect=False,        # 不重连
)
```

SUB socket 额外设置：`zmq.RCVHWM = 5_000_000`

## 采集指标

| 指标 | 说明 |
|------|------|
| 发送吞吐 (rec/s) | 100 万记录 / send_elapsed |
| 接收吞吐 (rec/s) | recv_count / recv_elapsed |
| 丢包率 (%) | 1 - recv_count / send_count |
| Payload 大小 (B) | 首条消息编码后字节 |
| 解码失败数 | SUB 端 payload=None 的次数 |

## 延迟采样

SUB 端解码限制导致 `_send_ts` 方案只在 msgpack+none 组合可用。采用**帧级别延迟**方案替代：

- SUB 端直接从 PulseClient 内部 `_sub` socket 收原始帧（绕过 `_decode_message`）
- 不依赖 payload 解码，帧到达即记录时间戳
- 使用 **消息序号** 关联 PUB/SUB 端：
  - PUB 每发送 N 条（N=1000），记录一次 `(seq, monotonic_ts)`
  - SUB 每收到 N 帧，记录一次 `(seq, monotonic_ts)`
  - 匹配相同 seq 的 PUB/SUB 时间戳，计算差值即为端到端延迟

该方案适用于所有 20 种组合，不受 ser/comp 影响。

采样频率：每 1000 条（单条/raw）或每 50 批（批量模式 500 批 → 10 个样本）采样一次。

## 终端输出

### 运行时进度

```
[ 1/20] single_msgpack × none       PUB  245,678 rec/s │ SUB  243,102 rec/s │ loss 1.0% │ payload 128B
[ 2/20] single_msgpack × snappy     PUB  198,432 rec/s │ SUB  197,001 rec/s │ loss 0.7% │ payload 89B
...
```

### 矩阵汇总

跑完全部 20 组后，打印 4 张矩阵表格：

1. **发送吞吐 (records/s)**
2. **接收吞吐 (records/s)**
3. **丢包率 (%)**
4. **Payload 大小 (bytes)**

```
■ 发送吞吐 (records/s)
                  │       none │     snappy │        lz4 │       zstd
──────────────────┼────────────┼────────────┼────────────┼────────────
single_msgpack    │    245,678 │    198,432 │    210,543 │    180,221
batch_msgpack     │  1,823,456 │  1,654,321 │  1,701,234 │  1,502,345
single_pyarrow    │    190,123 │    175,432 │    182,341 │    160,543
batch_pyarrow     │  2,105,432 │  1,890,123 │  1,956,234 │  1,723,456
single_raw        │    310,543 │    245,678 │    260,123 │    220,432
```

## CLI 参数

```
python scripts/bench_1m.py [--port PORT] [--records N]
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--port` | 58555 | 基础端口号 |
| `--records` | 1,000,000 | 每组总记录数 |

## 文件位置

重写 `scripts/bench_1m.py`，删除原有内容。

## 已知限制

1. SUB 端默认 msgpack+none 解码，非 msgpack+none 组合的 payload 会解码失败（已记录失败数，待 PulseClient 修复从 frame meta 提取 ser/comp）
2. 批量模式下延迟采样点较少（500 批中每 50 批采样 → 10 个样本），统计精度有限
3. 20 组测试串行执行，每组约 3-10 分钟（取决于硬件），总时间约 1-3 小时
