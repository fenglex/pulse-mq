# PulseMQ 全参数 PUB 基准测试设计

> 替代现有 `scripts/bench_1m.py`，使用 PulseClient 高级 API，覆盖所有 PUB 参数组合。

## 概述

- **目标**：100 万条记录为基准，测量 PulseMQ 在所有序列化×压缩×数据形态组合下的 PUB 吞吐性能
- **方式**：自包含脚本，自动启动/关闭独立 Broker
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

每个测试单元独立运行，拥有独立的 Broker 实例：

```
1. start_broker(port)                           — 启动独立 Broker
2. SUB 客户端连接 → subscribe(topic)            — 异步接收循环启动
3. sleep(0.5)                                   — 等待订阅生效
4. PUB 客户端连接                               — 连接同一 Broker
5. 发送 1 条诊断消息                             — 确认 SUB 能收到
6. ─── 计时开始 ───
7. PUB 循环发送 100 万记录                       — 每 1000 条采样 _send_ts
8. ─── 计时结束，记录 send_elapsed ───
9. 等待 SUB 接收完毕（最多 60s）                  — 或收齐全部消息
10. 记录 recv_count, recv_elapsed               — SUB 端统计
11. 关闭 PUB/SUB 客户端
12. stop_broker()                               — 关闭 Broker
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

- SUB 客户端固定用 `msgpack` 序列化器（PulseClient 默认行为）
- pyarrow/raw 发送的消息在 SUB 端解码可能失败（payload=None）
- 解码失败不影响吞吐量统计（帧仍然收到，计入接收数）
- **记录解码失败次数**，作为后续修复依据

## Broker 配置

```python
BrokerConfig(
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
    serializer=...,              # 与测试单元匹配
    compressor=...,              # 与测试单元匹配
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
| 延迟 p50/p99 (μs) | SUB 端采样，每 1000 条采 1 次（通过 `_send_ts` 计算） |
| 解码失败数 | SUB 端 payload=None 的次数 |

## 延迟采样

- PUB 端每 1000 条消息，在 payload 中嵌入 `_send_ts = time.time()`
- SUB 端每收 1000 条，检查 payload 中 `_send_ts`，计算 `(recv_time - _send_ts) * 1_000_000`
- 最终排序计算 p50 / p99

## 终端输出

### 运行时进度

```
[ 1/20] single_msgpack × none       PUB  245,678 rec/s │ SUB  243,102 rec/s │ loss 1.0% │ payload 128B
[ 2/20] single_msgpack × snappy     PUB  198,432 rec/s │ SUB  197,001 rec/s │ loss 0.7% │ payload 89B
...
```

### 矩阵汇总

跑完全部 20 组后，打印 5 张矩阵表格：

1. **发送吞吐 (records/s)**
2. **接收吞吐 (records/s)**
3. **丢包率 (%)**
4. **Payload 大小 (bytes)**
5. **延迟 P50 / P99 (μs)**

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

1. SUB 端默认 msgpack 解码，pyarrow/raw 消息的 payload 会解码失败，记录失败数待后续修复
2. 批量模式下 500 次发送采样的延迟样本较少（最多 500 个），p99 统计精度有限
3. 20 组测试串行执行，每组约 3-10 分钟（取决于硬件），总时间约 1-3 小时
