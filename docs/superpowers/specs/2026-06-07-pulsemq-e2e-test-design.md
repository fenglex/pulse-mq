# PulseMQ 端到端全格式/全压缩测试设计

## 目标

为 PulseMQ 增加一个端到端集成测试：启动一个真实服务端 + 两个真实客户端（publisher / subscriber），覆盖所有数据类型与所有压缩方式的笛卡尔积，验证每条消息都能完整往返。

## 覆盖范围

| 维度 | 取值 |
|------|------|
| 数据类型 | `str`、`bytes`、`DataFrame(msgpack)`、`DataFrame(pyarrow)` |
| 压缩方式 | `none`、`snappy`、`lz4`、`zstd` |
| 组合数 | 4 × 4 = 16 |

合计 16 条用例，每条独立 topic、独立原始数据。

## 文件结构

新增两个独立运行脚本（不依赖 pytest）：

```
scripts/test_server_runner.py    # 启禁用认证的 PulseServer
scripts/test_e2e_all.py          # 入口脚本，串起整个流程
```

无源码改动。复用现有 `PulseClient` 与 `PulseServer`。

## 端口约定

固定使用 `15555` / `15556`（ROUTER / XPUB），避开开发端口 `5555/5556`。
通过命令行参数 `--port` 可覆盖，便于多机/多实例。

## 组件

### 1. `test_server_runner.py`

与 `scripts/test_server.py` 类似但更轻量：

- 关闭认证（`auth_enabled=False`）
- 关闭指标（`metrics_enabled=False`）— 测试不需要 HTTP 指标
- 接收 `--port`（默认 15555）
- 启动后向 stdout 输出一行 `READY\n`，便于 e2e 脚本做同步
- 监听 SIGINT/SIGTERM 优雅关闭

### 2. `test_e2e_all.py`（主测试）

**启动 server**

- `subprocess.Popen([sys.executable, scripts/test_server_runner.py, "--port", "15555"])`
- 读到 `READY` 行后认为 server 就绪
- 等待最长 10s；超时则失败退出

**测试用例构造**

```python
CASES = []
for data_type in ["str", "bytes", "df-msgpack", "df-pyarrow"]:
    for compression in ["none", "snappy", "lz4", "zstd"]:
        CASES.append({"id": len(CASES), "data_type": data_type,
                      "compression": compression})
```

每个用例独立构造原始数据：

- `str`：JSON 字符串，键含 `case_id`、`ts`、随机字段
- `bytes`：`os.urandom(64)`，长度足以体现压缩效果
- `df-msgpack` / `df-pyarrow`：10 行 DataFrame，列含 `int / float / str / bytes`，带 `case_id` 列

**Publisher 协程**

- 对每个用例，构造唯一 topic：`test.e2e.{case_id}.{data_type}`
- 顺序调用 `client.publish(topic, original_data, format=..., compression=...)`
- 每条之间 `await asyncio.sleep(0.05)`，给 SUB 留缓冲时间
- 16 条发完后置 `publisher_done = asyncio.Event()` 并 set

**Subscriber 协程**

- 订阅通配 topic：`test.e2e.>` （一次性订阅，减少 SUB 确认延迟；理由见"实现细节"第 3 条）
- 循环收消息，按 `case_id` 路由到 `received[case_id]`
- 与 `expected[case_id]` 比较，**全部成功**才置 `subscriber_done`
- 任何一条不匹配：记录到 `errors` 列表，但**不立即终止**，让其它用例跑完

**并发编排**

```python
publisher_task = asyncio.create_task(publisher())
subscriber_task = asyncio.create_task(subscriber())

try:
    await asyncio.wait_for(
        asyncio.gather(publisher_task, subscriber_task),
        timeout=30.0,
    )
except asyncio.TimeoutError:
    publisher_task.cancel()
    subscriber_task.cancel()
    # 等待 cancel 真正生效，再清理 client/proc
```

**DataFrame payload 比较**

- `df-msgpack` 路径：客户端收到 `dict` 列表（msgpack 反序列化的结果）→ 用 `pd.DataFrame(list_of_dict)` 重建 → `pd.testing.assert_frame_equal`
- `df-pyarrow` 路径：客户端收到 `pa.Table` → `assert_frame_equal(table.to_pandas(), expected_df)`

> 注：实际拿到的对象类型需在实现时验证。`PulseMessage.payload` 实际是什么，取决于 `FrameCodec.decode_payload` 用 `_DEFAULT_SER` 解码。需要在实现阶段读 `async_client.py:392-395` 确认。当前设计假设见"实现细节"。

## 错误处理

| 场景 | 行为 |
|------|------|
| 子进程启动失败 | 退出码 1，打印 stderr |
| `READY` 等待超时 | 退出码 1，kill 子进程 |
| 单条 roundtrip 不匹配 | 累积到 `errors`，继续跑 |
| 30s 整体超时 | cancel 两个 task，打印 `已收 N/16，未收: [<case_ids>]` 形式，退出码 1 |
| 关闭时残留 | `proc.terminate()` → `proc.wait(5)` → 兜底 `proc.kill()` |

## 通过判定

- 16 条用例全部 roundtrip 一致 → stdout 输出 `✅ 16/16 cases passed`，退出码 0
- 任意失败/超时 → 退出码 1

## 关键实现细节（实施时需进一步确认）

1. **客户端 payload 解码**：当前 `async_client.py:392-395` 用 `_DEFAULT_SER = "msgpack"` 解码，意味着 `str`/`bytes`/`pyarrow` 类型的消息拿到的 `msg.payload` 可能不是原始类型（msgpack 把 str 解成 str，bytes 解成 bytes，pyarrow 表解成 pa.Table — 这部分要核 `FrameCodec.decode_payload` 行为）。

2. **取消传播**：`subscriber` 是 `async for msg in client.subscribe(...)` 循环，外层 `asyncio.wait_for` 取消后需要确保 client 干净断开（`async with` 退出或显式 `disconnect`）。

3. **topic 数量**：16 个 topic 全部用通配订阅 `test.e2e.>` 还是分别订阅 16 次？倾向通配订阅一次（减少 SUB 确认延迟）。

4. **数据断言 helper**：抽一个 `assert_payload_equal(received, expected, data_type)` 函数处理 str/bytes/df-msgpack/df-pyarrow 四种比较逻辑。

## 不做的事

- 不引入 pytest fixture（用户选择独立运行脚本）
- 不测认证/权限/错误注入（保持端到端 happy path 简洁）
- 不做性能/吞吐测量（性能用 `bench_1m.py` / `bench_live.py`）
- 不动 `src/pulsemq/*` 任何源码

## 受影响文件

- `scripts/test_server_runner.py` — 新增
- `scripts/test_e2e_all.py` — 新增
