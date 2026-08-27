# 10 Producer 调度设计

> 源码：`src/pulsemq/producers/manager.py` + `src/pulsemq/producers/types.py`

## 1. 设计意图

把「定时产生数据 → 发布到 topic」从业务脚手架中抽成声明式装饰器，同一套 `ProducerManager` 服务于两个宿主：

| 宿主 | 注册入口 | 数据出口 |
|------|----------|----------|
| `Server`（服务端内置推送） | `@srv.producer(topic, interval=5.0, serializer=None, compression="none")` / `@srv.burst_producer(topic)` | `_on_server_produce`：encode → 统计 → 路由 → `send_sync_data`（PUSH→PULL 投到数据面线程广播） |
| `ProducerClient`（客户端定时发布） | `@pc.producer(...)`（默认 serializer="msgpack"）/ `@pc.burst_producer(...)` | `_on_produce`：`await self.publish(topic, data, ...)`（走完整客户端链路） |

## 2. 类型契约（`types.py`）

```python
PubData: TypeAlias = Union[pd.DataFrame, dict, bytes, str]   # 数据白名单
ProducerCallback: TypeAlias = Callable[[], Awaitable[PubData | None]]
OnMessageCallback = Callable[[ProducerSpec, PubData], Awaitable[None]]
```

`PubData` 与 `frames.encode` 运行时白名单（推断为 UNKNOWN 即 TypeError）**一一对应**——静态类型与运行时校验同源，回 None 表示本轮不发（burst 模式下表示终止）。

## 3. ProducerSpec 与注册

```python
ProducerSpec(name, callback, interval=5.0, serializer=None, compression="none")
```

- `name` = topic 名（注册键，同名覆盖）。
- `interval=0.0` 是 burst 模式标记。
- `serializer=None` → encode 时按数据类型选默认（DATAFRAME→pyarrow，DICT→msgpack…）。

`register / register_burst` 只写 `_specs`，实际任务在 `start_all(on_message)` 时统一创建——支持"先装饰、后启动"的组装顺序。`stop_all` cancel 全部任务并 `gather(return_exceptions=True)`。

## 4. 两种调度循环

### 4.1 固定延迟调度（`_run_loop`，interval > 0）

```
loop：
  t0 = monotonic
  data = await callback()          # 异常 → warning 日志，继续下轮
  data 非 None → await on_message(spec, data)
  sleep(max(0, interval - (monotonic - t0)))   # 补齐剩余间隔
  执行耗时 ≥ interval 时 sleep(0) 让出控制权     # 不积压、不追赶
```

语义是**固定延迟**（fixed-delay）而非固定速率：回调耗时会被计入周期，慢回调自动降频而不产生任务堆积。

### 4.2 Burst 调度（`_run_burst_loop`，interval == 0）

```
loop：
  data = await callback()
  data is None → break            # 回调主动结束
  await on_message(spec, data)
  异常 → warning + sleep(0.1) 冷却  # 防错误空转
```

无间隔连发，用于极限性能测试；取消经 `CancelledError` 正常退出。

## 5. 容错边界

- 回调异常**不杀循环**（普通模式直接下轮，burst 模式冷却 0.1s），调度可用性优先。
- `on_message` 抛错同样被外层捕获——服务端 `_on_server_produce` 内部已自吞发送异常；客户端 `publish` 失败（未连接等）表现为该轮丢弃。
- 任务以 `producer-{name}` 命名，便于诊断。
