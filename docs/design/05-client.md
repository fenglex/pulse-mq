# 05 客户端设计

> 源码：`src/pulsemq/client.py`

## 1. 类层次与角色

```
Client                       # _roles = ["publisher", "subscriber"]，全能力
├── ProducerClient           # _roles = ["publisher"]；subscribe → NotImplementedError
│                            #   持有 ProducerManager，支持 @producer / @burst_producer 装饰器
└── ConsumerClient           # _roles = ["subscriber"]；publish → NotImplementedError
```

角色字符串进入 REGISTER payload，服务端 `_role_of` 据此把客户端归类为 producer/consumer/both（按 'pub'/'sub' 子串判定）。

## 2. 关键常量

| 常量 | 默认值 | 含义 |
|------|--------|------|
| `_STARTUP_MONITOR_TIMEOUT` | 5.0s | 启动时等 monitor 认证裁定上限 |
| `_REGISTER_REPLY_TIMEOUT` | 3.0s | REGISTER 回复超时 |
| `_HEARTBEAT_INTERVAL` | 1.0s | 心跳周期 |
| `_RECONNECT_INITIAL_DELAY` / `_BACKOFF_MULTIPLIER` / `_MAX_DELAY` | 1s / ×2 / 30s | 指数退避参数 |
| `_RECONNECT_MONITOR_TIMEOUT` | 5.0s | 重连单次认证裁定超时 |
| `latency_sample_rate` | 0.01 | 端到端延迟采样率（`random.random()`） |
| `decode_queue_size` | 0 | 0=内联解码（单线程）；>0=启用 worker 线程 + 丢弃队列 |

## 3. 启动状态机（`start()`）

认证检测采用 **monitor-based 设计**（而非"握手成功即认证成功"）：

```
数据面 DEALER connect(PLAIN, monitor, identity=client_id)
   └─ _on_startup_monitor：仅 handshake_ok / auth_failed 可 resolve future
        │（connected/disconnected/other 被忽略，服务器宕机表现为超时）
        ▼  asyncio.wait_for(startup_event, 5s)
handshake_ok ──▶ _connected=_authenticated=True
auth_failed  ──▶ 关闭半开 transport → raise AuthenticationError      # exit 3
超时/其他     ──▶ 关闭半开 transport → raise ClientStartupError     # exit 4
        ▼
控制面 DEALER connect(PLAIN, 无 monitor)          # 复用数据面认证态
        ▼
_register()：
    发 REGISTER{client_id, username, endpoint, roles, topics, request_id}
    _recv_control_reply 按 request_id 匹配（不匹配帧丢弃；旧 server 无 id 时退化直接返回）
    超时 → ClientStartupError("REGISTER_REJECTED")
    result != OK → ClientStartupError(reason=result)
        ▼
恢复既有订阅（重连场景）：逐 pattern _send_subscribe
        ▼
decode_queue_size>0 → 建 _DropQueue + worker 线程
        ▼
启动 _recv_loop + _heartbeat_loop；monitor 回调切换为 _on_runtime_monitor
        ▼
on_connected 生命周期回调（可选）
```

订阅 API `subscribe(pattern, callback, header_only=False)` 在未连接时仅缓存本地（`_subscriptions` + `_sub_table` + `_sub_header_only`），`start()` 末尾统一 flush——支持"先 subscribe 后 start"的写法。

## 4. 运行期重连状态机（`_reconnect_loop`）

触发：`_on_runtime_monitor` 收到 `disconnected` 且未在重连且未 stop。幂等保护：`_reconnecting` 标志防多事件派生多个重连任务。

```
清理：cancel recv/heartbeat → 置三标志 False → close 旧 transport（保留 _subscriptions 作恢复源）

loop（_stop 未置位）：
  new Transport → connect(data, monitor, 同一 identity)
  ├─ connect 异常 ────────────────▶ 退避重试
  ▼ 等认证裁定（5s）
  auth_failed ──▶ _reconnect_fatal = AuthenticationError；_stop.set()；return
  非 handshake_ok ──▶ 退避重试
  ▼
  self._transport = new_transport
  connect(control) → _register() → 逐 pattern _send_subscribe
  ├─ 任一失败（含 ALREADY_ONLINE）──▶ 关闭 in_flight、换占位 Transport、退避重试
  ▼ 全部成功
  重启 recv/heartbeat；monitor 切回 _on_runtime_monitor；_reconnecting=False；return

退避：delay = 1s → ×2 → 封顶 30s；_backoff_sleep 监听 _stop，触发立即返回（不阻塞优雅停机）
```

三个工程要点（源码注释沉淀）：

1. **后台任务不 raise**：`_reconnect_loop` 是后台任务，直接 raise 会被 asyncio GC 吞掉（进程不会 exit 3）。致命错误存 `_reconnect_fatal` 实例字段 + `_stop.set()`；`run_forever`/`_wait_stop_and_raise_fatal` 在**主任务上下文**重抛，CLI 经 `exit_code_for` 得到正确退出码。
2. **in_flight transport 管理**：重连未走完完整成功路径前，`in_flight` 指向本次新 transport；`except BaseException`（覆盖 `CancelledError`）统一关闭，防半连接 socket/monitor 任务泄漏；完整成功后才置 None，此后生命周期交给运行期 monitor 与 `stop()`（避免双关）。
3. **ALREADY_ONLINE 偏差**（源码显式记录）：服务端 stale 记录要等心跳超时（~6s）才释放，若按原始规范立即退出会让任何网络闪断后的重连被旧条目击落——故视为暂态退避重试。

## 5. 消费路径与两线程模型

### 5.1 `_recv_loop`（事件循环线程，轻量）

```
recv(data) → decode_header（失败丢弃）
→ 采样：random() < rate → LATENCY_REPORT{topic, latency_ns=now-ts} 回传控制面
→ 本地 _sub_table.match(topic)（前缀索引）→ [(callback, header_only)]
→ 无匹配 → continue
→ decode_queue 存在 → queue.put((frame, hdr, matched))
  否则 → _inline_decode_and_dispatch
```

`header_only=True` 的订阅回调只收 `FrameHeader`，跳过完整 decode（低延迟场景）。是否 decode 按「该消息的匹配订阅中存在任一非 header_only」决定。

### 5.2 `_DropQueue`（有界丢弃队列，线程安全）

- `deque(maxlen=N)` + `threading.Condition`；recv 线程 put / worker 线程 get。
- **满时丢最老**：append 自动挤出最左项，put 先 peek 最老项的 topic 计入 `_drop_counts`。
- `drain_drops()`：取走并清零计数 → 心跳上报 `drops`。
- `remaining()`：剩余容量 → 心跳上报 `credit`（服务端信用流控依据）。
- `get_batch(timeout, max_items=64)`：一次锁获取取多条，减少锁竞争。

### 5.3 `_decode_worker_loop`（worker 线程）

批量出队 → 逐条完整 decode（任一订阅需要时）→ 分发：
- 同步回调：worker 线程直接调用（零调度开销）；
- 异步回调：`run_coroutine_threadsafe(cb(target), self._loop)` 调度回事件循环。

回调异常逐条捕获记日志，不影响其余消息。

## 6. 心跳（`_heartbeat_loop`）

周期 1s，payload：`{client_id}` +（有解码队列时）`drops: {topic: count}`（drain 自上次心跳）+ `credit: 剩余容量`。发送失败仅 debug。心跳回复 ack 不消费（fire-and-forget，已知串扰限制见 §9）。

## 7. publish

`@require_connected` 装饰器：`_connected` 或 `_authenticated` 不满足直接抛 `ConnectionError`（项目自定义异常，exit 2）。编码经 `frames.encode`（类型白名单校验在此生效），经数据面 DEALER 发送。

## 8. 停机（`stop()`）与运行（`run_forever`）

```
run_forever：start → 注册 SIGINT/SIGTERM(_stop.set, Windows 跳过)
             → await _wait_stop_and_raise_fatal（退出时重抛 _reconnect_fatal）
             → finally stop()

stop：_stop.set → cancel(reconnect/recv/hb) 并等待
    → 关 _DropQueue + join worker（5s）
    → 已注册则发 DISCONNECT（失败仅 debug）
    → close transport → 触发 on_disconnected
```

`ProducerClient.run_forever` 在基类框架内插入 `ProducerManager.start_all/stop_all`，致命错误重抛复用基类机制。

## 9. 已知限制（源码自述）

- **控制面回复朴素匹配**：REGISTER/SUBSCRIBE 各做一次按 request_id 的 recv，心跳 ack 无 request_id 不消费，可能在控制面 socket 堆积并与下一次命令 recv 串扰；单客户端 e2e 场景可接受。
- `_recv_control_reply` 对无 `request_id` 的旧 server 回复退化直接返回（兼容模式）。
