# 04 服务端设计

> 源码：`src/pulsemq/server.py`（组装层）+ `src/pulsemq/lifecycle.py`（启停编排）

## 1. 定位

`Server` 是纯组装层：持有全部子系统实例，运行 3 个后台 asyncio 任务 + 1 个同步数据面线程 + 1 个 admin 线程，本身不含业务算法。生命周期编排（信号处理）在 `lifecycle.run_server`。

## 2. 组装清单（`__init__` 构造顺序）

| 组件 | 类型 | 配置来源 | 备注 |
|------|------|----------|------|
| `_cfg` | `ServerConfig` | TOML + env（`load_server_config(None)`） | 显式构造参数**优先于** config（空串视为未传） |
| `_credential_store` | `CredentialStore` | 见 §3.1 | |
| `_auth` | `PlainAuth` | 包装 store | ZAP 决策器 |
| `admin_token` / `_token_auth` | str / `TokenAuth` | 见 §5 | 空串 = 禁用校验 |
| `_transport` | `Transport` | sndhwm/rcvhwm | |
| `_routing` | `SubscriptionTable` | — | |
| `_registry` | `OnlineRegistry` | heartbeat_timeout（默认 6s） | |
| `_stats` | `TrafficStats` | stats_retention_minutes（默认 480=8h） | |
| `_storage` | `StatsStorage` | stats_db（默认 `sqlite://./data/pulsemq_stats.sqlite`） | |
| `_lat_half` / `_lat_e2e` | `LatencyStatsRegistry` ×2 | latency_sample_rate（默认 0.01） | 半程 / 端到端两套 |
| `_connections` | `ConnectionStats` | 注入 `registry.snapshot` 回调；ring 200 | 构造顺序须在 registry 后 |
| `_drop_stats` | `DropStats` | 1h 窗口 | |
| `_credits` | `dict[bytes, int]` | — | 信用流控表：ident → 剩余容量 |
| `_archive_writer` | `AsyncArchiveWriter` | batch 50 | 须在 storage 后构造 |
| `_ident_by_client_id` | `dict[str, bytes]` | — | client_id → bytes ident 反查 |
| `_producer_mgr` | `ProducerManager` | — | `@srv.producer` 注册 |

## 3. 凭据初始化（§3.1）与 admin token（§5）

### 3.1 凭据源三分支

```
credentials 显式 dict  → CredentialStore.from_dict()   # 内存态（测试/兼容），bcrypt 哈希后持有
否则 credentials_file（默认 ./data/pulsemq_users.toml）
  ├─ 文件存在 → 正常加载，log INFO
  └─ 文件不存在
       ├─ allow_auto_generated_credentials=false → ConfigurationError
       └─ 自动生成默认 admin（密码取 PULSEMQ_ADMIN_PASSWORD 或随机 16 位）
          → 哈希落盘，明文仅此一次输出到 stderr + WARNING 日志
```

### 3.2 SIGHUP 热更新

`_install_sighup_reload`：Linux 主线程注册 `SIGHUP → reload_credentials()`（`store.reload()` 原子替换内存白名单）；Windows/非主线程静默跳过（CLI 侧引导走 admin 接口）。

## 4. 启动顺序（`start()`）

```
1. _storage.connect()                       # SQLite 连接 + 建表（WAL）
2. _transport.bind_sync_data(5555, auth, on_auth, on_message, loop)
                                            # 同步数据面线程 + SyncZAP；
                                            # ZAP ctx 单例 ⇒ on_auth 必须在此提供
3. await _transport.bind(5556, "control", auth)
4. await _archive_writer.start()            # 必须先于 minute_roll_loop
5. _admin = AdminServer(...); await _admin.start()
6. 启动 3 个后台任务（见 §6）
7. logger.info 启动完成
8. _install_sighup_reload()
9. 若有已注册 producer → _producer_mgr.start_all(_on_server_produce)
```

## 5. admin token 解析（`_resolve_admin_token`）

优先级：**显式参数（含空串=禁用）> config `monitoring.admin_token`（含被 env 覆盖值）> 环境变量 `PULSEMQ_ADMIN_TOKEN` > 随机生成**。

随机生成路径：32 字节 `secrets.token_bytes` → base64url 去填充 → 写 `admin_token_file`（chmod 600，Windows 提示目录 ACL）→ stderr 输出一次 + WARNING 日志。写文件失败仅告警，token 仍生效（当次会话）。

## 6. 后台任务

| 任务 | 周期 | 职责 |
|------|------|------|
| `_control_loop` | 事件驱动 | recv 控制面 → `decode_control` → `_dispatch_control`；单帧异常不退出循环 |
| `_heartbeat_sweep_loop` | 1s | `registry.sweep_timeout()` 清超时条目 → 按 `_ident_by_client_id` 反查 bytes ident 清路由 + 信用 → `on_disconnect("heartbeat_timeout")` 事件 + WARNING 日志 |
| `_minute_roll_loop` | 60s | `traffic.roll_minute()` 有归档则 `archive_writer.enqueue`；半程/全程延迟 `roll_minute()`；`drop_stats.roll_minute()` |

心跳超时清理必须经过 `client_id → ident` 反查：registry 只存字符串 client_id，而路由表键是 bytes identity（架构 D4）。

## 7. 控制命令分发（`_dispatch_control`）

| 命令 | payload 字段 | 服务端动作 | 回复 |
|------|--------------|-----------|------|
| REGISTER | client_id, username, endpoint, roles, topics, request_id | `registry.register`：username 已在线 → `ALREADY_ONLINE`；OK 时写 `_ident_by_client_id`、逐 pattern 写路由表、`on_connect` 事件 | `{result, request_id}` |
| HEARTBEAT | client_id, drops?, credit? | `registry.heartbeat` 刷新 last_seen；`drops` 逐 topic 记入 `DropStats`（兼容老客户端无该字段）；`credit` 写 `_credits[ident]` | `{result:"OK", request_id}` |
| SUBSCRIBE | client_id, topic, request_id | 路由表 subscribe + `registry.subscribe` 回写 topics（保持监控计数一致）+ `on_subscribe` 事件 | OK |
| UNSUBSCRIBE | client_id, topic, request_id | 对称移除 + 事件 | OK |
| DISCONNECT | client_id, request_id | `routing.remove(ident)` + 清映射/信用 + `registry.unregister` + `on_disconnect("disconnect")`；**回执失败仅 debug**（客户端发完即关 socket，ROUTER_MANDATORY `Host unreachable` 是预期竞态） | OK（尽力） |
| LATENCY_REPORT | topic, latency_ns | `_lat_e2e.record`（fire-and-forget，无回复） | — |

回复均按 `request_id` 回写，供客户端 `_recv_control_reply` 匹配。

## 8. 数据面回调（`_on_data_message`，运行于数据面线程）

```
decode_header 失败 → debug 丢弃
TrafficStats.record(topic, record_count, len(raw_payload))
should_sample()（计数器采样）→ _lat_half.record(topic, now_ns - ts_ns)   # 半程延迟
matched = _routing.match(topic)
dropped = _transport.broadcast_sync(matched, frame, _credits)
dropped > 0 → _drop_stats.record(topic, dropped)
```

全程同步、无 asyncio 调度；`_credits` 由控制面写、数据面读（GIL 下 dict 读原子）。

## 9. 内置 producer（服务端侧定时推送）

```python
@srv.producer("market.tick", interval=2.0, serializer="msgpack")
async def gen(): return {...}

@srv.burst_producer("topic")               # 无间隔连发，回调返回 None 停止
```

回调由 `ProducerManager` 调度（见 [10-producers.md](10-producers.md)），产物经 `_on_server_produce`：

```
encode(spec.name, data, serializer, compression)
→ decode_header → TrafficStats.record + 延迟采样     # 与客户端消息同一统计口径
→ match(topic) → send_sync_data(ident, frame)        # PUSH→PULL 投递到数据面线程发出
```

发送失败静默（`except Exception: pass`）——内置推送属尽力而为。

## 10. 关闭顺序（`stop()`）与依赖关系

```
1. _running=False, _stop.set()；取消并等待 3 个后台任务
2. 最终 roll_minute() 一次并入队 archive_writer         # 停机前抢救当前分钟统计
3. _producer_mgr.stop_all()
4. _admin.stop()
5. _archive_writer.stop()        # drain 队列剩余 → 落库
6. _transport.close()            # 数据面线程 + 异步 sockets + ZAP
7. _storage.close()              # 必须最后：晚于 archive_writer 的 drain
```

顺序约束（源码注释）：`storage.close` 必须在 `archive_writer.stop` 之后，否则 drain 写入已关闭的 SQLite 连接。

## 11. lifecycle.run_server（进程级编排）

- 注册 SIGINT/SIGTERM → `_request_shutdown`：`server.stop()` 作为后台任务启动，置位 `_stop` 使 `wait_for_shutdown()` 返回。
- `await server.start()` → `await server.wait_for_shutdown()` → 等待 stop 任务完成，**超时 10s 强制返回**（防某个清理步骤卡死进程）。
- Windows 不支持 `add_signal_handler` 时静默跳过（靠 KeyboardInterrupt 兜底）。
