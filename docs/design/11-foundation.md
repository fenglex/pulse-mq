# 11 基础设施设计

> 源码：`config.py` / `errors.py` / `logging_setup.py` / `lifecycle.py`（见 04 文档 §11）/ `cli/*` / `pyproject.toml` / `__init__.py`

## 1. 配置体系（`config.py`）

加载顺序：**TOML 文件 → 环境变量覆盖 → 显式构造参数再覆盖**（Server 构造函数中显式传值优先于 config，空串视为未传）。零配置可启动（全字段有默认值）。

### 1.1 ServerConfig 字段 → TOML 段 → 环境变量

| 字段（默认值） | TOML 位置 | 环境变量 |
|----------------|-----------|----------|
| data_endpoint（tcp://0.0.0.0:5555） | `[server]` | `PULSEMQ_DATA_ENDPOINT` |
| control_endpoint（…:5556） | `[server]` | `PULSEMQ_CONTROL_ENDPOINT` |
| admin_endpoint（0.0.0.0:9090） | `[server]` | `PULSEMQ_ADMIN_BIND` |
| heartbeat_timeout（6.0） | `[server]` | `PULSEMQ_HEARTBEAT_TIMEOUT` |
| stats_db（sqlite://./data/pulsemq_stats.sqlite） | `[server]` | — |
| stats_retention_minutes（480） | `[server]` | `PULSEMQ_STATS_RETENTION_MINUTES` |
| sndhwm / rcvhwm（10000/10000） | `[server]` | `PULSEMQ_SNDHWM` / `PULSEMQ_RCVHWM` |
| credentials_file（./data/pulsemq_users.toml） | `[auth]` | `PULSEMQ_CREDENTIALS_FILE` |
| allow_auto_generated_credentials（true） | `[auth]` | — |
| password_hash_algo（bcrypt） | `[auth]`（非 bcrypt 回退+告警） | — |
| bcrypt_cost（12） | `[auth]` | `PULSEMQ_BCRYPT_COST` |
| admin_token（""） | `[monitoring]` | `PULSEMQ_ADMIN_TOKEN` |
| admin_token_file（./data/pulsemq_admin.token） | `[monitoring]` | — |
| sse_interval（1.0）*未接线* | `[monitoring]` | `PULSEMQ_SSE_INTERVAL` |
| latency_sample_rate（0.01） | `[monitoring]` | `PULSEMQ_LATENCY_SAMPLE_RATE` |
| event_ring_size（200） | `[monitoring]` | — |
| stats_archive_batch_size（50） | `[monitoring]` | — |
| admin_thread（true） | `[monitoring]` | — |
| ui_enabled（true）*未接线* | `[monitoring]` | — |
| retention_days（7）*未接线* | `[monitoring]` | `PULSEMQ_RETENTION_DAYS` |

`auth.type` 仅接受 plain/PLAIN，否则 `ConfigurationError`。`__post_init__` 强制创建 `data/` 目录（日志/SQLite/凭据/token 统一存放）。

另有一处仅环境变量入口：`PULSEMQ_ADMIN_PASSWORD`（首启自动生成 admin 的密码，`security.py` 内读取）。

### 1.2 ClientConfig

字段：data/control endpoint、username、password（`PULSEMQ_USERNAME/PASSWORD` 可覆盖）、client_id（uuid4）、heartbeat_interval=1.0、reconnect 三参数（1s/30s/×2）、decode_queue_size=0。`load_client_config` 为工具函数；`Client` 构造函数另支持全部参数直传（实际用户主要走直传）。

## 2. 异常体系与退出码（`errors.py`）

`PulseMQError` 基类携带 `exit_code` 类属性；`exit_code_for(exc)` 供 CLI 统一换算（非 PulseMQError → 1）。

| 异常 | exit_code | 场景 |
|------|-----------|------|
| `PulseMQError` | 1 | 兜底 |
| `TransportError` / `ConnectionError` | 2 | 传输/未连接操作（`ConnectionError` **故意遮蔽内置同名**，包内显式导入） |
| `AuthenticationError` | 3 | 认证失败（启动与重连两处；附 `reason`） |
| `ClientStartupError` | 4 | 服务器不可达 / REGISTER 被拒或超时（附 `reason/address/username`） |
| `FrameError` / `SerializationError` | 5 | 帧损坏、未注册格式 |
| `ConfigurationError` / `SecurityError` | 6 | 配置非法、凭据文件解析失败 |
| `ResourceExhaustedError` | 7 | 预留 |

## 3. 日志（`logging_setup.py`）

- loguru 双 sink：stderr（文本格式 `time | level | module:line | message`，可选 JSON serialize）+ 文件 `data/logs/pulsemq_YYYY-MM-DD.log`（每日滚动、保留 30 天、UTF-8）。
- `log_event(level, event_type, **fields)`：结构化生命周期事件（`[AUTH] action=... k=v` 风格），供安全/连接事件统一格式。
- `_CONFIGURED` 幂等标志；CLI 入口调用 `setup_logging()`。

## 4. CLI 入口（`cli/`）

| 命令 | 模块 | 行为 |
|------|------|------|
| `pulsemq` / `pulsemq-server` | `cli/server.py` | `setup_logging()` → `Server()` → `asyncio.run(run_server)`；`PulseMQError` 打印 FATAL + 按 exit_code 退出 |
| `pulsemq-users` | `cli/users.py` | 直接读写凭据文件，**不连 Server** |

users 子命令（`--file` 默认 `./data/pulsemq_users.toml`，置于子命令前后均可）：

```
add <username> [--password] [--roles=a,b]   # 密码缺省 getpass 交互输入
passwd <username> [--password]
list
disable <username> / enable <username>
reload                                     # POSIX + PULSEMQ_PID 才可用（发 SIGHUP），
                                           # Windows 明确提示走 admin 接口，exit 6
```

`add` 对不存在的新文件可直接创建（跳过 load）；`SecurityError` 统一 exit 6。

## 5. 打包与入口（`pyproject.toml`）

- 构建后端 hatchling；`requires-python >= 3.13`。
- wheel 收录 `src/pulsemq` + `artifacts = ["src/pulsemq/admin/static/*.js"]`（本地化 ECharts 必须随包分发）；sdist `exclude = ["nul"]`（Windows 下误生成 `nul` 文件的防御）。
- console scripts：`pulsemq` / `pulsemq-server` / `pulsemq-users`。
- dev 依赖组：pytest / pytest-asyncio（`asyncio_mode=auto`）/ hypothesis / build / twine。
- uv 源指向清华镜像（`[[tool.uv.index]]`）。

## 6. 包初始化（`__init__.py`）

- win32 平台导入即设 `WindowsSelectorEventLoopPolicy`（pyzmq 与 Proactor 不兼容的预防）。
- 导出面：`Client / ProducerClient / ConsumerClient / Server / PulseMessage / PubData / __version__`（版本集中于 `_version.py`，AdminServer 的 `SERVER_VERSION` 也从其读取，避免与包版本脱节）。

## 7. 现状备注（反向分析发现，非设计目标）

以下配置/能力已定义但**当前源码无调用方**，属预留或未接线状态，使用方不应依赖：

| 项 | 现状 |
|----|------|
| `monitoring.sse_interval` | SSE 广播周期硬编码 1s |
| `monitoring.ui_enabled` | UI 恒可访问 |
| `monitoring.retention_days` + `StatsStorage.cleanup()` | 无调用方（分钟统计表无自动清理） |
| `ClientConfig` / `load_client_config` | 工具函数齐备，`Client` 走构造参数直传 |
| `ResourceExhaustedError` | 无抛出点 |
| `Transport.send_sync_direct` | 数据面线程内直接发送的备用通道（当前广播走 `broadcast_sync`） |
| `tests/` 目录 | 仅有 `__pycache__`，无测试源文件 |
