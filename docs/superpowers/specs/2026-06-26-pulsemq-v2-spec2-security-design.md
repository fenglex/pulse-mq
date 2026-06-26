# PulseMQ v2 重构 · Spec 2：安全模块设计

> 版本：v1.0 ｜ 日期：2026-06-26
> 范围：PLAIN 认证的凭据安全加固 + admin HTTP 接口认证
> 关联文档：`docs/PulseMQ_重构架构设计_Client_Server.md`（§3.2.1 security、§5.2 凭据管理、§5.3 Admin 认证）
> 前置：Spec 1 核心架构骨架（`2026-06-26-pulsemq-v2-spec1-core-skeleton-design.md`）
> 执行策略：原地大改、不兼容旧版协议

---

## 1. 目标与非目标

### 1.1 目标

在 Spec 1 的最简明文凭据源基础上，补齐生产级安全能力：

- 密码**哈希存储**（bcrypt），严禁明文保存。
- **默认凭据自动生成**：未提供凭据文件时，Server 启动自动生成 `admin` 用户并输出随机密码到日志；支持环境变量覆盖。
- **用户管理 CLI**：`python -m pulsemq.users` 提供 add / passwd / list / disable / enable。
- 凭据**热更新**（reload）：不重启 Server 即可刷新白名单。
- 用户状态管理：启用 / 禁用，禁用用户认证失败原因 `user_disabled`。
- **admin HTTP 接口强制 token 认证**：当前项目 `:9090` 完全开放，是安全缺口，本 spec 修复。

### 1.2 非目标

- CURVE 非对称密钥 / TLS 隧道 → 阶段 5（跨不可信网络时再评估）。
- 按 topic 的 ACL 授权 → 阶段 5（`routing` 未来扩展）。
- 消息级鉴权 → 明确不做（当前为连接级认证）。
- 凭据的外部安全服务（Vault 等）加载 → 留接口，本 spec 不实现。

### 1.3 与 Spec 1 的衔接

Spec 1 §1.3 留了边界：transport/auth 接口稳定，凭据源可替换。本 spec 实现 `security` 模块替换 Spec 1 的最简 dict 白名单：

- Spec 1 的 `Server(credentials=dict)` 与 ZAP handler 从 dict 查密码的路径**改为**调 `PlainAuth.authenticate`（内部 `bcrypt.checkpw`），不再用明文 dict 查表。
- 本 spec 新增 `security.CredentialStore`，对外暴露 `verify(username, password) -> AuthResult`；`PlainAuth` 委托它做决策；ZAP handler 调 `PlainAuth.authenticate`。
- Spec 1 预留的 `AuthenticationError(reason=user_disabled)` 在本 spec 落地 enabled 字段。
- `config` 的 `[auth]` 块本 spec 完整实现（`credentials_file` / `allow_auto_generated_credentials` / `password_hash_algo`）。

---

## 2. 模块清单

| 模块 | 状态 | 职责 |
|------|------|------|
| `security` | 新增 | 凭据存储 / 哈希 / 加载 / 热更新 / 默认生成 |
| `auth` | 新增 | `PlainAuth` 认证决策（唯一实现，强制启用） |
| `admin/auth` | 新增 | admin HTTP token 认证中间件 |
| `config` | 扩展 | `[auth]` 块完整化 |
| `errors` | 扩展 | 认证失败原因细分落地 |
| `cli/users` | 新增 | `python -m pulsemq.users` 用户管理 CLI |

依赖方向：`security` 不依赖 `auth`；`auth.PlainAuth` 依赖 `security.CredentialStore`；`admin/auth` 依赖 `security`（token 也由 security 发放管理）；`cli/users` 仅依赖 `security`。

---

## 3. security 模块

### 3.1 凭据文件格式

`pulsemq_users.toml`：

```toml
[users.admin]
hashed_password = "$2b$12$N9qo8uLOickgx2ZMRZoMyeIjZAgd8r2nW..."   # bcrypt 哈希
roles = ["publisher", "subscriber"]
enabled = true
created_at = "2026-06-26T16:20:01Z"

[users.alice]
hashed_password = "$2b$12$..."
roles = ["subscriber"]
enabled = true
```

- `hashed_password`：bcrypt 哈希（默认 cost=12），严禁明文。
- `roles`：角色列表，供后续 ACL 与在线用户表展示。
- `enabled`：`false` 时认证返回 `user_disabled`。
- `created_at`：ISO8601，审计用。

### 3.2 核心接口

```python
@dataclass
class AuthResult:
    success: bool
    username: str
    reason: str | None = None   # user_not_found / invalid_password / user_disabled
    roles: list[str] = field(default_factory=list)

class CredentialStore:
    def __init__(self, credentials_file: str | Path,
                 allow_auto_generated: bool = True,
                 hash_algo: str = "bcrypt"):          # bcrypt（默认）/ argon2（预留）
    def load(self) -> None                            # 从文件加载白名单
    def reload(self) -> None                          # 热更新，重新加载文件
    def verify(self, username: str, password: str) -> AuthResult   # bcrypt.checkpw
    def add_user(self, username, password, roles, enabled=True) -> None
    def set_password(self, username, password) -> None
    def set_enabled(self, username, enabled: bool) -> None
    def list_users(self) -> list[UserInfo]
    def save(self) -> None                            # 写回 TOML
```

### 3.3 默认凭据自动生成

生成规则（优先级从高到低，照总体设计 §5.2）：

| 优先级 | 来源 | 行为 |
|--------|------|------|
| 1 | `pulsemq_users.toml` 存在 | 加载该文件，不生成默认用户 |
| 2 | 环境变量 `PULSEMQ_ADMIN_PASSWORD` | 生成 `admin` 用户，密码为环境变量值 |
| 3 | 无配置 | 生成 `admin` 用户，密码为 16 位随机字符串（含大小写字母+数字+符号） |

仅当 `allow_auto_generated_credentials=true` 且文件不存在时走优先级 2/3。`allow_auto_generated_credentials=false` 且文件不存在 → 抛 `ConfigurationError`，Server 拒绝启动。

自动生成后**写回** `pulsemq_users.toml`（哈希存储），后续重启走优先级 1。

Server 启动日志（`print` 到 stderr + `logging`，沿用 Spec 1 `_notice` 策略）：

```
[SECURITY] 未检测到 pulsemq_users.toml，已生成默认用户
[SECURITY] username=admin, password=Rx9#mK2pL$vQ
[SECURITY] 提示：默认凭据仅用于首次启动，请使用 `pulsemq.users` CLI 创建正式用户
```

> 明文密码仅在生成时输出一次到日志，落盘即哈希。

### 3.4 密码哈希

- 默认 `bcrypt`（`bcrypt` 库，cost=12）。`bcrypt` 列为 pyproject 硬依赖。
- `argon2` 作为 `hash_algo` 预留选项（`argon2-cffi`），Spec 2 不强制实现，留接口与配置项。
- 哈希与校验在 ZAP handler 调用栈内完成；bcrypt cost=12 单次校验约 200ms，可接受（仅连接建立阶段，不在消息路径）。

### 3.5 热更新

- `reload()` 重新读取 `pulsemq_users.toml`，原子替换内存白名单。
- 触发方式：CLI `pulsemq.users` 修改文件后发 SIGHUP（或调用 admin 接口，Spec 3 接入），Server 监听信号调 `reload`。
- 热更新不影响已在线连接（认证只在连接建立阶段）；新连接按新白名单判定。

---

## 4. auth 模块

### 4.1 PlainAuth

```python
class PlainAuth:
    """PLAIN 认证决策器。本项目唯一支持且强制启用。"""
    def __init__(self, store: CredentialStore): ...
    @classmethod
    def from_file(cls, credentials_file) -> "PlainAuth": ...
    def authenticate(self, username: str, password: str) -> AuthResult: ...
```

- `authenticate` 委托 `CredentialStore.verify`。
- ZAP handler 调 `authenticate`，根据 `AuthResult.success` 回 200/400，`reason` 进日志与监控事件。
- `auth` 与 `security` 解耦：`auth` 管认证决策，`security` 管凭据生命周期。

### 4.2 强制启用

- `auth.type` 配置项固定为 `plain`，设为其他值 → `ConfigurationError`。
- 不存在 `NullAuth` / `CurveAuth`。所有连接必须 PLAIN 认证，不可关闭。
- Spec 1 的两套 socket（数据面 + 控制面）ZAP 都接 `PlainAuth`。

### 4.3 认证失败原因落地

Spec 1 §8.2 预留的 reason 在本 spec 实现：

| reason | 触发 |
|--------|------|
| `user_not_found` | 用户名不在白名单 |
| `invalid_password` | 密码哈希不匹配 |
| `user_disabled` | `enabled=false` |

`AuthenticationError` 携带 reason，Client 日志按 Spec 1 §8.2 表输出，退出码 3。

---

## 5. admin HTTP token 认证

### 5.1 问题

当前项目 `:9090` 完全开放（Spec 1 沿用现有 AdminServer，仍开放）。本 spec 修复：所有 admin 路由（除 `/healthz`）必须携带有效 token。

### 5.2 token 发放

- 启动时生成随机 token（32 字节，base64url 编码），写入 `./pulsemq_admin.token`（0600 权限）。
- 优先级：配置文件预置 `monitoring.admin_token` > 环境变量 `PULSEMQ_ADMIN_TOKEN` > 随机生成。
- 随机生成时输出到日志：`[ADMIN] 管理接口 token: <token>`（同 `_notice` 策略）。

### 5.3 token 校验

- 请求通过 `?token=xxx` query 或 `Authorization: Bearer xxx` header 携带。
- `admin/auth.TokenAuth` 中间件：除 `/healthz` 外，token 不匹配 → 401。
- token 比较用 `hmac.compare_digest` 防时序攻击。
- 管理接口与数据接口分离：admin 被攻击不影响消息收发（admin 运行在独立线程/事件循环，Spec 3 落地）。

### 5.4 路由认证矩阵

| 路由 | 认证 |
|------|------|
| `GET /healthz` | 无需 token |
| 其余所有 `/`、`/static/*`、`/api/v1/*` | 必须有效 token |

UI 页面通过 `?token=xxx` 携带，前端 JS 从 URL 取 token 放入后续 fetch/SSE 请求 header。

---

## 6. CLI：python -m pulsemq.users

```text
python -m pulsemq.users add alice --password secret --roles publisher,subscriber
python -m pulsemq.users passwd alice
python -m pulsemq.users list
python -m pulsemq.users disable bob
python -m pulsemq.users enable bob
python -m pulsemq.users reload                 # 通知运行中的 Server 热更新（发 SIGHUP）
```

- `add`：用户已存在则报错；密码交互式输入（无 `--password` 时走 `getpass`）。
- `passwd`：交互式输入新密码，更新哈希。
- `list`：表格输出 username / roles / enabled / created_at。
- `disable` / `enable`：切换 enabled。
- CLI 直接读写 `pulsemq_users.toml`（通过 `CredentialStore`），不连 Server。
- `reload`：向 Server 进程发 SIGHUP（通过 pidfile 或 `PULSEMQ_PID` 环境变量定位），Server 收到后调 `CredentialStore.reload`。

CLI 入口在 `pyproject.toml` 注册：`pulsemq-users = "pulsemq.cli.users:main"`。

---

## 7. 配置扩展

`config` 的 `[auth]` 与 `[monitoring]` 块完整化：

```toml
[auth]
type = "plain"                                  # 固定，设其他值报错
credentials_file = "./pulsemq_users.toml"
allow_auto_generated_credentials = true
password_hash_algo = "bcrypt"                   # bcrypt（默认）/ argon2（预留）
bcrypt_cost = 12

[monitoring]
admin_token = ""                                # 空=随机生成；预置则用此值
admin_token_file = "./pulsemq_admin.token"
```

环境变量：`PULSEMQ_ADMIN_PASSWORD`、`PULSEMQ_ADMIN_TOKEN`、`PULSEMQ_CREDENTIALS_FILE`。

---

## 8. errors 扩展

- `AuthenticationError` 落地 `reason` 字段（`user_not_found` / `invalid_password` / `user_disabled`）。
- 新增 `SecurityError(PulseMQError)`：凭据文件解析失败、哈希格式非法等，退出码归 6（配置类）。
- CLI 退出码：用户已存在 / 文件不可写等 → 退出码 6。

---

## 9. 测试策略

### 9.1 新增测试

- `test_security.py`：
  - bcrypt 哈希往返（set_password → verify）。
  - 默认凭据生成三优先级（文件存在 / 环境变量 / 随机）。
  - `allow_auto_generated=false` 且无文件 → `ConfigurationError`。
  - `enabled=false` → `user_disabled`。
  - 热更新：reload 后新白名单生效、已在线连接不受影响。
  - 凭据文件落盘为哈希、不含明文。
- `test_auth.py`：`PlainAuth.authenticate` 各 reason；`auth.type` 非 plain 报错。
- `test_admin_token.py`：
  - `/healthz` 无 token 可访问。
  - 其余路由无 token / 错 token → 401。
  - token query 与 header 两种携带方式都生效。
  - `hmac.compare_digest` 路径（不直接测时序，测正确性）。
- `test_cli_users.py`：add/passwd/list/disable/enable 的文件读写往返；用户已存在报错。

### 9.2 沿用测试

Spec 1 的 e2e（Client/Server 认证闭环）继续通过；本 spec 把凭据源从明文 dict 切换到 `CredentialStore`，e2e 用自动生成的默认 admin 凭据。

### 9.3 安全约束测试

- 落盘文件 grep 不到明文密码。
- 自动生成密码满足复杂度（长度 16、含大小写+数字+符号）。
- token 长度 ≥ 32 字节、随机性（不复用）。

---

## 10. 关键设计决策

| 决策 | 说明 |
|------|------|
| bcrypt 默认哈希 | 标准库支持、成本可调、工业界主流；argon2 预留接口 |
| 默认凭据可进生产 | `admin` 自动生成满足零配置启动；可配置关闭 |
| 明文密码仅生成时输出一次 | 落盘即哈希，日志不残留明文 |
| auth/security 解耦 | auth 管决策，security 管生命周期，便于未来扩展凭据源 |
| admin 强制 token | 修复 `:9090` 开放缺口，与数据接口分离 |
| token 时序安全比较 | `hmac.compare_digest` 防时序攻击 |
| 热更新不影响在线连接 | 认证仅在连接建立阶段，reload 只影响新连接 |
| CLI 直接改文件 + SIGHUP reload | 简单可靠，不引入 admin 写接口（admin 只读） |

---

## 11. 边界与注意事项

1. **PLAIN 明文传输未变**：本 spec 只加固存储侧，传输侧仍是 PLAIN 明文，仅适用于可信内网；跨不可信网络留待阶段 5。
2. **bcrypt 校验延迟**：cost=12 单次约 200ms，连接建立阶段可接受；若高频重连需评估降低 cost 或缓存。
3. **默认凭据风险**：`admin` 自动生成密码虽随机，但首次启动日志可见，生产环境建议首次启动后立即 `passwd` 修改。
4. **token 文件权限**：`pulsemq_admin.token` 必须 0600，否则任意本地用户可读 token；CLI 启动时检查并警告。
5. **热更新竞态**：`reload` 用整体替换内存白名单（引用赋值，GIL 安全），不就地修改 dict，避免校验过程中白名单变动。
6. **CLI 与 Server 并发写文件**：CLI 写文件时 Server 可能正在读；用 `CredentialStore.reload` 重新读取 + 原子写（写临时文件再 rename）规避。
7. **admin token 不走 ZAP**：admin 是 HTTP，与 ZMQ PLAIN 认证是两套独立机制，互不影响。
8. **SIGHUP reload 跨平台**：Windows 无 SIGHUP，Windows 上 `reload` 命令改走 admin HTTP 接口（Spec 3 接入 `/api/v1/admin/reload`，需 token）；Linux 优先 SIGHUP。

---

## 12. 与 Spec 3 的衔接

- `monitoring.admin_token` 在 Spec 3 的 AdminServer 落地（独立线程 + token 中间件）。
- `CredentialStore.list_users` / `AuthResult` 供 Spec 3 的在线 Client 详情、认证事件流展示。
- 本 spec 的 `reload` 信号机制可被 Spec 3 的 admin 接口复用（`/api/v1/admin/reload`）。
