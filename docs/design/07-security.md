# 07 安全设计

> 源码：`src/pulsemq/security.py`（凭据存储）+ `src/pulsemq/auth.py`（认证决策器）+ `src/pulsemq/admin/auth.py`（Admin Token）

## 1. 三层安全面

| 层 | 机制 | 保护对象 |
|----|------|----------|
| 消息面接入 | ZAP PLAIN + bcrypt（`CredentialStore` + `PlainAuth` + 两个 ZAP Handler） | 数据面/控制面 socket |
| 管理面 | Token（`TokenAuth`，hmac.compare_digest） | Admin HTTP 全部路由（除 `/healthz`） |
| 静态凭据 | TOML 文件只存哈希；原子写；名称校验 | 落盘凭据文件 |

## 2. CredentialStore（`security.py`）

### 2.1 存储格式（TOML）

```toml
[users.admin]
hashed_password = "$2b$12$..."   # bcrypt
roles = ["admin"]
enabled = true
created_at = "2026-01-01T00:00:00Z"
```

内存模型 `UserInfo(username, hashed_password, roles, enabled, created_at)`。

### 2.2 关键行为

| 行为 | 实现要点 |
|------|----------|
| 加载 `load()` | 文件存在 → 解析 `[users]` 表；不存在 → 默认生成分支（§2.3）。返回值 = 自动生成的明文密码（仅供启动日志输出一次），已存在返回 None |
| 热更新 `reload()` | 重读文件整体替换 `_users`（原子换白名单）；内存态/文件缺失 no-op |
| 持久化 `save()` | **原子写**：写 `<path>.tmp` → `os.replace`；内存态 no-op |
| 校验 `verify()` | 依次判 user_not_found → user_disabled → bcrypt 比对 invalid_password → 成功返回 roles。返回 `AuthResult(success, username, reason, roles)` |
| 管理 | `add_user`（重名抛错）/ `set_password` / `set_enabled` / `list_users` |
| 密码生成 | 16 位，保证含大小写+数字+符号（`secrets.choice`） |
| 内存态 | `from_dict(creds)` 类方法：不落盘、save/reload no-op，供 Server 显式明文 dict（测试/兼容）与 CLI 场景 |

### 2.3 首启默认 admin

文件不存在时：`allow_auto_generated_credentials=false` → `ConfigurationError`；否则密码取环境变量 `PULSEMQ_ADMIN_PASSWORD` 或随机生成，建 `admin` 用户（roles=["admin"]），哈希落盘，**明文只在 stderr 打印一次**并记 WARNING 结构化日志。

### 2.4 防注入与算法策略

- **名称准入**：用户名与角色必须匹配 `^[A-Za-z0-9_-]{1,64}$`——阻断 `.`、`]`、`"`、换行等进入 `save()` 的 f-string 拼接，防 TOML 结构损坏/注入。
- `password_hash_algo` 配置接受 argon2 等值但**回退 bcrypt 并告警**（预留接口，防误导）。
- bcrypt cost 默认 12（`bcrypt_cost` 可配）。

## 3. PlainAuth（`auth.py`）

薄适配器，让 `CredentialStore` 满足 ZAP Handler 的决策器接口：

```python
verify(username, password) -> (bool, reason | None)   # ZAP 兼容签名
authenticate(...) -> AuthResult                         # 语义化接口
from_file(path)                                         # 便捷构造
```

模块 docstring 明确：PLAIN 是本项目唯一支持且强制启用的机制。

## 4. ZAP PLAIN 全链路

```
客户端                          服务端
DEALER 设 plain_username/pw
        │ zmq 握手
        ▼
ROUTER(plain_server=True) ──▶ ZAP REP（inproc://zeromq.zap.01）
                              │ username/password 取自 ZAP 帧 6/7
                              │ verify = bcrypt.checkpw（~200ms）
                              │   异步 ZAP：run_in_executor 防卡 loop
                              │   同步 ZAP：数据面自有线程内直接调用
                              ▼
                     200 OK / 400 INVALID（6 帧标准回复）
        │ monitor                        │ on_auth 回调
        ▼                                ▼
handshake_ok / auth_failed      ConnectionStats.on_auth(user, "", ok, reason)
```

- reason 词表：`user_not_found` / `invalid_password` / `user_disabled`（与 `AuthResult.reason` 一致）。
- 认证事件（含失败）进入事件环，在 Web UI 事件流中可见。

## 5. Admin Token（`admin/auth.py`）

```python
TokenAuth(expected_token)
  .enabled         # 空 token → 禁用校验（放行，向后兼容）
  .validate(headers, query)
```

- 携带方式：`Authorization: Bearer <token>` 优先，其次 `?token=<token>` query。
- 比较用 `hmac.compare_digest`（恒时比较，防时序侧信道）。
- 豁免：`/healthz`。
- token 解析优先级与随机生成流程见 [04-server.md](04-server.md) §5（显式参数 > config > env > 随机 32B base64url 写 0600 文件）。

## 6. 凭据热更新路径

```
方式一（POSIX）：修改 TOML → pulsemq-users reload（或 kill -HUP）
                → Server SIGHUP handler → store.reload() → 原子换内存白名单
方式二（Windows/通用）：admin 接口（代码中预留；当前 Server 未暴露 reload 路由）
```

CLI `reload` 子命令：仅 POSIX + `PULSEMQ_PID` 环境变量定位进程发 SIGHUP；Windows 明确提示不支持（见 [11-foundation.md](11-foundation.md) §5）。

## 7. 其他安全相关细节

- 静态资源路由拒绝 `..`、绝对路径、反斜杠，且 resolve 后必须仍在 `STATIC_ROOT` 内（路径穿越防护，见 [09-admin.md](09-admin.md)）。
- token 文件权限：POSIX 上 chmod 600 后校验实际 mode，组/其他位可读时告警；Windows 提示依赖目录 ACL。
- 认证失败与凭据文件解析失败分别有独立退出码/异常（`AuthenticationError` exit 3；`SecurityError` exit 6）。
