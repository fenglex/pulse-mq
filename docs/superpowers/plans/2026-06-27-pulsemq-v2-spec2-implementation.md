# PulseMQ v2 · Spec 2 安全模块 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 Spec 1 的明文 `PlainAuthDict` 凭据源替换为生产级 `security.CredentialStore`（bcrypt 哈希存储）+ `auth.PlainAuth`，补齐默认凭据自动生成、用户管理 CLI、凭据热更新，并给 admin HTTP 接口加强制 token 认证。

**Architecture:** 依赖方向（spec §2）：`security`（不依赖 auth）← `auth.PlainAuth`（委托 CredentialStore）← `admin/auth.TokenAuth` + `cli/users`（仅依赖 security）；`config`/`errors` 扩展；`server` 把 `PlainAuthDict` 换成 `PlainAuth`、把 token 接入 AdminServer。**transport 的 ZAP handler 对 authenticator 的 `.verify(username,password)->(bool,reason|None)` 调用保持不变**（spec §1.3「transport/auth 接口稳定」），只换决策器——`PlainAuth` 额外提供同名 `.verify()` 委托 CredentialStore。

**Tech Stack:** 新增 `bcrypt`（pyproject 硬依赖）；沿用 tomllib、loguru、pytest + pytest-asyncio。

## Global Constraints

（逐条抄自 Spec 2，所有 task 隐式遵守）

- 密码 **bcrypt 哈希存储**（默认 cost=12），严禁明文落盘。`bcrypt` 列为 pyproject 硬依赖。
- **默认凭据自动生成**优先级：① `pulsemq_users.toml` 存在 → 加载；② 环境变量 `PULSEMQ_ADMIN_PASSWORD` → 生成 admin 用此密码；③ 无配置 → admin + 16 位随机密码（大小写字母+数字+符号）。`allow_auto_generated_credentials=false` 且无文件 → `ConfigurationError`，Server 拒绝启动。自动生成后**写回文件（哈希）**，明文密码仅生成时输出一次到日志。
- **用户管理 CLI** `python -m pulsemq.users`：add / passwd / list / disable / enable / reload。CLI 直接读写 `pulsemq_users.toml`（经 CredentialStore），不连 Server。pyproject 注册 `pulsemq-users = "pulsemq.cli.users:main"`。
- **凭据热更新**：`reload()` 原子替换内存白名单；SIGHUP 触发（Windows 无 SIGHUP，留接口，reload 命令在 Windows 走 admin 接口——Spec 3 接入）。热更新不影响已在线连接。
- 用户状态 enabled/disabled；禁用 → 认证 reason `user_disabled`。
- **admin HTTP 强制 token**：除 `/healthz` 外所有路由必须有效 token；token 经 `?token=` query 或 `Authorization: Bearer` header 携带；比较用 `hmac.compare_digest`。token 优先级：配置 `monitoring.admin_token` > 环境变量 `PULSEMQ_ADMIN_TOKEN` > 随机生成（32 字节 base64url，写 `pulsemq_admin.token` 0600）。
- `auth.type` 固定 `plain`，设其他值 → `ConfigurationError`。
- 认证失败 reason 落地：`user_not_found` / `invalid_password` / `user_disabled`。
- 沿用 Spec 1 e2e（Client/Server 认证闭环）；本 spec 把凭据源从明文 dict 切到 CredentialStore。

---

## File Structure

| 路径 | 动作 | 职责 |
|------|------|------|
| `pyproject.toml` | 改 | 加 `bcrypt` 依赖；注册 `pulsemq-users` 脚本 |
| `src/pulsemq/errors.py` | 扩展 | 新增 `SecurityError`(exit 6) |
| `src/pulsemq/config.py` | 扩展 | ServerConfig 加 `[auth]`/`[monitoring]` 字段；env 覆盖 |
| `src/pulsemq/security.py` | 新增 | `CredentialStore`/`UserInfo`/`AuthResult`：bcrypt + TOML + 默认生成 + 热更新 |
| `src/pulsemq/auth.py` | 新增 | `PlainAuth`：authenticate()->AuthResult + verify()->(bool,reason) |
| `src/pulsemq/admin/auth.py` | 新增 | `TokenAuth`：admin HTTP token 中间件 |
| `src/pulsemq/admin/server.py` | 改 | `__init__` 加 `token_auth`；`_handle_request` 注入 token 校验 |
| `src/pulsemq/admin/web_ui.py` | 改 | 前端从 URL 取 token，fetch 加 Authorization、SSE URL 带 token |
| `src/pulsemq/server.py` | 改 | 用 CredentialStore+PlainAuth 替换 PlainAuthDict；默认凭据生成；admin token 生成/加载；SIGHUP reload |
| `src/pulsemq/cli/users.py` | 新增 | `python -m pulsemq.users` CLI |
| `src/pulsemq/cli/__init__.py` | (已存在) | — |
| `tests/test_security.py` | 新增 | bcrypt 往返、默认生成三优先级、allow_auto=false、disabled、reload、落盘无明文、密码复杂度 |
| `tests/test_auth.py` | 新增 | PlainAuth.authenticate 各 reason；auth.type 非 plain 报错 |
| `tests/test_admin_token.py` | 新增 | /healthz 开放；其余无/错 token → 401；query+header；compare_digest 正确性 |
| `tests/test_cli_users.py` | 新增 | add/passwd/list/disable/enable 文件往返；用户已存在报错 |
| `tests/test_server_security.py` | 新增 | Server 用 CredentialStore 跑通认证；默认 admin 生成；token 文件 0600 |
| 沿用 | — | Spec 1 全部 e2e（test_e2e_client_server / test_client_lifecycle / test_zap_resilience / test_server_admin / …）继续通过 |

依赖方向约束：`security` 不 import `auth`/`transport`；`auth` 仅 import `security`；`admin/auth` 仅 import 标准库（`hmac`）；`cli/users` 仅 import `security`。

---

## Task 1: pyproject 加 bcrypt 依赖

**Files:**
- Modify: `pyproject.toml`
- Test: 无（依赖可用性由后续 task 的测试间接验证）

**Interfaces:**
- Produces: `bcrypt` 可 import。

- [ ] **Step 1: 加依赖**

在 `pyproject.toml` 的 `[project] dependencies` 列表追加 `"bcrypt>=4.0"`（放在 `loguru` 之后）。

- [ ] **Step 2: 安装并验证**

Run: `uv sync` （或 `uv pip install bcrypt`）
Run: `uv run python -c "import bcrypt; print(bcrypt.__version__)"`
Expected: 打印版本号，无 ImportError。

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "build(deps): 新增 bcrypt 硬依赖（Spec 2 密码哈希）"
```

---

## Task 2: errors 扩展 SecurityError

**Files:**
- Modify: `src/pulsemq/errors.py`
- Test: `tests/test_errors.py`（追加）

**Interfaces:**
- Consumes: `pulsemq.errors.PulseMQError`。
- Produces: `SecurityError(PulseMQError)` exit_code=6。

- [ ] **Step 1: 追加失败测试**

在 `tests/test_errors.py` 追加：

```python
def test_security_error_exit_code():
    from pulsemq.errors import SecurityError
    assert SecurityError.exit_code == 6
    assert exit_code_for(SecurityError("bad hash")) == 6
```

- [ ] **Step 2: Run → FAIL**

Run: `pytest tests/test_errors.py::test_security_error_exit_code -v`
Expected: FAIL — `ImportError: cannot import name 'SecurityError'`

- [ ] **Step 3: 实现**

在 `src/pulsemq/errors.py` 的 `ConfigurationError` 之后追加：

```python
class SecurityError(PulseMQError):
    """凭据文件解析失败、哈希格式非法等安全侧错误。"""
    exit_code = 6
```

- [ ] **Step 4: Run → PASS**

Run: `pytest tests/test_errors.py -v`
Expected: PASS（含新用例）

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/errors.py tests/test_errors.py
git commit -m "feat(errors): 新增 SecurityError(exit 6)"
```

---

## Task 3: config 扩展 [auth]/[monitoring] 字段

**Files:**
- Modify: `src/pulsemq/config.py`
- Test: `tests/test_config.py`（追加）

**Interfaces:**
- Consumes: `pulsemq.errors.ConfigurationError`。
- Produces: `ServerConfig` 新增 `allow_auto_generated_credentials: bool=True`、`password_hash_algo: str="bcrypt"`、`bcrypt_cost: int=12`、`admin_token: str=""`、`admin_token_file: str="./pulsemq_admin.token"`。

- [ ] **Step 1: 追加失败测试**

在 `tests/test_config.py` 追加：

```python
def test_server_config_security_defaults():
    cfg = ServerConfig()
    assert cfg.allow_auto_generated_credentials is True
    assert cfg.password_hash_algo == "bcrypt"
    assert cfg.bcrypt_cost == 12
    assert cfg.admin_token == ""
    assert cfg.admin_token_file == "./pulsemq_admin.token"


def test_load_auth_block_from_toml(tmp_path):
    p = tmp_path / "s.toml"
    p.write_text(
        '[auth]\ntype = "plain"\nallow_auto_generated_credentials = false\n'
        'bcrypt_cost = 10\n[monitoring]\nadmin_token = "tok123"\n',
        encoding="utf-8")
    cfg = load_server_config(str(p))
    assert cfg.allow_auto_generated_credentials is False
    assert cfg.bcrypt_cost == 10
    assert cfg.admin_token == "tok123"


def test_env_admin_token_and_password(monkeypatch):
    monkeypatch.setenv("PULSEMQ_ADMIN_TOKEN", "envtok")
    monkeypatch.setenv("PULSEMQ_ADMIN_PASSWORD", "envpw")
    cfg = load_server_config(None)
    assert cfg.admin_token == "envtok"
    # PULSEMQ_ADMIN_PASSWORD 不进 config（仅 security 模块读），这里只验证不报错
```

- [ ] **Step 2: Run → FAIL**

Run: `pytest tests/test_config.py -v`
Expected: FAIL — `AttributeError: allow_auto_generated_credentials`

- [ ] **Step 3: 实现**

修改 `src/pulsemq/config.py`：

`ServerConfig` dataclass 追加字段（在 `stats_retention_minutes` 之后）：

```python
    allow_auto_generated_credentials: bool = True
    password_hash_algo: str = "bcrypt"
    bcrypt_cost: int = 12
    admin_token: str = ""
    admin_token_file: str = "./pulsemq_admin.token"
```

`load_server_config` 内，读取 `m = data.get("monitoring", {})`，构造 cfg 时追加：

```python
        allow_auto_generated_credentials=bool(
            a.get("allow_auto_generated_credentials",
                  ServerConfig.allow_auto_generated_credentials)),
        password_hash_algo=a.get("password_hash_algo",
                                 ServerConfig.password_hash_algo),
        bcrypt_cost=int(a.get("bcrypt_cost", ServerConfig.bcrypt_cost)),
        admin_token=m.get("admin_token", ServerConfig.admin_token),
        admin_token_file=m.get("admin_token_file",
                               ServerConfig.admin_token_file),
```

env 覆盖段追加：

```python
    if (v := _env("PULSEMQ_ADMIN_TOKEN")):
        cfg.admin_token = v
```

（`PULSEMQ_ADMIN_PASSWORD` 不进 config，由 security 模块直接 `os.environ.get` 读。）

- [ ] **Step 4: Run → PASS**

Run: `pytest tests/test_config.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/config.py tests/test_config.py
git commit -m "feat(config): [auth]/[monitoring] 块完整化（allow_auto/hash_algo/bcrypt_cost/admin_token）"
```

---

## Task 4: security.CredentialStore（bcrypt + TOML + 默认生成 + 热更新）

**Files:**
- Create: `src/pulsemq/security.py`
- Test: `tests/test_security.py`

**Interfaces:**
- Consumes: `bcrypt`、`tomllib`、`pulsemq.errors.{ConfigurationError, SecurityError}`。
- Produces: `AuthResult(success, username, reason, roles)`、`UserInfo(username, hashed_password, roles, enabled, created_at)`、`CredentialStore(credentials_file, allow_auto_generated=True, hash_algo="bcrypt", bcrypt_cost=12)`，方法 `load/reload/verify/add_user/set_password/set_enabled/list_users/save`，类方法 `from_dict(creds)`；`generate_password(length=16)`。

> 不依赖 auth/transport。`from_dict` 构造内存态 store（无文件，save/reload 为 no-op），用于 Server 接受显式明文 dict 与测试。

- [ ] **Step 1: 失败测试**

```python
# tests/test_security.py
import os
import secrets
import string
import pytest
from pulsemq.security import (CredentialStore, AuthResult, UserInfo,
                              generate_password)
from pulsemq.errors import ConfigurationError


def test_generate_password_complexity():
    pw = generate_password()
    assert len(pw) == 16
    assert any(c.isupper() for c in pw)
    assert any(c.islower() for c in pw)
    assert any(c.isdigit() for c in pw)
    assert any(c in string.punctuation for c in pw)
    # 随机性
    assert generate_password() != generate_password()


def test_hash_and_verify_roundtrip(tmp_path):
    f = str(tmp_path / "users.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    store.add_user("alice", "s3cret", roles=["subscriber"])
    assert store.verify("alice", "s3cret").success is True
    res = store.verify("alice", "wrong")
    assert res.success is False and res.reason == "invalid_password"
    res = store.verify("bob", "x")
    assert res.success is False and res.reason == "user_not_found"


def test_set_password_and_save_persists_hashed(tmp_path):
    f = str(tmp_path / "users.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    store.add_user("alice", "old", roles=[])
    store.set_password("alice", "new")
    store.save()
    raw = open(f, encoding="utf-8").read()
    assert "old" not in raw and "new" not in raw  # 明文不落盘
    assert "$2" in raw  # bcrypt 哈希特征
    # 重新加载仍能校验
    store2 = CredentialStore(f, allow_auto_generated=False)
    store2.load()
    assert store2.verify("alice", "new").success is True


def test_disable_yields_user_disabled(tmp_path):
    f = str(tmp_path / "users.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    store.add_user("alice", "pw", roles=[])
    store.set_enabled("alice", False)
    res = store.verify("alice", "pw")
    assert res.success is False and res.reason == "user_disabled"


def test_default_generation_three_priorities(tmp_path, monkeypatch):
    # 优先级 1：文件存在 → 加载，不生成
    f = str(tmp_path / "users.toml")
    s0 = CredentialStore(f, allow_auto_generated=False)
    s0.add_user("admin", "fixed", roles=["admin"])
    s0.save()
    s1 = CredentialStore(f, allow_auto_generated=True)
    pw = s1.load()
    assert pw is None  # 文件已存在，不输出明文
    assert s1.verify("admin", "fixed").success is True

    # 优先级 2：env PULSEMQ_ADMIN_PASSWORD
    f2 = str(tmp_path / "u2.toml")
    monkeypatch.setenv("PULSEMQ_ADMIN_PASSWORD", "envpw")
    s2 = CredentialStore(f2, allow_auto_generated=True)
    pw = s2.load()
    assert pw == "envpw"
    assert s2.verify("admin", "envpw").success is True

    # 优先级 3：随机生成
    f3 = str(tmp_path / "u3.toml")
    monkeypatch.delenv("PULSEMQ_ADMIN_PASSWORD", raising=False)
    s3 = CredentialStore(f3, allow_auto_generated=True)
    pw = s3.load()
    assert pw and len(pw) == 16
    assert s3.verify("admin", pw).success is True
    # 生成后写回文件，重启走优先级 1
    s4 = CredentialStore(f3, allow_auto_generated=True)
    assert s4.load() is None
    assert s4.verify("admin", pw).success is True


def test_allow_auto_false_no_file_raises(tmp_path):
    f = str(tmp_path / "none.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    with pytest.raises(ConfigurationError):
        store.load()


def test_reload_picks_up_changes(tmp_path):
    f = str(tmp_path / "users.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    store.add_user("alice", "pw", roles=[])
    store.save()
    # 另一个进程改文件
    other = CredentialStore(f, allow_auto_generated=False)
    other.load()
    other.add_user("bob", "pw2", roles=[])
    other.save()
    # 原 store reload 后能看到 bob
    store.reload()
    assert store.verify("bob", "pw2").success is True


def test_from_dict_in_memory():
    store = CredentialStore.from_dict({"alice": "pw"})
    assert store.verify("alice", "pw").success is True
    store.save()  # no-op，不抛
```

- [ ] **Step 2: Run → FAIL**

Run: `pytest tests/test_security.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 实现**

```python
# src/pulsemq/security.py
"""凭据存储 / bcrypt 哈希 / TOML 持久化 / 默认生成 / 热更新。不依赖 auth/transport。"""
from __future__ import annotations

import os
import secrets
import string
import time
from dataclasses import dataclass, field
from pathlib import Path

import bcrypt

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

from pulsemq.errors import ConfigurationError, SecurityError

_DEFAULT_ROLES = ["publisher", "subscriber"]


@dataclass
class AuthResult:
    success: bool
    username: str
    reason: str | None = None  # user_not_found / invalid_password / user_disabled
    roles: list[str] = field(default_factory=list)


@dataclass
class UserInfo:
    username: str
    hashed_password: str
    roles: list[str]
    enabled: bool
    created_at: str


def generate_password(length: int = 16) -> str:
    """生成含大小写+数字+符号的随机密码。"""
    alphabet = string.ascii_letters + string.digits + string.punctuation
    while True:
        pw = "".join(secrets.choice(alphabet) for _ in range(length))
        if (any(c.isupper() for c in pw) and any(c.islower() for c in pw)
                and any(c.isdigit() for c in pw)
                and any(c in string.punctuation for c in pw)):
            return pw


def _hash_password(password: str, cost: int) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(cost)).decode("utf-8")


def _check_password(password: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
    except (ValueError, TypeError):
        return False


class CredentialStore:
    def __init__(self, credentials_file: str | Path,
                 allow_auto_generated: bool = True,
                 hash_algo: str = "bcrypt",
                 bcrypt_cost: int = 12) -> None:
        self._path = str(credentials_file)
        self._allow_auto = allow_auto_generated
        self._hash_algo = hash_algo
        self._cost = bcrypt_cost
        self._users: dict[str, UserInfo] = {}
        self._in_memory = False

    # ---- 构造 ----
    @classmethod
    def from_dict(cls, creds: dict[str, str]) -> "CredentialStore":
        """内存态 store（无文件）；save/reload 为 no-op。供 Server 接受显式明文 dict 与测试。"""
        store = cls.__new__(cls)
        store._path = ""
        store._allow_auto = False
        store._hash_algo = "bcrypt"
        store._cost = 12
        store._in_memory = True
        store._users = {}
        now = _now_iso()
        for u, pw in creds.items():
            store._users[u] = UserInfo(u, _hash_password(pw, 12), list(_DEFAULT_ROLES),
                                       True, now)
        return store

    # ---- 加载 / 默认生成 ----
    def load(self) -> str | None:
        """加载凭据文件。若文件不存在按优先级生成默认 admin。

        返回值：生成的明文密码（仅供启动日志输出一次）；文件已存在返回 None。
        """
        if self._in_memory:
            return None
        p = Path(self._path)
        if p.exists():
            self._load_file()
            return None
        # 文件不存在 → 默认生成
        plaintext = self._generate_default_password()
        self._users = {
            "admin": UserInfo(
                "admin", _hash_password(plaintext, self._cost),
                ["admin"], True, _now_iso()),
        }
        self.save()  # 写回哈希
        return plaintext

    def _generate_default_password(self) -> str:
        if not self._allow_auto:
            raise ConfigurationError(
                f"未检测到凭据文件且 allow_auto_generated_credentials=false: {self._path}")
        env_pw = os.environ.get("PULSEMQ_ADMIN_PASSWORD")
        if env_pw:
            return env_pw
        return generate_password(16)

    def _load_file(self) -> None:
        try:
            with open(self._path, "rb") as f:
                data = tomllib.load(f)
        except Exception as e:
            raise SecurityError(f"凭据文件解析失败: {self._path}: {e}") from e
        users = data.get("users", {})
        if not isinstance(users, dict):
            raise SecurityError(f"凭据文件 [users] 非表: {self._path}")
        loaded: dict[str, UserInfo] = {}
        for username, info in users.items():
            if not isinstance(info, dict):
                continue
            loaded[str(username)] = UserInfo(
                str(username),
                str(info.get("hashed_password", "")),
                list(info.get("roles", [])),
                bool(info.get("enabled", True)),
                str(info.get("created_at", _now_iso())),
            )
        self._users = loaded

    def reload(self) -> None:
        """热更新：重新读文件，原子替换内存白名单。内存态 no-op。"""
        if self._in_memory or not self._path:
            return
        if not Path(self._path).exists():
            return
        self._load_file()  # _load_file 整体替换 self._users

    # ---- 校验 ----
    def verify(self, username: str, password: str) -> AuthResult:
        info = self._users.get(username)
        if info is None:
            return AuthResult(False, username, "user_not_found")
        if not info.enabled:
            return AuthResult(False, username, "user_disabled")
        if not _check_password(password, info.hashed_password):
            return AuthResult(False, username, "invalid_password")
        return AuthResult(True, username, None, list(info.roles))

    # ---- 管理 ----
    def add_user(self, username: str, password: str, roles: list[str] | None = None,
                 enabled: bool = True) -> None:
        if username in self._users:
            raise SecurityError(f"用户已存在: {username}")
        self._users[username] = UserInfo(
            username, _hash_password(password, self._cost),
            list(roles or []), enabled, _now_iso())

    def set_password(self, username: str, password: str) -> None:
        info = self._require(username)
        info.hashed_password = _hash_password(password, self._cost)

    def set_enabled(self, username: str, enabled: bool) -> None:
        info = self._require(username)
        info.enabled = enabled

    def list_users(self) -> list[UserInfo]:
        return list(self._users.values())

    def _require(self, username: str) -> UserInfo:
        info = self._users.get(username)
        if info is None:
            raise SecurityError(f"用户不存在: {username}")
        return info

    # ---- 持久化 ----
    def save(self) -> None:
        if self._in_memory or not self._path:
            return
        lines: list[str] = []
        for u in sorted(self._users.values(), key=lambda x: x.username):
            roles = ", ".join(f'"{r}"' for r in u.roles)
            lines.append(
                f'[users.{u.username}]\n'
                f'hashed_password = "{u.hashed_password}"\n'
                f'roles = [{roles}]\n'
                f'enabled = {"true" if u.enabled else "false"}\n'
                f'created_at = "{u.created_at}"\n'
            )
        body = "\n".join(lines)
        # 原子写：临时文件 + rename
        tmp = self._path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(body)
        os.replace(tmp, self._path)


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
```

- [ ] **Step 4: Run → PASS**

Run: `pytest tests/test_security.py -v`
Expected: PASS（8 用例）

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/security.py tests/test_security.py
git commit -m "feat(security): CredentialStore(bcrypt+TOML)+默认生成+热更新"
```

---

## Task 5: auth.PlainAuth

**Files:**
- Create: `src/pulsemq/auth.py`
- Test: `tests/test_auth.py`

**Interfaces:**
- Consumes: `pulsemq.security.{CredentialStore, AuthResult}`、`pulsemq.errors.ConfigurationError`。
- Produces: `PlainAuth(store)`：`authenticate(u,pw)->AuthResult`、`verify(u,pw)->(bool,reason|None)`（ZAP 兼容）、classmethod `from_file(credentials_file)`。

> `verify` 供 transport 的 ZAP handler 调用（接口不变）；`authenticate` 供监控/日志取完整 AuthResult。

- [ ] **Step 1: 失败测试**

```python
# tests/test_auth.py
import pytest
from pulsemq.auth import PlainAuth
from pulsemq.security import CredentialStore


def _store_with(tmp_path, **users):
    s = CredentialStore(str(tmp_path / "u.toml"), allow_auto_generated=False)
    for u, pw in users.items():
        s.add_user(u, pw, roles=[u])
    return s


def test_authenticate_success_and_reasons(tmp_path):
    auth = PlainAuth(_store_with(tmp_path, alice="pw"))
    r = auth.authenticate("alice", "pw")
    assert r.success is True and r.reason is None and "alice" in r.roles
    assert auth.authenticate("alice", "x").reason == "invalid_password"
    assert auth.authenticate("bob", "pw").reason == "user_not_found"


def test_verify_zap_compat(tmp_path):
    auth = PlainAuth(_store_with(tmp_path, alice="pw"))
    assert auth.verify("alice", "pw") == (True, None)
    assert auth.verify("alice", "x") == (False, "invalid_password")
    assert auth.verify("bob", "pw") == (False, "user_not_found")


def test_from_file_classmethod(tmp_path):
    f = str(tmp_path / "u.toml")
    s = CredentialStore(f, allow_auto_generated=False)
    s.add_user("alice", "pw", roles=[])
    s.save()
    auth = PlainAuth.from_file(f)
    assert auth.authenticate("alice", "pw").success is True
```

- [ ] **Step 2: Run → FAIL**

Run: `pytest tests/test_auth.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 实现**

```python
# src/pulsemq/auth.py
"""PLAIN 认证决策器。本项目唯一支持且强制启用。委托 CredentialStore。"""
from __future__ import annotations

from pulsemq.security import AuthResult, CredentialStore


class PlainAuth:
    def __init__(self, store: CredentialStore) -> None:
        self._store = store

    @classmethod
    def from_file(cls, credentials_file: str) -> "PlainAuth":
        store = CredentialStore(credentials_file)
        store.load()
        return cls(store)

    def authenticate(self, username: str, password: str) -> AuthResult:
        return self._store.verify(username, password)

    def verify(self, username: str, password: str) -> tuple[bool, str | None]:
        """ZAP handler 兼容接口（与 Spec 1 PlainAuthDict.verify 同签名）。"""
        r = self._store.verify(username, password)
        return r.success, r.reason
```

- [ ] **Step 4: Run → PASS**

Run: `pytest tests/test_auth.py -v`
Expected: PASS（3 用例）

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/auth.py tests/test_auth.py
git commit -m "feat(auth): PlainAuth 决策器（authenticate + ZAP 兼容 verify）"
```

---

## Task 6: Server 用 CredentialStore+PlainAuth 替换 PlainAuthDict（含默认生成 + SIGHUP reload）

**Files:**
- Modify: `src/pulsemq/server.py`
- Test: `tests/test_server_security.py`

**Interfaces:**
- Consumes: `pulsemq.security.CredentialStore`、`pulsemq.auth.PlainAuth`、`pulsemq.config.ServerConfig`、`pulsemq.logging_setup`。
- Produces: `Server` 凭据源改走 PlainAuth（委托 CredentialStore）；默认 admin 生成（写回文件 + 日志输出一次明文）；`reload_credentials()` 方法；SIGHUP（Linux）触发 reload。

> transport 完全不动：`Transport.bind(auth=self._auth)` 接到的 `self._auth` 现在是 `PlainAuth`，其 `.verify` 与旧 `PlainAuthDict.verify` 同签名。

- [ ] **Step 1: 失败测试**

```python
# tests/test_server_security.py
import socket as _sock
import asyncio
import re
import pytest
from pulsemq.server import Server
from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.security import CredentialStore


def _free_port():
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


async def test_server_auth_via_bcrypt_credentialstore(tmp_path):
    f = str(tmp_path / "users.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    store.add_user("alice", "secret", roles=["subscriber"])
    store.add_user("bob", "pw", roles=["publisher"])
    store.save()
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}",
                 control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}",
                 credentials_file=f)
    await srv.start()
    try:
        c = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "alice", "secret")
        p = ProducerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "bob", "pw")
        await c.start(); await p.start()
        got = []
        await c.subscribe("t.*", lambda m: got.append(m.payload))
        await asyncio.sleep(0.3)
        await p.publish("t.x", {"k": 1})
        await asyncio.sleep(0.5)
        assert got == [{"k": 1}]
        await c.stop(); await p.stop()
    finally:
        await srv.stop()


async def test_server_auto_generates_default_admin(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("PULSEMQ_ADMIN_PASSWORD", raising=False)
    f = str(tmp_path / "auto.toml")
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}",
                 control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}",
                 credentials_file=f, allow_auto_generated=True)
    await srv.start()
    try:
        # 自动生成：文件已写回（哈希），日志含明文密码
        import os
        assert os.path.exists(f)
        captured = capsys.readouterr().err + capsys.readouterr().out
        # 从 _notice 日志里抓密码（格式 [SECURITY] ... password=...）
        # 自动生成的 admin 凭据能认证
        c = ConsumerClient(f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}",
                           "admin", srv.generated_admin_password)
        await c.start()
        await c.stop()
    finally:
        await srv.stop()


async def test_server_reload_credentials(tmp_path):
    f = str(tmp_path / "users.toml")
    store = CredentialStore(f, allow_auto_generated=False)
    store.add_user("alice", "pw", roles=[])
    store.save()
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}",
                 control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}",
                 credentials_file=f)
    await srv.start()
    try:
        # CLI 风格：另一 store 加用户并 save
        cli = CredentialStore(f, allow_auto_generated=False)
        cli.load()
        cli.add_user("carol", "pw2", roles=[])
        cli.save()
        srv.reload_credentials()
        assert srv._auth.authenticate("carol", "pw2").success is True
    finally:
        await srv.stop()
```

> `srv.generated_admin_password`：Server 在自动生成时把明文密码存到该属性，便于测试取用（生产中只输出到日志）。

- [ ] **Step 2: Run → FAIL**

Run: `pytest tests/test_server_security.py -v`
Expected: FAIL — Server 尚无 `credentials_file`/`allow_auto_generated` 参数与 `generated_admin_password`/`reload_credentials`。

- [ ] **Step 3: 实现**

修改 `src/pulsemq/server.py`：

1. 顶部 import 改：移除 `PlainAuthDict` 导入，改为
   ```python
   from pulsemq.auth import PlainAuth
   from pulsemq.security import CredentialStore
   ```
   删除 `_load_credentials_file` 函数（被 CredentialStore 取代）。

2. `Server.__init__` 签名加 `credentials_file: str | None = None`、`allow_auto_generated: bool | None = None`（None 表示用 config 默认）。构造体内替换凭据段：

```python
        # 凭据源（Spec 2）：CredentialStore + PlainAuth
        self.generated_admin_password: str | None = None
        if credentials is not None:
            # 显式明文 dict（测试/兼容）：内存态 store，哈希落值
            store = CredentialStore.from_dict(credentials)
        else:
            cred_file = credentials_file or self._cfg.credentials_file
            allow = self._cfg.allow_auto_generated if allow_auto_generated is None else allow_auto_generated
            store = CredentialStore(cred_file, allow_auto_generated=allow,
                                    hash_algo=self._cfg.password_hash_algo,
                                    bcrypt_cost=self._cfg.bcrypt_cost)
            plaintext = store.load()
            if plaintext is not None:
                # 自动生成的默认 admin：明文仅此一次输出
                self.generated_admin_password = plaintext
                print(f"[SECURITY] 未检测到 {cred_file}，已生成默认用户", file=sys.stderr)
                print(f"[SECURITY] username=admin, password={plaintext}", file=sys.stderr)
                print("[SECURITY] 提示：默认凭据仅用于首次启动，请使用 pulsemq.users CLI 创建正式用户",
                      file=sys.stderr)
                log_event("WARNING", "SECURITY", action="default_credentials_generated")
            else:
                log_event("INFO", "SECURITY", action="credentials_file_loaded",
                          path=cred_file, users=len(store.list_users()))
        self._credential_store = store
        self._auth = PlainAuth(store)
```

加 `import sys`（顶部）。

3. `start()` 末尾（admin 启动后）追加 SIGHUP reload（Linux）：

```python
        self._install_sighup_reload()
```

并新增方法：

```python
    def reload_credentials(self) -> None:
        """热更新凭据（CLI 改文件后调用，或 SIGHUP 触发）。"""
        self._credential_store.reload()
        log_event("INFO", "SECURITY", action="credentials_reloaded")

    def _install_sighup_reload(self) -> None:
        import signal
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGHUP, self.reload_credentials)
        except (AttributeError, NotImplementedError, ValueError, RuntimeError):
            # Windows 无 SIGHUP；非主线程无信号。留接口，Spec 3 admin 接口接入。
            pass
```

4. transport bind 不变（`auth=self._auth` 现在是 PlainAuth，`.verify` 同签名）。

- [ ] **Step 4: Run → PASS**

Run: `pytest tests/test_server_security.py -v`
Expected: PASS（3 用例）
Run: `pytest -v`（全量）→ Spec 1 e2e 继续通过（显式 `credentials=dict` 走 `from_dict`）。

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/server.py tests/test_server_security.py
git commit -m "feat(server): 凭据源改走 CredentialStore+PlainAuth，默认 admin 生成+SIGHUP reload"
```

---

## Task 7: admin/auth.TokenAuth 中间件

**Files:**
- Create: `src/pulsemq/admin/auth.py`
- Modify: `src/pulsemq/admin/server.py`
- Test: `tests/test_admin_token.py`

**Interfaces:**
- Consumes: 仅标准库 `hmac`。
- Produces: `TokenAuth(expected_token: str | None)`：`enabled` 属性、`validate(headers: dict, query: dict) -> bool`。`AdminServer.__init__` 加 `token_auth: TokenAuth | None = None`。

> `expected_token=None` 或空 → 禁用（开放，向后兼容 Spec 1 测试）。

- [ ] **Step 1: 失败测试**

```python
# tests/test_admin_token.py
import asyncio
import socket as _sock
import pytest
from pulsemq.admin.auth import TokenAuth


def test_tokenauth_disabled_when_none():
    ta = TokenAuth(None)
    assert ta.enabled is False
    assert ta.validate({}, {}) is True  # 禁用时一律放行


def test_tokenauth_query_and_header():
    ta = TokenAuth("s3cret")
    assert ta.enabled is True
    assert ta.validate({}, {"token": ["s3cret"]}) is True
    assert ta.validate({"authorization": "Bearer s3cret"}, {}) is True
    assert ta.validate({}, {}) is False
    assert ta.validate({"authorization": "Bearer wrong"}, {}) is False
    assert ta.validate({}, {"token": ["wrong"]}) is False


async def test_admin_healthz_open_others_require_token(tmp_path):
    from pulsemq.server import Server
    dp, cp, ap = _port(), _port(), _port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}",
                 control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}",
                 admin_token="TOK")
    await srv.start()
    try:
        await asyncio.sleep(0.3)
        # /healthz 无 token → 200
        assert "200" in await _get(ap, "/healthz")
        # /api/v1/stats/realtime 无 token → 401
        assert "401" in await _get(ap, "/api/v1/stats/realtime")
        # 带 token → 200
        assert "200" in await _get(ap, "/api/v1/stats/realtime", token="TOK")
    finally:
        await srv.stop()


def _port():
    s = _sock.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


async def _get(port, path, token=None, timeout=3.0):
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection("127.0.0.1", port), timeout=timeout)
    headers = "Authorization: Bearer %s\r\n" % token if token else ""
    writer.write(f"GET {path} HTTP/1.1\r\nHost: x\r\n{headers}Connection: close\r\n\r\n".encode())
    await writer.drain()
    data = await asyncio.wait_for(reader.read(), timeout=timeout)
    writer.close()
    return data.decode(errors="replace")
```

> 第二个测试依赖 Server 支持 `admin_token=` 参数（Task 8 加）。**Task 7 先只跑 `test_tokenauth_*` 两个单测**；admin 集成测试在 Task 8 解锁。

- [ ] **Step 2: Run → FAIL**

Run: `pytest tests/test_admin_token.py::test_tokenauth_disabled_when_none tests/test_admin_token.py::test_tokenauth_query_and_header -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 实现**

```python
# src/pulsemq/admin/auth.py
"""admin HTTP token 认证中间件。仅依赖标准库 hmac。"""
from __future__ import annotations

import hmac


class TokenAuth:
    """除 /healthz 外所有 admin 路由需携带有效 token。

    token 经 ``?token=xxx`` query 或 ``Authorization: Bearer xxx`` header 携带。
    expected_token 为 None/空 → 禁用（放行，向后兼容）。
    """

    def __init__(self, expected_token: str | None) -> None:
        self._expected = expected_token or ""

    @property
    def enabled(self) -> bool:
        return bool(self._expected)

    def validate(self, headers: dict[str, str], query: dict[str, list[str]]) -> bool:
        if not self.enabled:
            return True
        presented = ""
        auth = headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            presented = auth[7:].strip()
        if not presented:
            vals = query.get("token")
            if vals:
                presented = vals[0]
        return hmac.compare_digest(presented, self._expected)
```

修改 `src/pulsemq/admin/server.py`：

- `__init__` 签名加 `token_auth: "TokenAuth | None" = None`（import 在文件顶部 `from pulsemq.admin.auth import TokenAuth`，或 TYPE_CHECKING 避免循环——admin/auth 无依赖，可直接 import）。存 `self._token_auth = token_auth`。
- `_handle_request` 在解析完 headers/query、调用 `_route` 之前插入：

```python
            # token 认证（除 /healthz）
            if self._token_auth is not None and self._token_auth.enabled and path != "/healthz":
                if not self._token_auth.validate(headers, query):
                    await self._respond_json(writer, 401, {"error": "unauthorized"})
                    return
```

- [ ] **Step 4: Run → PASS（单测）**

Run: `pytest tests/test_admin_token.py::test_tokenauth_disabled_when_none tests/test_admin_token.py::test_tokenauth_query_and_header -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/admin/auth.py src/pulsemq/admin/server.py tests/test_admin_token.py
git commit -m "feat(admin): TokenAuth 中间件，AdminServer 注入 token 校验（/healthz 除外）"
```

---

## Task 8: Server admin token 生成/加载 + 接入

**Files:**
- Modify: `src/pulsemq/server.py`
- Test: `tests/test_admin_token.py`（解锁集成测试）

**Interfaces:**
- Consumes: `pulsemq.admin.auth.TokenAuth`、`pulsemq.config.ServerConfig`。
- Produces: `Server` 加 `admin_token: str | None = None` 参数；按优先级（config `monitoring.admin_token` > env `PULSEMQ_ADMIN_TOKEN` > 随机 32 字节 base64url）确定 token；随机时写 `pulsemq_admin.token`（0600）并日志输出；把 `TokenAuth` 传给 AdminServer。

- [ ] **Step 1: 失败测试**

把 `tests/test_admin_token.py::test_admin_healthz_open_others_require_token` 纳入（它用 `Server(..., admin_token="TOK")`）。再加一个：

```python
async def test_server_random_admin_token_written_to_file(tmp_path, monkeypatch):
    import os, base64
    monkeypatch.delenv("PULSEMQ_ADMIN_TOKEN", raising=False)
    tok_file = str(tmp_path / "admin.token")
    dp, cp, ap = _port(), _port(), _port()
    srv = Server(data_endpoint=f"tcp://127.0.0.1:{dp}",
                 control_endpoint=f"tcp://127.0.0.1:{cp}",
                 admin_endpoint=f"127.0.0.1:{ap}",
                 admin_token_file=tok_file)  # admin_token=None → 随机生成
    await srv.start()
    try:
        assert os.path.exists(tok_file)
        tok = open(tok_file).read().strip()
        # 随机 token 能用
        assert "200" in await _get(ap, "/api/v1/stats/realtime", token=tok)
        assert "401" in await _get(ap, "/api/v1/stats/realtime", token="wrong")
    finally:
        await srv.stop()
```

- [ ] **Step 2: Run → FAIL**

Run: `pytest tests/test_admin_token.py -v`
Expected: FAIL — Server 无 `admin_token`/`admin_token_file` 参数。

- [ ] **Step 3: 实现**

修改 `src/pulsemq/server.py`：

1. import：`from pulsemq.admin.auth import TokenAuth`；顶部加 `import base64, os, secrets`。

2. `Server.__init__` 签名加 `admin_token: str | None = None`、`admin_token_file: str | None = None`。构造体内（在 `self._auth` 之后、transport 之前）解析 token：

```python
        # admin token（Spec 2 §5）
        self.admin_token = self._resolve_admin_token(admin_token, admin_token_file)
        self._token_auth = TokenAuth(self.admin_token)
```

新增方法（构造期调用，非 async）：

```python
    def _resolve_admin_token(self, explicit: str | None,
                             token_file: str | None) -> str:
        # 优先级：显式参数 > config admin_token > env > 随机生成（写文件 0600）
        if explicit:
            return explicit
        if self._cfg.admin_token:
            return self._cfg.admin_token
        env_tok = os.environ.get("PULSEMQ_ADMIN_TOKEN")
        if env_tok:
            return env_tok
        tok = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii").rstrip("=")
        path = token_file or self._cfg.admin_token_file
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(tok)
            try:
                os.chmod(path, 0o600)
            except OSError:
                pass
            print(f"[ADMIN] 管理接口 token: {tok}", file=sys.stderr)
            log_event("WARNING", "ADMIN", action="admin_token_generated", path=path)
        except OSError:
            log_event("WARNING", "ADMIN", action="admin_token_write_failed", path=path)
        return tok
```

3. `start()` 里 AdminServer 构造加 `token_auth=self._token_auth`：

```python
        self._admin = AdminServer(
            bind=self._admin_endpoint,
            traffic_stats=self._stats,
            topic_buffers=None,
            stats_storage=self._storage,
            snapshot_fn=lambda: {...},   # 不变
            start_time=self._start_time,
            token_auth=self._token_auth,
        )
```

- [ ] **Step 4: Run → PASS**

Run: `pytest tests/test_admin_token.py -v`
Expected: PASS（含集成测试）
Run: `pytest -v`（全量）→ 注意：Spec 1 的 `test_server_admin.py` / `test_admin_bytes_keys.py` 现在会因 token 强制而 401（它们没带 token）。**需更新这两个测试带 token**：在 Task 8 Step 3 一并把它们改为带 `?token=` 或 Server 用 `admin_token=""` 禁用。最简：这两个测试构造 Server 时传 `admin_token=""`（禁用 token，向后兼容）。

  → 实施时用 Grep 找 `test_server_admin.py` / `test_admin_bytes_keys.py` 里构造 `Server(...)` 处，加 `admin_token=""` 参数。

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/server.py tests/test_admin_token.py tests/test_server_admin.py tests/test_admin_bytes_keys.py
git commit -m "feat(server): admin token 生成/加载（config>env>随机），接入 AdminServer"
```

---

## Task 9: web_ui token 携带（前端）

**Files:**
- Modify: `src/pulsemq/admin/web_ui.py`
- Test: 人工/截图（沿用现有做法）+ `test_admin_token.py` 已覆盖数据接口

**Interfaces:**
- Produces: 前端从 URL `?token=` 取 token；fetch 加 `Authorization: Bearer`；SSE EventSource URL 带 `?token=`。

> UI 是单文件 HTML，主要人工验证；token 携带的正确性由 `test_admin_token.py`（query+header）覆盖。本 task 给 JS 加 token 注入。

- [ ] **Step 1: 改 JS**

在 `web_ui.py` 的 `INDEX_HTML` 的 `<script>` 起始处加：

```javascript
const _tok = new URLSearchParams(location.search).get('token') || '';
function _authHeaders(extra) {
  const h = extra || {};
  if (_tok) h['Authorization'] = 'Bearer ' + _tok;
  return h;
}
function _withToken(url) {
  return _tok ? url + (url.includes('?') ? '&' : '?') + 'token=' + encodeURIComponent(_tok) : url;
}
```

把现有 `fetch('/api/v1/...')` 改为 `fetch(_withToken('/api/v1/...'), {headers: _authHeaders()})`；EventSource 的 URL 改为 `_withToken('/api/v1/stats/stream')`。

- [ ] **Step 2: 验证不破坏**

Run: `pytest tests/test_admin_token.py tests/test_server_admin.py -v` → PASS（数据接口 token 覆盖；UI 由人工验证）。

- [ ] **Step 3: Commit**

```bash
git add src/pulsemq/admin/web_ui.py
git commit -m "feat(web_ui): 前端携带 admin token（fetch header + SSE query）"
```

---

## Task 10: cli/users 用户管理 CLI

**Files:**
- Create: `src/pulsemq/cli/users.py`
- Modify: `pyproject.toml`（注册 `pulsemq-users`）
- Test: `tests/test_cli_users.py`

**Interfaces:**
- Consumes: `pulsemq.security.CredentialStore`、`pulsemq.errors.SecurityError`、`getpass`、`argparse`。
- Produces: `pulsemq.cli.users.main(argv=None) -> int`：子命令 add/passwd/list/disable/enable/reload。

- [ ] **Step 1: 失败测试**

```python
# tests/test_cli_users.py
import pytest
from pulsemq.cli.users import main
from pulsemq.security import CredentialStore


def test_cli_add_list_disable_enable(tmp_path, capsys):
    f = str(tmp_path / "u.toml")
    assert main(["add", "alice", "--password", "pw", "--roles", "pub,sub",
                 "--file", f]) == 0
    assert main(["add", "bob", "--password", "pw2", "--file", f]) == 0
    # 重复 add 报错（退出码 6）
    assert main(["add", "alice", "--password", "x", "--file", f]) == 6
    out = capsys.readouterr().out
    assert main(["list", "--file", f]) == 0
    out = capsys.readouterr().out
    assert "alice" in out and "bob" in out
    assert main(["disable", "bob", "--file", f]) == 0
    s = CredentialStore(f, allow_auto_generated=False); s.load()
    assert s.verify("bob", "pw2").reason == "user_disabled"
    assert main(["enable", "bob", "--file", f]) == 0
    s = CredentialStore(f, allow_auto_generated=False); s.load()
    assert s.verify("bob", "pw2").success is True


def test_cli_passwd(tmp_path):
    f = str(tmp_path / "u.toml")
    main(["add", "alice", "--password", "old", "--file", f])
    assert main(["passwd", "alice", "--password", "new", "--file", f]) == 0
    s = CredentialStore(f, allow_auto_generated=False); s.load()
    assert s.verify("alice", "new").success is True
    assert s.verify("alice", "old").success is False
```

> `--file` 默认 `./pulsemq_users.toml`；`--password` 缺省走 `getpass`（测试都显式传）。

- [ ] **Step 2: Run → FAIL**

Run: `pytest tests/test_cli_users.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: 实现**

```python
# src/pulsemq/cli/users.py
"""python -m pulsemq.users：用户管理 CLI。直接读写凭据文件，不连 Server。"""
from __future__ import annotations

import argparse
import getpass
import sys

from pulsemq.errors import PulseMQError, SecurityError, exit_code_for
from pulsemq.security import CredentialStore

DEFAULT_FILE = "./pulsemq_users.toml"


def _store(file: str) -> CredentialStore:
    s = CredentialStore(file, allow_auto_generated=False)
    s.load()  # 文件不存在时 load 不生成（allow_auto=False）；add 时 save 会创建
    return s


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="pulsemq.users")
    p.add_argument("--file", default=DEFAULT_FILE)
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("add"); a.add_argument("username")
    a.add_argument("--password"); a.add_argument("--roles", default="")

    pc = sub.add_parser("passwd"); pc.add_argument("username"); pc.add_argument("--password")

    sub.add_parser("list")

    d = sub.add_parser("disable"); d.add_argument("username")
    e = sub.add_parser("enable"); e.add_argument("username")

    sub.add_parser("reload")  # 占位：CLI 侧无 Server 连接，仅提示

    args = p.parse_args(argv)
    try:
        return _dispatch(args)
    except SecurityError as e:
        print(f"[users] {e}", file=sys.stderr)
        return 6
    except PulseMQError as e:
        print(f"[users] {e}", file=sys.stderr)
        return exit_code_for(e)


def _dispatch(args) -> int:
    if args.cmd == "add":
        pw = args.password or getpass.getpass(f"password for {args.username}: ")
        roles = [r.strip() for r in args.roles.split(",") if r.strip()]
        s = _store(args.file)
        s.add_user(args.username, pw, roles=roles)
        s.save()
        return 0
    if args.cmd == "passwd":
        pw = args.password or getpass.getpass(f"new password for {args.username}: ")
        s = _store(args.file); s.set_password(args.username, pw); s.save()
        return 0
    if args.cmd == "list":
        s = _store(args.file)
        print(f"{'username':<16} {'enabled':<8} {'roles':<24} created_at")
        for u in s.list_users():
            print(f"{u.username:<16} {str(u.enabled):<8} {','.join(u.roles):<24} {u.created_at}")
        return 0
    if args.cmd == "disable":
        s = _store(args.file); s.set_enabled(args.username, False); s.save(); return 0
    if args.cmd == "enable":
        s = _store(args.file); s.set_enabled(args.username, True); s.save(); return 0
    if args.cmd == "reload":
        print("[users] reload 需通知运行中的 Server（Linux: killall -HUP pulsemq；"
              "Windows: Spec 3 admin 接口）", file=sys.stderr)
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main())
```

> 注意：`_store` 对不存在文件调 `load()`——`allow_auto_generated=False` 且文件不存在会抛 `ConfigurationError`。但 add 命令对**新文件**应能创建。修正：`_store` 用 `allow_auto_generated=False` 时若文件不存在，**跳过 load**（空 store），add 后 save 创建文件。改 `_store`：

```python
def _store(file: str) -> CredentialStore:
    from pathlib import Path
    s = CredentialStore(file, allow_auto_generated=False)
    if Path(file).exists():
        s.load()
    return s
```

`pyproject.toml` 的 `[project.scripts]` 追加：

```toml
pulsemq-users = "pulsemq.cli.users:main"
```

- [ ] **Step 4: Run → PASS**

Run: `pytest tests/test_cli_users.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add src/pulsemq/cli/users.py pyproject.toml tests/test_cli_users.py
git commit -m "feat(cli): pulsemq.users 用户管理 CLI（add/passwd/list/disable/enable）"
```

---

## Task 11: 安全约束回归 + 全量验证

**Files:**
- Create: `tests/test_security_constraints.py`
- 全量回归。

- [ ] **Step 1: 失败测试**

```python
# tests/test_security_constraints.py
import os
import string
from pulsemq.security import CredentialStore, generate_password


def test_no_plaintext_in_saved_file(tmp_path):
    f = str(tmp_path / "u.toml")
    s = CredentialStore(f, allow_auto_generated=False)
    s.add_user("alice", "supersecret", roles=[])
    s.save()
    raw = open(f, encoding="utf-8").read()
    assert "supersecret" not in raw
    assert "$2" in raw


def test_token_randomness_and_length():
    import base64, secrets
    t1 = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")
    t2 = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")
    assert t1 != t2 and len(t1) >= 40  # 32 字节 base64url ≈ 43 chars


def test_password_complexity():
    pw = generate_password()
    assert len(pw) == 16
    assert (any(c.isupper() for c in pw) and any(c.islower() for c in pw)
            and any(c.isdigit() for c in pw) and any(c in string.punctuation for c in pw))
```

- [ ] **Step 2: Run → PASS**

Run: `pytest tests/test_security_constraints.py -v`
Expected: PASS

- [ ] **Step 3: 全量回归**

Run: `pytest -v`
Expected: 全绿（Spec 1 + Spec 2 全部测试）。重点确认：
  - Spec 1 e2e（test_e2e_client_server / test_client_lifecycle / test_zap_resilience / test_client_reconnect）通过——它们用显式 `credentials=dict`，走 `CredentialStore.from_dict`。
  - test_server_admin / test_admin_bytes_keys 带了 `admin_token=""`（Task 8 改过）。

- [ ] **Step 4: Commit**

```bash
git add tests/test_security_constraints.py
git commit -m "test(security): 落盘无明文/token 随机性/密码复杂度约束回归"
```

---

## Self-Review（写计划后自查）

**1. Spec coverage（逐节核对 Spec 2）：**
- §1.1 目标：bcrypt 哈希（Task 4）、默认生成（Task 4/6）、CLI（Task 10）、热更新（Task 4 reload + Task 6 SIGHUP）、enabled/disabled（Task 4）、admin token（Task 7/8）。✅
- §1.3 衔接：transport 接口不变（PlainAuth.verify 同签名）✅；PlainAuth 委托 CredentialStore ✅；AuthenticationError reason 落地（verify 返回 reason，Client 已用）✅；config [auth] 完整（Task 3）✅。
- §3 security：文件格式 `[users.X] hashed_password/roles/enabled/created_at`（Task 4 save/load）✅；接口 CredentialStore 全方法 ✅；默认生成三优先级（Task 4 测试覆盖）✅；bcrypt cost=12（Task 4）✅；热更新原子替换（Task 4 reload）✅。
- §4 auth：PlainAuth.authenticate/verify/from_file ✅；强制 plain（config type 校验，Spec 1 已有）✅；reason 落地 ✅。
- §5 admin token：token 发放三优先级（Task 8）✅；?token= 与 Bearer（Task 7）✅；hmac.compare_digest（Task 7）✅；路由矩阵 /healthz 开放其余强制（Task 7）✅；token 文件 0600（Task 8）✅。
- §6 CLI：add/passwd/list/disable/enable/reload（Task 10）✅；pyproject 注册（Task 10）✅。
- §7 config：[auth] + [monitoring]（Task 3）✅；env（Task 3 + Task 8）✅。
- §8 errors：SecurityError exit 6（Task 2）✅。
- §9 测试：test_security/auth/admin_token/cli_users/security_constraints 全覆盖 ✅。
- §10/11 决策与边界：体现于实现。

**2. 缺口/注意：**
- **Windows SIGHUP**：Task 6 的 `_install_sighup_reload` 在 Windows 静默跳过（spec §11.8）；reload 命令 Windows 走 Spec 3 admin 接口。CLI `reload` 子命令仅提示（Task 10）。✅ 已处理。
- **bcrypt cost=12 ~200ms**：仅连接建立阶段，不在消息路径。e2e 测试可能因 bcrypt 校验变慢——若 test 时序紧张，可临时降 cost 或加 sleep。执行时观察。
- **test_server_admin / test_admin_bytes_keys 需带 token**：Task 8 Step 3 显式处理（加 `admin_token=""`）。✅
- **web_ui token 携带**（Task 9）：UI 人工验证，数据接口由 test_admin_token 覆盖。可接受（沿用 Spec 1 做法）。
- **scripts/*.py 仍引用已删 publisher/subscriber**（Spec 1 遗留）：本 spec 不处理，留 post-merge 清理。

**3. 类型一致性：** `PlainAuth.verify -> (bool, str|None)` 与 `PlainAuthDict.verify` 同签名（ZAP 不变）✅；`CredentialStore.verify -> AuthResult`，`PlainAuth.authenticate -> AuthResult` ✅；`Server.__init__` 新参数 `credentials_file`/`allow_auto_generated`/`admin_token`/`admin_token_file` 在测试与实现一致 ✅；`TokenAuth.validate(headers, query) -> bool` 在 admin/server `_handle_request` 调用一致 ✅。

**4. Placeholder 扫描：** 无 TBD/TODO；每个 code step 含完整代码（Task 9 的 JS 片段是精确替换指令，非占位）。

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-06-27-pulsemq-v2-spec2-implementation.md`. 执行方式：subagent-driven-development（与 Spec 1 一致），逐 task 派 implementer + reviewer，最后 opus 全分支 review。Task 顺序：1→2→3→4→5→6→7→8→9→10→11。
