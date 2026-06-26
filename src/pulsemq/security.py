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
