"""python -m pulsemq.users：用户管理 CLI。直接读写凭据文件，不连 Server。"""
from __future__ import annotations

import argparse
import getpass
import os
import signal
import sys
from pathlib import Path

from pulsemq.errors import PulseMQError, SecurityError, exit_code_for
from pulsemq.security import CredentialStore

DEFAULT_FILE = "./pulsemq_users.toml"


def _store(file: str) -> CredentialStore:
    """构造 CredentialStore；文件不存在时跳过 load（add 对新文件可创建）。"""
    s = CredentialStore(file, allow_auto_generated=False)
    if Path(file).exists():
        s.load()
    return s


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="pulsemq.users")
    # --file 走 parent parser：放在子命令前或后均可被识别。
    parent = argparse.ArgumentParser(add_help=False)
    parent.add_argument("--file", default=DEFAULT_FILE)
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("add", parents=[parent]); a.add_argument("username")
    a.add_argument("--password"); a.add_argument("--roles", default="")

    pc = sub.add_parser("passwd", parents=[parent])
    pc.add_argument("username"); pc.add_argument("--password")

    sub.add_parser("list", parents=[parent])

    d = sub.add_parser("disable", parents=[parent]); d.add_argument("username")
    e = sub.add_parser("enable", parents=[parent]); e.add_argument("username")

    sub.add_parser("reload", parents=[parent])  # 占位：CLI 侧无 Server 连接，仅提示

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
        return _reload_server()
    return 2


def _reload_server() -> int:
    """向 Server 进程发 SIGHUP 热更新凭据（Spec §6）。

    定位 PID：仅支持 ``PULSEMQ_PID`` 环境变量（pidfile 由 Spec 3+ 引入）。
    POSIX：os.kill(SIGHUP)；Windows：无 SIGHUP，提示走 admin 接口。
    """
    if os.name != "posix" or not hasattr(signal, "SIGHUP"):
        print(
            "[users] Windows 不支持 SIGHUP reload，请用 admin 接口（Spec 3）",
            file=sys.stderr,
        )
        return 6
    pid_env = os.environ.get("PULSEMQ_PID")
    if not pid_env:
        print(
            "[users] 未定位 Server PID：请设置 PULSEMQ_PID 环境变量"
            "（或 Linux 下手动 kill -HUP <pid>）",
            file=sys.stderr,
        )
        return 6
    try:
        pid = int(pid_env)
    except ValueError:
        print(f"[users] PULSEMQ_PID 非法整数: {pid_env!r}", file=sys.stderr)
        return 6
    try:
        os.kill(pid, signal.SIGHUP)
    except ProcessLookupError:
        print(f"[users] Server 进程不存在 (pid={pid})", file=sys.stderr)
        return 6
    except PermissionError:
        print(f"[users] 无权限向 Server 发信号 (pid={pid})", file=sys.stderr)
        return 6
    print(f"[users] 已通知 Server (pid={pid}) 热更新凭据")
    return 0


if __name__ == "__main__":
    sys.exit(main())
