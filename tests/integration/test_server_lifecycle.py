"""server 启动/关闭生命周期集成测试。

覆盖:
- 子进程启动后能 bind 到 bind/xpub_bind 端口（TCP 可连）
- 关闭后端口无 LISTENING 进程（不再接受新连接）
- READY 信号出现
- 反复 start/stop 不残留 LISTENING

注意：Windows 上关闭连接后端口会进入 TIME_WAIT 状态（保留 30-60s），
因此验证端口释放的标准是"无进程 LISTENING"，而不是"可立即 rebind"。
"""

from __future__ import annotations

import asyncio
import os
import re
import socket
import subprocess
import sys
import time

import pytest

# 必须在导入 zmq 相关模块前切换 loop policy（Windows 必需）
if sys.platform == "win32":
    from pulsemq.event_loop import install_event_loop

    install_event_loop(use_uvloop=False)


def _port_open(host: str, port: int, timeout: float = 0.5) -> bool:
    """TCP 端口是否可连接。"""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


def _is_listening(port: int) -> bool:
    """检查是否有进程在该端口 LISTENING。"""
    if sys.platform == "win32":
        # netstat -ano 输出含 LISTENING 的行
        out = subprocess.run(
            ["netstat", "-ano", "-p", "TCP"],
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
        for line in out.splitlines():
            # 格式: TCP    0.0.0.0:PORT    0.0.0.0:0    LISTENING    PID
            if "LISTENING" in line and f":{port}" in line:
                return True
        return False
    else:
        # Linux: ss 输出
        out = subprocess.run(
            ["ss", "-ltn", f"sport = :{port}"],
            capture_output=True,
            text=True,
            timeout=5,
        ).stdout
        return bool(out.strip())


def _wait_ready(proc: subprocess.Popen, timeout: float = 10.0) -> str:
    """等子进程输出 READY；超时返回空串。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = proc.stdout.readline() if proc.stdout else ""
        if not line:
            time.sleep(0.05)
            if proc.poll() is not None:
                return ""
            continue
        if line.strip() == "READY":
            return "READY"
    return ""


def _spawn_runner(port: int) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "scripts/test_server_runner.py", "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )


def _terminate(proc: subprocess.Popen) -> None:
    """优雅终止子进程，必要时强杀。"""
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=2)


@pytest.mark.asyncio
async def test_server_starts_and_binds_ports(port_pair):
    """server_runner 启动后 ROUTER + XPUB 端口都能 TCP 连接到。"""
    port, _ = port_pair
    proc = _spawn_runner(port)
    try:
        ready = _wait_ready(proc, timeout=10.0)
        assert ready == "READY", (
            f"server_runner 未在 10s 内输出 READY (rc={proc.returncode})"
        )
        # 等 200ms 让 bind 彻底生效
        await asyncio.sleep(0.2)
        assert _port_open("127.0.0.1", port), f"ROUTER 端口 {port} 不可连"
        assert _port_open("127.0.0.1", port + 1), f"XPUB 端口 {port + 1} 不可连"
        # 同时确认有进程在 LISTENING
        assert _is_listening(port), f"ROUTER 端口 {port} 无 LISTENING 进程"
        assert _is_listening(port + 1), f"XPUB 端口 {port + 1} 无 LISTENING 进程"
    finally:
        _terminate(proc)


@pytest.mark.asyncio
async def test_server_stop_releases_router_port(port_pair):
    """正常关闭后 ROUTER 端口应当无 LISTENING 进程（连接关闭后 TIME_WAIT 属正常）。"""
    port, _ = port_pair
    proc = _spawn_runner(port)
    try:
        ready = _wait_ready(proc, timeout=10.0)
        assert ready == "READY"
        await asyncio.sleep(0.2)
        assert _is_listening(port)
    finally:
        _terminate(proc)

    # 等 OS 完全释放 LISTENING 句柄
    deadline = time.time() + 5.0
    while time.time() < deadline and _is_listening(port):
        await asyncio.sleep(0.1)

    assert not _is_listening(port), (
        f"ROUTER 端口 {port} 关闭后仍有进程在 LISTENING（server 未释放 socket）"
    )


@pytest.mark.asyncio
async def test_server_stop_releases_xpub_port(port_pair):
    """正常关闭后 XPUB 端口应当无 LISTENING 进程。"""
    _, xpub_port = port_pair
    proc = _spawn_runner(xpub_port - 1)
    try:
        ready = _wait_ready(proc, timeout=10.0)
        assert ready == "READY"
        await asyncio.sleep(0.2)
        assert _is_listening(xpub_port)
    finally:
        _terminate(proc)

    deadline = time.time() + 5.0
    while time.time() < deadline and _is_listening(xpub_port):
        await asyncio.sleep(0.1)

    assert not _is_listening(xpub_port), (
        f"XPUB 端口 {xpub_port} 关闭后仍有进程在 LISTENING"
    )


@pytest.mark.asyncio
async def test_server_repeated_start_stop(port_pair):
    """同一对端口可以反复 start/stop 不残留 LISTENING 进程。"""
    port, _ = port_pair
    for cycle in range(2):
        proc = _spawn_runner(port)
        try:
            ready = _wait_ready(proc, timeout=10.0)
            assert ready == "READY", f"cycle {cycle}: 未就绪"
            await asyncio.sleep(0.2)
            assert _is_listening(port), f"cycle {cycle}: 端口未 LISTENING"
        finally:
            _terminate(proc)
        # 等端口 LISTENING 释放
        deadline = time.time() + 5.0
        while time.time() < deadline and _is_listening(port):
            await asyncio.sleep(0.1)
        assert not _is_listening(port), f"cycle {cycle}: 端口残留 LISTENING"
