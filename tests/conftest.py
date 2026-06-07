"""pytest 共享 fixtures。

提供:
  - port_pair: 分配一对相邻空闲端口 (router, xpub)
  - server_subprocess: 启一个无认证无指标的 PulseServer 子进程, 输出 READY 后 yield
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time

import pytest
import pytest_asyncio

# Windows 上 pyzmq 不兼容 ProactorEventLoop, 必须在导入 zmq/asyncio 相关模块前切换
if sys.platform == "win32":
    from pulsemq.event_loop import install_event_loop

    install_event_loop(use_uvloop=False)


def _free_port() -> int:
    """找一个当前空闲的 TCP 端口。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def port_pair() -> tuple[int, int]:
    """分配一对相邻空闲端口 (router, xpub)。"""
    p = _free_port()
    # 确保 p+1 也空闲
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", p + 1))
                break
            except OSError:
                p = _free_port()
    return p, p + 1


@pytest_asyncio.fixture
async def server_subprocess(port_pair):
    """启一个 server_runner 子进程, 等 READY 后 yield。"""
    port, _ = port_pair
    proc = subprocess.Popen(
        [sys.executable, "scripts/test_server_runner.py", "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
    )
    deadline = time.time() + 10.0
    ready = False
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            await asyncio.sleep(0.05)
            if proc.poll() is not None:
                stderr = proc.stderr.read() if proc.stderr else ""
                raise RuntimeError(
                    f"server_runner 提前退出 (rc={proc.returncode})\nstderr: {stderr}"
                )
            continue
        if line.strip() == "READY":
            ready = True
            break
    if not ready:
        proc.kill()
        stderr = proc.stderr.read() if proc.stderr else ""
        raise TimeoutError(f"server_runner 启动超时\nstderr: {stderr}")
    try:
        yield proc
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
