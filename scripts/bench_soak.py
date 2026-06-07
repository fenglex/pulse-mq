"""Soak 测试: 长时间运行, 检测内存/句柄泄漏。

启 server + 1 pub + 1 sub, 持续收发, 定期采样 RSS 内存。
"""
import argparse
import asyncio
import os
import subprocess
import sys
import time
import tracemalloc

sys.path.insert(0, "src")
from pulsemq.event_loop import install_event_loop
if sys.platform == "win32":
    install_event_loop(use_uvloop=False)
from pulsemq.client.async_client import PulseClient


async def soak(port: int, duration_min: int, rate_msg_s: int):
    address = f"tcp://localhost:{port}"
    xpub = f"tcp://localhost:{port + 1}"
    n_sent = 0
    received_count = 0
    tracemalloc.start()

    async def pub():
        nonlocal n_sent
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            await asyncio.sleep(0.3)
            end = time.time() + duration_min * 60
            while time.time() < end:
                await c.publish("soak.t", f"msg-{n_sent}")
                n_sent += 1
                await asyncio.sleep(1.0 / rate_msg_s)

    async def sub():
        nonlocal received_count
        async with PulseClient(address=address, xpub_address=xpub, auto_reconnect=False) as c:
            end = time.time() + duration_min * 60
            async for msg in c.subscribe("soak.>"):
                received_count += 1
                if time.time() > end:
                    return

    t0 = time.time()
    try:
        await asyncio.wait_for(asyncio.gather(pub(), sub()), timeout=duration_min * 60 + 30)
    except asyncio.TimeoutError:
        pass
    elapsed = time.time() - t0
    current, peak = tracemalloc.get_traced_memory()
    print(f"耗时: {elapsed:.0f}s")
    print(f"发送: {n_sent}, 接收: {received_count}")
    print(f"tracemalloc current={current/1024:.0f} KB, peak={peak/1024:.0f} KB")
    tracemalloc.stop()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=16020)
    parser.add_argument("--duration-min", type=int, default=30)
    parser.add_argument("--rate-msg-s", type=int, default=100)
    args = parser.parse_args()
    proc = subprocess.Popen(
        [sys.executable, "scripts/test_server_runner.py", "--port", str(args.port)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1,
        env={**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"},
    )
    deadline = time.time() + 10
    while time.time() < deadline:
        line = proc.stdout.readline()
        if line.strip() == "READY":
            break
        time.sleep(0.05)
    try:
        await soak(args.port, args.duration_min, args.rate_msg_s)
    finally:
        proc.terminate()
        try: proc.wait(timeout=5)
        except subprocess.TimeoutExpired: proc.kill()


if __name__ == "__main__":
    asyncio.run(main())
