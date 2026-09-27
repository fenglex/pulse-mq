"""多进程消费 worker 回调（模块级，可跨进程按引用导入；测试专用）。

9.2.4 起消费端唯一模式为多进程池：测试不能再用闭包/lambda 在测试进程内
收集回调副作用（worker 进程内是拷贝，不共享内存）。本模块提供进程外录制
回调：worker 把收到的消息以 JSON 行写入分片文件，测试进程用 read_records /
wait_records 轮询断言。

分片规则：on_msg_record 按 worker 进程 pid 分片（同一文件只有一个写者，
Windows 下并发 append 不保证原子，pid 分片天然避开）。
on_msg_record_worker 按 worker 编号分片（依赖 worker_init=init_recorder），
供 per-worker 路由/顺序断言。
"""
from __future__ import annotations

import asyncio
import glob
import json
import os
import time


def _shard_path(base: str, suffix: str) -> str:
    return f"{base}.{suffix}"


def _append(path: str, record: dict) -> None:
    line = json.dumps(record) + "\n"
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY)
    try:
        os.write(fd, line.encode("utf-8"))
    finally:
        os.close(fd)


def _common_fields(msg) -> dict:
    payload = msg.payload if isinstance(msg.payload, dict) else {}
    return {
        "topic": msg.topic,
        "seq": getattr(msg, "seq", None) or payload.get("seq"),
        "sym": payload.get("sym"),
        "pid": os.getpid(),
        "ts": msg.timestamp_ns,
        "rows": getattr(msg, "record_count", None),
    }


def on_msg_record(msg) -> None:
    """按 worker 进程 pid 分片追加（通用收流断言，无需 worker_init）。"""
    base = os.environ.get("PULSEMP_RECORD")
    if not base:
        return
    _append(_shard_path(base, os.getpid()), _common_fields(msg))


def on_msg_record_full(msg) -> None:
    """录制回调（含完整 payload 断言字段）。payload 需可 JSON 序列化。"""
    base = os.environ.get("PULSEMP_RECORD")
    if not base:
        return
    rec = _common_fields(msg)
    rec["payload"] = msg.payload if isinstance(msg.payload, dict) else None
    _append(_shard_path(base, os.getpid()), rec)


async def on_msg_record_async(msg) -> None:
    """异步录制回调（验证 worker 进程内事件循环执行异步回调）。"""
    on_msg_record(msg)


def on_msg_record_slow(msg) -> None:
    """慢录制回调（~50ms/条）：验证信用流控 + 服务端缓冲下不丢失。"""
    time.sleep(0.05)
    base = os.environ.get("PULSEMP_RECORD")
    if not base:
        return
    payload = msg.payload if isinstance(msg.payload, dict) else {}
    _append(_shard_path(base, os.getpid()),
            {"topic": msg.topic, "payload_i": payload.get("i"),
             "pid": os.getpid()})


def on_msg_record_worker(msg) -> None:
    """按 worker 编号分片追加（配合 worker_init=init_recorder）。"""
    base = os.environ.get("PULSEMP_RECORD")
    if not base:
        return
    _append(_shard_path(base, os.environ.get("PULSEMP_WORKER", "x")),
            _common_fields(msg))


def on_hdr_record(hdr) -> None:
    """header_only 订阅录制器（只记 header 字段）。"""
    base = os.environ.get("PULSEMP_RECORD")
    if not base:
        return
    _append(_shard_path(base, os.getpid()), {
        "topic": hdr.topic,
        "seq": getattr(hdr, "seq", None),
        "pid": os.getpid(),
        "ts": hdr.timestamp_ns,
        "rows": getattr(hdr, "record_count", None),
    })


def init_recorder(worker_index: int) -> None:
    """记录 worker 编号（供 on_msg_record_worker 选择分片文件）。"""
    os.environ["PULSEMP_WORKER"] = str(worker_index)


def init_stub(worker_index: int) -> None:
    """worker 初始化占位（可 pickle）。"""


# ---------------------------------------------------------------------------
# 测试进程侧读取/等待工具
# ---------------------------------------------------------------------------

def read_records(base: str) -> list[dict]:
    """读取全部分片文件（base.*），按文件内顺序返回记录列表。"""
    out: list[dict] = []
    for p in sorted(glob.glob(base + ".*")):
        try:
            with open(p, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        out.append(json.loads(line))
        except FileNotFoundError:
            continue
    return out


async def wait_records(base: str, n: int, timeout: float = 10.0,
                       pred=None) -> list[dict]:
    """轮询直到收集到 >= n 条（可选 pred 过滤）记录，超时 AssertionError。"""
    deadline = time.monotonic() + timeout
    last: list[dict] = []
    while time.monotonic() < deadline:
        last = read_records(base)
        got = [r for r in last if pred is None or pred(r)]
        if len(got) >= n:
            return got
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"等待 {n} 条记录超时（{timeout}s），实得 "
        f"{len([r for r in last if pred is None or pred(r)])} 条")


def clear_records(base: str) -> None:
    """删除 base.* 分片文件（测试间隔离）。"""
    for p in glob.glob(base + ".*"):
        try:
            os.unlink(p)
        except OSError:
            pass
