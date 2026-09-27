"""PulseMQ 测试共享 fixtures（重构中：仅保留 loguru 捕获 + 端口/SQLite）。"""

from __future__ import annotations

import asyncio
import os
import random
import sys
import tempfile
from typing import Any

import pytest
from loguru import logger

if sys.platform == "win32" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def _loguru_stderr_sink(message: Any) -> None:
    sys.stderr.write(str(message))
    sys.stderr.flush()


@pytest.fixture(autouse=True)
def _loguru_capture() -> Any:
    logger.remove()
    logger.add(_loguru_stderr_sink,
               format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}",
               level="DEBUG")
    yield
    logger.remove()
    logger.add(sys.stderr)


def _rand_port() -> int:
    return random.randint(25000, 35000)


@pytest.fixture
def random_port_pair() -> tuple[int, int]:
    p = _rand_port()
    a = _rand_port()
    while a == p:
        a = _rand_port()
    return p, a


@pytest.fixture
def tmp_sqlite_url() -> str:
    fd, path = tempfile.mkstemp(suffix=".sqlite")
    os.close(fd)
    try:
        yield f"sqlite://{path}"
    finally:
        for ext in ("", "-shm", "-wal"):
            try:
                os.unlink(path + ext)
            except OSError:
                pass


@pytest.fixture
def record_dir(tmp_path):
    """多进程回调录制目录：设置 PULSEMP_RECORD 环境变量（spawn 时被 worker
    继承），测试结束恢复并清理分片文件。配合 mp_callbacks.on_msg_record 使用。"""
    from tests import mp_callbacks

    base = str(tmp_path / "rec")
    old = os.environ.get("PULSEMP_RECORD")
    os.environ["PULSEMP_RECORD"] = base
    mp_callbacks.clear_records(base)
    yield base
    if old is None:
        os.environ.pop("PULSEMP_RECORD", None)
    else:
        os.environ["PULSEMP_RECORD"] = old
    mp_callbacks.clear_records(base)
