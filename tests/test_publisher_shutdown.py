"""Publisher 资源生命周期测试。

覆盖 B3 回归：_run() 的初始化阶段（transport/admin/storage/tasks）在 try 块之前，
任一初始化失败时 _shutdown() 不会执行，导致 ZMQ context / PUB socket /
SQLite 连接 / asyncio 任务泄漏。

本测试通过 monkeypatch 强制 AdminServer.start() 抛异常触发该路径，
断言 _shutdown 仍被调用、资源被释放（None 化）。
"""

from __future__ import annotations

import pytest

from pulsemq import admin  # noqa: F401  # 确保 admin 包可被 monkeypatch
from pulsemq.admin.server import AdminServer
from pulsemq.config import PublisherConfig
from pulsemq.publisher import PulsePublisher


@pytest.mark.asyncio
async def test_setup_failure_releases_resources(
    random_port_pair: tuple[int, int],
    tmp_sqlite_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """初始化阶段抛异常时，已创建的资源（transport context/storage）必须被释放。"""
    pub_port, admin_port = random_port_pair
    pub = PulsePublisher(
        config=PublisherConfig(
            bind=f"tcp://127.0.0.1:{pub_port}",
            admin_bind=f"127.0.0.1:{admin_port}",
            stats_db=tmp_sqlite_url,
        ),
    )

    # 注册一个 producer，确保 _run 能走到 start_all 之前的初始化
    async def _factory():  # type: ignore[no-untyped-def]
        return {"x": 1}

    pub.register_producer(fn=_factory, name="probe", interval=1.0)

    # 强制 AdminServer.start() 失败（模拟端口被占用等真实失败路径）
    async def _boom(self: AdminServer) -> None:
        raise OSError("模拟 admin 端口被占用")

    monkeypatch.setattr(AdminServer, "start", _boom)

    # start_async 应在初始化阶段抛 OSError
    with pytest.raises(OSError, match="端口被占用"):
        await pub.start_async()

    # 核心断言：尽管 setup 中途失败，_shutdown 仍应执行并释放资源
    assert pub._transport is None or pub._transport._ctx is None, (
        "setup 失败后 transport context 应被关闭，实际仍存活（资源泄漏）"
    )
    assert pub._storage is None or pub._storage._conn is None, (
        "setup 失败后 SQLite 连接应被关闭，实际仍存活（资源泄漏）"
    )
