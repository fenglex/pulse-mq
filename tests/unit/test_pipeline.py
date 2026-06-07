"""engine/pipeline.py 单元测试。

覆盖:
- InterceptorChain 顺序执行
- 拦截器异常隔离（异常应传播, 不被吞掉）
- Interceptor 链式 next_fn 调用
- AuthInterceptor / PermissionInterceptor / MonitorInterceptor
- AuthError / PermissionError 异常类型
- PipelineContext dataclass
"""

from __future__ import annotations

import pytest

from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.auth.permission import PermissionService
from pulsemq.engine.pipeline import (
    AuthError,
    AuthInterceptor,
    Interceptor,
    InterceptorChain,
    MonitorInterceptor,
    PermissionError,
    PermissionInterceptor,
    PipelineContext,
)
from pulsemq.models import AuthUser
from pulsemq.protocol.msg_type import MsgType


# ---- PipelineContext ----


def test_pipeline_context_defaults():
    ctx = PipelineContext(identity=b"c1", msg_type=0, topic="t", meta=b"", payload=b"")
    assert ctx.user is None
    assert ctx.record_count == 0
    assert ctx.timestamp > 0


def test_pipeline_context_user_set():
    ctx = PipelineContext(identity=b"c1", msg_type=0, topic="t", meta=b"", payload=b"")
    ctx.user = AuthUser(user_id=1, role="user", groups=[], api_key="k")
    assert ctx.user.user_id == 1


# ---- InterceptorChain 顺序与隔离 ----


class _RecordingInterceptor(Interceptor):
    """记录 intercept 调用顺序, 含 next_fn 调用。"""

    def __init__(self, name, raise_on=None, skip_next=False):
        self.name = name
        self.raise_on = raise_on
        self.skip_next = skip_next
        self.calls = []

    async def intercept(self, context, next_fn):
        self.calls.append(f"{self.name}.before")
        if self.raise_on == "before":
            raise RuntimeError(f"{self.name} 故意抛错")
        if not self.skip_next:
            await next_fn()
        self.calls.append(f"{self.name}.after")
        if self.raise_on == "after":
            raise RuntimeError(f"{self.name} after 抛错")


@pytest.mark.asyncio
async def test_chain_calls_interceptors_in_order():
    """拦截器按列表顺序调用 before, 反序调用 after。"""
    a = _RecordingInterceptor("a")
    b = _RecordingInterceptor("b")
    chain = InterceptorChain([a, b])

    ctx = PipelineContext(identity=b"c", msg_type=0, topic="t", meta=b"", payload=b"")
    terminal_calls = []

    async def terminal():
        terminal_calls.append("terminal")

    await chain.execute(ctx, terminal_handler=terminal)

    # a.before, b.before, terminal, b.after, a.after
    assert a.calls == ["a.before", "a.after"]
    assert b.calls == ["b.before", "b.after"]
    assert terminal_calls == ["terminal"]


@pytest.mark.asyncio
async def test_chain_empty_calls_terminal_directly():
    """无拦截器时, terminal_handler 应被直接调用。"""
    chain = InterceptorChain([])
    ctx = PipelineContext(identity=b"c", msg_type=0, topic="t", meta=b"", payload=b"")
    called = []

    async def terminal():
        called.append("t")

    await chain.execute(ctx, terminal_handler=terminal)
    assert called == ["t"]


@pytest.mark.asyncio
async def test_chain_exception_in_before_propagates():
    """拦截器 before 抛错, 应传播 (不吞)。"""
    a = _RecordingInterceptor("a", raise_on="before")
    b = _RecordingInterceptor("b")
    chain = InterceptorChain([a, b])

    ctx = PipelineContext(identity=b"c", msg_type=0, topic="t", meta=b"", payload=b"")

    async def noop():
        pass

    with pytest.raises(RuntimeError, match="a 故意抛错"):
        await chain.execute(ctx, terminal_handler=noop)
    # b 不应被调用
    assert b.calls == []


@pytest.mark.asyncio
async def test_chain_exception_in_after_propagates():
    """拦截器 after 抛错, 应传播。"""
    a = _RecordingInterceptor("a", raise_on="after")
    chain = InterceptorChain([a])

    ctx = PipelineContext(identity=b"c", msg_type=0, topic="t", meta=b"", payload=b"")

    async def noop():
        pass

    with pytest.raises(RuntimeError, match="a after 抛错"):
        await chain.execute(ctx, terminal_handler=noop)


@pytest.mark.asyncio
async def test_chain_skip_next_does_not_call_terminal():
    """拦截器跳过 next_fn 时, terminal_handler 不应被调用。"""
    a = _RecordingInterceptor("a", skip_next=True)
    chain = InterceptorChain([a])
    ctx = PipelineContext(identity=b"c", msg_type=0, topic="t", meta=b"", payload=b"")
    called = []

    async def terminal():
        called.append("t")

    await chain.execute(ctx, terminal_handler=terminal)
    assert called == []
    # a.before 和 a.after 都应被记录 (skip 不影响 after)
    assert a.calls == ["a.before", "a.after"]


@pytest.mark.asyncio
async def test_chain_no_terminal_handler_runs_all_interceptors():
    """terminal_handler=None 时, 链应能正常完成, 不抛错。"""
    a = _RecordingInterceptor("a")
    chain = InterceptorChain([a])
    ctx = PipelineContext(identity=b"c", msg_type=0, topic="t", meta=b"", payload=b"")
    await chain.execute(ctx, terminal_handler=None)
    assert a.calls == ["a.before", "a.after"]


# ---- AuthInterceptor ----


@pytest.mark.asyncio
async def test_auth_interceptor_passes_when_user_known():
    store = AuthMemoryStore()
    user = AuthUser(user_id=1, role="user", groups=[], api_key="k")
    store.register(b"c1", user)
    chain = InterceptorChain([AuthInterceptor(store)])
    ctx = PipelineContext(identity=b"c1", msg_type=MsgType.PUB, topic="t", meta=b"", payload=b"")

    async def noop():
        pass

    await chain.execute(ctx, terminal_handler=noop)
    assert ctx.user is user


@pytest.mark.asyncio
async def test_auth_interceptor_raises_when_unknown():
    store = AuthMemoryStore()
    chain = InterceptorChain([AuthInterceptor(store)])
    ctx = PipelineContext(identity=b"unknown", msg_type=MsgType.PUB, topic="t", meta=b"", payload=b"")

    async def noop():
        pass

    with pytest.raises(AuthError, match="未认证连接"):
        await chain.execute(ctx, terminal_handler=noop)


# ---- PermissionInterceptor ----


class _StubPermRepo:
    def __init__(self, perms):
        self._perms = perms

    async def get_user_expanded_permissions(self, user_id):
        return self._perms.get(user_id, {})


@pytest.mark.asyncio
async def test_permission_interceptor_passes_admin():
    """admin 跳过权限检查。"""
    user = AuthUser(user_id=1, role="admin", groups=[], api_key="k")
    repo = _StubPermRepo({})
    perm = PermissionService(repo)
    chain = InterceptorChain([PermissionInterceptor(perm)])
    ctx = PipelineContext(identity=b"c1", msg_type=MsgType.PUB, topic="t", meta=b"", payload=b"")
    ctx.user = user

    async def noop():
        pass

    await chain.execute(ctx, terminal_handler=noop)


@pytest.mark.asyncio
async def test_permission_interceptor_passes_when_allowed():
    user = AuthUser(user_id=1, role="user", groups=[], api_key="k")
    repo = _StubPermRepo({1: {"pub": ["team-a.>"]}})
    perm = PermissionService(repo)
    chain = InterceptorChain([PermissionInterceptor(perm)])
    ctx = PipelineContext(identity=b"c1", msg_type=MsgType.PUB,
                          topic="team-a.mkt.sh.600000", meta=b"", payload=b"")
    ctx.user = user

    async def noop():
        pass

    await chain.execute(ctx, terminal_handler=noop)


@pytest.mark.asyncio
async def test_permission_interceptor_denies_unauthorized():
    user = AuthUser(user_id=1, role="user", groups=[], api_key="k")
    repo = _StubPermRepo({1: {"pub": ["team-a.>"]}})
    perm = PermissionService(repo)
    chain = InterceptorChain([PermissionInterceptor(perm)])
    ctx = PipelineContext(identity=b"c1", msg_type=MsgType.PUB,
                          topic="other.x.y", meta=b"", payload=b"")
    ctx.user = user

    async def noop():
        pass

    with pytest.raises(PermissionError, match="权限不足"):
        await chain.execute(ctx, terminal_handler=noop)


@pytest.mark.asyncio
async def test_permission_interceptor_passes_non_permission_message():
    """非权限敏感消息 (如 PING) 不走权限检查。"""
    user = AuthUser(user_id=1, role="user", groups=[], api_key="k")
    repo = _StubPermRepo({})  # 无任何权限
    perm = PermissionService(repo)
    chain = InterceptorChain([PermissionInterceptor(perm)])
    ctx = PipelineContext(identity=b"c1", msg_type=MsgType.PING, topic="t", meta=b"", payload=b"")
    ctx.user = user

    async def noop():
        pass

    # PING 不需要权限, 应通过
    await chain.execute(ctx, terminal_handler=noop)


# ---- MonitorInterceptor ----


@pytest.mark.asyncio
async def test_monitor_interceptor_increments_count():
    chain = InterceptorChain([MonitorInterceptor()])
    ctx = PipelineContext(identity=b"c1", msg_type=MsgType.PING, topic="t", meta=b"", payload=b"x" * 10)

    async def noop():
        pass

    await chain.execute(ctx, terminal_handler=noop)
    chain_interceptor = chain._interceptors[0]
    stats = chain_interceptor.stats
    assert stats["msg_count"] == 1
    assert stats["error_count"] == 0


@pytest.mark.asyncio
async def test_monitor_interceptor_counts_errors():
    """终端 handler 抛错, monitor 应记录 error_count=1。"""
    chain = InterceptorChain([MonitorInterceptor()])
    ctx = PipelineContext(identity=b"c1", msg_type=MsgType.PING, topic="t", meta=b"", payload=b"")

    async def raise_handler():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        await chain.execute(ctx, terminal_handler=raise_handler)
    monitor = chain._interceptors[0]
    assert monitor.stats["error_count"] == 1


def test_monitor_interceptor_remove_identity_updates_count():
    """remove_identity 后 active_connections 应减少。"""
    monitor = MonitorInterceptor()
    monitor._seen_identities.add(b"c1")
    monitor._seen_identities.add(b"c2")
    monitor.remove_identity(b"c1")
    assert b"c1" not in monitor._seen_identities
    assert b"c2" in monitor._seen_identities


# ---- 链式组合 ----


@pytest.mark.asyncio
async def test_full_chain_auth_perm_monitor():
    """Auth + Permission + Monitor 链组合: 全部通过。"""
    store = AuthMemoryStore()
    user = AuthUser(user_id=1, role="user", groups=[], api_key="k")
    store.register(b"c1", user)
    repo = _StubPermRepo({1: {"pub": ["team-a.>"]}})
    perm = PermissionService(repo)
    chain = InterceptorChain([
        AuthInterceptor(store),
        PermissionInterceptor(perm),
        MonitorInterceptor(),
    ])
    ctx = PipelineContext(identity=b"c1", msg_type=MsgType.PUB,
                          topic="team-a.mkt.sh.600000", meta=b"", payload=b"")

    async def noop():
        pass

    await chain.execute(ctx, terminal_handler=noop)
    assert ctx.user is user
    monitor = chain._interceptors[2]
    assert monitor.stats["msg_count"] == 1
