"""拦截器链测试。"""

import pytest
from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.auth.permission import PermissionService
from pulsemq.engine.pipeline import (
    AuthInterceptor,
    AuthError,
    InterceptorChain,
    MonitorInterceptor,
    PermissionInterceptor,
    PermissionError,
    PipelineContext,
)
from pulsemq.models import AuthUser
from pulsemq.protocol.msg_type import MsgType


def _make_context(
    identity=b"test_id",
    msg_type=MsgType.PUB,
    topic="team-a.mkt.sh.600000",
) -> PipelineContext:
    return PipelineContext(
        identity=identity,
        msg_type=msg_type,
        topic=topic,
        meta=bytes([msg_type, 0x20]),
        payload=b"",
    )


class TestInterceptorChain:
    async def test_chain_executes_all(self):
        order = []

        class A(InterceptorChain if False else object):
            async def intercept(self, ctx, next_fn):
                order.append("A")
                await next_fn()

        class InterceptorA:
            async def intercept(self, ctx, next_fn):
                order.append("A")
                await next_fn()

        class InterceptorB:
            async def intercept(self, ctx, next_fn):
                order.append("B")
                await next_fn()

        from pulsemq.engine.pipeline import Interceptor
        chain = InterceptorChain([InterceptorA(), InterceptorB()])
        await chain.execute(_make_context())
        assert order == ["A", "B"]

    async def test_short_circuit(self):
        """异常终止后续拦截器。"""
        order = []

        class InterceptorA:
            async def intercept(self, ctx, next_fn):
                order.append("A")
                raise AuthError("test")

        class InterceptorB:
            async def intercept(self, ctx, next_fn):
                order.append("B")
                await next_fn()

        chain = InterceptorChain([InterceptorA(), InterceptorB()])
        with pytest.raises(AuthError):
            await chain.execute(_make_context())
        assert order == ["A"]

    async def test_context_passing(self):
        """拦截器修改 context，下游可见。"""
        class InterceptorA:
            async def intercept(self, ctx, next_fn):
                ctx.topic = "modified"
                await next_fn()

        ctx = _make_context()
        chain = InterceptorChain([InterceptorA()])
        await chain.execute(ctx)
        assert ctx.topic == "modified"


class TestAuthInterceptor:
    async def test_authenticated_passes(self):
        store = AuthMemoryStore()
        user = AuthUser(user_id=1, role="admin", groups=[], api_key="k", namespace="")
        store.register(b"test_id", user)

        interceptor = AuthInterceptor(store)
        ctx = _make_context()

        async def noop():
            pass

        await interceptor.intercept(ctx, noop)
        assert ctx.user == user

    async def test_unauthenticated_raises(self):
        store = AuthMemoryStore()
        interceptor = AuthInterceptor(store)
        ctx = _make_context()

        async def noop():
            pass

        with pytest.raises(AuthError):
            await interceptor.intercept(ctx, noop)


class TestPermissionInterceptor:
    async def test_admin_bypasses(self):
        perm_svc = PermissionService(perm_repo=None)
        interceptor = PermissionInterceptor(perm_svc)
        ctx = _make_context(msg_type=MsgType.PUB)
        ctx.user = AuthUser(user_id=1, role="admin", groups=[], api_key="k", namespace="")
        # 不应抛异常
        called = False

        async def next_fn():
            nonlocal called
            called = True

        await interceptor.intercept(ctx, next_fn)
        assert called

    async def test_ping_bypasses_permission(self):
        """PING 不需要权限校验。"""
        perm_svc = PermissionService(perm_repo=None)
        interceptor = PermissionInterceptor(perm_svc)
        ctx = _make_context(msg_type=MsgType.PING, topic="")
        ctx.user = AuthUser(user_id=1, role="user", groups=[], api_key="k", namespace="")
        called = False

        async def next_fn():
            nonlocal called
            called = True

        await interceptor.intercept(ctx, next_fn)
        assert called


class TestMonitorInterceptor:
    async def test_counts_messages(self):
        monitor = MonitorInterceptor()
        ctx = _make_context()

        async def next_fn():
            pass

        await monitor.intercept(ctx, next_fn)
        assert monitor.stats["msg_count"] == 1
        assert monitor.stats["avg_latency_ms"] >= 0

    async def test_counts_errors(self):
        monitor = MonitorInterceptor()
        ctx = _make_context()

        async def next_fn():
            raise RuntimeError("test")

        with pytest.raises(RuntimeError):
            await monitor.intercept(ctx, next_fn)
        assert monitor.stats["error_count"] == 1
