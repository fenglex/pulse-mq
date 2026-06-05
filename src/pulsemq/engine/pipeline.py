"""拦截器链框架 + 内置拦截器。

链顺序: AuthInterceptor → PermissionInterceptor → MonitorInterceptor
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable, Awaitable

from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.auth.permission import PermissionService
from pulsemq.models import AuthUser

logger = logging.getLogger(__name__)


@dataclass
class PipelineContext:
    """拦截器链传递的上下文。"""

    identity: bytes
    msg_type: int
    topic: str
    meta: bytes
    payload: bytes
    record_count: int = 0
    user: AuthUser | None = None
    timestamp: float = field(default_factory=time.time)


class Interceptor(ABC):
    """拦截器抽象接口。"""

    @abstractmethod
    async def intercept(self, context: PipelineContext, next_fn: Callable[[], Awaitable[None]]) -> None: ...


class InterceptorChain:
    """拦截器链：按顺序执行每个拦截器。"""

    def __init__(self, interceptors: list[Interceptor]):
        self._interceptors = interceptors

    async def execute(self, context: PipelineContext) -> None:
        """执行拦截器链。"""
        await self._run(context, 0)

    async def _run(self, context: PipelineContext, index: int) -> None:
        if index >= len(self._interceptors):
            return

        interceptor = self._interceptors[index]

        async def next_fn() -> None:
            await self._run(context, index + 1)

        await interceptor.intercept(context, next_fn)


# ---- 内置拦截器 ----


class AuthInterceptor(Interceptor):
    """认证拦截器：从 identity 查用户，注入 context.user。

    未认证的消息抛出 AuthError。
    """

    def __init__(self, auth_store: AuthMemoryStore):
        self._auth_store = auth_store

    async def intercept(self, context: PipelineContext, next_fn: Callable[[], Awaitable[None]]) -> None:
        user = self._auth_store.get_user(context.identity)
        if user is None:
            raise AuthError("未认证连接")
        context.user = user
        await next_fn()


class PermissionInterceptor(Interceptor):
    """权限拦截器：校验用户对 topic 的 action 权限。

    admin 跳过，其他用户查权限缓存校验。
    """

    def __init__(self, permission_service: PermissionService):
        self._perm_service = permission_service

    async def intercept(self, context: PipelineContext, next_fn: Callable[[], Awaitable[None]]) -> None:
        # 非权限敏感消息直接通过
        from pulsemq.protocol.msg_type import MsgType
        action = self._msg_type_to_action(context.msg_type)
        if action is None:
            await next_fn()
            return

        # admin 跳过
        if context.user is not None and context.user.is_admin:
            await next_fn()
            return

        # 校验权限
        if context.user is None:
            raise AuthError("未认证")

        allowed = await self._perm_service.check_permission(
            context.user, action, context.topic
        )
        if not allowed:
            raise PermissionError(f"权限不足: {action} {context.topic}")

        await next_fn()

    @staticmethod
    def _msg_type_to_action(msg_type: int) -> str | None:
        from pulsemq.protocol.msg_type import MsgType
        if msg_type == MsgType.PUB:
            return "pub"
        elif msg_type in (MsgType.SUB, MsgType.UNSUB):
            return "sub"
        elif msg_type == MsgType.QUERY:
            return "query"
        return None


class MonitorInterceptor(Interceptor):
    """监控拦截器：记录每条消息的处理延迟。"""

    def __init__(self):
        self._msg_count = 0
        self._error_count = 0
        self._total_latency_ms = 0.0

    async def intercept(self, context: PipelineContext, next_fn: Callable[[], Awaitable[None]]) -> None:
        start = time.monotonic()
        try:
            await next_fn()
        except Exception:
            self._error_count += 1
            raise
        finally:
            elapsed = (time.monotonic() - start) * 1000
            self._msg_count += 1
            self._total_latency_ms += elapsed

    @property
    def stats(self) -> dict:
        avg = self._total_latency_ms / self._msg_count if self._msg_count > 0 else 0
        return {
            "msg_count": self._msg_count,
            "error_count": self._error_count,
            "avg_latency_ms": avg,
        }


# ---- 异常类型 ----


class AuthError(Exception):
    pass


class PermissionError(Exception):
    pass
