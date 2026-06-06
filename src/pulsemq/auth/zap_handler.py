"""ZMQ ZAP (ZeroMQ Authentication Protocol) Handler。

ZMQ 在连接握手阶段自动调用此 Handler 验证客户端。
客户端通过 ZMQ_PLAIN_USERNAME 传递 api_key。
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
from dataclasses import dataclass

from pulsemq.auth.memory_store import AuthMemoryStore
from pulsemq.models import AuthUser
from pulsemq.storage.interfaces import User

logger = logging.getLogger(__name__)

# 全局持久化线程池，避免每次认证创建新线程池
_ZAP_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=2, thread_name_prefix="zap")


@dataclass
class ZapResponse:
    """ZAP 响应。"""

    status_code: str    # "200" = OK, "400" = Error
    status_text: str    # 原因描述
    user_id: str = ""   # ZAP metadata（可选）


class PulseMQZAPHandler:
    """ZMQ ZAP 认证处理器。

    工作流程:
    1. ZMQ 连接握手时自动调用 __call__
    2. 从 username 字段提取 api_key
    3. 查 DB 验证 api_key
    4. 通过 → 写入 AuthMemoryStore + 返回 ACCEPT
    5. 拒绝 → 返回 REJECT（ZMQ 直接断开连接）
    """

    def __init__(
        self,
        auth_store: AuthMemoryStore,
        user_lookup_fn,          # async (api_key: str) -> User | None
    ):
        self._auth_store = auth_store
        self._user_lookup_fn = user_lookup_fn

    def handle_zap_request(
        self,
        domain: str,
        address: str,
        mechanism: str,
        username: str,
        password: str,
        client_key: bytes | None = None,
    ) -> ZapResponse:
        """处理 ZAP 请求（同步，由 ZMQ IO 线程调用）。

        使用持久化线程池执行 async 查询，避免资源泄漏。
        """
        api_key = username  # PLAIN 模式下 username = api_key

        if not api_key:
            return ZapResponse("400", "Empty API key")

        # 使用持久化线程池执行异步查询
        try:
            future = _ZAP_EXECUTOR.submit(asyncio.run, self._user_lookup_fn(api_key))
            user = future.result(timeout=5.0)
        except Exception as e:
            logger.error("ZAP 查询用户失败: %s", e)
            return ZapResponse("400", "Internal auth error")

        if user is None:
            logger.warning("ZAP 拒绝: 无效 api_key")
            return ZapResponse("400", "Invalid API key")

        if user.disabled:
            return ZapResponse("400", "Account disabled")

        # 检查连接数限制
        if self._auth_store.connection_count(user.id) >= user.max_connections:
            return ZapResponse("400", "Too many connections")

        # 认证通过 → 写入内存
        identity = address.encode("utf-8") if address else b"unknown"
        auth_user = AuthUser(
            user_id=user.id,
            role=user.role,
            groups=[],  # 权限组在权限服务中按需加载
            api_key=user.api_key,
            namespace=user.namespace,
        )
        self._auth_store.register(identity, auth_user)

        logger.info("ZAP 认证通过: user=%s role=%s", user.username, user.role)
        return ZapResponse("200", "OK", user_id=str(user.id))
