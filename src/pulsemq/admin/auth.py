"""admin HTTP token 认证中间件。仅依赖标准库 hmac。"""
from __future__ import annotations

import hmac


class TokenAuth:
    """除 /healthz 外所有 admin 路由需携带有效 token。

    token 经 ``?token=xxx`` query 或 ``Authorization: Bearer xxx`` header 携带。
    expected_token 为 None/空 → 禁用（放行，向后兼容 Spec 1 测试）。
    """

    def __init__(self, expected_token: str | None) -> None:
        self._expected = expected_token or ""

    @property
    def enabled(self) -> bool:
        return bool(self._expected)

    def validate(self, headers: dict[str, str], query: dict[str, list[str]]) -> bool:
        if not self.enabled:
            return True
        presented = ""
        auth = headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            presented = auth[7:].strip()
        if not presented:
            vals = query.get("token")
            if vals:
                presented = vals[0]
        return hmac.compare_digest(presented, self._expected)
