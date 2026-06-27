import asyncio
import socket as _sock

from pulsemq.admin.auth import TokenAuth


def test_tokenauth_disabled_when_none():
    ta = TokenAuth(None)
    assert ta.enabled is False
    assert ta.validate({}, {}) is True  # 禁用时一律放行


def test_tokenauth_query_and_header():
    ta = TokenAuth("s3cret")
    assert ta.enabled is True
    assert ta.validate({}, {"token": ["s3cret"]}) is True
    assert ta.validate({"authorization": "Bearer s3cret"}, {}) is True
    assert ta.validate({}, {}) is False
    assert ta.validate({"authorization": "Bearer wrong"}, {}) is False
    assert ta.validate({}, {"token": ["wrong"]}) is False


# 注：admin 集成测试 test_admin_healthz_open_others_require_token 依赖
# Server(admin_token=...) 参数（Task 8 引入），本任务（Task 7）暂不实现。
