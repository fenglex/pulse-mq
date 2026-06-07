"""Web UI (HTML/CSS/JS 字符串常量) 单元测试。"""

from __future__ import annotations

import sys

import pytest

if sys.platform == "win32":
    from pulsemq.event_loop import install_event_loop

    install_event_loop(use_uvloop=False)

from pulsemq.monitoring.web_ui import INDEX_HTML


def test_index_html_basic():
    assert "<!DOCTYPE html>" in INDEX_HTML
    assert "PulseMQ" in INDEX_HTML
    assert "管理后台" in INDEX_HTML
    assert "lang=\"zh-CN\"" in INDEX_HTML


def test_index_html_has_all_tabs():
    for tab in ("overview", "topics", "clients", "users"):
        assert f'data-tab="{tab}"' in INDEX_HTML, f"missing tab: {tab}"
    # batch tab 已移除 (batcher 策略撤销)
    assert 'data-tab="batch"' not in INDEX_HTML, "batch tab 不应再存在"


def test_index_html_has_sse_support():
    """Web UI 必须使用 EventSource 接入 SSE。"""
    assert "EventSource" in INDEX_HTML
    assert "/api/v1/metrics/stream" in INDEX_HTML


def test_index_html_no_external_cdn():
    """不应引用任何外部 CDN / 资源。"""
    forbidden_substrings = [
        "https://cdn.",
        "https://unpkg.com",
        "https://cdnjs.",
        "<script src=\"http",
        "<link href=\"http",
    ]
    for sub in forbidden_substrings:
        assert sub not in INDEX_HTML, f"检测到外部依赖: {sub}"


def test_index_html_uses_fetch_api():
    assert "fetch(" in INDEX_HTML


def test_index_html_handles_all_apis():
    """Web UI 应至少调用下列 API。"""
    # realtime 通过 SSE stream 走, 不在 fetch 列表中
    for api in (
        "/api/v1/system/status",
        "/api/v1/topics",
        "/api/v1/clients",
        "/api/v1/users",
        "/api/v1/permissions",
    ):
        assert api in INDEX_HTML, f"missing API: {api}"


def test_index_html_has_modal_for_create_user():
    assert "modal-user" in INDEX_HTML
    assert "新建用户" in INDEX_HTML or "添加用户" in INDEX_HTML
    assert "submitCreateUser" in INDEX_HTML


def test_index_html_has_modal_for_grant_perm():
    assert "modal-perm" in INDEX_HTML
    assert "submitGrantPerm" in INDEX_HTML


def test_index_html_handles_topic_history():
    assert "showTopicHistory" in INDEX_HTML
    assert "/history" in INDEX_HTML


def test_index_html_basic_styling():
    """CSS 应当包含响应式布局的基础元素。"""
    assert "<style>" in INDEX_HTML
    assert "background" in INDEX_HTML
    assert "color:" in INDEX_HTML or "color " in INDEX_HTML


def test_index_html_size_reasonable():
    """Web UI 字符串不应过大 (< 100KB)."""
    assert len(INDEX_HTML) < 100_000
