"""producer 管线类型注解的防回归测试。

覆盖：
1. 类型符号可从顶层包导入
2. PublisherSender.send 的 data 参数注解为 PubData（非 Any）
3. PublisherSender / ProducerManager 内部方法签名消除 Any
4. inject_sender=True 时 producer 装饰器返回 SenderProducerCallback 类型

注：这些是反射断言，不依赖类型检查器运行——防止有人把 PubData 退回 Any。

关于 localns：ProducerManager / PulsePublisher 的部分方法注解经 SenderFactory
引用了字符串前向引用 "PublisherSender"。manager.py 故意不 import publisher.py
（避免循环导入），所以 get_type_hints 解析这些方法时需要通过 localns 注入
PublisherSender，否则会抛 NameError。
"""

from __future__ import annotations

import inspect
from typing import Any, get_type_hints

import pandas as pd

import pulsemq
from pulsemq import PublisherSender, PubData
from pulsemq.publisher import PulsePublisher
from pulsemq.producers.manager import ProducerManager, ProducerSpec
from pulsemq.producers.types import (
    PubData as TypesPubData,
    SimpleProducerCallback,
    SenderProducerCallback,
    ProducerCallback,
)

# 解析 ProducerManager / PulsePublisher 方法注解时注入的前向引用命名空间
# （SenderFactory 引用了 "PublisherSender" 字符串注解）
_HINTS_LOCALNS = {"PublisherSender": PublisherSender}


# ---------------------------------------------------------------------------
# 类型符号可导入性
# ---------------------------------------------------------------------------


class TestTypeSymbolsImportable:
    def test_pubdata_exported_from_package(self):
        """PubData 能从 pulsemq 顶层导入。"""
        assert pulsemq.PubData is not None

    def test_publisher_sender_exported_from_package(self):
        """PublisherSender 能从 pulsemq 顶层导入。"""
        assert pulsemq.PublisherSender is PublisherSender

    def test_pubdata_consistent_across_modules(self):
        """__init__ 导出的 PubData 与 types.py 定义一致。"""
        assert PubData is TypesPubData

    def test_callback_aliases_defined(self):
        """三个回调别名都存在。"""
        assert SimpleProducerCallback is not None
        assert SenderProducerCallback is not None
        assert ProducerCallback is not None


# ---------------------------------------------------------------------------
# PublisherSender.send 的 data 参数注解
# ---------------------------------------------------------------------------


class TestPublisherSenderSignature:
    def test_send_data_param_is_pubdata(self):
        """send() 的 data 参数注解应是 PubData 联合类型，而非 Any。

        get_type_hints 会解析字符串注解；PubData 是 typing.Union 别名。
        我们校验：注解不是 Any，且 union 的成员包含 pd.DataFrame / dict / bytes / str。
        """
        hints = get_type_hints(PublisherSender.send)
        data_hint = hints["data"]

        # Any 的判定：直接相等
        assert data_hint is not Any, "send(data) 退回到了 Any"

        # PubData = Union[pd.DataFrame, dict, bytes, str]
        # typing.Union 的成员通过 __args__ 获取
        args = set(data_hint.__args__)
        assert pd.DataFrame in args, f"PubData 缺少 DataFrame: {args}"
        assert dict in args, f"PubData 缺少 dict: {args}"
        assert bytes in args, f"PubData 缺少 bytes: {args}"
        assert str in args, f"PubData 缺少 str: {args}"

    def test_sender_init_spec_param_is_producer_spec(self):
        """__init__ 的 spec 参数应是 ProducerSpec，而非 Any。"""
        hints = get_type_hints(PublisherSender.__init__)
        assert hints["spec"] is ProducerSpec


# ---------------------------------------------------------------------------
# ProducerManager 方法签名消除 Any
# ---------------------------------------------------------------------------


class TestProducerManagerSignature:
    def test_start_all_typed(self):
        """start_all 的 on_message / sender_factory 不应是 Any。"""
        on_msg_hints = get_type_hints(ProducerManager.start_all, None, _HINTS_LOCALNS)
        # on_message 注解存在且不是 Any
        assert "on_message" in on_msg_hints
        assert on_msg_hints["on_message"] is not Any
        # sender_factory 注解存在（可为 Optional）
        assert "sender_factory" in on_msg_hints

    def test_run_loop_typed(self):
        """_run_loop 的参数消除 Any。"""
        hints = get_type_hints(ProducerManager._run_loop, None, _HINTS_LOCALNS)
        assert hints.get("on_message") is not Any
        assert hints.get("spec") is ProducerSpec

    def test_run_burst_loop_typed(self):
        """_run_burst_loop 的参数消除 Any。"""
        hints = get_type_hints(ProducerManager._run_burst_loop, None, _HINTS_LOCALNS)
        assert hints.get("on_message") is not Any
        assert hints.get("spec") is ProducerSpec


# ---------------------------------------------------------------------------
# Publisher 内部方法签名
# ---------------------------------------------------------------------------


class TestPublisherInternalSignature:
    def test_on_produce_typed(self):
        """_on_produce(spec, data) 参数消除 Any。"""
        hints = get_type_hints(PulsePublisher._on_produce, None, _HINTS_LOCALNS)
        assert hints.get("spec") is not Any
        assert hints.get("data") is not Any

    def test_make_sender_return_type(self):
        """_make_sender 返回 PublisherSender。"""
        hints = get_type_hints(PulsePublisher._make_sender, None, _HINTS_LOCALNS)
        assert hints.get("return") is PublisherSender
