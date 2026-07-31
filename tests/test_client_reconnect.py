# tests/test_client_reconnect.py
"""Task 12b：运行期断线自动重连 + 订阅透明恢复（Spec 1 §8.3）。

E2E：consumer 订阅 → server 停 → 同端口重启 → consumer 自动重连
+ 重新认证 + 恢复订阅 → producer 发布后仍能收到。业务层不需要再次 subscribe()。

第二个测试覆盖 review Fix 1：重连时认证失败（凭据被服务端改掉）→
``_reconnect_loop`` 把 ``AuthenticationError`` 存到 ``consumer._reconnect_fatal``
而非在后台任务里 raise（那样会被 asyncio GC 吞掉），run_forever 在主上下文重抛。
"""
from __future__ import annotations

import asyncio
import socket as _sock

import pytest

from pulsemq import ConsumerClient, ProducerClient, Server
from pulsemq.errors import AuthenticationError


def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def test_client_reconnects_and_restores_subscription():
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    data_ep = f"tcp://127.0.0.1:{dp}"
    ctrl_ep = f"tcp://127.0.0.1:{cp}"
    creds = {"c": "c", "p": "p"}
    srv = Server(
        data_endpoint=data_ep,
        control_endpoint=ctrl_ep,
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials=creds,
    )
    await srv.start()
    await asyncio.sleep(0.2)  # ZAP/ROUTER 稳定
    try:
        consumer = ConsumerClient(data_ep, ctrl_ep, "c", "c")
        await consumer.start()
        got: list = []
        await consumer.subscribe("market.*", lambda m: got.append(m.topic))
        await asyncio.sleep(0.3)

        # 断开 Server —— client 的 monitor 应收到 disconnected，触发重连循环
        await srv.stop()
        await asyncio.sleep(1.0)  # 让 client 检测到 disconnected + 启动重连

        # 同端口重启 Server
        srv2 = Server(
            data_endpoint=data_ep,
            control_endpoint=ctrl_ep,
            admin_endpoint=f"127.0.0.1:{ap}",
            credentials=creds,
        )
        await srv2.start()
        await asyncio.sleep(0.3)  # 新 server bind 稳定

        producer = ProducerClient(data_ep, ctrl_ep, "p", "p")
        await producer.start()

        # 等待 client 重连（初始退避 1s）+ 重新认证 + 恢复订阅
        await asyncio.sleep(3.0)
        await producer.publish("market.stock.x", {"k": 1})
        await asyncio.sleep(1.0)
        assert "market.stock.x" in got, (
            f"订阅透明恢复失败：got={got}"
        )

        await producer.stop()
        await consumer.stop()
        await srv2.stop()
    except Exception:
        # 异常路径也要清理
        try:
            await consumer.stop()
        except Exception:
            pass
        try:
            await srv2.stop()
        except Exception:
            pass
        raise


async def test_run_forever_reraises_reconnect_auth_failure():
    """Round-2 Fix A：重连认证失败时，``ProducerClient.run_forever`` 必须在
    主任务上下文重新抛出 ``AuthenticationError``（exit 3 路径的真正契约），
    而不只是设置 ``_reconnect_fatal`` 实例字段。

    流程：启动带 creds 的 server → 启动注册了 producer 的 ProducerClient →
    在 task 里跑 ``run_forever`` → 停 server → 同端口用错密码重启 →
    重连 PLAIN 认证失败 → 轮询 ``_reconnect_fatal`` 直到非空 → 断言
    run_forever task 已结束且 ``task.exception()`` 是 ``AuthenticationError``。
    """
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    data_ep = f"tcp://127.0.0.1:{dp}"
    ctrl_ep = f"tcp://127.0.0.1:{cp}"
    creds = {"c": "c", "p": "p"}
    srv = Server(
        data_endpoint=data_ep,
        control_endpoint=ctrl_ep,
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials=creds,
    )
    await srv.start()
    await asyncio.sleep(0.2)
    producer = ProducerClient(data_ep, ctrl_ep, "p", "p")

    @producer.producer("p.topic", interval=10.0)
    def _produce():
        return {"k": 1}

    # run_forever 内部会 start()，所以这里不再预启动。
    rf_task = asyncio.create_task(producer.run_forever())
    # 等 start() 完成（批量跑中固定 0.5s 可能不够，REGISTER 未完成就停 server
    # 会导致 ClientStartupError 而非期望的 AuthenticationError，用轮询确认 connected）
    for _ in range(30):
        if producer._connected:
            break
        await asyncio.sleep(0.1)
    await asyncio.sleep(0.3)  # producer 调度稳定后进入主循环
    try:
        # 断开 server → producer 检测 disconnected → _reconnect_loop。
        await srv.stop()
        await asyncio.sleep(1.0)

        # 同端口重启，但把 "p" 的密码改掉 → 重连 PLAIN 认证失败。
        srv2 = Server(
            data_endpoint=data_ep,
            control_endpoint=ctrl_ep,
            admin_endpoint=f"127.0.0.1:{ap}",
            credentials={"c": "c", "p": "WRONG"},
        )
        await srv2.start()
        await asyncio.sleep(0.3)
        try:
            # 初始退避 1s；轮询直到 run_forever task 结束。它应当以
            # AuthenticationError 异常结束（_reconnect_loop 把异常存到
            # _reconnect_fatal + set _stop → run_forever 主循环退出 → finally
            # 在主任务上下文重抛）。注意：不能轮询 _reconnect_fatal 本身，因为
            # run_forever 的 finally 会在重抛前把它清空，可能错过窗口。
            for _ in range(80):  # 80 × 0.5s = 40s 上限，覆盖退避+认证裁定
                if rf_task.done():
                    break
                await asyncio.sleep(0.5)
            assert rf_task.done(), (
                "run_forever task 未在超时内结束（重连认证失败未触发重抛）"
            )

            # 核心契约：run_forever 必须在主任务上下文重抛 AuthenticationError。
            exc = rf_task.exception()
            assert isinstance(exc, AuthenticationError), (
                f"run_forever 应重抛 AuthenticationError，实际 exception={exc!r}"
            )
            assert exc.reason == "invalid_password", (
                f"reason 应为 invalid_password，实际 {exc.reason!r}"
            )
        finally:
            try:
                await srv2.stop()
            except Exception:
                pass
    finally:
        if not rf_task.done():
            rf_task.cancel()
            try:
                await rf_task
            except (asyncio.CancelledError, Exception):
                pass
        try:
            await producer.stop()
        except Exception:
            pass


async def test_reconnect_auth_failure_sets_reconnect_fatal():
    """Review Fix 1：重连认证失败 → ``_reconnect_fatal`` 被设为
    ``AuthenticationError(reason="invalid_password")``，且 ``_stop`` 被置位。

    断网后用错误密码重启服务端凭据表，使 consumer 重连时 PLAIN 认证失败。
    断言实例属性（不依赖后台任务异常传播，避免 asyncio GC 吞错带来的不确定）。
    """
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    data_ep = f"tcp://127.0.0.1:{dp}"
    ctrl_ep = f"tcp://127.0.0.1:{cp}"
    creds = {"c": "c", "p": "p"}
    srv = Server(
        data_endpoint=data_ep,
        control_endpoint=ctrl_ep,
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials=creds,
    )
    await srv.start()
    await asyncio.sleep(0.2)
    consumer = ConsumerClient(data_ep, ctrl_ep, "c", "c")
    await consumer.start()
    await asyncio.sleep(0.3)

    try:
        # 断开 server → client 检测 disconnected → 进入 _reconnect_loop。
        await srv.stop()
        await asyncio.sleep(1.0)

        # 同端口重启，但把 client "c" 的密码改掉 → 重连时 PLAIN 认证失败。
        srv2 = Server(
            data_endpoint=data_ep,
            control_endpoint=ctrl_ep,
            admin_endpoint=f"127.0.0.1:{ap}",
            credentials={"c": "WRONG", "p": "p"},
        )
        await srv2.start()
        await asyncio.sleep(0.3)
        try:
            # 初始退避 1s；给足时间让认证裁定到达 auth_failed 分支。
            for _ in range(40):
                await asyncio.sleep(0.5)
                if consumer._reconnect_fatal is not None:
                    break

            fatal = consumer._reconnect_fatal
            assert isinstance(fatal, AuthenticationError), (
                f"_reconnect_fatal 不是 AuthenticationError: {fatal!r}"
            )
            assert fatal.reason == "invalid_password", (
                f"reason 应为 invalid_password，实际 {fatal.reason!r}"
            )
            # _stop 被置位 → run_forever 主循环会退出并重抛。
            assert consumer._stop.is_set(), "_stop 未被置位"
        finally:
            try:
                await srv2.stop()
            except Exception:
                pass
    finally:
        try:
            await consumer.stop()
        except Exception:
            pass
