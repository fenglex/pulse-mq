# tests/test_client_reconnect.py
"""Task 12b：运行期断线自动重连 + 订阅透明恢复（Spec 1 §8.3）。

E2E：consumer 订阅 → server 停 → 同端口重启 → consumer 自动重连
+ 重新认证 + 恢复订阅 → producer 发布后仍能收到。业务层不需要再次 subscribe()。
"""
from __future__ import annotations

import asyncio
import socket as _sock

from pulsemq import ConsumerClient, ProducerClient, Server


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
