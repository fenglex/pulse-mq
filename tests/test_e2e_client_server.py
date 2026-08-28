# tests/test_e2e_client_server.py
"""End-to-end Client/Server 验证测试（Task 15）。

覆盖 Spec 1 §12 的两条消息流：
- 多 producer 扇入单 consumer（前缀订阅 ``market.*``）。
- 单用户单在线：同名 consumer 第二次 REGISTER 被拒，首个连接保持。

复用 ``tests/test_client_lifecycle.py`` 的端口/server-fixture 模式。
"""
from __future__ import annotations

import asyncio
import socket as _sock

import pytest

from pulsemq.client import ConsumerClient, ProducerClient
from pulsemq.errors import ClientStartupError
from pulsemq.server import Server


def _free_port() -> int:
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def _start_server(creds: dict[str, str]) -> tuple[Server, int, int]:
    """启动 Server 并返回 (srv, data_port, control_port)。"""
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials=creds,
    )
    await srv.start()
    # 给 ZAP/ROUTER bind 一点时间稳定下来。
    await asyncio.sleep(0.2)
    return srv, dp, cp


async def test_multi_producer_single_consumer():
    """1 consumer 订阅 ``market.*``；2 producer 各发一条 → consumer 收到两条。"""
    srv, dp, cp = await _start_server({"c": "c", "p1": "p", "p2": "p"})
    try:
        c = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="c",
            password="c",
        )
        p1 = ProducerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="p1",
            password="p",
        )
        p2 = ProducerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="p2",
            password="p",
        )
        await c.start()
        await p1.start()
        await p2.start()
        got: list[str] = []
        await c.subscribe("market.*", lambda m: got.append(m.topic))
        # 让 SUBSCRIBE 控制帧被服务端处理并写入路由表。
        await asyncio.sleep(0.3)
        await p1.publish("market.stock.a", {"x": 1})
        await p2.publish("market.bond.b", {"x": 2})
        # 等数据帧被转发并由 recv_loop 投递到回调。
        await asyncio.sleep(0.5)
        assert sorted(got) == ["market.bond.b", "market.stock.a"]
        await c.stop()
        await p1.stop()
        await p2.stop()
    finally:
        await srv.stop()


async def test_server_producer_traffic_is_counted_by_stats():
    """服务端内置 producer 推送的消息必须计入 TrafficStats（监控可见）。

    回归 Bug：``_on_server_produce`` 曾绕过 ``_data_loop`` 直接 encode→route→send，
    导致 server 端 producer 推送的 topic 在 ``/api/v1/stats/realtime`` 的 topics 中
    完全不出现（消费者收得到、监控看不到）。
    """
    import json as _json
    import urllib.request as _url

    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"alice": "s"},
        admin_token="",  # 禁用 token，便于诊断直接 GET
    )
    await srv.start()
    await asyncio.sleep(0.2)

    @srv.producer("server.tick", interval=0.1, serializer="msgpack")
    async def _gen():
        return {"val": 1}

    # producer 注册发生在 start() 之后，需手动启动调度。
    await srv._producer_mgr.start_all(srv._on_server_produce)
    try:
        c = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="alice",
            password="s",
        )
        await c.start()
        got: list[str] = []
        await c.subscribe("server.tick", lambda m: got.append(m.topic))
        # 让 producer 发若干条并被消费。
        await asyncio.sleep(0.6)

        base = f"http://127.0.0.1:{ap}"
        with _url.urlopen(f"{base}/api/v1/stats/realtime", timeout=5) as r:
            snap = _json.loads(r.read().decode("utf-8"))
        topics = snap.get("topics", {})
        # 消费者确实收到了消息
        assert len(got) > 0, "consumer 未收到 server producer 消息"
        # server.tick 必须出现在流量统计里
        assert "server.tick" in topics, (
            f"server producer 流量未计入 stats（topics={list(topics)})")
        # 且消息计数 > 0
        assert topics["server.tick"]["msg_count_current"] > 0
        await c.stop()
    finally:
        await srv._producer_mgr.stop_all()
        await srv.stop()


async def test_single_user_single_online():
    """同名 consumer 第二次 REGISTER 被拒（ALREADY_ONLINE）；首个连接保持。"""
    srv, dp, cp = await _start_server({"alice": "s"})
    try:
        c1 = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="alice",
            password="s",
        )
        await c1.start()
        c2 = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="alice",
            password="s",
        )
        with pytest.raises(ClientStartupError):
            await c2.start()
        # c1 必须仍然在线：能正常订阅并停机。
        await c1.subscribe("t.*", lambda m: None)
        await c1.stop()
    finally:
        await srv.stop()


async def test_subscriptions_reflected_in_monitor():
    """client subscribe 后，监控的「订阅总数」与在线 client 的 topics 必须反映实时订阅。

    回归 Bug：registry.topics 仅在 REGISTER 写一次，SUBSCRIBE/UNSUBSCRIBE 不回写 →
    ``total_subscriptions`` 恒为 0、/api/v1/clients 的 topics 永远空。
    """
    import json as _json
    import urllib.request as _url

    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"alice": "s"},
        admin_token="",  # 禁用 token，便于诊断直接 GET
    )
    await srv.start()
    await asyncio.sleep(0.2)
    try:
        c = ConsumerClient(
            data_endpoint=f"tcp://127.0.0.1:{dp}",
            control_endpoint=f"tcp://127.0.0.1:{cp}",
            username="alice",
            password="s",
        )
        await c.start()
        await c.subscribe("market.*", lambda m: None)
        await c.subscribe("sports.news", lambda m: None)
        # 等控制帧被处理。
        await asyncio.sleep(0.3)

        base = f"http://127.0.0.1:{ap}"
        with _url.urlopen(f"{base}/api/v1/stats/realtime", timeout=5) as r:
            snap = _json.loads(r.read().decode("utf-8"))
        with _url.urlopen(f"{base}/api/v1/clients", timeout=5) as r:
            clients = _json.loads(r.read().decode("utf-8"))

        # 订阅总数 = 2（不是 0）
        assert snap["total_subscriptions"] == 2, snap.get("total_subscriptions")
        # 在线 client 的 topics 列表反映实时订阅
        alice = next(cl for cl in clients["clients"] if cl["username"] == "alice")
        assert set(alice["topics"]) == {"market.*", "sports.news"}

        # 事件流应包含 subscribe 事件（小写 type，与前端 tCls 对齐）
        ev_types = {e["type"] for e in snap.get("sse_events", [])}
        assert "subscribe" in ev_types, ev_types
        await c.stop()
    finally:
        await srv.stop()


async def test_graceful_disconnect_logs_no_error():
    """client 正常 stop()（发 DISCONNECT 后立即关 socket）不应在 server 侧打异常栈。

    回归 Bug：DISCONNECT 分支处理完后给已离开的 peer 回 ``{"result":"OK"}``，
    ROUTER_MANDATORY 抛 ``Host unreachable``，被 ``_control_loop`` 的 except 捕获并
    ``logger.exception`` 打整条栈 → 每次正常下线都刷一条错误日志，误导排查。
    正常 DISCONNECT 的回执失败属于预期行为，应静默（debug）处理。

    注：该竞态概率性触发，故循环多次 connect/disconnect 以稳定复现。
    """
    from io import StringIO
    from loguru import logger

    buf = StringIO()
    handle = logger.add(buf, level="DEBUG",
                        format="{level}|{message}", catch=False)

    srv, dp, cp = await _start_server({"alice": "s"})
    # 清空启动期日志，只观察断开阶段。
    buf.truncate(0); buf.seek(0)
    try:
        # 反复 connect/disconnect 多次以稳定触发 DISCONNECT 回执竞态。
        for _ in range(10):
            c = ConsumerClient(
                data_endpoint=f"tcp://127.0.0.1:{dp}",
                control_endpoint=f"tcp://127.0.0.1:{cp}",
                username="alice",
                password="s",
            )
            await c.start()
            await asyncio.sleep(0.05)
            await c.stop()
        # 给服务端控制循环排空 DISCONNECT 回执（含竞态日志）足够时间。
        await asyncio.sleep(0.3)
    finally:
        await srv.stop()
        logger.remove(handle)

    log_text = buf.getvalue()
    # 不应出现「控制命令处理异常」错误级日志（DISCONNECT 回执失败属预期）
    assert "控制命令处理异常" not in log_text, log_text
    assert "Host unreachable" not in log_text, log_text
