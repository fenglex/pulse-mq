import asyncio
import socket as _sock

from pulsemq import Server, ProducerClient, ConsumerClient


def _free_port():
    s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


async def test_producer_decorator_publishes_periodically():
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"c": "c", "p": "p"},
    )
    await srv.start()
    try:
        consumer = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "c", "c"
        )
        producer = ProducerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "p", "p"
        )
        await consumer.start()

        @producer.producer("market.stock.tick", interval=0.2)
        async def gen():
            return {"price": 12.3}

        rf = asyncio.create_task(producer.run_forever())
        got = []
        await consumer.subscribe("market.stock.*", lambda m: got.append(m.payload))
        await asyncio.sleep(0.8)  # 让 producer 跑几轮
        assert len(got) >= 1
        assert got[0] == {"price": 12.3}
        await producer.stop()  # _stop → run_forever 退出
        await rf
        await consumer.stop()
    finally:
        await srv.stop()


async def test_producer_callback_non_whitelist_skips_but_keeps_running():
    """producer 回调返回非白名单类型：该轮被 encode 抛 TypeError → ProducerManager
    吞成 warning 跳过，服务不崩溃，后续轮次正常推送。

    验证核心契约"raise 但不关闭服务"：非白名单类型不会撑爆 producer 调度循环，
    下一轮仍会执行，且坏数据不会被推送至订阅端。
    """
    dp, cp, ap = _free_port(), _free_port(), _free_port()
    srv = Server(
        data_endpoint=f"tcp://127.0.0.1:{dp}",
        control_endpoint=f"tcp://127.0.0.1:{cp}",
        admin_endpoint=f"127.0.0.1:{ap}",
        credentials={"c": "c", "p": "p"},
    )
    await srv.start()
    try:
        consumer = ConsumerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "c", "c"
        )
        producer = ProducerClient(
            f"tcp://127.0.0.1:{dp}", f"tcp://127.0.0.1:{cp}", "p", "p"
        )
        await consumer.start()

        round_no = 0

        @producer.producer("market.stock.tick", interval=0.2)
        async def gen():
            nonlocal round_no
            round_no += 1
            if round_no == 1:
                return [1, 2, 3]  # 首轮非白名单 → encode raise → 被吞成 warning 跳过
            return {"price": 12.3}

        rf = asyncio.create_task(producer.run_forever())
        got = []
        await consumer.subscribe("market.stock.*", lambda m: got.append(m.payload))
        await asyncio.sleep(1.2)  # 让 producer 跑多轮（首轮失败 + 后续成功）

        # 服务存活：run_forever 未因首轮 raise 而崩溃退出
        assert not rf.done(), "producer 任务不应因非白名单返回值崩溃"
        # 首轮 list 不应送达消费者
        assert all(not isinstance(p, list) for p in got), "非白名单 list 不应被推送"
        # 后续 dict 应正常送达
        assert len(got) >= 1, "首轮跳过后，后续 dict 应正常推送"
        assert got[0] == {"price": 12.3}

        await producer.stop()
        await rf
        await consumer.stop()
    finally:
        await srv.stop()
