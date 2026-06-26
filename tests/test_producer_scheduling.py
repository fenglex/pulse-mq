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
