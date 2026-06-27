import asyncio
import pytest
from pulsemq.stats.storage import StatsStorage, AsyncArchiveWriter
from pulsemq.stats.traffic import MinuteSlot


async def test_async_writer_batches_and_persists(tmp_path):
    db = f"sqlite://{tmp_path / 's.sqlite'}"
    storage = StatsStorage(db)
    storage.connect()
    writer = AsyncArchiveWriter(storage, batch_size=2)
    await writer.start()
    try:
        await writer.enqueue({"a": MinuteSlot(timestamp=1000, msg_count=1, record_count=1, bytes_total=10)})
        await writer.enqueue({"b": MinuteSlot(timestamp=2000, msg_count=2, record_count=2, bytes_total=20)})
        await asyncio.sleep(0.2)  # consumer 写完
        hist = storage.load_history("a", 0)
        assert any(h["timestamp"] == 1000 for h in hist)
        hist_b = storage.load_history("b", 0)
        assert any(h["timestamp"] == 2000 for h in hist_b)
    finally:
        await writer.stop()
        storage.close()


async def test_async_writer_drains_on_stop(tmp_path):
    db = f"sqlite://{tmp_path / 's2.sqlite'}"
    storage = StatsStorage(db)
    storage.connect()
    writer = AsyncArchiveWriter(storage, batch_size=100)  # 大 batch，不自动 flush
    await writer.start()
    await writer.enqueue({"c": MinuteSlot(timestamp=3000, msg_count=1, record_count=1, bytes_total=5)})
    await writer.stop()  # stop 时 drain
    hist = storage.load_history("c", 0)
    assert any(h["timestamp"] == 3000 for h in hist)
    storage.close()
