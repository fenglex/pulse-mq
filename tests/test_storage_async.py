import asyncio
import threading
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


def test_load_history_works_across_threads(tmp_path):
    """load_history 必须能在「非 connect 线程」上安全执行。

    回归 Bug：StatsStorage 用默认 ``check_same_thread=True`` 打开连接（主线程），
    而 AdminServer 默认 ``admin_thread=True`` 跑在独立线程，admin 线程调
    ``load_history`` 会抛 ``ProgrammingError: SQLite objects created in a
    thread can only be used in that same thread``，被 except 吞掉返回 []，
    导致 ``/api/v1/topics/{topic}/history`` 永远返回空、历史曲线加载不到数据。

    该测试在主线程写入、在子线程读取，断言能读到写入的数据（当前实现会失败）。
    """
    db = f"sqlite://{tmp_path / 'cross.sqlite'}"
    storage = StatsStorage(db)
    storage.connect()
    # 主线程写入一条
    storage.save_minute("cross.topic",
                        MinuteSlot(timestamp=1000, msg_count=3, record_count=3, bytes_total=30))
    # 主线程能读到
    assert any(h["timestamp"] == 1000 for h in storage.load_history("cross.topic", 0))

    # 子线程读取（模拟 admin 独立线程）—— 当前实现会抛跨线程异常被吞返回 []
    result: dict = {}
    def _read_from_other_thread():
        try:
            hist = storage.load_history("cross.topic", 0)
            result["rows"] = [h["timestamp"] for h in hist]
        except Exception as e:  # load_history 内部已 try/except，正常不会抛到这
            result["err"] = f"{type(e).__name__}: {e}"

    t = threading.Thread(target=_read_from_other_thread)
    t.start()
    t.join()

    assert "err" not in result, f"跨线程读抛异常: {result.get('err')}"
    # 关键断言：跨线程读必须能读到主线程写入的数据（而非被吞成空）
    assert 1000 in result.get("rows", []), f"跨线程 load_history 读不到数据: {result}"
    storage.close()
