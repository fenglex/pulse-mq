from pulsemq.routing import SubscriptionTable


def test_prefix_match():
    t = SubscriptionTable()
    t.subscribe("id1", "market.stock.*")
    assert t.match("market.stock.600000") == {"id1"}
    assert t.match("market.stock.sh.600001") == {"id1"}
    assert t.match("market.bond.001") == set()


def test_exact_match():
    t = SubscriptionTable()
    t.subscribe("id1", "market.stock.600000")
    assert t.match("market.stock.600000") == {"id1"}
    assert t.match("market.stock.600001") == set()


def test_multi_pattern_one_identity():
    t = SubscriptionTable()
    t.subscribe("id1", "a.*")
    t.subscribe("id1", "b.*")
    assert t.match("a.x") == {"id1"}
    assert t.match("b.x") == {"id1"}
    assert t.subscribers_of("id1") == {"a.*", "b.*"}


def test_idempotent_subscribe():
    t = SubscriptionTable()
    t.subscribe("id1", "a.*")
    t.subscribe("id1", "a.*")
    assert t.subscribers_of("id1") == {"a.*"}


def test_remove_clears_all():
    t = SubscriptionTable()
    t.subscribe("id1", "a.*")
    t.subscribe("id1", "b.*")
    t.remove("id1")
    assert t.match("a.x") == set()
    assert t.subscribers_of("id1") == set()


def test_unsubscribe():
    t = SubscriptionTable()
    t.subscribe("id1", "a.*")
    t.subscribe("id1", "b.*")
    t.unsubscribe("id1", "a.*")
    assert t.match("a.x") == set()
    assert t.match("b.x") == {"id1"}


def test_snapshot():
    t = SubscriptionTable()
    t.subscribe("id1", "a.*")
    snap = t.snapshot()
    assert "id1" in snap


def test_match_returns_results_without_lock():
    """match 无锁读（COW 后读路径不持有锁）。"""
    table = SubscriptionTable()
    table.subscribe(b"id1", "foo.*")
    assert table.match("foo.bar") == {b"id1"}


def test_concurrent_subscribe_and_match():
    """并发：一线程高频 match，另一线程 subscribe/unsubscribe，不抛异常。"""
    import threading
    table = SubscriptionTable()
    table.subscribe(b"id1", "foo.*")
    errors = []
    def reader():
        for _ in range(10000):
            try:
                table.match("foo.bar")
            except Exception as e:
                errors.append(e)
    def writer():
        for i in range(1000):
            table.subscribe(b"id2", f"topic{i}.*")
            table.unsubscribe(b"id2", f"topic{i}.*")
    t1 = threading.Thread(target=reader)
    t2 = threading.Thread(target=writer)
    t1.start(); t2.start()
    t1.join(); t2.join()
    assert not errors
