import zlib

from feedz.feed import CollectListener, Consumer, Publisher, fetch_entries
from feedz.queries import FeedCursorQueries, KvQueries, SeqQueries

FEED = "feed-kv"


def seed(engine, n=3, ns="ns"):
    kq = KvQueries(engine)
    for i in range(n):
        kq.put(ns, f"k{i}", f"v{i}")
    return kq


def test_publish_assigns_monotonic_fsi_in_id_order(engine):
    kq = seed(engine)
    assert Publisher(engine, FEED).publish() == 3
    assert [kq.get("ns", f"k{i}").feed_sync_id for i in range(3)] == [1, 2, 3]
    assert Publisher(engine, FEED).publish() == 0  # idempotent
    assert SeqQueries(engine).get(FEED).value == 3


def test_consumer_processes_in_order_and_advances_cursor(engine):
    seed(engine, 3)
    Publisher(engine, FEED).publish()
    listener = CollectListener()
    c = Consumer(engine, "c1", listener)
    assert c.consume() == 3
    assert [r.k for r in listener.records] == ["k0", "k1", "k2"]
    assert FeedCursorQueries(engine, "c1").get().token == 3
    assert c.consume() == 0  # caught up
    # new data flows through
    KvQueries(engine).put("ns", "k9", "v9")
    Publisher(engine, FEED).publish()
    assert c.consume() == 1
    assert listener.records[-1].k == "k9"


def test_batch_size_and_consume_all(engine):
    seed(engine, 5)
    Publisher(engine, FEED).publish()
    listener = CollectListener()
    c = Consumer(engine, "c1", listener, batch_size=2)
    assert c.consume() == 2
    assert c.consume_all() == 3
    assert len(listener.records) == 5


def test_pause_resume_reset_seek(engine):
    seed(engine, 3)
    Publisher(engine, FEED).publish()
    c = Consumer(engine, "c1", CollectListener())
    fcq = FeedCursorQueries(engine, "c1")
    fcq.ensure()
    fcq.pause()
    assert c.consume() == 0
    fcq.resume()
    assert c.consume() == 3
    fcq.reset()
    assert c.consume() == 3  # full replay
    fcq.seek(2)
    listener = CollectListener()
    assert Consumer(engine, "c1", listener).consume() == 1
    assert listener.records[0].feed_sync_id == 3


def test_republish_gets_fresh_higher_fsi(engine):
    kq = seed(engine, 2)
    Publisher(engine, FEED).publish()
    assert kq.republish("ns", "k0") is True
    Publisher(engine, FEED).publish()
    assert kq.get("ns", "k0").feed_sync_id == 3  # never reuses 1
    # a consumer already at token=2 still sees the republished record
    listener = CollectListener()
    fcq = FeedCursorQueries(engine, "c1")
    fcq.ensure()
    fcq.seek(2)
    Consumer(engine, "c1", listener).consume()
    assert [r.k for r in listener.records] == ["k0"]


def test_republish_all_respects_ns_and_skips_unpublished(engine):
    kq = KvQueries(engine)
    kq.put("a", "k1", "v")
    kq.put("b", "k2", "v")
    Publisher(engine, FEED).publish()
    kq.unpublish("a", "k1")
    assert kq.republish_all() == 1  # only b/k2; a/k1 is shard=-1
    assert kq.republish_all(ns="missing") == 0


def test_unpublish_hides_record_until_republished(engine):
    kq = seed(engine, 3)
    Publisher(engine, FEED).publish()
    assert kq.unpublish("ns", "k1") is True
    row = kq.get("ns", "k1")
    assert (row.shard, row.feed_sync_id) == (-1, None)
    # publisher skips it; consumers never see it
    assert Publisher(engine, FEED).publish() == 0
    assert kq.unpublished_count() == 0
    listener = CollectListener()
    Consumer(engine, "c1", listener).consume_all()
    assert [r.k for r in listener.records] == ["k0", "k2"]
    # put() on an unpublished key does NOT resurrect it (shard stays -1)...
    kq.put("ns", "k1", "v1'")
    assert Publisher(engine, FEED).publish() == 0
    # ...but republish() does
    assert kq.republish("ns", "k1") is True
    assert Publisher(engine, FEED).publish() == 1
    assert kq.get("ns", "k1").feed_sync_id == 4


def test_sharded_publish_and_disjoint_consumers(engine):
    kq = seed(engine, 20)
    Publisher(engine, FEED, shards=4).publish()
    for i in range(20):
        row = kq.get("ns", f"k{i}")
        assert row.shard == zlib.crc32(f"ns:k{i}".encode()) % 4
    listeners = [CollectListener(), CollectListener()]
    for cs in (0, 1):
        Consumer(engine, "cgroup", listeners[cs], shard=cs, shard_count=2).consume_all()
    seen = [{r.k for r in l.records} for l in listeners]
    assert seen[0] | seen[1] == {f"k{i}" for i in range(20)}
    assert seen[0] & seen[1] == set()
    # each consumer shard owns data shards where ds % 2 == cs
    for cs in (0, 1):
        assert all(r.shard % 2 == cs for r in listeners[cs].records)
    # cursor rows are tracked per (consumer, shard)
    assert len([c for c in FeedCursorQueries.all(engine) if c.consumer == "cgroup"]) == 2


def test_fetch_entries_pagination(engine):
    seed(engine, 5)
    Publisher(engine, FEED).publish()
    page1 = fetch_entries(engine, after=0, limit=2)
    assert [e.feed_sync_id for e in page1] == [1, 2]
    page2 = fetch_entries(engine, after=page1[-1].feed_sync_id, limit=10)
    assert [e.feed_sync_id for e in page2] == [3, 4, 5]
