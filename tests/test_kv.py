from feedz.feed import Publisher
from feedz.queries import KvQueries


def test_put_get_roundtrip(engine):
    kq = KvQueries(engine)
    kq.put("ns1", "a", "hello")
    kq.put("ns1", "b", b"\x00\x01binary")
    assert kq.get("ns1", "a").text() == "hello"
    assert kq.get("ns1", "b").v == b"\x00\x01binary"
    assert kq.get("ns1", "missing") is None
    assert kq.get("other", "a") is None  # ns isolation


def test_put_is_upsert_and_requeues_feed(engine):
    kq = KvQueries(engine)
    kq.put("ns", "k", "v1")
    Publisher(engine, "feed-kv").publish()
    assert kq.get("ns", "k").feed_sync_id == 1
    kq.put("ns", "k", "v2")  # mutation must reset feed_sync_id
    row = kq.get("ns", "k")
    assert row.text() == "v2"
    assert row.feed_sync_id is None
    assert kq.unpublished_count() == 1


def test_text_typed_v_column_reads_as_bytes(engine):
    # A TEXT/longtext v column (as in traviscj_localdev) hands back str rows;
    # KV.v must still come out as bytes.
    from sqlalchemy import text as sqltext

    with engine.begin() as conn:
        conn.execute(sqltext("INSERT INTO kv (ns, k, v) VALUES ('ns', 'txt', 'plain string')"))
    kq = KvQueries(engine)
    row = kq.get("ns", "txt")
    assert row.v == b"plain string"
    assert kq.scan("ns")[0].v == b"plain string"


def test_scan_prefix_and_like_escaping(engine):
    kq = KvQueries(engine)
    for k in ["a_1", "ab1", "a%2", "b1"]:
        kq.put("ns", k, k)
    assert [r.k for r in kq.scan("ns")] == ["a%2", "a_1", "ab1", "b1"]
    # '_' and '%' in the prefix are literals, not wildcards
    assert [r.k for r in kq.scan("ns", "a_")] == ["a_1"]
    assert [r.k for r in kq.scan("ns", "a%")] == ["a%2"]


def test_rec_generates_unique_keys(engine):
    kq = KvQueries(engine)
    k1, k2 = kq.rec("ns", "v").k, kq.rec("ns", "v").k
    assert k1 != k2
    assert len(kq.scan("ns")) == 2


def test_delete_and_namespaces(engine):
    kq = KvQueries(engine)
    kq.put("ns1", "a", "1")
    kq.put("ns1", "b", "2")
    kq.put("ns2", "c", "3")
    assert kq.namespaces() == [("ns1", 2), ("ns2", 1)]
    assert kq.delete("ns1", "a") is True
    assert kq.delete("ns1", "a") is False
    assert kq.namespaces() == [("ns1", 1), ("ns2", 1)]
