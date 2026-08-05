import pytest

from feedz.feed import Publisher
from feedz.queries import FeedCursorQueries, KvQueries
from feedz.web import create_app


@pytest.fixture()
def client(engine):
    return create_app(engine).test_client()


def seed_published(engine, n=3):
    kq = KvQueries(engine)
    for i in range(n):
        kq.put("ns", f"k{i}", f"v{i}")
    Publisher(engine, "feed-kv").publish()
    return kq


def test_dashboard_renders(engine, client):
    seed_published(engine)
    FeedCursorQueries(engine, "c1").ensure()
    body = client.get("/").get_data(as_text=True)
    assert "max_fsi=3" in body
    assert "feed-kv" in body
    assert "c1" in body
    assert client.get("/ns/ns").status_code == 200


def test_feeds_fetch_pagination_contract(engine, client):
    seed_published(engine, 5)
    d = client.get("/_feeds/fetch/kv?after=0&limit=2").get_json()
    assert [e["feed_sync_id"] for e in d["entries"]] == [1, 2]
    assert d["cursor"] == 2
    d2 = client.get(f"/_feeds/fetch/kv?after={d['cursor']}&limit=10").get_json()
    assert [e["feed_sync_id"] for e in d2["entries"]] == [3, 4, 5]
    assert d2["cursor"] == 5
    # empty page: cursor echoes `after`
    d3 = client.get("/_feeds/fetch/kv?after=5").get_json()
    assert (d3["entries"], d3["cursor"]) == ([], 5)


def test_feeds_fetch_sharded_and_bad_table(engine, client):
    seed_published(engine, 6)
    d0 = client.get("/_feeds/fetch/kv?shard=0&shard_count=2").get_json()
    d1 = client.get("/_feeds/fetch/kv?shard=1&shard_count=2").get_json()
    ks = {e["k"] for e in d0["entries"]} | {e["k"] for e in d1["entries"]}
    assert ks == {f"k{i}" for i in range(6)}
    assert client.get("/_feeds/fetch/kv;drop").status_code in (400, 404)


def test_api_status(engine, client):
    seed_published(engine)
    KvQueries(engine).put("ns", "pending", "x")
    d = client.get("/api/status").get_json()
    assert d["max_fsi"] == 3
    assert d["unpublished"] == 1
    assert d["namespaces"] == [{"ns": "ns", "rows": 4}]
    assert d["sequences"][0]["name"] == "feed-kv"
