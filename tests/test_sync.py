"""Cross-instance sync: a 'remote' engine served by the Flask test client,
pulled into a separate 'local' engine."""

import pytest

from feedz import sync
from feedz.feed import Publisher
from feedz.queries import FeedCursorQueries, KvQueries
from feedz.web import create_app

from tests.conftest import make_engine


@pytest.fixture()
def remote():
    eng = make_engine()
    yield eng
    eng.dispose()


def client_fetch(client):
    def fetch(base_url, table="kv", after=0, limit=100):
        return client.get(f"/_feeds/fetch/{table}?after={after}&limit={limit}").get_json()

    return fetch


def test_pull_is_incremental(engine, remote):
    rkq = KvQueries(remote)
    rkq.put("ns", "a", "1")
    rkq.put("ns", "b", "2")
    Publisher(remote, "feed-kv").publish()
    fetch = client_fetch(create_app(remote).test_client())

    assert sync.pull(engine, "http://remote:5001", fetch_fn=fetch, limit=1) == 2
    lkq = KvQueries(engine)
    assert lkq.get("ns", "a").text() == "1"
    assert FeedCursorQueries(engine, "pull:remote:5001").get().token == 2

    # nothing new -> no-op; new remote write -> only the delta transfers
    assert sync.pull(engine, "http://remote:5001", fetch_fn=fetch) == 0
    rkq.put("ns", "c", "3")
    Publisher(remote, "feed-kv").publish()
    assert sync.pull(engine, "http://remote:5001", fetch_fn=fetch) == 1
    assert lkq.get("ns", "c").text() == "3"


def test_pulled_records_reenter_local_feed(engine, remote):
    rkq = KvQueries(remote)
    rkq.put("ns", "a", "1")
    Publisher(remote, "feed-kv").publish()
    fetch = client_fetch(create_app(remote).test_client())
    sync.pull(engine, "http://remote:5001", fetch_fn=fetch)
    # applied via put() -> feed_sync_id NULL -> local publisher re-feeds it
    assert KvQueries(engine).unpublished_count() == 1


def test_identical_values_do_not_ping_pong(engine, remote):
    KvQueries(engine).put("ns", "a", "same")
    KvQueries(remote).put("ns", "a", "same")
    Publisher(remote, "feed-kv").publish()
    fetch = client_fetch(create_app(remote).test_client())
    assert sync.pull(engine, "http://remote:5001", fetch_fn=fetch) == 0


def test_conflict_rename_preserves_local_value(engine, remote):
    KvQueries(engine).put("ns", "a", "local")
    KvQueries(remote).put("ns", "a", "remote")
    Publisher(remote, "feed-kv").publish()
    fetch = client_fetch(create_app(remote).test_client())
    sync.pull(engine, "http://remote:5001", fetch_fn=fetch, conflict="rename")
    lkq = KvQueries(engine)
    assert lkq.get("ns", "a").text() == "local"
    assert lkq.get("ns", "a-Conflicted-remote:5001-1").text() == "remote"


def test_paused_pull_cursor_is_respected(engine, remote):
    KvQueries(remote).put("ns", "a", "1")
    Publisher(remote, "feed-kv").publish()
    fetch = client_fetch(create_app(remote).test_client())
    fcq = FeedCursorQueries(engine, "pull:remote:5001")
    fcq.ensure()
    fcq.pause()
    assert sync.pull(engine, "http://remote:5001", fetch_fn=fetch) == 0
