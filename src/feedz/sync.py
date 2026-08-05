"""Cross-instance sync: consume a remote feedz /_feeds/fetch endpoint into the
local kv table (the "cross-dc sync with feed-published kv" pattern).

Applied writes go through KvQueries.put, which NULLs feed_sync_id — so pulled
records re-enter the local feed and chain onward. Ping-pong between two
instances pulling each other is prevented by skipping value-identical applies.
"""

from __future__ import annotations

import json
from urllib.parse import urlparse
from urllib.request import urlopen

from sqlalchemy import Engine

from feedz.queries import FeedCursorQueries, KvQueries


def fetch_remote(base_url: str, table: str = "kv", after: int = 0, limit: int = 100) -> dict:
    url = f"{base_url.rstrip('/')}/_feeds/fetch/{table}?after={after}&limit={limit}"
    with urlopen(url) as resp:
        return json.load(resp)


def apply_entries(
    engine: Engine, entries: list[dict], conflict: str = "overwrite", source: str = "remote"
) -> int:
    """Apply feed entries to the local kv. conflict='rename' preserves a
    differing local value's key and writes the remote value under
    '<k>-Conflicted-<source>-<fsi>' instead of overwriting."""
    kq = KvQueries(engine)
    applied = 0
    for e in entries:
        ns, k, v = e["ns"], e["k"], e["v"]
        existing = kq.get(ns, k)
        if existing is not None and existing.text() == v:
            continue  # no-op apply; breaks bidirectional ping-pong
        if existing is not None and conflict == "rename":
            kq.put(ns, f"{k}-Conflicted-{source}-{e['feed_sync_id']}", v)
        else:
            kq.put(ns, k, v)
        applied += 1
    return applied


def pull(
    engine: Engine,
    base_url: str,
    table: str = "kv",
    consumer: str | None = None,
    limit: int = 100,
    conflict: str = "overwrite",
    fetch_fn=fetch_remote,
) -> int:
    """Catch the local kv up to a remote feed; returns records applied.

    Remote position persists in feed_cursors under 'pull:<host>', so repeated
    pulls are incremental.
    """
    host = urlparse(base_url).netloc or "remote"
    fcq = FeedCursorQueries(engine, consumer or f"pull:{host}")
    cur = fcq.ensure()
    if not cur.enabled:
        return 0
    applied = 0
    after = cur.token
    while True:
        resp = fetch_fn(base_url, table=table, after=after, limit=limit)
        entries = resp["entries"]
        if not entries:
            return applied
        applied += apply_entries(engine, entries, conflict=conflict, source=host)
        after = resp["cursor"]
        fcq.seek(after)
