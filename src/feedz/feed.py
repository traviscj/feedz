from __future__ import annotations

import re
import zlib

from sqlalchemy import Engine, text

from feedz.queries import KV, FeedCursorQueries, SeqQueries, _as_bytes

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _safe_table(table: str) -> str:
    if not _TABLE_RE.match(table):
        raise ValueError(f"invalid table name: {table!r}")
    return table


def _data_shard(ns: str, k: str, shards: int) -> int:
    # Deterministic by key so a given record always lands in the same shard,
    # preserving per-key ordering within a shard.
    return zlib.crc32(f"{ns}:{k}".encode()) % shards


def fetch_entries(
    engine: Engine,
    *,
    table: str = "kv",
    after: int = 0,
    limit: int = 100,
    shard: int | None = None,
    shard_count: int | None = None,
) -> list[KV]:
    """One page of the feed: records with feed_sync_id > after, in order.

    shard/shard_count map consumer shards onto data shards by modulus, so the
    data-shard count (chosen at publish time) can be much larger than the
    consumer parallelism. shard < 0 rows (unpublished) never appear.
    """
    _safe_table(table)
    clauses = ["feed_sync_id > :fsi", "shard >= 0"]
    params: dict = {"fsi": after, "n": limit}
    if shard is not None:
        clauses.append("shard % :cc = :cs")
        params |= {"cc": shard_count or 1, "cs": shard}
    stmt = (
        f"SELECT ns, k, v, feed_sync_id, shard FROM {table}"
        f" WHERE {' AND '.join(clauses)} ORDER BY feed_sync_id LIMIT :n"
    )
    with engine.connect() as conn:
        rows = conn.execute(text(stmt), params).all()
    return [KV(r.ns, r.k, _as_bytes(r.v), r.feed_sync_id, r.shard) for r in rows]


class Listener:
    def process(self, record: KV) -> None:
        raise NotImplementedError


class PrintListener(Listener):
    def process(self, record: KV) -> None:
        print(record)


class CollectListener(Listener):
    def __init__(self):
        self.records: list[KV] = []

    def process(self, record: KV) -> None:
        self.records.append(record)


class Publisher:
    """Assigns feed_sync_ids (from `sequences`) and data shards to rows whose
    feed_sync_id is NULL. The whole batch commits in one transaction, so a
    concurrent feed fetch never observes a partially-advanced sequence."""

    def __init__(self, engine: Engine, feed_name: str, table: str = "kv", shards: int = 1):
        if shards < 1:
            raise ValueError("shards must be >= 1")
        self.engine = engine
        self.feed_name = feed_name
        self.table = _safe_table(table)
        self.shards = shards

    def publish(self) -> int:
        with self.engine.begin() as conn:
            rows = conn.execute(
                text(
                    f"SELECT id, ns, k FROM {self.table}"
                    " WHERE feed_sync_id IS NULL AND shard >= 0 ORDER BY id"
                )
            ).all()
            for r in rows:
                fsi = SeqQueries.next_on(conn, self.feed_name)
                conn.execute(
                    text(f"UPDATE {self.table} SET feed_sync_id = :fsi, shard = :s WHERE id = :id"),
                    {"fsi": fsi, "s": _data_shard(r.ns, r.k, self.shards), "id": r.id},
                )
        return len(rows)


class Consumer:
    """Reads the feed past a persisted cursor and hands records to a listener.

    At-least-once: the cursor advances after each record is processed, so a
    crash mid-batch replays from the failed record. Pass shard/shard_count to
    run N consumers in parallel over disjoint slices of the feed.
    """

    def __init__(
        self,
        engine: Engine,
        consumer_name: str,
        listener: Listener,
        table: str = "kv",
        batch_size: int = 100,
        shard: int | None = None,
        shard_count: int = 1,
    ):
        self.engine = engine
        self.consumer_name = consumer_name
        self.listener = listener
        self.table = _safe_table(table)
        self.batch_size = batch_size
        self.shard = shard
        self.shard_count = shard_count

    def _cursors(self) -> FeedCursorQueries:
        return FeedCursorQueries(
            self.engine, self.consumer_name, shard=self.shard or 0, shard_count=self.shard_count
        )

    def consume(self) -> int:
        """Process up to batch_size records; returns how many were processed."""
        cursors = self._cursors()
        cur = cursors.ensure()
        if not cur.enabled:
            return 0
        entries = fetch_entries(
            self.engine,
            table=self.table,
            after=cur.token,
            limit=self.batch_size,
            shard=self.shard,
            shard_count=self.shard_count if self.shard is not None else None,
        )
        for kv in entries:
            self.listener.process(kv)
            cursors.seek(kv.feed_sync_id)
        return len(entries)

    def consume_all(self) -> int:
        total = 0
        while (n := self.consume()) > 0:
            total += n
        return total
