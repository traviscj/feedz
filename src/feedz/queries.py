from __future__ import annotations

import uuid
from dataclasses import dataclass

from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import IntegrityError


@dataclass(frozen=True)
class KV:
    ns: str
    k: str
    v: bytes
    feed_sync_id: int | None = None
    shard: int = 0

    def text(self) -> str:
        return self.v.decode("utf-8", errors="replace")

    def as_dict(self) -> dict:
        return {
            "ns": self.ns,
            "k": self.k,
            "v": self.text(),
            "feed_sync_id": self.feed_sync_id,
            "shard": self.shard,
        }


@dataclass(frozen=True)
class Sequence:
    name: str
    value: int
    version: int


@dataclass(frozen=True)
class FeedCursor:
    consumer: str
    token: int
    enabled: bool
    shard: int = 0
    shard_count: int = 1


def _as_bytes(v: str | bytes) -> bytes:
    # Also normalizes read rows: a TEXT-typed v column yields str, BLOB yields
    # bytes; KV.v is always bytes.
    return v.encode("utf-8") if isinstance(v, str) else bytes(v)


def _like_prefix(prefix: str) -> str:
    # '|' as the LIKE escape char: identical quoting semantics on MySQL and
    # SQLite, unlike backslash.
    return prefix.replace("|", "||").replace("%", "|%").replace("_", "|_") + "%"


class KvQueries:
    def __init__(self, engine: Engine):
        self.engine = engine

    def get(self, ns: str, k: str) -> KV | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT ns, k, v, feed_sync_id, shard FROM kv WHERE ns = :ns AND k = :k"),
                {"ns": ns, "k": k},
            ).first()
        return KV(row.ns, row.k, _as_bytes(row.v), row.feed_sync_id, row.shard) if row else None

    def scan(self, ns: str, prefix: str = "", limit: int = 1000) -> list[KV]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT ns, k, v, feed_sync_id, shard FROM kv"
                    " WHERE ns = :ns AND k LIKE :pat ESCAPE '|' ORDER BY k LIMIT :n"
                ),
                {"ns": ns, "pat": _like_prefix(prefix), "n": limit},
            ).all()
        return [KV(r.ns, r.k, _as_bytes(r.v), r.feed_sync_id, r.shard) for r in rows]

    def put(self, ns: str, k: str, v: str | bytes) -> KV:
        """Upsert. Mutations reset feed_sync_id to NULL so the next publish
        re-feeds the record — this is what makes feeds usable for cache
        invalidation and cross-instance sync."""
        vb = _as_bytes(v)
        if self.engine.dialect.name == "mysql":
            stmt = (
                "INSERT INTO kv (ns, k, v) VALUES (:ns, :k, :v)"
                " ON DUPLICATE KEY UPDATE v = VALUES(v), feed_sync_id = NULL"
            )
        else:
            stmt = (
                "INSERT INTO kv (ns, k, v) VALUES (:ns, :k, :v)"
                " ON CONFLICT (ns, k) DO UPDATE SET v = excluded.v, feed_sync_id = NULL"
            )
        with self.engine.begin() as conn:
            conn.execute(text(stmt), {"ns": ns, "k": k, "v": vb})
        return KV(ns, k, vb)

    def rec(self, ns: str, v: str | bytes) -> KV:
        """Record a value under a fresh random key."""
        return self.put(ns, str(uuid.uuid4()), v)

    def delete(self, ns: str, k: str) -> bool:
        with self.engine.begin() as conn:
            res = conn.execute(
                text("DELETE FROM kv WHERE ns = :ns AND k = :k"), {"ns": ns, "k": k}
            )
        return res.rowcount > 0

    def unpublish(self, ns: str, k: str) -> bool:
        """Remove a record from the feed without deleting it: shard = -1.

        The publisher skips shard < 0 rows and consumers filter shard >= 0, so
        the record becomes invisible to the feed until republished. Consumers
        that already saw it are not retracted."""
        with self.engine.begin() as conn:
            res = conn.execute(
                text("UPDATE kv SET shard = -1, feed_sync_id = NULL WHERE ns = :ns AND k = :k"),
                {"ns": ns, "k": k},
            )
        return res.rowcount > 0

    def republish(self, ns: str, k: str) -> bool:
        """Queue one record for re-feeding (also restores an unpublished one)."""
        with self.engine.begin() as conn:
            res = conn.execute(
                text("UPDATE kv SET feed_sync_id = NULL, shard = 0 WHERE ns = :ns AND k = :k"),
                {"ns": ns, "k": k},
            )
        return res.rowcount > 0

    def republish_all(self, ns: str | None = None) -> int:
        """Queue every published record for re-feeding. Leaves shard = -1
        (unpublished) rows alone."""
        stmt = "UPDATE kv SET feed_sync_id = NULL WHERE shard >= 0 AND feed_sync_id IS NOT NULL"
        params: dict = {}
        if ns is not None:
            stmt += " AND ns = :ns"
            params["ns"] = ns
        with self.engine.begin() as conn:
            res = conn.execute(text(stmt), params)
        return res.rowcount

    def namespaces(self) -> list[tuple[str, int]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT ns, COUNT(*) AS n FROM kv GROUP BY ns ORDER BY ns")
            ).all()
        return [(r.ns, r.n) for r in rows]

    def max_feed_sync_id(self) -> int:
        with self.engine.connect() as conn:
            return conn.execute(
                text("SELECT COALESCE(MAX(feed_sync_id), 0) FROM kv")
            ).scalar_one()

    def unpublished_count(self) -> int:
        """Records awaiting publish (excludes shard = -1 tombstones)."""
        with self.engine.connect() as conn:
            return conn.execute(
                text("SELECT COUNT(*) FROM kv WHERE feed_sync_id IS NULL AND shard >= 0")
            ).scalar_one()


class SeqQueries:
    """Named monotonic counters. Allocating feed_sync_id from a sequence (vs
    MAX(fsi)+1) keeps ids monotonic across republishes, which is what upgrades
    the feed from at-most-once to at-least-once."""

    def __init__(self, engine: Engine):
        self.engine = engine

    def get(self, name: str) -> Sequence | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT name, value, version FROM sequences WHERE name = :name"),
                {"name": name},
            ).first()
        return Sequence(row.name, row.value, row.version) if row else None

    def all(self) -> list[Sequence]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("SELECT name, value, version FROM sequences ORDER BY name")
            ).all()
        return [Sequence(r.name, r.value, r.version) for r in rows]

    def set(self, name: str, value: int) -> None:
        with self.engine.begin() as conn:
            self.ensure_on(conn, name)
            conn.execute(
                text(
                    "UPDATE sequences SET value = :value, version = version + 1"
                    " WHERE name = :name"
                ),
                {"name": name, "value": value},
            )

    def next(self, name: str) -> int:
        with self.engine.begin() as conn:
            return self.next_on(conn, name)

    @staticmethod
    def next_on(conn: Connection, name: str) -> int:
        """Allocate the next value inside an existing transaction, so fsi
        assignment and sequence advancement commit atomically."""
        SeqQueries.ensure_on(conn, name)
        conn.execute(
            text(
                "UPDATE sequences SET value = value + 1, version = version + 1"
                " WHERE name = :name"
            ),
            {"name": name},
        )
        return conn.execute(
            text("SELECT value FROM sequences WHERE name = :name"), {"name": name}
        ).scalar_one()

    @staticmethod
    def ensure_on(conn: Connection, name: str) -> None:
        exists = conn.execute(
            text("SELECT 1 FROM sequences WHERE name = :name"), {"name": name}
        ).first()
        if exists:
            return
        try:
            conn.execute(text("INSERT INTO sequences (name) VALUES (:name)"), {"name": name})
        except IntegrityError:
            pass  # concurrent creator won


class FeedCursorQueries:
    """Per-(consumer, shard) feed positions. An unsharded consumer is shard 0."""

    def __init__(self, engine: Engine, consumer: str, shard: int = 0, shard_count: int = 1):
        self.engine = engine
        self.consumer = consumer
        self.shard = shard
        self.shard_count = shard_count

    def _where(self) -> dict:
        return {"consumer": self.consumer, "shard": self.shard}

    def get(self) -> FeedCursor | None:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT consumer, token, enabled, shard, shard_count"
                    " FROM feed_cursors WHERE consumer = :consumer AND shard = :shard"
                ),
                self._where(),
            ).first()
        if row is None:
            return None
        return FeedCursor(
            row.consumer, int(row.token or 0), bool(row.enabled), row.shard, row.shard_count
        )

    def ensure(self) -> FeedCursor:
        cur = self.get()
        if cur is not None:
            return cur
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO feed_cursors (consumer, shard, shard_count, token, enabled)"
                    " VALUES (:consumer, :shard, :shard_count, '0', 1)"
                ),
                self._where() | {"shard_count": self.shard_count},
            )
        return self.get()

    def seek(self, fsi: int) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "UPDATE feed_cursors SET token = :fsi, last_fetched_at = CURRENT_TIMESTAMP"
                    " WHERE consumer = :consumer AND shard = :shard"
                ),
                self._where() | {"fsi": fsi},
            )

    def reset(self) -> None:
        self.seek(0)

    def _set_enabled(self, enabled: int) -> None:
        # Applies to every shard row of this consumer.
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE feed_cursors SET enabled = :e WHERE consumer = :consumer"),
                {"consumer": self.consumer, "e": enabled},
            )

    def pause(self) -> None:
        self._set_enabled(0)

    def resume(self) -> None:
        self._set_enabled(1)

    @staticmethod
    def all(engine: Engine) -> list[FeedCursor]:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT consumer, token, enabled, shard, shard_count"
                    " FROM feed_cursors ORDER BY consumer, shard"
                )
            ).all()
        return [
            FeedCursor(r.consumer, int(r.token or 0), bool(r.enabled), r.shard, r.shard_count)
            for r in rows
        ]
