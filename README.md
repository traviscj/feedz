# feedz

kv store + change feed over plain SQL tables. No broker: the database itself
is the feed, served over HTTP by the producer. Design notes:
[feeds-docs](https://traviscj.com/blog/feeds-docs/).

## how it works

Every feed-published table carries two columns:

- `feed_sync_id` — position in the feed. `NULL` means "pending publish";
  **every mutation resets it to `NULL`**, which is what makes feeds usable for
  cache invalidation and replication.
- `shard` — data shard, assigned at publish time (`crc32(ns:k) % shards`).
  **`shard = -1` means unpublished**: the record stays in the table but is
  invisible to the feed until explicitly republished.

A **publisher** assigns monotonic `feed_sync_id`s from the `sequences` table
(one transaction per batch — republishes get fresh, higher ids, so consumers
never miss an update: at-least-once, latest-value semantics). **Consumers**
page through `feed_sync_id > cursor` and persist their position in
`feed_cursors`, one row per `(consumer, shard)`. Consumer shards map onto data
shards by modulus, so publish-time shard count can far exceed consumer
parallelism.

## setup

    uv sync
    mysql traviscj < schema/mysql.sql   # or any SQLAlchemy-supported DB
    export FEEDZ_DB_URL='mysql+pymysql://root@localhost/traviscj'

`schema/mysql.sql` also contains the migration from the previous feedz schema
and the ALTER recipe for feed-publishing an arbitrary existing table.
`schema/sqlite.sql` is the same schema for SQLite (the test suite runs on it).

## cli

    uv run feedzcli kv put NS K V        # upsert (requeues for the feed)
    uv run feedzcli kv get NS K
    uv run feedzcli kv scan NS [PREFIX]
    uv run feedzcli kv record NS V       # store under a random key
    uv run feedzcli kv ls                # namespaces w/ counts
    uv run feedzcli kv watchpb NS        # record clipboard URLs
    uv run feedzcli kv delete NS K

    uv run feedzcli feed publish [--shards N]
    uv run feedzcli feed consume [CONSUMER] [--all] [--shard I --shard-count N]
    uv run feedzcli feed status          # sequences, cursors, lag
    uv run feedzcli feed unpublish NS K  # shard=-1: drop from feed, keep row
    uv run feedzcli feed republish NS K  # requeue (also restores unpublished)
    uv run feedzcli feed republish-all [--ns NS]
    uv run feedzcli feed reset|pause|resume CONSUMER
    uv run feedzcli feed seek CONSUMER FSI
    uv run feedzcli feed pull URL [--conflict overwrite|rename]
    uv run feedzcli repl

## web

    uv run feedz-web                     # http://127.0.0.1:5001

- `/` — dashboard: namespaces, sequences, cursors w/ lag, recent records
- `/ns/<ns>?prefix=` — browse a namespace
- `/_feeds/fetch/<table>?after=&limit=&shard=&shard_count=` — the feed
  endpoint: `{"entries": [...], "cursor": N}`; poll with `after=<cursor>`
- `/api/status` — dashboard data as JSON

Config: `FEEDZ_WEB_HOST`, `FEEDZ_WEB_PORT`, `FEEDZ_WEB_DEBUG`.

## cross-instance sync

`feedzcli feed pull http://other-host:5001` consumes another instance's feed
endpoint into the local kv, tracking position in `feed_cursors`
(`pull:<host>`). Pulled writes re-enter the local feed, so syncs chain;
value-identical applies are skipped, so two instances pulling each other
converge instead of ping-ponging. `--conflict rename` keeps a differing local
value and lands the remote one under `K-Conflicted-<host>-<fsi>`.

## cache invalidation pattern

Cache a table alongside `max(feed_sync_id)`; a cron compares that against
`KvQueries.max_feed_sync_id()` (or `/api/status`) and reloads only on change.
Since mutations null `feed_sync_id` and publish advances it, the poll is
almost always a no-op.

## tests

    uv run pytest
