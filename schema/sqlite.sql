-- feedz schema (SQLite). Mirrors schema/mysql.sql; used by the test suite.

CREATE TABLE kv (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  feed_sync_id BIGINT DEFAULT NULL,
  shard        INTEGER NOT NULL DEFAULT 0,
  ns           TEXT NOT NULL DEFAULT '-',
  k            TEXT NOT NULL,
  v            BLOB NOT NULL,
  UNIQUE (ns, k),
  UNIQUE (feed_sync_id)
);

CREATE INDEX k_fsi_s ON kv (feed_sync_id, shard);

CREATE TABLE feed_cursors (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  consumer        TEXT NOT NULL,
  shard           INTEGER NOT NULL DEFAULT 0,
  shard_count     INTEGER NOT NULL DEFAULT 1,
  token           TEXT DEFAULT NULL,
  enabled         INTEGER NOT NULL DEFAULT 1,
  last_fetched_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE (consumer, shard)
);

CREATE TABLE sequences (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  name    TEXT NOT NULL UNIQUE,
  value   BIGINT NOT NULL DEFAULT 0,
  version BIGINT NOT NULL DEFAULT 0
);
