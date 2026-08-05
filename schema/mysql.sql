-- feedz schema (MySQL).
-- Consolidated from the incremental ALTERs that lived in feedz.py docstrings,
-- plus the feeds-docs blog series (https://traviscj.com/blog/feeds-docs/):
--   feed_sync_id  NULL     => pending publish (every mutation resets it to NULL)
--   feed_sync_id  NOT NULL => position in the feed, allocated from `sequences`
--   shard         >= 0     => data shard, assigned at publish time (hash % shards)
--   shard         = -1     => unpublished: permanently excluded from the feed

CREATE TABLE `kv` (
  `id`           BIGINT       NOT NULL AUTO_INCREMENT,
  `created_at`   TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  `updated_at`   TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  `feed_sync_id` BIGINT       DEFAULT NULL,
  `shard`        INT          NOT NULL DEFAULT 0,
  `ns`           VARCHAR(255) NOT NULL DEFAULT '-',
  `k`            VARCHAR(255) NOT NULL,
  `v`            LONGBLOB     NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `u_ns_k` (`ns`, `k`),
  UNIQUE KEY `u_fsi` (`feed_sync_id`),
  KEY `k_fsi_s` (`feed_sync_id`, `shard`),
  KEY `idx_ca` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- One row per (consumer, shard). An unsharded consumer is (consumer, 0).
CREATE TABLE `feed_cursors` (
  `id`              BIGINT       NOT NULL AUTO_INCREMENT,
  `consumer`        VARCHAR(255) NOT NULL,
  `shard`           INT          NOT NULL DEFAULT 0,
  `shard_count`     INT          NOT NULL DEFAULT 1,
  `token`           VARCHAR(255) DEFAULT NULL,
  `enabled`         TINYINT(1)   NOT NULL DEFAULT 1,
  `last_fetched_at` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `u_consumer_shard` (`consumer`, `shard`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Monotonic feed_sync_id allocation (survives republishes, unlike MAX(fsi)+1).
CREATE TABLE `sequences` (
  `id`      BIGINT       NOT NULL AUTO_INCREMENT,
  `name`    VARCHAR(255) NOT NULL,
  `value`   BIGINT       NOT NULL DEFAULT 0,
  `version` BIGINT       NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `u_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Migrating a database that has the previous feedz schema:
--
-- ALTER TABLE `kv`
--   ADD COLUMN `shard` INT NOT NULL DEFAULT 0 AFTER `feed_sync_id`,
--   ADD KEY `k_fsi_s` (`feed_sync_id`, `shard`);
--
-- ALTER TABLE `feed_cursors`
--   ADD COLUMN `shard` INT NOT NULL DEFAULT 0 AFTER `consumer`,
--   ADD COLUMN `shard_count` INT NOT NULL DEFAULT 1 AFTER `shard`,
--   ADD UNIQUE KEY `u_consumer_shard` (`consumer`, `shard`),
--   DROP KEY `u_consumer`;
--
-- ---------------------------------------------------------------------------
-- Feed-publishing an arbitrary existing table `t` (not just kv):
--
-- ALTER TABLE `t`
--   ADD COLUMN `feed_sync_id` BIGINT DEFAULT NULL,
--   ADD COLUMN `shard` INT NOT NULL DEFAULT 0,
--   ADD UNIQUE KEY `u_fsi` (`feed_sync_id`),
--   ADD KEY `k_fsi_s` (`feed_sync_id`, `shard`);
--
-- then Publisher(engine, "feed-t", table="t") / Consumer(..., table="t"),
-- and make every application write to `t` also SET feed_sync_id = NULL.
