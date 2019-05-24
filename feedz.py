
from dataclasses import dataclass
import uuid


@dataclass
class KV:
    ns: str
    k: str
    v: bytes

# import json
class KvQueries(object):
    def __init__(self, db):
        self.db = db
    def get(self, k):
        Q = self.db.query('SELECT * FROM kv WHERE k=:k', k=k)
        for qq in Q:
            print(qq.as_dict())
        return None # Q.all()
    def scan(self, prefix):
        query = "SELECT * FROM kv WHERE k LIKE CONCAT(:k, '%')"
        Q = self.db.query(query, k=prefix)
        for r in Q:
            kv = KV(r.ns, r.k, r.v)
            yield kv
            # print(qq.as_dict())
        # return None
    def put(self, k, v):
        self.db.query('INSERT INTO kv (k, v) VALUES (:k, :v)', k=k, v=v)
    def rec(self, v):
        return self.put(uuid.uuid4(), v)
        # self.db.query('UPDATE kv SET token = :fsi WHERE consumer=:consumer', consumer=self.consumer, fsi=fsi)

class FeedCursorQueries(object):
    def __init__(self, db, consumer):
        self.db = db
        self.consumer = consumer
    def reset(self):
        self.seek('')
        # self.db.query('UPDATE feed_cursors SET token = '' WHERE consumer=:consumer', consumer=self.consumer)
    def seek(self, fsi):
        self.db.query('UPDATE feed_cursors SET token = :fsi WHERE consumer=:consumer', consumer=self.consumer, fsi=fsi)
    def pause(self):
        self.db.query('UPDATE feed_cursors SET enabled = 0 WHERE consumer=:consumer', consumer=self.consumer)
    def resume(self):
        self.db.query('UPDATE feed_cursors SET enabled = 1 WHERE consumer=:consumer', consumer=self.consumer)

# class KvQueries(object):
#     def __init__(self, db, table):
#         self.db = db
#         self.table = db
#     def republish(self):
#         self.db.query(f"UPDATE {self.table} SET feed_sync_id = NULL")

class SeqQueries(object):
    def __init__(self, db):
        self.db = db
    # TODO: insert
    # TODO: get
    # TODO: set

class FeedCursorQueries(object):
    def __init__(self, db):
        self.db = db
    # TODO: get
    # TODO: insert
    # TODO: set

class Publisher(object):
    def __init__(self, db, feed_name, table):
        self.db = db
        self.feed_name = feed_name
        self.table = table
    def publish(self):
        rows = self.db.query('SELECT * FROM {} WHERE feed_sync_id IS NULL'.format(self.table))
        for r in rows:
            seq = self.db.query('SELECT * FROM sequences WHERE name = :name', name=self.feed_name)
            if len(seq.all()) == 0:
                self.db.query('INSERT INTO sequences (name) VALUES (:name)', name=self.feed_name)
                seq = self.db.query('SELECT * FROM sequences WHERE name = :name', name=self.feed_name)
            next_val = seq[0].value + 1
            self.db.query(
                """UPDATE {} 
                   SET feed_sync_id = :feed_sync_id 
                   WHERE id = :id""".format(self.table), 
                feed_sync_id=next_val, id=r.id)
            self.db.query("UPDATE sequences SET value = :value, version = version + 1 WHERE name = :name", 
                value=next_val, name=self.feed_name)


class Listener(object):
    def __init__(self):
        pass
    def process(self, record):
        raise Exception("need to override!")


class PrintListener(object):
    def process(self, record):
        print(record)


class Consumer(object):
    def __init__(self, db, consumer_name, table, listener):
        self.db = db
        self.consumer_name = consumer_name
        self.table = table
        self.listener = listener
        # TODO: batch_size
    def consume(self):
        cursors = self.db.query('SELECT * FROM feed_cursors WHERE consumer=:consumer', consumer=self.consumer_name)
        if len(cursors.all()) == 0:
            self.db.query("INSERT INTO feed_cursors (consumer, token, enabled) VALUES (:consumer, 0, 1)", consumer=self.consumer_name)
            cursors = self.db.query('SELECT * FROM feed_cursors WHERE consumer=:consumer', consumer=self.consumer_name)
        cursor = cursors[0]
        records = self.db.query("SELECT * FROM {} WHERE feed_sync_id > :fsi ORDER BY feed_sync_id LIMIT 1".format(self.table), fsi=cursor.token)
        if len(records.all()) == 0:
            print("No records!")
            return False
        r = records[0]
        # TODO: consider r.v.decode("utf-8")
        # TODO: consider r.feed_sync_id
        kv = KV(r.ns, r.k, r.v)
        self.listener.process(kv)
    
        self.db.query("UPDATE feed_cursors SET token = :token WHERE consumer=:consumer", 
            token=r.feed_sync_id,
            consumer=self.consumer_name,
        )
        return True

# this isn't super useful, so killing for now...
# class Inserter(object):
#     def __init__(self, db):
#         self.db = db
#     def insert(self, v):
#         self.db.query("INSERT INTO kv (k, v) VALUES (:k, :v)",
#             k=uuid.uuid4(),
#             v=v,
#         )



"""

CREATE TABLE `kv` (
  `id` bigint(22) NOT NULL AUTO_INCREMENT,
  `k` varchar(255) NOT NULL,
  `v` longblob NOT NULL,
  `feed_sync_id` bigint(22) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `u_k` (`k`),
  UNIQUE KEY `u_fsi` (`feed_sync_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE kv
    ADD COLUMN `created_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) AFTER `id`,
    ADD COLUMN `updated_at` TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) AFTER `created_at`,
    ADD COLUMN `feed_sync_id` BIGINT(22) AFTER `updated_at`,
    ADD UNIQUE KEY `u_fsi` (`feed_sync_id`),
    ADD KEY `idx_ca` (`created_at`);

ALTER TABLE kv
    ADD COLUMN `ns` VARCHAR(255) NOT NULL DEFAULT '-' AFTER `feed_sync_id`,
    ADD UNIQUE KEY `u_ns_k` (`ns`, `k`),
    DROP KEY `u_k`;
"""

"""
CREATE TABLE `feed_cursors` (
    id bigint(22) NOT NULL AUTO_INCREMENT,
    consumer varchar(255) NOT NULL,
    token varchar(255) DEFAULT NULL,
    enabled tinyint(1) DEFAULT 0,
    last_fetched_at datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
PRIMARY KEY (`id`),
UNIQUE KEY `u_consumer` (`consumer`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
"""

"""
CREATE TABLE sequences (
    id bigint(22) NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    value BIGINT(22) NOT NULL DEFAULT 0,
    version BIGINT(22) NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `u_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
"""
