#!/usr/bin/env python3

import click
import feedz
import records
from click_repl import register_repl

db = records.Database('mysql://root@localhost/traviscj')

PN = "feed-kv"
CN = "show"
TABLE = "kv"
p = feedz.Publisher(db, PN, TABLE)
l = feedz.PrintListener()
c = feedz.Consumer(db, CN, TABLE, l)

def publish():
    p.publish()

def consume_all():
    while c.consume():
        pass

def reset(consumer):
    fcq = feedz.FeedCursorQueries(db, consumer)
    fcq.reset()
def seek(consumer, fsi):
    fcq = feedz.FeedCursorQueries(db, consumer)
    fcq.seek(fsi)
def pause(consumer):
    fcq = feedz.FeedCursorQueries(db, consumer)
    fcq.pause()
def resume(consumer):
    fcq = feedz.FeedCursorQueries(db, consumer)
    fcq.resume()

def republish_all():
    pass



def main():
    consume_all()

if __name__ == "__main__":
    main()
