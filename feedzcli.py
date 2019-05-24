#!/usr/bin/env python

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

@click.group()
def cli(): pass

@cli.group()
def feed(): pass

@feed.command()
def publish():
    p.publish()

@feed.command()
def consume_all():
    while c.consume():
        pass

@feed.command()
def reset(consumer):
    fcq = feedz.FeedCursorQueries(db, consumer)
    fcq.reset()

@feed.command()
def seek(consumer, fsi):
    fcq = feedz.FeedCursorQueries(db, consumer)
    fcq.seek(fsi)

@feed.command()
def pause(consumer):
    fcq = feedz.FeedCursorQueries(db, consumer)
    fcq.pause()

@feed.command()
def resume(consumer):
    fcq = feedz.FeedCursorQueries(db, consumer)
    fcq.resume()

@feed.command()
def republish_all():
    pass

@cli.command()
def ls():
    kq = feedz.KvQueries(db)
    for kv in kq.scan(""):
        print(kv)

@cli.group()
def kv(): pass

@kv.command()
@click.argument("k")
@click.argument("v")
def put(k, v):
    pass

@kv.command()
@click.argument("k")
def get(k):
    print(k)
    kq = feedz.KvQueries(db)
    print(kq)
    kq.get(k)
    print()

@kv.command()
@click.argument("prefix")
def scan(prefix):
    kq = feedz.KvQueries(db)
    for kv in kq.scan(prefix):
        print(kv)

@kv.command()
@click.argument("v")
def record(v):
    pass

@kv.command()
@click.argument("ns")
def namespace(ns):
    pass

def main():
    # consume_all()
    cli()

if __name__ == "__main__":
    main()
