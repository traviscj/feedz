#!/usr/bin/env python3
import time

import click
import pyperclip
from click_repl import register_repl

from feedz import sync as sync_mod
from feedz.db import get_engine
from feedz.feed import Consumer, PrintListener, Publisher
from feedz.queries import FeedCursorQueries, KvQueries, SeqQueries

FEED_NAME = "feed-kv"
DEFAULT_CONSUMER = "show"

_engine = None


def db():
    global _engine
    if _engine is None:
        _engine = get_engine()
    return _engine


@click.group()
def cli():
    """kv store + change feed. DB from $FEEDZ_DB_URL (any SQLAlchemy URL)."""


# --- kv ---------------------------------------------------------------------


@cli.group()
def kv():
    """Key-value operations."""


@kv.command()
@click.argument("ns")
@click.argument("k")
@click.argument("v")
def put(ns, k, v):
    """Upsert NS/K = V (queues it for the feed)."""
    KvQueries(db()).put(ns, k, v)
    click.echo(f"{ns}/{k} ok")


@kv.command()
@click.argument("ns")
@click.argument("k")
def get(ns, k):
    row = KvQueries(db()).get(ns, k)
    if row is None:
        raise click.ClickException(f"no such key: {ns}/{k}")
    click.echo(row.text())


@kv.command()
@click.argument("ns")
@click.argument("prefix", default="")
def scan(ns, prefix):
    for row in KvQueries(db()).scan(ns, prefix):
        fsi = row.feed_sync_id if row.feed_sync_id is not None else "null"
        click.echo(f"{row.ns}\t{row.k}\t{fsi}\t{row.shard}\t{row.text()}")


@kv.command()
@click.argument("ns")
@click.argument("v")
def record(ns, v):
    """Store V under a fresh random key in NS; prints the key."""
    click.echo(KvQueries(db()).rec(ns, v).k)


@kv.command()
@click.argument("ns")
@click.argument("k")
def delete(ns, k):
    ok = KvQueries(db()).delete(ns, k)
    click.echo("deleted" if ok else "no such key")


@kv.command("ls")
def kv_ls():
    """Namespaces with row counts."""
    for ns, n in KvQueries(db()).namespaces():
        click.echo(f"{ns}\t{n}")


@kv.command()
@click.argument("ns")
def watchpb(ns):
    """Record every new http(s) URL that lands on the clipboard into NS."""
    click.echo(f"watchpb w/ ns={ns}")
    kq = KvQueries(db())
    recent: set[str] = set()
    last_paste = ""
    while True:
        cur_paste = pyperclip.paste()
        if cur_paste.startswith("http") and cur_paste != last_paste and cur_paste not in recent:
            kq.rec(ns, cur_paste)
            last_paste = cur_paste
            recent.add(cur_paste)
            click.echo(f"{cur_paste} -- {len(recent)}")
        time.sleep(1)


# --- feed -------------------------------------------------------------------


@cli.group()
def feed():
    """Publish/consume the change feed."""


@feed.command()
@click.option("--shards", default=1, show_default=True, help="data-shard count for new assignments")
def publish(shards):
    """Assign feed_sync_ids to all pending records."""
    n = Publisher(db(), FEED_NAME, shards=shards).publish()
    click.echo(f"published {n}")


@feed.command()
@click.argument("consumer", default=DEFAULT_CONSUMER)
@click.option("--all", "consume_all_", is_flag=True, help="drain the feed")
@click.option("--shard", type=int, default=None, help="consumer shard to run as")
@click.option("--shard-count", type=int, default=1, show_default=True)
def consume(consumer, consume_all_, shard, shard_count):
    """Print feed records past CONSUMER's cursor."""
    c = Consumer(db(), consumer, PrintListener(), shard=shard, shard_count=shard_count)
    n = c.consume_all() if consume_all_ else c.consume()
    click.echo(f"consumed {n}")


@feed.command()
@click.argument("ns")
@click.argument("k")
def unpublish(ns, k):
    """Remove NS/K from the feed (shard = -1) without deleting it."""
    ok = KvQueries(db()).unpublish(ns, k)
    click.echo("unpublished" if ok else "no such key")


@feed.command()
@click.argument("ns")
@click.argument("k")
def republish(ns, k):
    """Queue NS/K for re-feeding (also restores an unpublished record)."""
    ok = KvQueries(db()).republish(ns, k)
    click.echo("queued" if ok else "no such key")


@feed.command("republish-all")
@click.option("--ns", default=None, help="restrict to one namespace")
def republish_all(ns):
    """Queue every published record for re-feeding."""
    click.echo(f"queued {KvQueries(db()).republish_all(ns)}")


@feed.command()
@click.argument("consumer")
def reset(consumer):
    FeedCursorQueries(db(), consumer).reset()
    click.echo(f"{consumer} reset to 0")


@feed.command()
@click.argument("consumer")
@click.argument("fsi", type=int)
def seek(consumer, fsi):
    FeedCursorQueries(db(), consumer).seek(fsi)
    click.echo(f"{consumer} at {fsi}")


@feed.command()
@click.argument("consumer")
def pause(consumer):
    FeedCursorQueries(db(), consumer).pause()
    click.echo(f"{consumer} paused")


@feed.command()
@click.argument("consumer")
def resume(consumer):
    FeedCursorQueries(db(), consumer).resume()
    click.echo(f"{consumer} resumed")


@feed.command()
def status():
    """Sequences, cursors w/ lag, and pending-publish count."""
    kq = KvQueries(db())
    max_fsi = kq.max_feed_sync_id()
    click.echo(f"max_fsi={max_fsi} unpublished={kq.unpublished_count()}")
    for s in SeqQueries(db()).all():
        click.echo(f"seq\t{s.name}\tvalue={s.value}\tversion={s.version}")
    for c in FeedCursorQueries.all(db()):
        state = "live" if c.enabled else "paused"
        lag = max(0, max_fsi - c.token)
        click.echo(
            f"cursor\t{c.consumer}[{c.shard}/{c.shard_count}]\ttoken={c.token}\tlag={lag}\t{state}"
        )


@feed.command()
@click.argument("base_url")
@click.option(
    "--conflict",
    type=click.Choice(["overwrite", "rename"]),
    default="overwrite",
    show_default=True,
)
def pull(base_url, conflict):
    """Sync a remote feedz instance's feed into the local kv."""
    n = sync_mod.pull(db(), base_url, conflict=conflict)
    click.echo(f"pulled {n}")


@cli.command()
def web():
    """Run the web dashboard (also: feedz-web)."""
    from feedz.web import main as web_main

    web_main()


register_repl(cli)


def main():
    cli()


if __name__ == "__main__":
    main()
