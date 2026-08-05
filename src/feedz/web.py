"""feedz web: dense status dashboard + the HTTP feed endpoint.

GET /                          dashboard
GET /ns/<ns>?prefix=           browse a namespace
GET /_feeds/fetch/<table>      feed page: ?after=&limit=&shard=&shard_count=
                               -> {"entries": [...], "cursor": <max fsi seen>}
GET /api/status                dashboard data as JSON
"""

from __future__ import annotations

import os

from flask import Flask, abort, jsonify, render_template_string, request
from sqlalchemy import text

from feedz.db import get_engine
from feedz.feed import fetch_entries
from feedz.queries import FeedCursorQueries, KvQueries, SeqQueries

_CSS = """
* { margin:0; padding:0; box-sizing:border-box; }
body { background:#0d1117; color:#c9d1d9; font:12px/1.45 "SF Mono",Menlo,monospace; padding:8px 12px; }
h1 { font-size:13px; color:#58a6ff; margin:6px 0 2px; }
h1 small { color:#484f58; font-weight:normal; }
table { border-collapse:collapse; margin:2px 0 10px; }
th { text-align:left; color:#8b949e; font-weight:normal; border-bottom:1px solid #21262d; padding:1px 10px 1px 0; }
td { padding:1px 10px 1px 0; border-bottom:1px solid #161b22; white-space:nowrap; }
td.v { white-space:normal; word-break:break-all; color:#8b949e; }
a { color:#58a6ff; text-decoration:none; }
.ok { color:#3fb950; } .warn { color:#d29922; } .bad { color:#f85149; } .dim { color:#484f58; }
.num { text-align:right; }
"""

_PAGE = """<!doctype html><title>feedz</title><style>""" + _CSS + """</style>
<h1>feedz <small>max_fsi={{max_fsi}} unpublished={{unpublished}}</small></h1>

<h1>namespaces <small>{{namespaces|length}}</small></h1>
<table><tr><th>ns</th><th class=num>rows</th></tr>
{% for ns, n in namespaces %}<tr><td><a href="/ns/{{ns}}">{{ns}}</a></td><td class=num>{{n}}</td></tr>{% endfor %}
</table>

<h1>sequences <small>{{sequences|length}}</small></h1>
<table><tr><th>name</th><th class=num>value</th><th class=num>version</th></tr>
{% for s in sequences %}<tr><td>{{s.name}}</td><td class=num>{{s.value}}</td><td class=num>{{s.version}}</td></tr>{% endfor %}
</table>

<h1>cursors <small>{{cursors|length}}</small></h1>
<table><tr><th>consumer</th><th class=num>shard</th><th class=num>/count</th><th class=num>token</th><th class=num>lag</th><th>state</th></tr>
{% for c in cursors %}<tr><td>{{c.consumer}}</td><td class=num>{{c.shard}}</td><td class=num>{{c.shard_count}}</td>
<td class=num>{{c.token}}</td><td class="num {{'bad' if c.lag else 'ok'}}">{{c.lag}}</td>
<td class="{{'ok' if c.enabled else 'warn'}}">{{'live' if c.enabled else 'paused'}}</td></tr>{% endfor %}
</table>

<h1>recent <small>last {{recent|length}} by id</small></h1>
<table><tr><th>ns</th><th>k</th><th class=num>fsi</th><th class=num>shard</th><th>v</th></tr>
{% for r in recent %}<tr><td><a href="/ns/{{r.ns}}">{{r.ns}}</a></td><td>{{r.k}}</td>
<td class="num {{'dim' if r.feed_sync_id is none else ''}}">{{r.feed_sync_id if r.feed_sync_id is not none else 'null'}}</td>
<td class="num {{'bad' if r.shard < 0 else ''}}">{{r.shard}}</td><td class=v>{{r.text()[:200]}}</td></tr>{% endfor %}
</table>
"""

_NS_PAGE = """<!doctype html><title>feedz {{ns}}</title><style>""" + _CSS + """</style>
<h1><a href="/">feedz</a> / {{ns}} <small>{{rows|length}} rows{% if prefix %} prefix={{prefix}}{% endif %}</small></h1>
<table><tr><th>k</th><th class=num>fsi</th><th class=num>shard</th><th>v</th></tr>
{% for r in rows %}<tr><td>{{r.k}}</td>
<td class="num {{'dim' if r.feed_sync_id is none else ''}}">{{r.feed_sync_id if r.feed_sync_id is not none else 'null'}}</td>
<td class="num {{'bad' if r.shard < 0 else ''}}">{{r.shard}}</td><td class=v>{{r.text()[:500]}}</td></tr>{% endfor %}
</table>
"""


def create_app(engine=None) -> Flask:
    app = Flask(__name__)
    state = {"engine": engine}

    def eng():
        if state["engine"] is None:
            state["engine"] = get_engine()
        return state["engine"]

    def status_data():
        kq = KvQueries(eng())
        max_fsi = kq.max_feed_sync_id()
        cursors = [
            {
                "consumer": c.consumer,
                "shard": c.shard,
                "shard_count": c.shard_count,
                "token": c.token,
                "enabled": c.enabled,
                # Approximate for sharded consumers (their slice may not
                # contain max_fsi itself).
                "lag": max(0, max_fsi - c.token),
            }
            for c in FeedCursorQueries.all(eng())
        ]
        return {
            "max_fsi": max_fsi,
            "unpublished": kq.unpublished_count(),
            "namespaces": kq.namespaces(),
            "sequences": SeqQueries(eng()).all(),
            "cursors": cursors,
        }

    @app.get("/")
    def index():
        d = status_data()
        with eng().connect() as conn:
            recent = [
                KvQueries(eng()).get(r.ns, r.k)
                for r in conn.execute(
                    text("SELECT ns, k FROM kv ORDER BY id DESC LIMIT 50")
                ).all()
            ]
        # dict cursors -> attribute-style access for the template
        d["cursors"] = [type("C", (), c)() for c in d["cursors"]]
        return render_template_string(_PAGE, recent=[r for r in recent if r], **d)

    @app.get("/ns/<ns>")
    def ns_view(ns):
        prefix = request.args.get("prefix", "")
        rows = KvQueries(eng()).scan(ns, prefix)
        return render_template_string(_NS_PAGE, ns=ns, prefix=prefix, rows=rows)

    @app.get("/_feeds/fetch/<table>")
    def feeds_fetch(table):
        after = request.args.get("after", 0, type=int)
        limit = min(request.args.get("limit", 100, type=int), 1000)
        shard = request.args.get("shard", type=int)
        shard_count = request.args.get("shard_count", type=int)
        try:
            entries = fetch_entries(
                eng(), table=table, after=after, limit=limit, shard=shard, shard_count=shard_count
            )
        except ValueError:
            abort(400)
        cursor = entries[-1].feed_sync_id if entries else after
        return jsonify({"entries": [e.as_dict() for e in entries], "cursor": cursor})

    @app.get("/api/status")
    def api_status():
        d = status_data()
        d["sequences"] = [
            {"name": s.name, "value": s.value, "version": s.version} for s in d["sequences"]
        ]
        d["namespaces"] = [{"ns": ns, "rows": n} for ns, n in d["namespaces"]]
        return jsonify(d)

    return app


app = create_app()


def main():
    create_app().run(
        host=os.environ.get("FEEDZ_WEB_HOST", "127.0.0.1"),
        port=int(os.environ.get("FEEDZ_WEB_PORT", "5001")),
        debug=bool(os.environ.get("FEEDZ_WEB_DEBUG")),
    )
