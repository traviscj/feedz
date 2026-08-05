import pytest
from click.testing import CliRunner
from sqlalchemy import create_engine, text

import feedz.cli
from tests.conftest import SCHEMA


@pytest.fixture()
def runner(tmp_path, monkeypatch):
    db_path = tmp_path / "feedz.db"
    eng = create_engine(f"sqlite:///{db_path}")
    sql = "\n".join(
        line for line in SCHEMA.read_text().splitlines() if not line.lstrip().startswith("--")
    )
    with eng.begin() as conn:
        for stmt in sql.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
    eng.dispose()
    monkeypatch.setenv("FEEDZ_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(feedz.cli, "_engine", None)
    return CliRunner()


def run(runner, *args):
    result = runner.invoke(feedz.cli.cli, list(args))
    assert result.exit_code == 0, result.output
    return result.output


def test_kv_lifecycle(runner):
    run(runner, "kv", "put", "ns", "k1", "hello")
    assert run(runner, "kv", "get", "ns", "k1").strip() == "hello"
    assert "k1" in run(runner, "kv", "scan", "ns")
    assert run(runner, "kv", "ls").strip() == "ns\t1"
    run(runner, "kv", "delete", "ns", "k1")
    assert run(runner, "kv", "ls").strip() == ""


def test_feed_lifecycle(runner):
    run(runner, "kv", "put", "ns", "k1", "v1")
    assert "published 1" in run(runner, "feed", "publish")
    assert "consumed 1" in run(runner, "feed", "consume", "--all")
    assert "consumed 0" in run(runner, "feed", "consume")
    out = run(runner, "feed", "status")
    assert "max_fsi=1" in out
    assert "feed-kv" in out
    run(runner, "feed", "unpublish", "ns", "k1")
    assert "published 0" in run(runner, "feed", "publish")
    run(runner, "feed", "republish", "ns", "k1")
    assert "published 1" in run(runner, "feed", "publish")
