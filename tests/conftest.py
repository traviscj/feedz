from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool

SCHEMA = Path(__file__).parents[1] / "schema" / "sqlite.sql"


def make_engine():
    eng = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    sql = "\n".join(
        line for line in SCHEMA.read_text().splitlines() if not line.lstrip().startswith("--")
    )
    with eng.begin() as conn:
        for stmt in sql.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))
    return eng


@pytest.fixture()
def engine():
    eng = make_engine()
    yield eng
    eng.dispose()
