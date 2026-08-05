import os

from sqlalchemy import Engine, create_engine

DEFAULT_URL = "mysql+pymysql://root@localhost/traviscj_localdev"


def get_engine(url: str | None = None) -> Engine:
    """Engine from an explicit URL, $FEEDZ_DB_URL, or the local-dev default.

    Connections are lazy; nothing touches the database until first use.
    """
    return create_engine(url or os.environ.get("FEEDZ_DB_URL", DEFAULT_URL))
