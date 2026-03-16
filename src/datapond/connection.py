"""
Database connection management for datapond.

Provides DuckDB connections to remote or local databases.
"""

from pathlib import Path
from typing import Union

import duckdb

from datapond.registry import get_database


def connect(db_id: Union[str, list], local: bool = False):
    """Connect to one or more datapond databases and return a DuckDB connection.

    Args:
        db_id: A single database ID string, or a list of database IDs.
        local: If True, attach from local ~/.datapond/{db_id}.duckdb files
               instead of remote URLs. The files must already be downloaded.

    Returns:
        A duckdb.Connection with the database(s) attached.
    """
    if isinstance(db_id, str):
        return _connect_single(db_id, local=local)
    if isinstance(db_id, list):
        return _connect_multi(db_id, local=local)
    raise TypeError(f"db_id must be a string or list, got {type(db_id).__name__}")


def _connect_single(db_id: str, local: bool = False):
    """Connect to a single database, attaching it and setting it as default."""
    db = get_database(db_id)
    con = duckdb.connect()

    if local:
        path = _local_path(db_id)
        con.execute(f"ATTACH '{path}' AS {db_id} (READ_ONLY)")
    else:
        attach_url = db["attach_url"]
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        con.execute(f"ATTACH '{attach_url}' AS {db_id} (READ_ONLY)")

    con.execute(f"USE {db_id}")
    return con


def _connect_multi(db_ids: list, local: bool = False):
    """Connect to multiple databases, attaching each under its own schema name."""
    if not db_ids:
        raise ValueError("db_id list must not be empty")

    con = duckdb.connect()
    installed_httpfs = False

    for db_id in db_ids:
        db = get_database(db_id)

        if local:
            path = _local_path(db_id)
            con.execute(f"ATTACH '{path}' AS {db_id} (READ_ONLY)")
        else:
            if not installed_httpfs:
                con.install_extension("httpfs")
                con.load_extension("httpfs")
                installed_httpfs = True
            attach_url = db["attach_url"]
            con.execute(f"ATTACH '{attach_url}' AS {db_id} (READ_ONLY)")

    return con


def _local_path(db_id: str) -> str:
    """Return the expected local path for a downloaded database file."""
    path = Path.home() / ".datapond" / f"{db_id}.duckdb"
    if not path.exists():
        raise FileNotFoundError(
            f"Local database file not found: {path}\n"
            f"Download it first with: datapond.download('{db_id}')"
        )
    return str(path)
