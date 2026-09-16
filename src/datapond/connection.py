"""
Database connection management for datapond.

``connect()`` returns a plain ``duckdb.DuckDBPyConnection`` with the requested
databases attached (read-only). Everything DuckDB's Python API offers works on
it unchanged, including replacement scans of pandas/Arrow objects referenced by
name in SQL and ``con.register()``.
"""

from pathlib import Path
from typing import Union

import duckdb

from datapond.registry import get_database


def connect(db_id: Union[str, list], local: bool = False, quiet: bool = False) -> duckdb.DuckDBPyConnection:
    """Connect to one or more datapond databases.

    Args:
        db_id: A single database ID string, or a list of database IDs.
        local: If True, attach from local ~/.datapond/{db_id}.duckdb files
               instead of remote URLs. The files must already be downloaded.
        quiet: If True, suppress all progress messages.

    Returns:
        A ``duckdb.DuckDBPyConnection``. With a single id that database is the
        default schema (``USE``); with several, qualify tables as
        ``"<id>".<table>``.
    """
    if isinstance(db_id, str):
        db_ids = [db_id]
    elif isinstance(db_id, list):
        if not db_id:
            raise ValueError("db_id list must not be empty")
        db_ids = db_id
    else:
        raise TypeError(f"db_id must be a string or list, got {type(db_id).__name__}")

    entries = [(did, get_database(did)) for did in db_ids]
    return attach(entries, local=local, quiet=quiet, single=isinstance(db_id, str))


def attach(entries, local: bool = False, quiet: bool = False, single: bool = True) -> duckdb.DuckDBPyConnection:
    """ATTACH every (db_id, registry entry) pair on a fresh DuckDB connection."""
    con = duckdb.connect()
    installed_httpfs = False

    for db_id, db in entries:
        name = db.get("name", db_id)
        size = db.get("size_gb", "?")
        quoted = _quote_ident(db_id)

        if local:
            if not quiet:
                print(f"Connecting to {name} (local)...")
            source = _local_path(db_id)
        else:
            if not quiet:
                print(f"Connecting to {name} ({size} GB remote)...")
            source = db["attach_url"]
            if not installed_httpfs:
                con.install_extension("httpfs")
                con.load_extension("httpfs")
                installed_httpfs = True
        con.execute(f"ATTACH {_quote_literal(source)} AS {quoted} (READ_ONLY)")

    if single:
        con.execute(f"USE {_quote_ident(entries[0][0])}")

    if not quiet:
        tables = con.sql(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema != 'information_schema' "
            "AND table_name NOT IN ('_metadata', '_columns')"
        ).fetchone()[0]
        if len(entries) == 1:
            print(f"Connected. {tables} tables available.")
        else:
            print(f"Connected. {tables} tables available across {len(entries)} databases.")
    return con


def _quote_ident(name: str) -> str:
    """Double-quote an identifier so ids with hyphens (ipeds-db) are valid."""
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    """Single-quote a SQL string literal (paths with apostrophes included)."""
    return "'" + str(value).replace("'", "''") + "'"


def _local_path(db_id: str) -> str:
    """Return the expected local path for a downloaded database file."""
    path = Path.home() / ".datapond" / f"{db_id}.duckdb"
    if not path.exists():
        raise FileNotFoundError(
            f"Local database file not found: {path}\n"
            f"Download it first with: datapond.download('{db_id}')"
        )
    return str(path)
