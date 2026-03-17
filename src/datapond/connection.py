"""
Database connection management for datapond.

Provides lazy DuckDB connections to remote or local databases.
"""

from pathlib import Path
from typing import Union

import duckdb

from datapond.registry import get_database


def connect(db_id: Union[str, list], local: bool = False, quiet: bool = False):
    """Connect to one or more datapond databases and return a lazy DuckDB connection.

    The returned connection does not attach the database until the first query.
    This makes connect() return instantly.

    Args:
        db_id: A single database ID string, or a list of database IDs.
        local: If True, attach from local ~/.datapond/{db_id}.duckdb files
               instead of remote URLs. The files must already be downloaded.
        quiet: If True, suppress all progress messages.

    Returns:
        A LazyConnection that behaves like a duckdb.Connection.
    """
    if isinstance(db_id, str):
        db_ids = [db_id]
    elif isinstance(db_id, list):
        if not db_id:
            raise ValueError("db_id list must not be empty")
        db_ids = db_id
    else:
        raise TypeError(f"db_id must be a string or list, got {type(db_id).__name__}")

    # Pre-fetch registry entries now so errors surface immediately
    entries = []
    for did in db_ids:
        entries.append((did, get_database(did)))

    single = isinstance(db_id, str)
    return LazyConnection(entries, local=local, quiet=quiet, single=single)


class LazyConnection:
    """A wrapper around duckdb.Connection that ATTACHes on first use."""

    def __init__(self, entries, local, quiet, single):
        self._entries = entries  # list of (db_id, registry_dict)
        self._local = local
        self._quiet = quiet
        self._single = single
        self._con = None

    def _ensure_attached(self):
        """Perform the actual ATTACH if not yet done."""
        if self._con is not None:
            return

        con = duckdb.connect()
        installed_httpfs = False

        for db_id, db in self._entries:
            name = db.get("name", db_id)
            size = db.get("size_gb", "?")

            if self._local:
                if not self._quiet:
                    print(f"Connecting to {name} (local)...")
                path = _local_path(db_id)
                con.execute(f"ATTACH '{path}' AS {db_id} (READ_ONLY)")
            else:
                if not self._quiet:
                    print(f"Connecting to {name} ({size} GB remote)...")
                attach_url = db["attach_url"]
                if not installed_httpfs:
                    con.install_extension("httpfs")
                    con.load_extension("httpfs")
                    installed_httpfs = True
                con.execute(f"ATTACH '{attach_url}' AS {db_id} (READ_ONLY)")

        if self._single:
            con.execute(f"USE {self._entries[0][0]}")

        # Count tables
        if not self._quiet:
            tables = con.sql(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema != 'information_schema'"
            ).fetchone()[0]
            if len(self._entries) == 1:
                print(f"Connected. {tables} tables available.")
            else:
                print(
                    f"Connected. {tables} tables available "
                    f"across {len(self._entries)} databases."
                )

        self._con = con

    # --- Proxy common methods ---

    def sql(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.sql(*args, **kwargs)

    def execute(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.execute(*args, **kwargs)

    def executemany(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.executemany(*args, **kwargs)

    def table(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.table(*args, **kwargs)

    def view(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.view(*args, **kwargs)

    def values(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.values(*args, **kwargs)

    def from_csv_auto(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.from_csv_auto(*args, **kwargs)

    def from_parquet(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.from_parquet(*args, **kwargs)

    def fetchone(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.fetchone(*args, **kwargs)

    def fetchmany(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.fetchmany(*args, **kwargs)

    def fetchall(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.fetchall(*args, **kwargs)

    def fetchnumpy(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.fetchnumpy(*args, **kwargs)

    def fetchdf(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.fetchdf(*args, **kwargs)

    def fetch_df(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.fetch_df(*args, **kwargs)

    def fetch_arrow_table(self, *args, **kwargs):
        self._ensure_attached()
        return self._con.fetch_arrow_table(*args, **kwargs)

    def close(self):
        if self._con is not None:
            self._con.close()
            self._con = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __getattr__(self, name):
        """Proxy any other attribute access to the underlying connection."""
        self._ensure_attached()
        return getattr(self._con, name)

    def __del__(self):
        self.close()


def _local_path(db_id: str) -> str:
    """Return the expected local path for a downloaded database file."""
    path = Path.home() / ".datapond" / f"{db_id}.duckdb"
    if not path.exists():
        raise FileNotFoundError(
            f"Local database file not found: {path}\n"
            f"Download it first with: datapond.download('{db_id}')"
        )
    return str(path)
