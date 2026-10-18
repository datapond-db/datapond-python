"""Exercise identifier quoting through real DuckDB parsing, without network I/O."""
from types import SimpleNamespace

import duckdb
import pytest

from datapond import cli, connection, registry


@pytest.fixture
def databases(tmp_path, monkeypatch):
    original_connect = duckdb.connect
    paths = []
    for index in (1, 2, 3):
        path = tmp_path / ("fixture%d.duckdb" % index)
        con = original_connect(str(path), config={"threads": "1", "memory_limit": "128MiB"})
        con.execute("CREATE TABLE records (value INTEGER)")
        con.execute("INSERT INTO records VALUES (?)", [index])
        con.close()
        paths.append(str(path))

    opened = []
    extensions = []

    class OfflineConnection:
        def __init__(self, *args, **kwargs):
            kwargs["config"] = {"threads": "1", "memory_limit": "128MiB",
                                "max_temp_directory_size": "0B"}
            self.con = original_connect(*args, **kwargs)
            opened.append(self.con)

        def install_extension(self, name):
            assert name == "httpfs"
            extensions.append(("install", name))

        def load_extension(self, name):
            assert name == "httpfs"
            extensions.append(("load", name))

        def __getattr__(self, name):
            return getattr(self.con, name)

    monkeypatch.setattr(duckdb, "connect", OfflineConnection)
    monkeypatch.setattr(connection, "get_database", lambda identifier: {
        "id": identifier, "attach_url": paths[{"cms-medicare": 1, 'id"quoted': 2}.get(identifier, 0)]})
    monkeypatch.setattr(connection, "_local_path",
                        lambda identifier: paths[{"cms-medicare": 1, 'id"quoted': 2}.get(identifier, 0)])
    yield SimpleNamespace(paths=paths, extensions=extensions, opened=opened, original_connect=original_connect)
    for con in opened:
        con.close()


@pytest.mark.parametrize("identifier", ["eoir", "fjc-judges", "cms-medicare", "ipeds-db", 'id"quoted'])
@pytest.mark.parametrize("local", [True, False])
def test_single_database_identifier_selects_default_and_stays_read_only(databases, identifier, local):
    con = connection.connect(identifier, local=local, quiet=True)
    try:
        expected = {"cms-medicare": 2, 'id"quoted': 3}.get(identifier, 1)
        assert con.execute("SELECT value FROM records").fetchall() == [(expected,)]
        assert con.execute("SELECT current_database()").fetchone() == (identifier,)
        with pytest.raises(duckdb.Error, match="(?i)read.only"):
            con.execute("INSERT INTO records VALUES (99)")
        assert bool(databases.extensions) is (not local)
    finally:
        con.close()


@pytest.mark.parametrize("local", [True, False])
def test_multiple_hyphenated_catalogs_can_be_queried_together(databases, local):
    con = connection.connect(["fjc-judges", "cms-medicare", 'id"quoted'], local=local, quiet=True)
    try:
        assert con.execute('SELECT f.value, c.value, q.value FROM "fjc-judges".records f '
                           'CROSS JOIN "cms-medicare".records c '
                           'CROSS JOIN "id""quoted".records q').fetchall() == [(1, 2, 3)]
        assert databases.extensions == ([] if local else [("install", "httpfs"), ("load", "httpfs")])
    finally:
        con.close()


@pytest.mark.parametrize("identifier", ["eoir", "fjc-judges", 'id"quoted'])
def test_cli_initialization_sql_handles_the_same_identifiers(databases, monkeypatch, identifier):
    monkeypatch.setattr(registry, "get_database", lambda name: {"attach_url": databases.paths[0]})
    monkeypatch.setattr(cli.shutil, "which", lambda name: "existing-duckdb-cli")
    executed = []

    def run(args, check):
        assert args[:2] == ["existing-duckdb-cli", "-cmd"] and check is True
        # Execute the actual CLI-generated ATTACH and USE; fixtures need no extension.
        sql = args[2].replace("INSTALL httpfs;", "").replace("LOAD httpfs;", "")
        con = databases.original_connect(config={"threads": "1", "memory_limit": "128MiB"})
        try:
            con.execute(sql)
            assert con.execute("SELECT current_database(), value FROM records").fetchall() == [(identifier, 1)]
            executed.append(True)
        finally:
            con.close()
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(cli.subprocess, "run", run)
    cli._cmd_connect(identifier)
    assert executed == [True]


def test_connection_attachment_timing_is_preserved(databases):
    con = connection.connect("fjc-judges", local=True, quiet=True)
    try:
        assert len(databases.opened) == 1
        assert con.execute("SELECT value FROM records").fetchall() == [(1,)]
        assert len(databases.opened) == 1
    finally:
        con.close()
