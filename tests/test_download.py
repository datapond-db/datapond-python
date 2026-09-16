"""Offline tests for the download/update paths (no network: requests is patched)."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest
import requests

import importlib

from datapond import connection

dl = importlib.import_module("datapond.download")


def make_db(path: Path, value: int) -> bytes:
    con = duckdb.connect(str(path))
    con.execute(f"CREATE TABLE t AS SELECT {value} AS n")
    con.close()
    return path.read_bytes()


class Response:
    """A fake streaming response; ``chunks`` may end with an exception to raise."""

    def __init__(self, chunks, size=None, etag=None):
        self.chunks = chunks
        self.headers = {"content-length": str(size)} if size is not None else {}
        if etag:
            self.headers["etag"] = '"' + etag + '"'

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        for c in self.chunks:
            if isinstance(c, Exception):
                raise c
            yield c


@pytest.fixture
def no_hf(monkeypatch):
    monkeypatch.setattr(dl, "_try_hf_download", lambda *a, **k: False)
    monkeypatch.setattr(dl, "remote_identity", lambda url, timeout=30: {"etag": "abc", "size": 0})


def test_interrupted_download_preserves_existing_copy(tmp_path, no_hf):
    dest = tmp_path / "x.duckdb"
    good = make_db(dest, 1)
    entry = {"attach_url": "https://example.test/x.duckdb"}
    broken = Response([b"partial", requests.ConnectionError("cut")])
    with patch.object(dl, "get_database", return_value=entry), patch.object(dl.requests, "get", return_value=broken):
        with pytest.raises(requests.ConnectionError):
            dl.download("x", path=str(dest), quiet=True)
    assert dest.read_bytes() == good
    assert not (tmp_path / "x.duckdb.part").exists()


def test_truncated_or_corrupt_download_is_rejected(tmp_path, no_hf):
    dest = tmp_path / "x.duckdb"
    good = make_db(dest, 1)
    entry = {"attach_url": "https://example.test/x.duckdb"}
    with patch.object(dl, "get_database", return_value=entry), patch.object(dl.requests, "get", return_value=Response([b"abc"], size=10)):
        with pytest.raises(IOError):
            dl.download("x", path=str(dest), quiet=True)
    assert dest.read_bytes() == good
    with patch.object(dl, "get_database", return_value=entry), patch.object(dl.requests, "get", return_value=Response([b"not a database"], size=14)):
        with pytest.raises(Exception):
            dl.download("x", path=str(dest), quiet=True)
    assert dest.read_bytes() == good


def test_successful_download_replaces_and_records_identity(tmp_path, no_hf):
    dest = tmp_path / "x.duckdb"
    make_db(dest, 1)
    new = make_db(tmp_path / "new.duckdb", 2)
    entry = {"attach_url": "https://example.test/x.duckdb"}
    with patch.object(dl, "get_database", return_value=entry), patch.object(dl.requests, "get", return_value=Response([new], size=len(new))):
        out = dl.download("x", path=str(dest), quiet=True)
    assert out == dest and dest.read_bytes() == new
    side = json.loads((tmp_path / "x.duckdb.datapond.json").read_text())
    assert side["etag"] == "abc" and side["local_size"] == len(new)


def test_directory_destinations(tmp_path, no_hf):
    new = make_db(tmp_path / "new.duckdb", 2)
    entry = {"attach_url": "https://example.test/x.duckdb"}
    for target in (str(tmp_path / "newdir") + os.sep, str(tmp_path / "other")):
        with patch.object(dl, "get_database", return_value=entry), patch.object(dl.requests, "get", return_value=Response([new], size=len(new))):
            out = dl.download("x", path=target, quiet=True)
        assert out == Path(target) / "x.duckdb" and out.is_file()


def test_update_compares_remote_identity_not_mtime(tmp_path, monkeypatch):
    monkeypatch.setattr(dl, "DATAPOND_DIR", tmp_path)
    dest = tmp_path / "x.duckdb"
    make_db(dest, 1)
    entry = {"attach_url": "https://example.test/x.duckdb", "updated": "2026-09-15"}
    dl._write_sidecar(dest, "x", entry["attach_url"], {"etag": "v1", "size": dest.stat().st_size})
    with patch.object(dl, "get_database", return_value=entry), patch.object(dl, "download") as d:
        with patch.object(dl, "remote_identity", return_value={"etag": "v1"}):
            dl.update("x")
            assert not d.called, "same ETag: no download"
        with patch.object(dl, "remote_identity", return_value={"etag": "v2"}):
            dl.update("x")
            assert d.called, "different ETag: re-download"


def test_update_without_sidecar_does_not_trust_same_day_mtime(tmp_path, monkeypatch):
    monkeypatch.setattr(dl, "DATAPOND_DIR", tmp_path)
    dest = tmp_path / "x.duckdb"
    dest.write_bytes(b"old")
    t = datetime(2026, 9, 15, 8, tzinfo=timezone.utc).timestamp()
    os.utime(dest, (t, t))
    entry = {"attach_url": "https://example.test/x.duckdb", "updated": "2026-09-15"}
    with patch.object(dl, "get_database", return_value=entry), patch.object(dl, "remote_identity", return_value={}), patch.object(dl, "download") as d:
        dl.update("x")
    assert d.called


def test_local_connection_quotes_paths_and_supports_replacement_scans(tmp_path):
    dest = tmp_path / "O'Brien.duckdb"
    make_db(dest, 7)
    with patch.object(connection, "_local_path", return_value=str(dest)):
        con = connection.attach([("fixture", {})], local=True, quiet=True, single=True)
    assert isinstance(con, duckdb.DuckDBPyConnection)
    assert con.sql("SELECT n FROM t").fetchone()[0] == 7
    pd = pytest.importorskip("pandas")
    frame = pd.DataFrame({"x": [1, 2]})  # noqa: F841 - referenced by name in SQL
    assert con.sql("SELECT SUM(x) FROM frame").fetchone()[0] == 3
    con.close()


def test_update_never_trusts_size_alone(tmp_path, monkeypatch):
    monkeypatch.setattr(dl, "DATAPOND_DIR", tmp_path)
    dest = tmp_path / "x.duckdb"
    make_db(dest, 1)
    entry = {"attach_url": "https://example.test/x.duckdb", "updated": "2026-09-15"}
    dl._write_sidecar(dest, "x", entry["attach_url"], {"size": dest.stat().st_size})
    with patch.object(dl, "get_database", return_value=entry), patch.object(dl, "download") as d:
        with patch.object(dl, "remote_identity", return_value={"size": dest.stat().st_size}):
            dl.update("x")
            assert d.called, "same size but no validator: re-download"
        d.reset_mock()
        dl._write_sidecar(dest, "x", entry["attach_url"], {"last_modified": "Tue, 15 Sep 2026 00:00:00 GMT", "size": 1})
        with patch.object(dl, "remote_identity", return_value={"last_modified": "Tue, 15 Sep 2026 00:00:00 GMT", "size": 1}):
            dl.update("x")
            assert not d.called, "same Last-Modified is a validator"


def test_download_redoes_transfer_when_file_changed_between_head_and_get(tmp_path, monkeypatch):
    monkeypatch.setattr(dl, "_try_hf_download", lambda *a, **k: False)
    monkeypatch.setattr(dl, "remote_identity", lambda url, timeout=30: {"etag": "old"})
    dest = tmp_path / "x.duckdb"
    v1 = make_db(tmp_path / "v1.duckdb", 1)
    v2 = make_db(tmp_path / "v2.duckdb", 2)
    entry = {"attach_url": "https://example.test/x.duckdb"}
    responses = [Response([v1], size=len(v1), etag="new"), Response([v2], size=len(v2), etag="new")]
    with patch.object(dl, "get_database", return_value=entry), patch.object(dl.requests, "get", side_effect=responses) as get:
        dl.download("x", path=str(dest), quiet=True)
    assert get.call_count == 2 and dest.read_bytes() == v2
    assert json.loads((tmp_path / "x.duckdb.datapond.json").read_text())["etag"] == "new"
