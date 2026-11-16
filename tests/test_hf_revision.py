"""The optional backend must fetch the same resource named by the registry URL."""
import hashlib
import importlib
import sys
from types import SimpleNamespace

import duckdb
import pytest

dl = importlib.import_module('datapond.download')


def database(path, value):
    con = duckdb.connect(str(path), config={'threads': '1', 'memory_limit': '128MiB'})
    con.execute('CREATE TABLE selected AS SELECT ? AS value', [value])
    con.close()
    return path


def hub(data, download, commit, metadata_calls):
    def url(**kwargs):metadata_calls.append(kwargs);return 'https://huggingface.co/test-metadata'
    return SimpleNamespace(hf_hub_download=download, hf_hub_url=url,
                           get_hf_file_metadata=lambda _: SimpleNamespace(
                               commit_hash=commit, etag=hashlib.sha256(data.read_bytes()).hexdigest(),
                               size=data.stat().st_size))


@pytest.mark.parametrize('revision,url_revision,filename', [
    ('a' * 40, 'a' * 40, 'data/sahie.duckdb'),
    ('main', 'main', 'sahie.duckdb'),
    ('release/2024', 'release%2F2024', 'data/sahie.duckdb'),
])
def test_optional_backend_honors_registry_revision_and_nested_path(tmp_path, monkeypatch, revision, url_revision, filename):
    intended = database(tmp_path / 'intended.duckdb', 2024)
    wrong = database(tmp_path / 'wrong.duckdb', 2025)
    calls = []
    metadata_calls = []
    commit = revision if len(revision) == 40 else 'c' * 40

    def fake_download(**kwargs):
        calls.append(kwargs)
        return intended if kwargs.get('revision') == commit and kwargs['filename'] == filename else wrong

    monkeypatch.setitem(sys.modules, 'huggingface_hub', hub(intended, fake_download, commit, metadata_calls))
    url = f'https://huggingface.co/datasets/Nason/sahie-database/resolve/{url_revision}/{filename}?download=true'
    monkeypatch.setattr(dl, 'get_database', lambda *_: {'huggingface': 'https://huggingface.co/datasets/Nason/sahie-database', 'attach_url': url})
    monkeypatch.setattr(dl, 'remote_identity', lambda _: {'etag': 'intended', 'size': intended.stat().st_size})
    out = dl.download('sahie', path=str(tmp_path / 'result.duckdb'), quiet=True)
    with duckdb.connect(str(out), read_only=True) as con:
        assert con.execute('SELECT value FROM selected').fetchone() == (2024,)
    assert metadata_calls == [dict(repo_id='Nason/sahie-database', filename=filename, repo_type='dataset', revision=revision)]
    assert calls == [dict(repo_id='Nason/sahie-database', filename=filename, repo_type='dataset', revision=commit)]
    assert dl.read_sidecar(out)['url'] == url


def test_explicit_download_url_controls_backend_revision(tmp_path, monkeypatch):
    data = database(tmp_path / 'source.duckdb', 7)
    seen = []
    metadata_calls = []
    monkeypatch.setitem(sys.modules, 'huggingface_hub', hub(data, lambda **kw: seen.append(kw) or data, 'c' * 40, metadata_calls))
    root = 'https://huggingface.co/datasets/Nason/sahie-database'
    entry = {'huggingface': root, 'attach_url': root + '/resolve/main/old.duckdb', 'download_url': root + '/resolve/v2024/nested/new.duckdb'}
    monkeypatch.setattr(dl, 'get_database', lambda *_: entry)
    monkeypatch.setattr(dl, 'remote_identity', lambda _: {})
    dl.download('sahie', path=str(tmp_path / 'result.duckdb'), quiet=True)
    assert metadata_calls[0]['revision'] == 'v2024' and metadata_calls[0]['filename'] == 'nested/new.duckdb'
    assert seen[0]['revision'] == 'c' * 40 and seen[0]['filename'] == 'nested/new.duckdb'


@pytest.mark.parametrize('url', ['https://example.test/data.duckdb', 'https://huggingface.co/datasets/Elsewhere/other/resolve/main/data.duckdb'])
def test_different_download_resource_uses_requests(tmp_path, monkeypatch, url):
    calls = []
    monkeypatch.setattr(dl, '_try_hf_download', lambda *a, **kw: pytest.fail('Must not replace explicit resource with HF default'))
    monkeypatch.setattr(dl, 'get_database', lambda *_: {'huggingface': 'https://huggingface.co/datasets/Nason/sahie-database', 'download_url': url})
    monkeypatch.setattr(dl, 'remote_identity', lambda _: {})
    def request_download(actual, dest, db_id):
        calls.append(actual);database(dest, 8);return {'etag': 'requested'}
    monkeypatch.setattr(dl, '_download_with_requests', request_download)
    dl.download('sahie', path=str(tmp_path / 'result.duckdb'), quiet=True)
    assert calls == [url]
