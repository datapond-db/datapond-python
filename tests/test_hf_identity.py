"""Small deterministic cache/race regressions; no live service or full data files."""
import hashlib
import importlib
import sys
from types import SimpleNamespace
from urllib.parse import quote

import duckdb
import pytest
import requests

dl = importlib.import_module('datapond.download')
REMOTE_IDENTITY = dl.remote_identity
COMMIT = 'b' * 40
ROOT = 'https://huggingface.co/datasets/Example/source'


def make_db(path, value):
    with duckdb.connect(str(path), config={'threads': '1', 'memory_limit': '128MiB'}) as con:
        con.execute('CREATE TABLE t AS SELECT ? AS value', [value])
    return path.read_bytes()


class Response:
    def __init__(self, data, etag=None):
        self.data = data
        self.headers = {'content-length': str(len(data))}
        if etag:
            self.headers['etag'] = '"' + etag + '"'
    def __enter__(self):return self
    def __exit__(self, *args):pass
    def raise_for_status(self):pass
    def iter_content(self, chunk_size):yield self.data


@pytest.fixture
def case(tmp_path, monkeypatch):
    old_path, new_path = tmp_path / 'old.duckdb', tmp_path / 'new.duckdb'
    old, new = make_db(old_path, 1), make_db(new_path, 2)
    identity = {'etag': hashlib.sha256(new).hexdigest(), 'size': len(new)}
    url = ROOT + '/resolve/main/nested/source.duckdb?download=true'
    entry = {'huggingface': ROOT, 'download_url': url, 'updated': '2020-01-01'}
    metadata = SimpleNamespace(commit_hash=COMMIT, **identity)
    calls, gets, metadata_urls = [], [], []
    def hf_url(repo_id, filename, repo_type, revision):
        return 'https://huggingface.co/datasets/' + repo_id + '/resolve/' + quote(revision, safe='') + '/' + quote(filename, safe='/')
    def get_metadata(url):metadata_urls.append(url);return metadata
    def hf_download(**kwargs):
        calls.append(kwargs)
        # Models fallback to a warm mutable-ref cache after metadata failure.
        return new_path if kwargs.get('revision') == COMMIT else old_path
    hub = SimpleNamespace(hf_hub_download=hf_download, hf_hub_url=hf_url, get_hf_file_metadata=get_metadata)
    def get(url, **kwargs):gets.append(url);return Response(new, identity['etag'])
    monkeypatch.setitem(sys.modules, 'huggingface_hub', hub)
    monkeypatch.setattr(dl, 'get_database', lambda *a, **kw: entry)
    monkeypatch.setattr(dl, 'remote_identity', lambda *a, **kw: dict(identity))
    monkeypatch.setattr(dl.requests, 'get', get)
    return SimpleNamespace(old=old, new=new, old_path=old_path, new_path=new_path,
                           identity=identity, metadata=metadata, entry=entry, hub=hub,
                           calls=calls, gets=gets, metadata_urls=metadata_urls,
                           dest=tmp_path / 'result.duckdb')


@pytest.mark.parametrize('cache_state', ['warm', 'cold'])
@pytest.mark.parametrize('revision', ['main', 'release%2F2024', COMMIT])
def test_hub_binds_metadata_to_resolved_commit(case, cache_state, revision):
    case.entry['download_url'] = ROOT + '/resolve/' + revision + '/nested/source.duckdb?download=true'
    if cache_state == 'cold':
        def cold(**kwargs):case.calls.append(kwargs);return case.new_path
        case.hub.hf_hub_download = cold
        # HEAD saw the preceding branch version; a cold download sees the new one.
        case.identity['etag'] = hashlib.sha256(case.old).hexdigest()
    out = dl.download('source', path=str(case.dest), quiet=True)
    assert out.read_bytes() == case.new
    sidecar = dl.read_sidecar(out)
    assert sidecar['etag'] == hashlib.sha256(out.read_bytes()).hexdigest()
    assert sidecar['resolved_revision'] == COMMIT
    assert case.calls == [dict(repo_id='Example/source', filename='nested/source.duckdb', repo_type='dataset', revision=COMMIT)]
    assert case.gets == []
    assert sidecar['url'] == case.entry['download_url']


@pytest.mark.parametrize('bad_cache', ['older_valid_db', 'truncated', 'oversized'])
def test_wrong_cached_bytes_are_rejected_before_fallback(case, bad_cache):
    if bad_cache == 'truncated':case.old_path.write_bytes(case.new[:-1])
    if bad_cache == 'oversized':case.old_path.write_bytes(case.new + b'extra')
    case.hub.hf_hub_download = lambda **kwargs: case.old_path
    out = dl.download('source', path=str(case.dest), quiet=True)
    assert out.read_bytes() == case.new
    assert case.gets == [case.entry['download_url']]
    assert dl.read_sidecar(out)['etag'] == hashlib.sha256(case.new).hexdigest()


@pytest.mark.parametrize('problem', ['old_api', 'old_download_api', 'metadata_error', 'missing_commit', 'missing_etag', 'missing_size', 'pinned_commit_mismatch'])
def test_unverifiable_hub_identity_uses_exact_http_url(case, problem):
    if problem == 'old_api':del case.hub.get_hf_file_metadata
    elif problem == 'old_download_api':
        def old_download(repo_id, filename, repo_type):pytest.fail('An unpinned call must not be used')
        case.hub.hf_hub_download = old_download
    elif problem == 'metadata_error':
        def fail(url):raise requests.Timeout('simulated metadata failure')
        case.hub.get_hf_file_metadata = fail
    elif problem == 'missing_commit':case.metadata.commit_hash = None
    elif problem == 'missing_etag':case.metadata.etag = None
    elif problem == 'missing_size':case.metadata.size = None
    else:case.entry['download_url'] = ROOT + '/resolve/' + 'a' * 40 + '/nested/source.duckdb?download=true'
    out = dl.download('source', path=str(case.dest), quiet=True)
    assert out.read_bytes() == case.new
    assert case.gets == [case.entry['download_url']]
    assert case.calls == []


def test_regular_git_blob_etag_is_verified(case):
    case.metadata.etag = hashlib.sha1(b'blob ' + str(len(case.new)).encode() + b'\0' + case.new).hexdigest()
    out = dl.download('source', path=str(case.dest), quiet=True)
    assert out.read_bytes() == case.new and not case.gets
    assert dl.read_sidecar(out)['etag'] == case.metadata.etag


def test_http_without_validator_does_not_inherit_unrelated_head(case, monkeypatch):
    case.entry.pop('huggingface')
    monkeypatch.setattr(dl.requests, 'get', lambda *a, **kw: Response(case.old))
    out = dl.download('source', path=str(case.dest), quiet=True)
    assert out.read_bytes() == case.old
    assert 'etag' not in dl.read_sidecar(out)
    assert dl._needs_update(case.entry, out) is not None


def test_http_response_identity_needs_no_second_transfer(case, monkeypatch):
    case.entry.pop('huggingface')
    case.identity['etag'] = hashlib.sha256(case.old).hexdigest()
    # Keep the GET identity tied to the returned bytes, despite the changed HEAD.
    def get(url, **kwargs):case.gets.append(url);return Response(case.new, hashlib.sha256(case.new).hexdigest())
    monkeypatch.setattr(dl.requests, 'get', get)
    out = dl.download('source', path=str(case.dest), quiet=True)
    assert case.gets == [case.entry['download_url']]
    assert dl.read_sidecar(out)['etag'] == hashlib.sha256(case.new).hexdigest()


def test_bad_cache_and_failed_fallback_preserve_database_and_sidecar(case, monkeypatch):
    case.dest.write_bytes(case.new)
    dl._write_sidecar(case.dest, 'source', case.entry['download_url'], {'etag': 'preserved'})
    side = dl._sidecar_path(case.dest).read_bytes()
    case.hub.hf_hub_download = lambda **kwargs: case.old_path
    def fail(*a, **kw):raise requests.ConnectionError('simulated interrupted fallback')
    monkeypatch.setattr(dl.requests, 'get', fail)
    with pytest.raises(requests.ConnectionError):dl.download('source', path=str(case.dest), quiet=True)
    assert case.dest.read_bytes() == case.new
    assert dl._sidecar_path(case.dest).read_bytes() == side
    assert not dl._temp_path(case.dest).exists()


@pytest.mark.parametrize('filename', ['nested/a space.duckdb', 'nested/100%.duckdb',
                                     'nested/\u2603.duckdb', 'nested/q?x#y+.duckdb',
                                     'nested/%2F.duckdb'])
@pytest.mark.parametrize('revision', ['a' * 40, 'release/2024'])
def test_real_hub_url_round_trip_preserves_paths(filename, revision):
    hf = pytest.importorskip('huggingface_hub')
    url = hf.hf_hub_url('Example/source', filename, repo_type='dataset', revision=revision)
    assert dl._hf_download_target(url + '?download=true#fragment', 'Example/source') == (filename, revision)


def test_warm_cache_update_detects_same_date_replacement(case, monkeypatch):
    monkeypatch.setattr(dl, 'DATAPOND_DIR', case.dest.parent)
    first_commit = 'a' * 40
    case.metadata.commit_hash = first_commit
    case.metadata.etag = hashlib.sha256(case.old).hexdigest()
    case.metadata.size = len(case.old)
    def hf_download(**kwargs):
        case.calls.append(kwargs)
        return case.old_path if kwargs['revision'] == first_commit else case.new_path
    case.hub.hf_hub_download = hf_download
    monkeypatch.setattr(dl, 'remote_identity', lambda *a, **kw: {'etag': case.metadata.etag, 'size': case.metadata.size})
    out = dl.download('source', quiet=True)
    assert out.read_bytes() == case.old
    # Registry release date and local file size remain unchanged.
    case.metadata.commit_hash = COMMIT
    case.metadata.etag = hashlib.sha256(case.new).hexdigest()
    case.metadata.size = len(case.new)
    assert dl.update('source').read_bytes() == case.new
    assert [call['revision'] for call in case.calls] == [first_commit, COMMIT]
    assert dl.read_sidecar(out)['etag'] == case.metadata.etag
    assert not case.gets


def redirected_response(case, data, *, source_host='huggingface.co'):
    response = Response(data, 'd' * 64)
    response.url = 'https://us.aws.cdn.hf.co/content'
    response.history = [SimpleNamespace(
        url='https://' + source_host + '/datasets/Example/source/resolve/main/nested/source.duckdb',
        headers={'x-linked-etag': '"' + case.metadata.etag + '"',
                 'x-linked-size': str(case.metadata.size), 'x-repo-commit': COMMIT})]
    return response


def test_redirected_head_and_hub_use_same_identity(case, monkeypatch):
    out = dl.download('source', path=str(case.dest), quiet=True)
    monkeypatch.setattr(dl.requests, 'head', lambda *a, **kw: redirected_response(case, case.new))
    monkeypatch.setattr(dl, 'remote_identity', REMOTE_IDENTITY)
    assert dl.remote_identity(case.entry['download_url'])['etag'] == case.metadata.etag
    assert dl._needs_update(case.entry, out) is None


def test_http_uses_and_verifies_its_own_hf_redirect_identity(case, monkeypatch):
    case.entry.pop('huggingface')
    monkeypatch.setattr(dl.requests, 'get', lambda *a, **kw: redirected_response(case, case.new))
    monkeypatch.setattr(dl, 'remote_identity', lambda *a, **kw: pytest.fail('GET must not borrow a HEAD identity'))
    out = dl.download('source', path=str(case.dest), quiet=True)
    assert dl.read_sidecar(out)['etag'] == hashlib.sha256(case.new).hexdigest()
    assert dl.read_sidecar(out)['resolved_revision'] == COMMIT


def test_http_rejects_bytes_that_disagree_with_its_hf_redirect_hash(case, monkeypatch):
    case.entry.pop('huggingface')
    case.dest.write_bytes(case.new)
    dl._write_sidecar(case.dest, 'source', case.entry['download_url'], {'etag': 'preserved'})
    before = dl._sidecar_path(case.dest).read_bytes()
    monkeypatch.setattr(dl.requests, 'get', lambda *a, **kw: redirected_response(case, case.old))
    with pytest.raises(IOError):dl.download('source', path=str(case.dest), quiet=True)
    assert case.dest.read_bytes() == case.new and dl._sidecar_path(case.dest).read_bytes() == before
    assert not dl._temp_path(case.dest).exists()


def test_foreign_redirect_does_not_supply_hf_content_identity(case, monkeypatch):
    case.entry.pop('huggingface')
    monkeypatch.setattr(dl.requests, 'get', lambda *a, **kw: redirected_response(case, case.new, source_host='example.test'))
    out = dl.download('source', path=str(case.dest), quiet=True)
    assert dl.read_sidecar(out)['etag'] == 'd' * 64


def test_http_regular_hf_git_blob_uses_its_content_identity(case, monkeypatch):
    case.entry.pop('huggingface')
    etag = hashlib.sha1(b'blob ' + str(len(case.new)).encode() + b'\0' + case.new).hexdigest()
    response = Response(case.new, etag)
    response.url = case.entry['download_url']
    response.headers['x-repo-commit'] = COMMIT
    monkeypatch.setattr(dl.requests, 'get', lambda *a, **kw: response)
    out = dl.download('source', path=str(case.dest), quiet=True)
    assert dl.read_sidecar(out)['etag'] == etag
    assert dl.read_sidecar(out)['resolved_revision'] == COMMIT
