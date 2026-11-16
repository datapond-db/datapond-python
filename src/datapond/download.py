"""
Download management for datapond databases.

Downloads go to a temporary sibling file and replace the destination only after
the transfer completed and the file opens as a DuckDB database, so an existing
good copy is never damaged by a failed or interrupted download. A small sidecar
(``<file>.datapond.json``) records the remote file identity (Hugging Face ETag /
size) so ``update()`` compares versions instead of modification times.
"""

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit

import requests

from datapond.registry import get_database

DATAPOND_DIR = Path.home() / ".datapond"
SIDECAR_SUFFIX = ".datapond.json"


def download(db_id: str, path: str = None, quiet: bool = False) -> Path:
    """Download a database file.

    Args:
        db_id: The database ID to download.
        path: Destination path. Defaults to ~/.datapond/{db_id}.duckdb.
              A path that is an existing directory, ends with a path separator,
              or has no ``.duckdb`` suffix is treated as a directory (created if
              needed) and the file is saved as {path}/{db_id}.duckdb.
        quiet: If True, suppress progress messages.

    Returns:
        The path to the downloaded file.
    """
    db = get_database(db_id)
    dest = resolve_destination(db_id, path)
    dest.parent.mkdir(parents=True, exist_ok=True)

    name = db.get("name", db_id)
    size = db.get("size_gb", "?")
    if not quiet:
        print(f"Downloading {name} ({size} GB)...")

    download_url = db.get("download_url") or db.get("attach_url")
    hf_url = db.get("huggingface")
    if hf_url:
        repo_id = _extract_hf_repo_id(hf_url)
        # Preserve the actual download URL's revision and complete repository path.
        # A different host/repository must use that URL, not the HF default branch.
        target = _hf_download_target(download_url, repo_id) if download_url else (dest.name, "main")
        identity = _try_hf_download(repo_id, target[0], dest, revision=target[1]) if target else None
        if identity:
            _write_sidecar(dest, db_id, download_url, identity)
            if not quiet:
                print(f"Saved to {dest}")
            return dest

    # Fallback: stream via requests from the attach_url or a direct link
    if not download_url:
        raise ValueError(f"No download URL available for '{db_id}'")

    # Only the GET response identifies the bytes actually transferred. A separate
    # HEAD can describe another revision, especially when a mutable branch moves.
    identity = _download_with_requests(download_url, dest, db_id)
    _write_sidecar(dest, db_id, download_url, identity)
    if not quiet:
        print(f"Saved to {dest}")
    return dest


def update(db_id: str) -> Path:
    """Re-download a database if the remote file differs from the local copy.

    The comparison uses the remote file identity (ETag and size from a HEAD
    request) recorded when the local copy was downloaded. Copies without a
    sidecar (downloaded by older versions, or copied in by hand) fall back to
    the registry's ``updated`` date versus the file's modification time and
    are re-downloaded when that comparison is inconclusive.

    Args:
        db_id: The database ID to update.

    Returns:
        The path to the local file.
    """
    db = get_database(db_id, refresh=True)
    local_path = DATAPOND_DIR / f"{db_id}.duckdb"

    if not local_path.exists():
        print(f"No local copy found. Downloading {db_id}...")
        return download(db_id)

    reason = _needs_update(db, local_path)
    if reason is None:
        print(f"{db_id} is already up to date.")
        return local_path

    print(f"Updating {db_id} ({reason})...")
    return download(db_id)


def resolve_destination(db_id: str, path) -> Path:
    """Where ``download`` will write: ``path`` as a file, or ``{path}/{db_id}.duckdb``."""
    filename = f"{db_id}.duckdb"  # local files are always named {db_id}.duckdb so connect(local=True) finds them
    if path is None:
        return DATAPOND_DIR / filename
    raw = str(path)
    dest = Path(raw)
    looks_like_dir = dest.is_dir() or raw.endswith(("/", os.sep)) or dest.suffix.lower() != ".duckdb"
    if looks_like_dir:
        dest.mkdir(parents=True, exist_ok=True)
        dest = dest / filename
    return dest


def remote_identity(url: str, timeout: int = 30) -> dict:
    """ETag, size and Last-Modified of the remote file (HEAD, following redirects). Empty on failure."""
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
    except requests.RequestException:
        return {}
    # The final Xet/CDN ETag can identify transport storage rather than the Hub
    # file. Prefer the content identity supplied by HF's own resolve response.
    linked = _hf_response_identity(r)
    if linked:
        return linked
    h = r.headers
    size = h.get("x-linked-size") or h.get("content-length")
    ident = _identity_from_headers(h)
    if size:
        try:
            ident["size"] = int(size)
        except ValueError:
            pass
    return ident


def _sidecar_path(dest: Path) -> Path:
    return dest.with_name(dest.name + SIDECAR_SUFFIX)


def _write_sidecar(dest: Path, db_id: str, url, identity: dict) -> None:
    data = {
        "db_id": db_id,
        "url": url,
        "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "local_size": dest.stat().st_size,
        **identity,
    }
    try:
        with open(_sidecar_path(dest), "w") as f:
            json.dump(data, f, indent=2)
    except OSError:
        pass


def read_sidecar(dest: Path) -> dict:
    """The identity recorded for a downloaded file, or {} when there is none."""
    try:
        with open(_sidecar_path(dest)) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _needs_update(db: dict, local_path: Path):
    """None when the local copy matches the remote file; otherwise a short reason."""
    url = db.get("download_url") or db.get("attach_url")
    local = read_sidecar(local_path)
    if local.get("local_size") not in (None, local_path.stat().st_size):
        return "local file changed since it was downloaded"
    remote = remote_identity(url) if url else {}
    # Only a strong validator (ETag, else Last-Modified) can declare the copy current;
    # equal sizes are not evidence of the same revision.
    if local.get("etag") and remote.get("etag"):
        return None if local["etag"] == remote["etag"] else "remote file changed"
    if local.get("last_modified") and remote.get("last_modified"):
        return None if local["last_modified"] == remote["last_modified"] else "remote file changed"
    if local and remote:
        return "remote revision cannot be verified (no ETag or Last-Modified)"
    # No identity to compare: fall back to the registry date, and only trust a
    # local copy that is strictly newer than the registry's release date.
    remote_updated = db.get("updated")
    if remote_updated:
        remote_dt = datetime.fromisoformat(remote_updated)
        if remote_dt.tzinfo is None:
            remote_dt = remote_dt.replace(tzinfo=timezone.utc)
        if len(remote_updated) <= 10:  # a date, not a timestamp: the release could be any time that day
            remote_dt = remote_dt.replace(hour=23, minute=59, second=59)
        local_mtime = datetime.fromtimestamp(local_path.stat().st_mtime, tz=timezone.utc)
        if local_mtime > remote_dt:
            return None
        return "registry lists a newer release"
    return "no version information for the local copy"


def _hf_download_target(url: str, repo_id: str):
    """Return (repository path, revision) only for a matching HF resolve URL."""
    parsed = urlsplit(url)
    prefix = f"/datasets/{repo_id}/resolve/"
    if parsed.scheme != "https" or parsed.netloc != "huggingface.co" or not parsed.path.startswith(prefix):
        return None
    revision, separator, filename = parsed.path[len(prefix):].partition("/")
    if not separator or not revision or not filename:
        return None
    return unquote(filename), unquote(revision)


def _extract_hf_repo_id(hf_url: str) -> str:
    """Extract the repo ID from a Hugging Face URL.

    Example: "https://huggingface.co/datasets/Nason/eoir-database"
             -> "Nason/eoir-database"
    """
    parts = hf_url.rstrip("/").split("/")
    # URL format: https://huggingface.co/datasets/{org}/{repo}
    # We want the last two path segments
    return "/".join(parts[-2:])


def _try_hf_download(repo_id: str, filename: str, dest: Path, revision: str = "main"):
    """Return the verified Hub identity on success, or None for HTTP fallback."""
    try:
        from huggingface_hub import get_hf_file_metadata, hf_hub_download, hf_hub_url
    except ImportError:
        return None

    try:
        metadata = get_hf_file_metadata(hf_hub_url(
            repo_id=repo_id, filename=filename, repo_type="dataset", revision=revision,
        ))
        commit = metadata.commit_hash
        etag = metadata.etag
        size = metadata.size
        if not isinstance(commit, str) or not re.fullmatch(r"[0-9a-fA-F]{40}", commit):
            raise IOError("Hub did not resolve an immutable commit")
        if re.fullmatch(r"[0-9a-fA-F]{40}", revision) and revision.lower() != commit.lower():
            raise IOError("Hub metadata does not match the requested commit")
        if not isinstance(etag, str) or not re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", etag):
            raise IOError("Hub did not provide a supported content hash")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise IOError("Hub did not provide a valid file size")
        cached_path = hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            repo_type="dataset",
            revision=commit,
        )
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = _temp_path(dest)
        try:
            _copy_verified_hf_file(cached_path, tmp, etag, size)
            _install(tmp, dest)
        finally:
            _unlink_quietly(tmp)
        return {"etag": etag.lower(), "size": size, "resolved_revision": commit}
    except Exception as e:
        print(f"huggingface_hub download failed ({e}), falling back to requests...")
        return None


def _copy_verified_hf_file(source, dest: Path, etag: str, size: int) -> None:
    """Copy once while verifying HF's SHA-256 (LFS) or Git blob SHA-1 identity."""
    digest = _hf_hasher(etag, size)
    received = 0
    with open(source, "rb") as src, open(dest, "wb") as dst:
        while True:
            chunk = src.read(1 << 20)
            if not chunk:
                break
            if received + len(chunk) > size:
                raise IOError("Hub cached file exceeds its resolved size")
            dst.write(chunk)
            digest.update(chunk)
            received += len(chunk)
    if received != size or digest.hexdigest() != etag.lower():
        raise IOError("Hub cached file does not match its resolved content identity")


def _hf_hasher(etag: str, size: int):
    digest = hashlib.sha256() if len(etag) == 64 else hashlib.sha1()
    if len(etag) == 40:
        digest.update(b"blob " + str(size).encode("ascii") + b"\0")
    return digest


def _verify_hf_file(path: Path, identity: dict) -> None:
    """Verify a completed HTTP transfer against its own HF response identity."""
    if path.stat().st_size != identity["size"]:
        raise IOError("HF response size does not match the transferred file")
    digest = _hf_hasher(identity["etag"], identity["size"])
    with open(path, "rb") as src:
        while True:
            chunk = src.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
    if digest.hexdigest() != identity["etag"]:
        raise IOError("HF response content hash does not match the transferred file")


def _temp_path(dest: Path) -> Path:
    return dest.with_name(dest.name + ".part")


def _install(tmp: Path, dest: Path) -> None:
    """Atomically move a completed, verified download over the destination."""
    _verify_duckdb_file(tmp)
    os.replace(tmp, dest)


def _verify_duckdb_file(path: Path) -> None:
    """Raise if ``path`` is not a readable DuckDB database."""
    import duckdb

    if path.stat().st_size == 0:
        raise IOError("downloaded file is empty")
    con = duckdb.connect(str(path), read_only=True)
    try:
        con.execute("SELECT 1").fetchone()
    finally:
        con.close()


def _unlink_quietly(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def _identity_from_headers(headers) -> dict:
    ident = {}
    etag = headers.get("x-linked-etag") or headers.get("etag")
    if etag:
        ident["etag"] = etag.strip().removeprefix("W/").strip('"')
    if headers.get("last-modified"):
        ident["last_modified"] = headers["last-modified"]
    return ident


def _hf_response_identity(response):
    """Canonical identity from this request's HF resolve response, if present."""
    for part in [*getattr(response, "history", []), response]:
        url = urlsplit(getattr(part, "url", ""))
        if (url.scheme != "https" or url.netloc != "huggingface.co"
                or not re.fullmatch(r"/datasets/[^/]+/[^/]+/resolve/[^/]+/.+", url.path)):
            continue
        headers = part.headers
        if headers.get("x-linked-etag"):
            size = headers.get("x-linked-size")
        elif part is response and headers.get("x-repo-commit"):
            size = headers.get("content-length")
        else:
            continue
        etag = _identity_from_headers(headers).get("etag", "")
        if not re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", etag):
            continue
        try:
            size = int(size)
        except (TypeError, ValueError):
            continue
        if size < 0:
            continue
        identity = {"etag": etag.lower(), "size": size}
        commit = headers.get("x-repo-commit", "")
        if re.fullmatch(r"[0-9a-fA-F]{40}", commit):
            identity["resolved_revision"] = commit
        return identity
    return None


def _download_with_requests(url: str, dest: Path, db_id: str) -> dict:
    """Stream ``url`` into a temporary file next to ``dest`` and install it on success.
    Returns the identity (ETag / Last-Modified) the response reported."""
    tmp = _temp_path(dest)
    try:
        with requests.get(url, stream=True, timeout=30) as resp:
            resp.raise_for_status()
            linked = _hf_response_identity(resp)
            got = linked or _identity_from_headers(resp.headers)
            total = int(resp.headers.get("content-length", 0))
            try:
                from tqdm import tqdm  # noqa: F401
                received = _download_with_tqdm(resp, tmp, total, db_id)
            except ImportError:
                received = _download_with_print(resp, tmp, total, db_id)
        if total and received != total:
            raise IOError(f"incomplete download: {received} of {total} bytes")
        if linked:
            _verify_hf_file(tmp, linked)
        _install(tmp, dest)
        got["size"] = received
        return got
    finally:
        _unlink_quietly(tmp)


def _download_with_tqdm(resp, dest: Path, total: int, db_id: str) -> int:
    """Download with tqdm progress bar. Returns the number of bytes written."""
    from tqdm import tqdm

    received = 0
    with open(dest, "wb") as f:
        with tqdm(
            total=total or None,
            unit="B",
            unit_scale=True,
            desc=db_id,
        ) as bar:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                received += len(chunk)
                bar.update(len(chunk))
    return received


def _download_with_print(resp, dest: Path, total: int, db_id: str) -> int:
    """Download with simple printed progress. Returns the number of bytes written."""
    downloaded = 0
    last_pct = -1

    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1 << 20):
            f.write(chunk)
            downloaded += len(chunk)
            if total > 0:
                pct = int(downloaded * 100 / total)
                if pct != last_pct and pct % 10 == 0:
                    print(f"  {db_id}: {pct}%")
                    last_pct = pct

    size_mb = (total or downloaded) / (1024 * 1024)
    print(f"  {db_id}: complete ({size_mb:.1f} MB)")
    return downloaded
