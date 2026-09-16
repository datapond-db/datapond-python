"""
Download management for datapond databases.

Downloads go to a temporary sibling file and replace the destination only after
the transfer completed and the file opens as a DuckDB database, so an existing
good copy is never damaged by a failed or interrupted download. A small sidecar
(``<file>.datapond.json``) records the remote file identity (Hugging Face ETag /
size) so ``update()`` compares versions instead of modification times.
"""

import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

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
    identity = remote_identity(download_url) if download_url else {}

    hf_url = db.get("huggingface")
    if hf_url:
        repo_id = _extract_hf_repo_id(hf_url)
        # The file on Hugging Face is not always named {db_id}.duckdb
        # (e.g. cms-medicare -> cms_medicare.duckdb), so take the name from attach_url.
        hf_filename = _hf_filename(db, dest.name)
        if _try_hf_download(repo_id, hf_filename, dest):
            _write_sidecar(dest, db_id, download_url, identity)
            if not quiet:
                print(f"Saved to {dest}")
            return dest

    # Fallback: stream via requests from the attach_url or a direct link
    if not download_url:
        raise ValueError(f"No download URL available for '{db_id}'")

    _download_with_requests(download_url, dest, db_id)
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
    h = r.headers
    etag = h.get("x-linked-etag") or h.get("etag")
    size = h.get("x-linked-size") or h.get("content-length")
    ident = {}
    if etag:
        ident["etag"] = etag.strip('"').replace("W/", "")
    if size:
        try:
            ident["size"] = int(size)
        except ValueError:
            pass
    if h.get("last-modified"):
        ident["last_modified"] = h["last-modified"]
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
    if local.get("etag") and remote.get("etag"):
        return None if local["etag"] == remote["etag"] else "remote file changed"
    if local.get("size") and remote.get("size"):
        return None if local["size"] == remote["size"] else "remote size changed"
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


def _hf_filename(db: dict, default: str) -> str:
    """Return the filename of the .duckdb inside the HF repo (last segment of attach_url)."""
    attach_url = db.get("attach_url") or ""
    name = attach_url.rstrip("/").rsplit("/", 1)[-1]
    return name if name.endswith(".duckdb") else default


def _extract_hf_repo_id(hf_url: str) -> str:
    """Extract the repo ID from a Hugging Face URL.

    Example: "https://huggingface.co/datasets/Nason/eoir-database"
             -> "Nason/eoir-database"
    """
    parts = hf_url.rstrip("/").split("/")
    # URL format: https://huggingface.co/datasets/{org}/{repo}
    # We want the last two path segments
    return "/".join(parts[-2:])


def _try_hf_download(repo_id: str, filename: str, dest: Path) -> bool:
    """Try downloading via huggingface_hub. Returns True on success."""
    try:
        from huggingface_hub import hf_hub_download
    except ImportError:
        return False

    try:
        cached_path = hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            repo_type="dataset",
        )
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = _temp_path(dest)
        try:
            shutil.copy2(cached_path, tmp)
            _install(tmp, dest)
        finally:
            _unlink_quietly(tmp)
        return True
    except Exception as e:
        print(f"huggingface_hub download failed ({e}), falling back to requests...")
        return False


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


def _download_with_requests(url: str, dest: Path, db_id: str):
    """Stream ``url`` into a temporary file next to ``dest`` and install it on success."""
    tmp = _temp_path(dest)
    try:
        with requests.get(url, stream=True, timeout=30) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            try:
                from tqdm import tqdm  # noqa: F401
                received = _download_with_tqdm(resp, tmp, total, db_id)
            except ImportError:
                received = _download_with_print(resp, tmp, total, db_id)
        if total and received != total:
            raise IOError(f"incomplete download: {received} of {total} bytes")
        _install(tmp, dest)
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
