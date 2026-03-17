"""
Download management for datapond databases.

Supports downloading via huggingface_hub (preferred) or requests (fallback).
"""

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import requests

from datapond.registry import get_database

DATAPOND_DIR = Path.home() / ".datapond"


def download(db_id: str, path: str = None, quiet: bool = False) -> Path:
    """Download a database file.

    Args:
        db_id: The database ID to download.
        path: Destination path. Defaults to ~/.datapond/{db_id}.duckdb.
              If path is a directory, saves as {path}/{db_id}.duckdb.
        quiet: If True, suppress progress messages.

    Returns:
        The path to the downloaded file.
    """
    db = get_database(db_id)
    filename = f"{db_id}.duckdb"

    if path is None:
        dest = DATAPOND_DIR / filename
    else:
        dest = Path(path)
        if dest.is_dir():
            dest = dest / filename

    dest.parent.mkdir(parents=True, exist_ok=True)

    name = db.get("name", db_id)
    size = db.get("size_gb", "?")
    if not quiet:
        print(f"Downloading {name} ({size} GB)...")

    hf_url = db.get("huggingface")
    if hf_url:
        repo_id = _extract_hf_repo_id(hf_url)
        if _try_hf_download(repo_id, filename, dest):
            if not quiet:
                print(f"Saved to {dest}")
            return dest

    # Fallback: download via requests from the attach_url or a direct link
    download_url = db.get("download_url") or db.get("attach_url")
    if not download_url:
        raise ValueError(f"No download URL available for '{db_id}'")

    _download_with_requests(download_url, dest, db_id)
    if not quiet:
        print(f"Saved to {dest}")
    return dest


def update(db_id: str) -> Path:
    """Re-download a database if the remote version is newer.

    Args:
        db_id: The database ID to update.

    Returns:
        The path to the local file.
    """
    db = get_database(db_id)
    local_path = DATAPOND_DIR / f"{db_id}.duckdb"

    if not local_path.exists():
        print(f"No local copy found. Downloading {db_id}...")
        return download(db_id)

    remote_updated = db.get("updated")
    if remote_updated:
        remote_dt = datetime.fromisoformat(remote_updated)
        if remote_dt.tzinfo is None:
            remote_dt = remote_dt.replace(tzinfo=timezone.utc)
        local_mtime = datetime.fromtimestamp(
            local_path.stat().st_mtime, tz=timezone.utc
        )
        if local_mtime >= remote_dt:
            print(f"{db_id} is already up to date.")
            return local_path

    print(f"Updating {db_id}...")
    return download(db_id)


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
        shutil.copy2(cached_path, dest)
        return True
    except Exception as e:
        print(f"huggingface_hub download failed ({e}), falling back to requests...")
        return False


def _download_with_requests(url: str, dest: Path, db_id: str):
    """Download a file using requests, with a progress indicator."""
    resp = requests.get(url, stream=True, timeout=30)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))

    # Try to use tqdm for a nice progress bar
    try:
        from tqdm import tqdm

        _download_with_tqdm(resp, dest, total, db_id)
    except ImportError:
        _download_with_print(resp, dest, total, db_id)


def _download_with_tqdm(resp, dest: Path, total: int, db_id: str):
    """Download with tqdm progress bar."""
    from tqdm import tqdm

    with open(dest, "wb") as f:
        with tqdm(
            total=total or None,
            unit="B",
            unit_scale=True,
            desc=db_id,
        ) as bar:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))


def _download_with_print(resp, dest: Path, total: int, db_id: str):
    """Download with simple printed progress."""
    downloaded = 0
    last_pct = -1

    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total > 0:
                pct = int(downloaded * 100 / total)
                if pct != last_pct and pct % 10 == 0:
                    print(f"  {db_id}: {pct}%")
                    last_pct = pct

    if total > 0:
        size_mb = total / (1024 * 1024)
        print(f"  {db_id}: complete ({size_mb:.1f} MB)")
    else:
        size_mb = downloaded / (1024 * 1024)
        print(f"  {db_id}: complete ({size_mb:.1f} MB)")
