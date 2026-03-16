"""
Registry client for datapond.

Fetches and caches the database registry from GitHub.
"""

import json
import os
import time
from pathlib import Path

import requests

REGISTRY_URL = (
    "https://raw.githubusercontent.com/datapond-db/registry/main/registry.json"
)
CACHE_DIR = Path.home() / ".datapond"
CACHE_FILE = CACHE_DIR / "registry.json"
CACHE_TTL = 3600  # 1 hour in seconds


def _cache_is_fresh() -> bool:
    """Check if the local registry cache exists and is less than 1 hour old."""
    if not CACHE_FILE.exists():
        return False
    age = time.time() - CACHE_FILE.stat().st_mtime
    return age < CACHE_TTL


def _fetch_registry() -> dict:
    """Fetch the registry from GitHub and cache it locally."""
    try:
        resp = requests.get(REGISTRY_URL, timeout=15)
        resp.raise_for_status()
    except requests.RequestException as e:
        # If we have a stale cache, use it rather than failing
        if CACHE_FILE.exists():
            with open(CACHE_FILE, "r") as f:
                return json.load(f)
        raise ConnectionError(
            f"Failed to fetch registry and no local cache available: {e}"
        ) from e

    data = resp.json()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(CACHE_FILE, "w") as f:
        json.dump(data, f, indent=2)

    return data


def get_registry() -> dict:
    """Return the parsed registry dict, using cache if fresh."""
    if _cache_is_fresh():
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return _fetch_registry()


def get_database(db_id: str) -> dict:
    """Return a single database entry from the registry.

    Raises ValueError if the database ID is not found.
    """
    registry = get_registry()
    databases = registry.get("databases", [])
    for db in databases:
        if db.get("id") == db_id:
            return db
    available = [db.get("id") for db in databases]
    raise ValueError(
        f"Database '{db_id}' not found in registry. "
        f"Available databases: {', '.join(available)}"
    )


def list_databases() -> list:
    """Return a list of all database IDs in the registry."""
    registry = get_registry()
    return [db["id"] for db in registry.get("databases", [])]
