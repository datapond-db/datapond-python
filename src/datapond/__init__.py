"""
datapond - Public data, instantly queryable.

Instantly connect to curated DuckDB databases built from public data.
"""

__version__ = "0.1.0"

from datapond.registry import list_databases, get_database, get_registry
from datapond.connection import connect
from datapond.download import download, update
from datapond.describe import describe


def list():
    """Return a list of all available database IDs."""
    return list_databases()


def info(db_id: str):
    """Print formatted information about a database."""
    db = get_database(db_id)

    rows = db.get("rows", 0)
    if rows >= 1_000_000:
        rows_str = f"{rows / 1_000_000:.1f}M"
    elif rows >= 1_000:
        rows_str = f"{rows / 1_000:.1f}K"
    else:
        rows_str = str(rows)

    print(f"  {db['name']}")
    print(f"  {rows_str} rows | {db.get('tables', '?')} tables | {db.get('size_gb', '?')} GB")
    print(f"  Source: {db.get('source', 'Unknown')}")
    if db.get("github"):
        print(f"  GitHub: {db['github'].replace('https://', '')}")
    if db.get("huggingface"):
        print(f"  Hugging Face: {db['huggingface'].replace('https://', '')}")
    if db.get("license"):
        print(f"  License: {db['license']}")
    if db.get("updated"):
        print(f"  Updated: {db['updated']}")
