"""
Describe databases, tables, and columns using the _columns data dictionary.
"""

from datapond.connection import connect


def describe(db_id: str, table: str = None, search: str = None):
    """Describe a database's tables and columns.

    Args:
        db_id: The database ID.
        table: If provided, show columns for this specific table.
        search: If provided, search column names across all tables.
    """
    con = connect(db_id, quiet=True)

    if search:
        _search_columns(con, search)
    elif table:
        _describe_table(con, table)
    else:
        _describe_database(con)


def _describe_database(con):
    """Print all tables with row counts."""
    try:
        rows = con.execute(
            "SELECT table_name, row_count, description "
            "FROM _metadata "
            "WHERE table_name NOT IN ('_metadata', '_columns') "
            "ORDER BY table_name"
        ).fetchall()
    except Exception:
        # Fall back to information_schema if _metadata is missing
        rows = con.execute(
            "SELECT table_name, NULL, NULL "
            "FROM information_schema.tables "
            "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') "
            "  AND table_name NOT IN ('_metadata', '_columns') "
            "ORDER BY table_name"
        ).fetchall()

    if not rows:
        print("  No tables found.")
        return

    # Calculate column widths
    name_w = max(len(r[0]) for r in rows)
    name_w = max(name_w, 5)

    for table_name, row_count, description in rows:
        count_str = f"{row_count:>12,}" if row_count else "            "
        desc_str = f"  {description}" if description else ""
        print(f"  {table_name:<{name_w}}  {count_str} rows{desc_str}")


def _describe_table(con, table):
    """Print all columns in a table with types, null%, examples, join hints."""
    try:
        cols = con.execute(
            "SELECT column_name, data_type, null_pct, example_value, join_hint "
            "FROM _columns WHERE table_name = ? ORDER BY rowid",
            [table],
        ).fetchall()
    except Exception:
        # Fall back to information_schema
        cols = con.execute(
            "SELECT column_name, data_type, NULL, NULL, NULL "
            "FROM information_schema.columns "
            "WHERE table_name = ? ORDER BY ordinal_position",
            [table],
        ).fetchall()

    if not cols:
        print(f"  Table '{table}' not found.")
        return

    # Get row count
    try:
        meta = con.execute(
            "SELECT row_count FROM _metadata WHERE table_name = ?", [table]
        ).fetchone()
        if meta and meta[0]:
            print(f"  {table} ({meta[0]:,} rows)")
        else:
            print(f"  {table}")
    except Exception:
        print(f"  {table}")

    print()

    # Calculate column widths
    name_w = max(max(len(c[0]) for c in cols), 6)
    type_w = max(max(len(c[1]) for c in cols), 4)

    header = f"  {'Column':<{name_w}}  {'Type':<{type_w}}  {'Nulls':>6}  {'Example':<40}  Join"
    print(header)
    print(f"  {'-' * name_w}  {'-' * type_w}  {'-' * 6}  {'-' * 40}  {'-' * 4}")

    for col_name, dtype, null_pct, example, join_hint in cols:
        null_str = f"{null_pct:5.1f}%" if null_pct is not None else "      "
        ex_str = (example[:40] if example else "")
        join_str = join_hint if join_hint else ""
        print(f"  {col_name:<{name_w}}  {dtype:<{type_w}}  {null_str}  {ex_str:<40}  {join_str}")


def _search_columns(con, pattern):
    """Search column names across all tables."""
    pattern_upper = pattern.upper()

    try:
        cols = con.execute(
            "SELECT table_name, column_name, data_type, join_hint "
            "FROM _columns "
            "WHERE UPPER(column_name) LIKE '%' || ? || '%' "
            "ORDER BY table_name, column_name",
            [pattern_upper],
        ).fetchall()
    except Exception:
        cols = con.execute(
            "SELECT table_name, column_name, data_type, NULL "
            "FROM information_schema.columns "
            "WHERE table_schema NOT IN ('information_schema', 'pg_catalog') "
            "  AND UPPER(column_name) LIKE '%' || ? || '%' "
            "ORDER BY table_name, column_name",
            [pattern_upper],
        ).fetchall()

    if not cols:
        print(f"  No columns matching '{pattern}'.")
        return

    print(f"  {len(cols)} columns matching '{pattern}':")
    print()

    tbl_w = max(len(c[0]) for c in cols)
    name_w = max(len(c[1]) for c in cols)
    type_w = max(len(c[2]) for c in cols)

    for table_name, col_name, dtype, join_hint in cols:
        join_str = f"  ({join_hint})" if join_hint else ""
        print(f"  {table_name:<{tbl_w}}  {col_name:<{name_w}}  {dtype:<{type_w}}{join_str}")
