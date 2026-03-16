"""
Command-line interface for datapond.
"""

import argparse
import shutil
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(
        prog="datapond",
        description="Public data, instantly queryable.",
    )
    subparsers = parser.add_subparsers(dest="command")

    # datapond list
    subparsers.add_parser("list", help="List all available databases")

    # datapond info <db_id>
    info_parser = subparsers.add_parser("info", help="Show database details")
    info_parser.add_argument("db_id", help="Database ID")

    # datapond download <db_id> [--path PATH]
    dl_parser = subparsers.add_parser("download", help="Download a database")
    dl_parser.add_argument("db_id", help="Database ID")
    dl_parser.add_argument("--path", default=None, help="Destination path")

    # datapond describe <db_id> [--table TABLE] [--search PATTERN]
    desc_parser = subparsers.add_parser(
        "describe", help="Describe tables, columns, and join keys"
    )
    desc_parser.add_argument("db_id", help="Database ID")
    desc_parser.add_argument("--table", default=None, help="Show columns for a specific table")
    desc_parser.add_argument("--search", default=None, help="Search column names")

    # datapond connect <db_id>
    connect_parser = subparsers.add_parser(
        "connect", help="Open an interactive session with a database"
    )
    connect_parser.add_argument("db_id", help="Database ID")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == "list":
        _cmd_list()
    elif args.command == "info":
        _cmd_info(args.db_id)
    elif args.command == "download":
        _cmd_download(args.db_id, args.path)
    elif args.command == "describe":
        _cmd_describe(args.db_id, args.table, args.search)
    elif args.command == "connect":
        _cmd_connect(args.db_id)


def _cmd_list():
    from datapond.registry import list_databases

    databases = list_databases()
    if not databases:
        print("No databases found in registry.")
        return
    for db_id in databases:
        print(db_id)


def _cmd_info(db_id):
    import datapond

    datapond.info(db_id)


def _cmd_download(db_id, path):
    from datapond.download import download

    try:
        download(db_id, path=path)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _cmd_describe(db_id, table, search):
    from datapond.describe import describe

    try:
        describe(db_id, table=table, search=search)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def _cmd_connect(db_id):
    from datapond.registry import get_database

    db = get_database(db_id)
    attach_url = db["attach_url"]

    # Try the duckdb CLI first
    duckdb_bin = shutil.which("duckdb")
    if duckdb_bin:
        init_sql = (
            f"INSTALL httpfs; LOAD httpfs; "
            f"ATTACH '{attach_url}' AS {db_id} (READ_ONLY); "
            f"USE {db_id};"
        )
        try:
            subprocess.run([duckdb_bin, "-cmd", init_sql], check=True)
        except subprocess.CalledProcessError as e:
            sys.exit(e.returncode)
        return

    # Fallback: Python REPL
    print(f"duckdb CLI not found. Starting Python REPL for {db_id}...")
    print("Type SQL queries, or 'exit' to quit.\n")

    from datapond.connection import connect

    con = connect(db_id)

    while True:
        try:
            query = input(f"{db_id}> ")
        except (EOFError, KeyboardInterrupt):
            print()
            break

        query = query.strip()
        if not query:
            continue
        if query.lower() in ("exit", "quit", ".exit", ".quit"):
            break

        try:
            result = con.execute(query)
            rows = result.fetchall()
            if rows:
                columns = [desc[0] for desc in result.description]
                col_widths = [len(c) for c in columns]
                for row in rows:
                    for i, val in enumerate(row):
                        col_widths[i] = max(col_widths[i], len(str(val)))

                header = " | ".join(
                    c.ljust(col_widths[i]) for i, c in enumerate(columns)
                )
                print(header)
                print("-+-".join("-" * w for w in col_widths))
                for row in rows:
                    line = " | ".join(
                        str(v).ljust(col_widths[i]) for i, v in enumerate(row)
                    )
                    print(line)
                print(f"({len(rows)} rows)")
            else:
                print("OK")
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
