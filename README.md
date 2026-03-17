# datapond

**Public data, instantly queryable.**

datapond gives you instant SQL access to curated DuckDB databases built from public data sources -- no downloads, no API keys, no setup.

## Install

```bash
pip install datapond
```

For faster downloads from Hugging Face:

```bash
pip install datapond[download]
```

## Quick start

### Browse available databases

```python
import datapond

# See what's available
datapond.list()

# Get details about a specific database
datapond.info("eoir")
```

### Connect and query

```python
import datapond

con = datapond.connect("eoir")
con.sql("SHOW TABLES").show()
con.sql("SELECT * FROM cases LIMIT 10").show()
```

The connection is a standard [duckdb.Connection](https://duckdb.org/docs/api/python/overview) -- use it however you normally use DuckDB, including with pandas and Polars.

```python
df = con.sql("SELECT * FROM cases LIMIT 1000").df()  # pandas
pl = con.sql("SELECT * FROM cases LIMIT 1000").pl()   # polars
```

### Remote vs local

Every database can be queried remotely in seconds with no download required, or downloaded locally for full speed.

```python
# Remote -- streams over HTTP, no download needed
con = datapond.connect("eoir")
con.sql("SELECT * FROM proceedings LIMIT 5").show()

# Local -- download once, query at full disk speed
datapond.download("eoir")
con = datapond.connect("eoir", local=True)
```

### Download for offline use

```python
datapond.download("eoir")

# Later, connect locally
con = datapond.connect("eoir", local=True)
```

### Update a local database

```python
datapond.update("eoir")
```

## Multi-database queries

Attach multiple databases at once and query across them:

```python
con = datapond.connect(["eoir", "foia"])

# Tables are namespaced by database ID
con.sql("SELECT * FROM eoir.cases LIMIT 5").show()
con.sql("SELECT * FROM foia.requests LIMIT 5").show()
```

## CLI

datapond also includes a command-line interface:

```bash
# List available databases
datapond list

# Show database details
datapond info eoir

# Download a database
datapond download eoir --path ./data/

# Open an interactive SQL session
datapond connect eoir
```

## How it works

datapond connects to read-only DuckDB files hosted remotely via the [httpfs extension](https://duckdb.org/docs/extensions/httpfs/overview). The [registry](https://github.com/datapond-db/registry) maintains a catalog of available databases with their URLs and metadata.

When you call `datapond.connect()`, it:
1. Looks up the database in the registry
2. Installs and loads the httpfs extension
3. Attaches the remote DuckDB file as read-only
4. Returns a connection ready for queries

No data is downloaded unless you explicitly call `datapond.download()`.

## Links

- [Website](https://datapond-db.github.io/website)
- [Registry](https://github.com/datapond-db/registry) -- catalog of available databases
- [Source](https://github.com/datapond-db/datapond-python)

## Contributing

Contributions are welcome. To add a new database to datapond, submit a pull request to the [registry](https://github.com/datapond-db/registry) repository.

## License

MIT
