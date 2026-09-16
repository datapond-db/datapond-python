# datapond

**Public data, instantly queryable.**

datapond gives you instant SQL access to curated DuckDB databases built from public data sources -- no full download, no API keys, no setup. DuckDB attaches the remote file over HTTP and fetches only the byte ranges your query touches.

## Install

```bash
uv pip install datapond
```

Or with pip: `pip install datapond`

Using R? See [datapond-r](https://github.com/datapond-db/datapond-r) (`pak::pak("datapond-db/datapond-r")`).

For faster downloads from Hugging Face:

```bash
uv pip install "datapond[download]"
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
con.sql("SELECT * FROM proceedings LIMIT 10").show()
```

`connect()` returns a plain [`duckdb.DuckDBPyConnection`](https://duckdb.org/docs/api/python/overview) with the database attached read-only (it takes a second or two to attach). Use it exactly as you use DuckDB: pandas and Polars conversions, `con.register()`, and referencing a DataFrame by name in SQL all work.

```python
df = con.sql("SELECT * FROM proceedings LIMIT 1000").df()  # pandas
pl = con.sql("SELECT * FROM proceedings LIMIT 1000").pl()   # polars
```

### Explore the schema

Every database ships with a data dictionary (`_metadata` and `_columns` tables). `describe()` reads it:

```python
datapond.describe("eoir")                        # tables with row counts and descriptions
datapond.describe("eoir", table="proceedings")   # columns, types, null %, examples, join hints
datapond.describe("eoir", search="judge")        # find columns by name across all tables
```

### Remote vs local

Every database can be queried remotely in seconds with no download required, or downloaded locally for full speed.

```python
# Remote -- attaches over HTTP; only the bytes your query needs are transferred
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

`download()` streams to a temporary file and replaces the destination only after the transfer is complete and the file opens as a DuckDB database, so a failed download never damages an existing copy. It records the remote file's identity (Hugging Face ETag and size) in `<file>.datapond.json`; `update()` re-fetches the registry, compares that identity with the remote file, and re-downloads only when it differs. A `--path` that ends in a separator or has no `.duckdb` suffix is treated as a directory and created.

## Multi-database queries

Attach multiple databases at once and query across them. Tables are namespaced by
database ID; IDs that contain a hyphen must be double-quoted in SQL:

```python
con = datapond.connect(["cms-medicare", "openpayments"])

con.sql('SELECT * FROM "cms-medicare".physician_summary LIMIT 5').show()
con.sql("SELECT * FROM openpayments.general_payments LIMIT 5").show()
```

Both of those databases key providers by NPI (`"cms-medicare".physician_summary.Rndrng_NPI`
and `openpayments.general_payments.covered_recipient_npi`), so they can be joined
directly. Remote joins across large tables transfer a lot of data -- download both
first (`datapond.download(...)`, then `connect([...], local=True)`) for anything heavier
than a quick look.

## CLI

datapond also includes a command-line interface:

```bash
# List available databases
datapond list

# Show database details
datapond info eoir

# Download a database
datapond download eoir --path ./data/

# Re-download if the file on Hugging Face has changed
datapond update eoir

# Describe tables and columns
datapond describe eoir
datapond describe eoir --table proceedings
datapond describe eoir --search judge

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

Remote mode does transfer data: DuckDB issues HTTP range requests for the metadata and
row groups a query touches, so `SELECT COUNT(*)` on a 37 GB table is cheap but `SELECT *`
is not. What it avoids is the *full* download. `datapond.download()` fetches the whole
file once so that every later query runs at disk speed.

## Links

- [Website](https://datapond-db.github.io/website)
- [Registry](https://github.com/datapond-db/registry) -- catalog of available databases
- [Source](https://github.com/datapond-db/datapond-python)
- [R package](https://github.com/datapond-db/datapond-r)

## Contributing

Contributions are welcome. To add a new database to datapond, submit a pull request to the [registry](https://github.com/datapond-db/registry) repository.

## Credits

datapond is built and maintained by [Ian Nason](https://github.com/ian-nason): the registry, this client, the website, and seven of the databases. The IPEDS database is built and maintained by [Paul Goldsmith-Pinkham](https://github.com/paulgp). See the registry's [contributors section](https://github.com/datapond-db/registry#contributors) for the per-database breakdown.

## License

MIT
