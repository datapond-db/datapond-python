# datapond

**Public data, instantly queryable.**

datapond gives you SQL access to curated DuckDB databases built from public data sources. After installing the package, you can query a remote file without first downloading it in full or obtaining an API key. DuckDB fetches byte ranges over HTTP and runs the query on your machine.

## Install

```bash
uv pip install datapond
```

Or with pip: `pip install datapond`

For an existing environment, use `python -m pip install --upgrade datapond` with
the interpreter that runs your queries. Version 0.1.1 cannot attach IDs such as
`fjc-judges`; identifier quoting was fixed in 0.1.2. Check the actual import with
`python -c "import datapond; print(datapond.__version__, datapond.__file__)"`.
Current source already includes that fix; changing SQL quoting in your query
alone does not repair the older client's internally generated ATTACH statement.

Using R? Install the released [CRAN package](https://cran.r-project.org/package=datapond) with `install.packages("datapond")`. The [R source documentation](https://github.com/datapond-db/datapond-r) distinguishes CRAN 0.1.0 from development 0.1.1.

An optional Hugging Face download backend is available:

```bash
uv pip install "datapond[download]"
```

In version 0.1.5, this optional backend follows the Hugging Face repository's default branch even when the registry URL names an immutable revision. For a reproducible local copy, use the immutable URL and SHA-256 on the dataset page. Installing the extra is not required for remote queries.

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

`connect()` returns a plain [`duckdb.DuckDBPyConnection`](https://duckdb.org/docs/api/python/overview) with the database attached read-only. Attachment time depends on the network and file. Use it exactly as you use DuckDB: pandas and Polars conversions, `con.register()`, and referencing a DataFrame by name in SQL all work.

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

Remote mode transfers the metadata and data ranges needed by each query. Local mode downloads the whole file first, then reads it from disk. Neither mode guarantees an execution time; joins, aggregations and result size also matter.

```python
# Remote -- attaches over HTTP; query shape determines the transferred ranges
con = datapond.connect("eoir")
con.sql("SELECT * FROM proceedings LIMIT 5").show()

# Local -- download once, then read from disk
datapond.download("eoir")
con = datapond.connect("eoir", local=True)
```

### Bound memory before a query

The Python client returns a DuckDB connection. Set limits after connecting and before running a query:

```python
import datapond
con = datapond.connect("onet")
con.execute("SET memory_limit='128MiB'; SET threads=1; SET max_temp_directory_size='0B'")
con.sql("SELECT onet_soc_code, title FROM occupation_data ORDER BY onet_soc_code LIMIT 5").show()
con.close()
```

These settings take effect after attachment. To configure DuckDB before attachment, create a `duckdb.connect(config=...)` connection and use a dataset page's direct-URL example. The buffer-pool limit is not a cap on the entire Python process or a collected DataFrame. With spill disabled, larger queries can fail at the limit. Filter and select needed columns; `LIMIT` after a join or aggregate does not guarantee a small scan.

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

`download()` streams to a temporary file and replaces the destination only after the transfer is complete and the file opens as a DuckDB database, so a failed download never damages an existing copy. It records the remote file's identity (Hugging Face ETag and size) in `<file>.datapond.json`; `update()` re-fetches the registry, compares that identity with the remote file, and re-downloads only when it differs. A `path=` argument (CLI `--path`) that ends in a separator or has no `.duckdb` suffix is treated as a directory and created.

## Multi-database queries

Attach multiple databases at once and query across them. Tables are namespaced by
database ID; IDs that contain a hyphen must be double-quoted in SQL:

```python
con = datapond.connect(["cms-medicare", "openpayments"])

con.sql('SELECT * FROM "cms-medicare".physician_summary LIMIT 5').show()
con.sql("SELECT * FROM openpayments.general_payments LIMIT 5").show()
```

Both of those databases key providers by NPI (`"cms-medicare".physician_summary.Rndrng_NPI`
and `openpayments.general_payments.covered_recipient_npi`). Before joining, choose the time period and unit of analysis and aggregate each side to the intended unique provider key. Joining multiple rows per NPI on both sides multiplies records. Remote joins across large tables transfer a lot of data -- download both
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

Remote mode does transfer data: DuckDB issues HTTP range requests for metadata and row groups. A simple base-table count may use metadata, while filtered counts, views and joins can scan substantial data. Even a small result can require a large transfer. `datapond.download()` fetches the whole file for later disk reads; allow space for the new file, temporary download bytes and any existing copy. File size is not a RAM estimate.

## Links

- [Website](https://datapond-db.github.io/website)
- [Registry](https://github.com/datapond-db/registry) -- catalog of available databases
- [Source](https://github.com/datapond-db/datapond-python)
- [R package](https://github.com/datapond-db/datapond-r)

## Contributing

Contributions are welcome. To add a new database to datapond, submit a pull request to the [registry](https://github.com/datapond-db/registry) repository.

## Credits

datapond is built and maintained by [Ian Nason](https://github.com/ian-nason): the registry, this client, the website, and the databases attributed to him in the registry. The IPEDS database is built and maintained by [Paul Goldsmith-Pinkham](https://github.com/paulgp). See the registry's [contributors section](https://github.com/datapond-db/registry#contributors) for the per-database breakdown.

## License

MIT
