# AGENTS.md - csv-to-duckdb

## Project Overview

- **Language**: Rust (edition 2024)
- **Purpose**: Import Binance Vision CSV files into local DuckDB databases

## Dependencies

```toml
anyhow = "1.0.102"           # Error handling
clap = { version = "4.6.1", features = ["derive"] }  # CLI argument parsing
csv = "1.4.0"                # CSV reading
duckdb = { version = "1.10504", features = ["bundled"] }  # DuckDB (columnar DB)
indicatif = "0.18"            # Progress bar UI
```

## CLI Usage

### Basic Commands

```bash
# Import one symbol directory into one row-oriented DB
cargo run --release -- --dir ../data/BTCUSDT/

# Import all symbols into one wide, time-aligned DB plus metadata DB
cargo run --release -- --one-file

# Import all symbols into mirrored per-symbol DB files
cargo run --release -- --multiple-files

# Import up to 4 CSV directories in parallel with --multiple-files
cargo run --release -- --multiple-files --jobs 4

# Full single-symbol command
cargo run --release -- \
  --dir ../data/BTCUSDT/ \
  --db ../db/BTCUSDT/BTCUSDT.duckdb \
  --interval 1s \
  --table klines
```

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-d, --dir` | `../data/BTCUSDT/` | Directory containing CSV files; with recursive modes defaults to `../data/` |
| `-o, --db` | `../db/BTCUSDT/BTCUSDT.duckdb` | Output DB path; with `--multiple-files` this is an output root directory |
| `-i, --interval` | `1s` | Time interval for single-directory mode |
| `-t, --table` | `klines` | SQL table name |
| `-b, --batch-size` | `250000` | Commit every N rows in safe mode; flush every N rows in balanced Appender mode |
| `--progress-every` | `1000000` | Print progress every N rows |
| `--has-header` | `false` | CSV has header row |
| `--recreate` | `false` | Delete DB file and recreate from scratch |
| `--recreate-pragmatic` | `false` | Delete DB file only when this is the only user table; otherwise drop this table, vacuum, and checkpoint |
| `--import-mode` | `balanced` | Insert strategy: safe (prepared stmts), balanced (Appender+flush), unsafe (Appender) |
| `--replace-existing` | `false` | Replace duplicates vs skip; not supported with `--one-file` |
| `--skip-order-check` | `false` | Allow non-increasing open_time |
| `--auto` | none | Import only the newest N matching CSV files per job/symbol |
| `--one-file` | `false` | Recursively import every symbol into one wide time-aligned DuckDB file |
| `--multiple-files` | `false` | Recursively import every symbol into mirrored per-symbol DuckDB files |
| `--jobs` | `1` | Number of CSV directories to process in parallel with `--multiple-files`; clamped to 1 with `--one-file` |

`--all` is a hidden compatibility alias for `--one-file`.

### Import Modes

| Mode | Strategy | Best for |
|------|----------|----------|
| `safe` | Row-by-row prepared statements with periodic `COMMIT` | Maximum crash safety |
| `balanced` (default) | DuckDB Appender with periodic `flush()` calls | Good performance + durability |
| `unsafe` | DuckDB Appender with auto flush only at end | Maximum throughput |

When `--replace-existing` is set, row-oriented imports fall back to prepared statements since Appender does not support ON CONFLICT.

### Verification

```bash
# Per-symbol DB count
duckdb ../db/BTCUSDT/BTCUSDT.duckdb 'SELECT COUNT(*) FROM klines;'

# One-file wide DB table list
duckdb ../db/klines.duckdb '.tables'

# One-file metadata
duckdb ../db/klines_metadata.duckdb 'SELECT * FROM symbol_columns;'

# Check time range in a row-oriented DB
duckdb ../db/BTCUSDT/BTCUSDT.duckdb 'SELECT MIN(open_time), MAX(open_time) FROM klines;'
```

## File Naming Convention

CSV files must follow: `SYMBOL-INTERVAL-YYYY-MM.csv`

Examples:
- `BTCUSDT-5m-2024-01.csv`
- `ETHUSDT-1s-2020-08.csv`

## Database Schemas

### Row-Oriented `klines` Table

Used by single-directory mode and `--multiple-files`.

```sql
CREATE TABLE klines (
    open_time BIGINT NOT NULL,
    open DOUBLE NOT NULL,
    high DOUBLE NOT NULL,
    low DOUBLE NOT NULL,
    close DOUBLE NOT NULL,
    volume DOUBLE NOT NULL,
    close_time BIGINT NOT NULL,
    quote_asset_volume DOUBLE NOT NULL,
    number_of_trades BIGINT NOT NULL,
    taker_buy_base_asset_volume DOUBLE NOT NULL,
    taker_buy_quote_asset_volume DOUBLE NOT NULL,
    ignore_col VARCHAR,
    rsi_1 DOUBLE,
    rsi_2 DOUBLE,
    PRIMARY KEY (open_time)
);
```

### One-File Wide `klines` Table

Used by `--one-file`. The main table has one shared `open_time` column, then one block of data columns per symbol. Missing data for a symbol at a given `open_time` remains `NULL`.

```sql
CREATE TABLE klines (
    open_time BIGINT PRIMARY KEY,
    BTCUSDT_open DOUBLE,
    BTCUSDT_high DOUBLE,
    ...,
    ETHUSDT_open DOUBLE,
    ETHUSDT_high DOUBLE,
    ...
);
```

### One-File Metadata DB

The metadata DB path is derived from the main DB path: `klines.duckdb` -> `klines_metadata.duckdb`.

```sql
CREATE TABLE symbol_columns (
    currency VARCHAR NOT NULL PRIMARY KEY,
    start_column INTEGER NOT NULL,
    end_column INTEGER NOT NULL,
    first_open_time BIGINT NOT NULL
);
```

`start_column` and `end_column` are actual 1-indexed column numbers in the main wide table. `open_time` is column 1, so the first symbol block starts at column 2.

## Key Implementation Details

### Insert Strategies

1. **Appender path** (balanced/unsafe): Uses DuckDB's native `Appender` API for columnar bulk inserts. Much faster than row-by-row prepared statements.
2. **Prepared statement path** (safe): Uses `INSERT OR IGNORE`/`INSERT OR REPLACE` with DuckDB prepared statements. Periodic `BEGIN`/`COMMIT` at `--batch-size` intervals.
3. **One-file path**: Stages one symbol at a time into `__csv_to_duckdb_staging`, adds a new column block to the wide table, inserts missing `open_time` rows, updates that symbol's columns by `open_time`, then writes metadata.

### One-File Alignment

- Symbols are processed alphabetically.
- Files inside each symbol are processed by year/month order.
- `open_time` is the alignment key.
- If a newly added symbol has data before the current first `open_time`, those earlier `open_time` rows are inserted and all existing symbol columns are naturally `NULL` there.
- If an existing `open_time` has no row for the new symbol, that symbol's columns remain `NULL`.
- `--one-file` clamps `--jobs` to 1 because a single DuckDB database has one writer.

### Resume/Skip Logic

1. **Row-oriented imports**: Rows with `open_time` <= DB max are skipped unless `--replace-existing` is set.
2. **One-file imports**: Symbols already present in metadata are skipped. Use `--recreate` to rebuild from scratch.

### File Processing

- Single-import files are discovered by interval, with the symbol inferred from CSV filenames.
- `--multiple-files` recursively discovers every directory with valid CSVs and mirrors its relative path under the output root.
- `--one-file` recursively discovers every directory with valid CSVs, requires one interval across all symbols, and imports symbols alphabetically into one wide DB.
- `--multiple-files` requires each CSV directory to contain one symbol and one interval.
- `--jobs` controls how many `--multiple-files` CSV directories are imported in parallel using `std::thread`; files inside each directory remain sequential.
- Two-pass scan: first collects file stats (row count, last open_time), then imports.
- Column inference: first row determines RSI column count from extra CSV columns.

## Constants

```rust
const BINANCE_COLS: usize = 12;
const PROGRESS_FLUSH_ROWS: u64 = 8192;
const DEFAULT_ONE_FILE_DB: &str = "../db/klines.duckdb";
const METADATA_TABLE: &str = "symbol_columns";
```

## Important Functions

| Function | Purpose |
|----------|---------|
| `run_one_file_import()` | Import recursive symbols into one wide DB and metadata DB |
| `merge_staging_into_wide_table()` | Add symbol columns, align by open_time, and update wide rows |
| `build_import_jobs()` | Build one import job or recursive mirrored jobs |
| `create_table()` | Create row-oriented klines table with inferred RSI columns |
| `create_wide_table()` | Create one-file wide table with shared open_time |
| `import_file_with_appender()` | Import single CSV file using DuckDB Appender API |
| `import_file_with_prepared_stmt()` | Import single CSV file using prepared statements |
| `determine_recreate_action()` | Choose full file deletion vs table-only recreation |

## Testing

```bash
# Format check
cargo fmt --check

# Build
cargo build --release

# Run CLI help
cargo run --release -- --help

# One-file smoke test
cargo run --release -- --one-file --recreate

# Multiple-file smoke test
cargo run --release -- --multiple-files

# Parallel multiple-file smoke test
cargo run --release -- --multiple-files --jobs 4
```

## Notes for AI Agents

1. **Never commit secrets**: Don't add API keys, credentials, or secrets to the repo
2. **Preserve terminating newlines**: Ensure `Cargo.toml` and `src/main.rs` end with a newline
3. **DuckDB bundled**: Uses `bundled` feature which compiles DuckDB from source on first build
4. **Large imports**: Appender mode (balanced/unsafe) is significantly faster for 100M+ rows
5. **View inspection**: Use `duckdb` CLI directly.
6. **Sync API**: DuckDB uses a fully synchronous Rust API
7. **Thread safety**: `--multiple-files --jobs N` uses `std::thread` with a shared `ProgressUi` (Arc). `--one-file` is single-writer and clamps jobs to 1.
