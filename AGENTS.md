# AGENTS.md - csv-to-duckdb

## Project Overview

- **Language**: Rust (edition 2024)
- **Purpose**: Import Binance Vision CSV files into a local DuckDB database

## Dependencies

```toml
anyhow = "1.0.102"           # Error handling
clap = { version = "4.6.1", features = ["derive"] }  # CLI argument parsing
csv = "1.4.0"                # CSV reading
indicatif = "0.18"            # Progress bar UI
duckdb = { version = "1.10504", features = ["bundled"] }  # DuckDB (columnar DB)
```

## CLI Usage

### Basic Commands

```bash
# Import with default paths (BTCUSDT, 1s, klines, ../db/BTCUSDT/BTCUSDT.duckdb)
cargo run --release -- --dir ../data/BTCUSDT/

# Specify output database
cargo run --release -- --db ../db/BTCUSDT/BTCUSDT.duckdb

# Import all symbols under ../data/ into mirrored DB folders under ../db/
cargo run --release -- --all

# Import up to 4 CSV directories in parallel with --all
cargo run --release -- --all --jobs 4

# Full long-form command
cargo run --release -- \
  --dir ../data/BTCUSDT/ \
  --db ../db/BTCUSDT/BTCUSDT.duckdb \
  --interval 1s \
  --table klines
```

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-d, --dir` | `../data/BTCUSDT/` | Directory containing CSV files |
| `-o, --db` | `../db/BTCUSDT/BTCUSDT.duckdb` | Output DuckDB database path, or output root directory with `--all` |
| `-i, --interval` | `1s` | Time interval (1s, 1m, etc.) |
| `-t, --table` | `klines` | SQL table name |
| `-b, --batch-size` | `250000` | Commit every N rows (only used in safe mode) |
| `--progress-every` | `1000000` | Print progress every N rows |
| `--has-header` | `false` | CSV has header row |
| `--recreate` | `false` | Delete DB file and recreate from scratch |
| `--recreate-pragmatic` | `false` | Delete DB file only when this is the only user table; otherwise drop this table, vacuum, and checkpoint |
| `--import-mode` | `balanced` | Insert strategy: safe (prepared stmts), balanced (Appender+flush), unsafe (Appender) |
| `--replace-existing` | `false` | Replace duplicates vs skip (forces prepared-statement mode) |
| `--skip-order-check` | `false` | Allow non-increasing open_time |
| `--auto` | none | Import only the newest N matching CSV files per job |
| `--all` | `false` | Recursively import every CSV directory; defaults become `--dir ../data/` and `--db ../db/` |
| `--jobs` | `1` | Number of CSV directories to process in parallel with `--all` |

### Import Modes

| Mode | Strategy | Best for |
|------|----------|----------|
| `safe` | Row-by-row prepared statements with periodic `COMMIT` | Maximum crash safety |
| `balanced` (default) | DuckDB Appender with periodic `flush()` calls | Good performance + durability |
| `unsafe` | DuckDB Appender with auto flush only at end | Maximum throughput |

When `--replace-existing` is set, the tool falls back to prepared statements (INSERT OR REPLACE) regardless of import mode, since Appender does not support ON CONFLICT.

### Verification

```bash
# List tables
duckdb ../db/BTCUSDT/BTCUSDT.duckdb '.tables'

# Count rows
duckdb ../db/BTCUSDT/BTCUSDT.duckdb 'SELECT COUNT(*) FROM klines;'

# Example --all output
duckdb ../db/ETHUSDT/ETHUSDT.duckdb 'SELECT COUNT(*) FROM klines;'

# Check time range
duckdb ../db/BTCUSDT/BTCUSDT.duckdb 'SELECT MIN(open_time), MAX(open_time) FROM klines;'
```

## File Naming Convention

CSV files must follow: `SYMBOL-INTERVAL-YYYY-MM.csv`

Examples:
- `BTCUSDT-5m-2024-01.csv`
- `ETHUSDT-1s-2020-08.csv`

## Database Schema

### Table: `klines` (default)

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
    rsi_1 DOUBLE,           -- Optional RSI columns inferred from CSV
    rsi_2 DOUBLE,
    rsi_3 DOUBLE,
    PRIMARY KEY (open_time)
);
```

## Key Implementation Details

### Insert Strategies

1. **Appender path** (balanced/unsafe): Uses DuckDB's native `Appender` API for columnar bulk inserts. Much faster than row-by-row prepared statements. The `--replace-existing` flag forces prepared statements since Appender lacks ON CONFLICT support.
2. **Prepared statement path** (safe): Uses `INSERT OR IGNORE`/`INSERT OR REPLACE` with DuckDB prepared statements. Periodic `BEGIN`/`COMMIT` at `--batch-size` intervals.

### Progress Bar

- Uses `indicatif` crate
- Updates batched every 8192 rows (`PROGRESS_FLUSH_ROWS` constant)
- Total + per-file progress bars via `MultiProgress`

### Resume/Skip Logic

1. **open_time skip**: Rows with `open_time` <= DB max are skipped
2. **Conflict mode**: Uses `INSERT OR IGNORE` by default in safe mode; Appender path filters by resume_open_time
3. **set `--replace-existing`**: Uses `INSERT OR REPLACE` (forces prepared-statement path)

### File Processing

- Single-import files are discovered by interval, with the symbol inferred from CSV filenames
- `--all` recursively discovers every directory with valid CSVs and mirrors its relative path under the output root, creating one `{symbol}.duckdb` database per CSV file group
- `--all` requires each CSV directory to contain one symbol and one interval
- `--jobs` controls how many `--all` CSV directories are imported in parallel using `std::thread`; files inside each directory remain sequential
- Sorted by year-month order
- Two-pass scan: first collects file stats (row count, last open_time), then imports
- Column inference: first row determines RSI column count from extra CSV columns

## Constants

```rust
const BINANCE_COLS: usize = 12;      // Standard Binance kline columns
const PROGRESS_FLUSH_ROWS: u64 = 8192;  // Progress bar update batch size
```

## Important Functions

| Function | Purpose |
|----------|---------|
| `build_import_jobs()` | Build one import job or all mirrored recursive import jobs |
| `create_table()` | Create klines table with inferred RSI columns |
| `import_file_with_appender()` | Import single CSV file using DuckDB Appender API |
| `import_file_with_prepared_stmt()` | Import single CSV file using prepared statements |
| `max_open_time()` | Get max open_time from DB for resume |
| `determine_recreate_action()` | Choose full file deletion vs table-only recreation |
| `remove_database_files()` | Delete DB file for full recreation |
| `apply_import_mode()` | Set synchronous PRAGMA for durability/speed tradeoff |

## Testing

```bash
# Format check
cargo fmt --check

# Build
cargo build --release

# Run CLI help
cargo run --release -- --help

# Quick smoke test (will skip existing data). Do not run against production DBs casually.
cargo run --release -- --table klines

# All-symbol smoke test
cargo run --release -- --all

# Parallel all-symbol smoke test
cargo run --release -- --all --jobs 4
```

## Notes for AI Agents

1. **Never commit secrets**: Don't add API keys, credentials, or secrets to the repo
2. **Preserve terminating newlines**: Ensure `Cargo.toml` and `src/main.rs` end with a newline
3. **DuckDB bundled**: Uses `bundled` feature which compiles DuckDB from source on first build
4. **Large imports**: Appender mode (balanced/unsafe) is significantly faster for 100M+ rows
5. **View inspection**: Use `duckdb` CLI directly.
6. **Sync API**: DuckDB uses a fully synchronous Rust API
7. **Thread safety**: `--jobs N` uses `std::thread` with a shared `ProgressUi` (Arc). ProgressBar handles are cloned, not moved.
