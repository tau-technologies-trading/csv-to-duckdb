# csv-to-duckdb

Import Binance Vision CSV kline files into local DuckDB databases.

The importer is built for large monthly Binance CSV datasets. It processes files in year-month order, supports resumable imports, can recreate databases safely, and can recursively mirror a whole data directory into matching database folders.

## Requirements

- Rust with Cargo
- Binance Vision CSV files named `SYMBOL-INTERVAL-YYYY-MM.csv`
- Optional: `duckdb` CLI for manual verification

## Quick Start

```bash
cargo run --release -- --auto 3
```

Default behavior:

- Input directory: `../data/BTCUSDT/`
- Output database: `../db/BTCUSDT/BTCUSDT.duckdb`
- Interval: `1s`
- Table: `klines`
- Symbol: inferred from CSV filenames

## Import All Symbols

```bash
cargo run --release -- --all
```

With `--all`, the default input root changes to `../data/` and the default output root changes to `../db/`. Every directory containing a valid CSV file group is imported into its own `{symbol}.duckdb` database, and the relative directory structure is mirrored exactly. Use `--jobs N` to process up to N CSV directories in parallel; files inside each directory are still imported sequentially in year-month order.

Example:

```text
../data/BTCUSDT/*.csv        -> ../db/BTCUSDT/BTCUSDT.duckdb
../data/ETHUSDT/*.csv        -> ../db/ETHUSDT/ETHUSDT.duckdb
../data/spot/SOLUSDT/*.csv   -> ../db/spot/SOLUSDT/SOLUSDT.duckdb
```

Directories may contain both CSV files and subdirectories. Subdirectories are processed independently.

## Examples

```bash
# Import BTCUSDT using default paths
cargo run --release -- --dir ../data/BTCUSDT/

# Import only the newest 3 matching files
cargo run --release -- --auto 3

# Import one ETHUSDT directory into a specific DB
cargo run --release -- \
  --dir ../data/ETHUSDT/ \
  --db ../db/ETHUSDT/ETHUSDT.duckdb \
  --interval 1m

# Recreate the default BTCUSDT database from scratch
cargo run --release -- --recreate

# Import every CSV directory under ../data/ into mirrored folders under ../db/
cargo run --release -- --all

# Import up to 4 CSV directories in parallel
cargo run --release -- --all --jobs 4

# Use explicit roots for all-symbol import
cargo run --release -- --all --dir ../data/ --db ../db/
```

## CLI Options

| Flag | Default | Description |
|------|---------|-------------|
| `-d, --dir` | `../data/BTCUSDT/` | Directory containing CSV files, or input root with `--all` |
| `-o, --db` | `../db/BTCUSDT/BTCUSDT.duckdb` | Output database path, or output root directory with `--all` |
| `-i, --interval` | `1s` | Time interval to import in single-directory mode |
| `-t, --table` | `klines` | SQL table name |
| `-b, --batch-size` | `250000` | Commit every N rows (only used in safe mode) |
| `--progress-every` | `1000000` | Print progress every N rows |
| `--has-header` | `false` | CSV files include a header row |
| `--recreate` | `false` | Delete DB file and recreate from scratch |
| `--recreate-pragmatic` | `false` | Delete DB file only when this is the only user table; otherwise drop this table, vacuum, and checkpoint |
| `--import-mode` | `balanced` | Insert strategy: `safe`, `balanced`, or `unsafe` |
| `--replace-existing` | `false` | Replace duplicate primary keys instead of ignoring them |
| `--skip-order-check` | `false` | Allow non-increasing `open_time` values |
| `--auto` | none | Import only the newest N matching CSV files per job |
| `--all` | `false` | Recursively import every CSV directory and mirror the directory structure |
| `--jobs` | `1` | Number of CSV directories to process in parallel with `--all` |

## CSV Naming

CSV files must follow this pattern:

```text
SYMBOL-INTERVAL-YYYY-MM.csv
```

Examples:

- `BTCUSDT-1s-2024-01.csv`
- `ETHUSDT-5m-2023-12.csv`

In single-directory mode, the symbol is inferred from filenames and `--interval` selects which interval to import. If the selected interval has multiple symbols in the same directory, the import fails instead of guessing.

In `--all` mode, each CSV directory must contain one symbol and one interval. Mixed-symbol or mixed-interval directories fail with a clear error.

## Database Schema

The default table is `klines`:

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
    rsi_3 DOUBLE,
    PRIMARY KEY (open_time)
);
```

The standard Binance kline columns are always imported. Extra CSV columns are inferred as optional RSI columns (`rsi_1`, `rsi_2`, and so on).

## Resume And Conflicts

By default, existing rows are skipped:

- The importer reads `MAX(open_time)` from the target table.
- Rows with `open_time <= MAX(open_time)` are skipped.
- Safe-mode inserts use `INSERT OR IGNORE` unless `--replace-existing` is set; Appender modes skip rows by `open_time`.

Use `--replace-existing` when recomputing generated columns or intentionally replacing duplicate primary keys.

## Recreate Modes

```bash
# Delete the DB file before importing
cargo run --release -- --recreate

# Preserve other user tables when possible
cargo run --release -- --recreate-pragmatic
```

`--recreate-pragmatic` deletes the DB file only when the requested table is the only user table. Otherwise it drops the requested table, vacuums the database, and checkpoints it.

## Import Modes

| Mode | Description |
|------|-------------|
| `safe` | Prepared statements with periodic commits; maximum crash safety, slower |
| `balanced` | DuckDB Appender with periodic flushes; default |
| `unsafe` | DuckDB Appender with minimal flushing; fastest, but rebuild the DB if interrupted |

For large imports, start with `balanced`.

## Verification

```bash
# List tables
duckdb ../db/BTCUSDT/BTCUSDT.duckdb '.tables'

# Count rows
duckdb ../db/BTCUSDT/BTCUSDT.duckdb 'SELECT COUNT(*) FROM klines;'

# Check time range
duckdb ../db/BTCUSDT/BTCUSDT.duckdb 'SELECT MIN(open_time), MAX(open_time) FROM klines;'
```

## Development

```bash
# Format check
cargo fmt --check

# Build
cargo build --release

# Run CLI help
cargo run --release -- --help
```
