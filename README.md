# csv-to-turso

Import Binance Vision CSV kline files into local Turso/libSQL databases.

The importer is built for large monthly Binance CSV datasets. It processes files in year-month order, supports resumable imports, can recreate databases safely, and can recursively mirror a whole data directory into matching database folders.

## Requirements

- Rust with Cargo
- Binance Vision CSV files named `SYMBOL-INTERVAL-YYYY-MM.csv`
- Optional: `tursodb` for manual verification

## Quick Start

```bash
cargo run --release --
```

Default behavior:

- Input directory: `../data/BTCUSDT/`
- Output database: `../db/BTCUSDT/BTCUSDT.db`
- Interval: `1s`
- Table: `klines`
- Symbol: inferred from CSV filenames

## Import All Symbols

```bash
cargo run --release -- --all
```

With `--all`, the default input root changes to `../data/` and the default output root changes to `../db/`. Every directory containing valid CSV files is imported into its own `{symbol}.db`, and the relative directory structure is mirrored exactly. Use `--jobs N` to process up to N CSV directories in parallel; files inside each directory are still imported sequentially in year-month order.

Example:

```text
../data/BTCUSDT/*.csv        -> ../db/BTCUSDT/BTCUSDT.db
../data/ETHUSDT/*.csv        -> ../db/ETHUSDT/ETHUSDT.db
../data/spot/SOLUSDT/*.csv   -> ../db/spot/SOLUSDT/SOLUSDT.db
```

Directories may contain both CSV files and subdirectories. Subdirectories are processed independently.

## Examples

```bash
# Import BTCUSDT using all defaults
cargo run --release --

# Import only the newest 3 matching files
cargo run --release -- --auto 3

# Import one ETHUSDT directory into a specific DB
cargo run --release -- \
  --dir ../data/ETHUSDT/ \
  --db ../db/ETHUSDT/ETHUSDT.db \
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
| `-o, --db` | `../db/BTCUSDT/BTCUSDT.db` | Output database path, or output root directory with `--all` |
| `-i, --interval` | `1s` | Time interval to import in single-directory mode |
| `-t, --table` | `klines` | SQL table name |
| `-b, --batch-size` | `250000` | Rows per transaction commit |
| `--progress-every` | `1000000` | Print progress every N rows |
| `--has-header` | `false` | CSV files include a header row |
| `--recreate` | `false` | Delete DB/WAL/SHM files and recreate from scratch |
| `--recreate-pragmatic` | `false` | Delete DB files only when this is the only user table; otherwise drop this table, vacuum, and truncate WAL |
| `--import-mode` | `balanced` | Durability mode: `safe`, `balanced`, or `unsafe` |
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
    open_time INTEGER NOT NULL,
    open REAL NOT NULL,
    high REAL NOT NULL,
    low REAL NOT NULL,
    close REAL NOT NULL,
    volume REAL NOT NULL,
    close_time INTEGER NOT NULL,
    quote_asset_volume REAL NOT NULL,
    number_of_trades INTEGER NOT NULL,
    taker_buy_base_asset_volume REAL NOT NULL,
    taker_buy_quote_asset_volume REAL NOT NULL,
    ignore_col TEXT,
    rsi_1 REAL,
    rsi_2 REAL,
    rsi_3 REAL,
    PRIMARY KEY (open_time)
);
```

The standard Binance kline columns are always imported. Extra CSV columns are inferred as optional RSI columns (`rsi_1`, `rsi_2`, and so on).

## Resume And Conflicts

By default, existing rows are skipped:

- The importer reads `MAX(open_time)` from the target table.
- Rows with `open_time <= MAX(open_time)` are skipped.
- Inserts use `INSERT OR IGNORE` unless `--replace-existing` is set.

Use `--replace-existing` when recomputing generated columns or intentionally replacing duplicate primary keys.

## Recreate Modes

```bash
# Delete DB, WAL, and SHM files before importing
cargo run --release -- --recreate

# Preserve other user tables when possible
cargo run --release -- --recreate-pragmatic
```

`--recreate-pragmatic` deletes the DB file family only when the requested table is the only user table. Otherwise it drops the requested table, vacuums the database, and truncates the WAL.

## Import Modes

| Mode | Description |
|------|-------------|
| `safe` | Maximum crash safety, slower |
| `balanced` | Good speed with reasonable durability; default |
| `unsafe` | Fastest, but delete and rebuild the DB if the machine crashes |

For large imports, start with `balanced`.

## Verification

```bash
# List tables
tursodb --readonly --experimental-views ../db/BTCUSDT/BTCUSDT.db '.tables'

# Count rows
tursodb --readonly --experimental-views ../db/BTCUSDT/BTCUSDT.db 'SELECT COUNT(*) FROM klines;'

# Check time range
tursodb --readonly --experimental-views ../db/BTCUSDT/BTCUSDT.db 'SELECT MIN(open_time), MAX(open_time) FROM klines;'
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
