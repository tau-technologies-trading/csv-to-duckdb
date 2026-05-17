# AGENTS.md - csv-to-turso

## Project Overview

- **Name**: csv-to-turso
- **Version**: 0.3.2
- **Language**: Rust (edition 2024)
- **Purpose**: Import Binance Vision CSV files into a local Turso (libSQL) database
- **Repository**: `/home/nikolai/Coding/Quant/csv-to-turso`

## Dependencies

```toml
anyhow = "1.0.102"           # Error handling
clap = { version = "4.6.1", features = ["derive"] }  # CLI argument parsing
csv = "1.4.0"                # CSV reading
indicatif = "0.17"            # Progress bar UI
tokio = { version = "1.52.3", features = ["full"] } # Async runtime
turso = "0.5.3"              # Local Turso/SQLite database
```

## CLI Usage

### Basic Commands

```bash
# Import with all defaults (SOLUSDT, 1s, klines, market_data.turso)
cargo run --release -- --dir ../data

# Specify output database
cargo run --release -- --dir ../data --db ../db/solusdt.db

# Full long-form command
cargo run --release -- \
  --dir ../data \
  --db ../db/solusdt.db \
  --symbol SOLUSDT \
  --interval 1s \
  --table klines
```

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-d, --dir` | `.` | Directory containing CSV files |
| `-o, --db` | `market_data.turso` | Output Turso database path |
| `-s, --symbol` | `SOLUSDT` | Trading symbol to import |
| `-i, --interval` | `1s` | Time interval (1s, 1m, etc.) |
| `-t, --table` | `klines` | SQL table name |
| `-b, --batch-size` | `250000` | Rows per transaction commit |
| `--progress-every` | `1000000` | Print progress every N rows |
| `--has-header` | `false` | CSV has header row |
| `--recreate` | `false` | Delete DB/WAL/SHM files and recreate from scratch |
| `--recreate-pragmatic` | `false` | Delete DB file family only when this is the only user table; otherwise drop this table, vacuum, and truncate WAL |
| `--import-mode` | `balanced` | Durability mode: safe/balanced/unsafe |
| `--replace-existing` | `false` | Replace duplicates vs skip |
| `--skip-order-check` | `false` | Allow non-increasing open_time |

### Verification

```bash
# List tables
tursodb --readonly --experimental-views ../db/solusdt.db '.tables'

# Count rows
tursodb --readonly --experimental-views ../db/solusdt.db 'SELECT COUNT(*) FROM klines;'
```

## File Naming Convention

CSV files must follow: `SYMBOL-INTERVAL-YYYY-MM.csv`

Examples:
- `SOLUSDT-1s-2020-08.csv`
- `BTCUSDT-5m-2024-01.csv`

## Database Schema

### Table: `klines` (default)

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
    rsi_1 REAL,           -- Optional RSI columns inferred from CSV
    rsi_2 REAL,
    rsi_3 REAL,
    PRIMARY KEY (open_time)
);
```

## Key Implementation Details

### Turso Compatibility Quirks

1. **PRAGMA returns rows**: `PRAGMA journal_mode = WAL` returns a row, must use `conn.query()` not `conn.execute()`.
2. **No WITHOUT ROWID**: Turso 0.5.3 does not support `WITHOUT ROWID` tables - creates standard tables only.
3. **WAL checkpoint returns rows**: `PRAGMA wal_checkpoint(TRUNCATE)` returns rows, so consume it with `query()`.

### Progress Bar

- Uses `indicatif` crate
- Updates batched every 8192 rows (`PROGRESS_FLUSH_ROWS` constant)
- Total + per-file progress bars via `MultiProgress`

### Resume/Skip Logic

1. **open_time skip**: Rows with `open_time` <= DB max are skipped
2. **Conflict mode**: Uses `INSERT OR IGNORE` by default; set `--replace-existing` for `INSERT OR REPLACE`

### File Processing

- Files discovered via `find_files()` matching naming pattern
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
| `find_files()` | Discover CSV files matching pattern |
| `create_table()` | Create klines table with inferred RSI columns |
| `import_file()` | Import single CSV file with batching |
| `max_open_time()` | Get max open_time from DB for resume |
| `determine_recreate_action()` | Choose full file deletion vs table-only pragmatic recreation |
| `remove_database_files()` | Delete DB, WAL, and SHM files for full recreation |
| `apply_journal_mode()` | Set WAL mode (query-based workaround) |
| `apply_wal_checkpoint_truncate()` | Flush DB changes and truncate WAL with row-consuming PRAGMA handling |

## Testing

```bash
# Format check
cargo fmt --check

# Build
cargo build --release

# Run CLI help
cargo run --release -- --help

# Quick smoke test (will skip existing data). Do not run against production DBs casually.
cargo run --release -- --dir ../data --db ../db/solusdt.db --table klines
```

## Notes for AI Agents

1. **Never commit secrets**: Don't add API keys, credentials, or secrets to the repo
2. **Preserve terminating newlines**: Ensure `Cargo.toml` and `src/main.rs` end with a newline
3. **Turso 0.5.3 limitations**: No encryption, no WITHOUT ROWID, experimental features need flags
4. **Large imports**: For 100M+ rows, use `--import-mode balanced` (default) - safe but fast
5. **View inspection**: Always use `--experimental-views` flag with tursodb when querying views
