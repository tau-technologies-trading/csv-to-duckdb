# AGENTS.md - csv-to-turso

## Project Overview

- **Name**: csv-to-turso
- **Version**: 0.3.0
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
# Import with all defaults (SOLUSDT, 1s, klines_1s, market_data.turso)
cargo run --release -- --dir ../data

# Specify output database
cargo run --release -- --dir ../data --db ../db/solusdt.db

# Full long-form command
cargo run --release -- \
  --dir ../data \
  --db ../db/solusdt.db \
  --symbol SOLUSDT \
  --interval 1s \
  --table klines_1s
```

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-d, --dir` | `.` | Directory containing CSV files |
| `-o, --db` | `market_data.turso` | Output Turso database path |
| `-s, --symbol` | `SOLUSDT` | Trading symbol to import |
| `-i, --interval` | `1s` | Time interval (1s, 1m, etc.) |
| `-t, --table` | `klines_1s` | SQL table name |
| `-b, --batch-size` | `250000` | Rows per transaction commit |
| `--progress-every` | `1000000` | Print progress every N rows |
| `--has-header` | `false` | CSV has header row |
| `--recreate` | `false` | Drop and recreate table/views |
| `--import-mode` | `balanced` | Durability mode: safe/balanced/unsafe |
| `--replace-existing` | `false` | Replace duplicates vs skip |
| `--skip-order-check` | `false` | Allow non-increasing open_time |

### Verification

```bash
# List tables
tursodb --readonly --experimental-views ../db/solusdt.db '.tables'

# Count rows
tursodb --readonly --experimental-views ../db/solusdt.db 'SELECT COUNT(*) FROM klines_1s;'

# List monthly views
tursodb --readonly --experimental-views ../db/solusdt.db \
  "SELECT name FROM sqlite_master WHERE type = 'view';"
```

## File Naming Convention

CSV files must follow: `SYMBOL-INTERVAL-YYYY-MM.csv`

Examples:
- `SOLUSDT-1s-2020-08.csv`
- `BTCUSDT-5m-2024-01.csv`

## Database Schema

### Table: `klines_1s` (default)

```sql
CREATE TABLE klines_1s (
    symbol TEXT NOT NULL,
    interval TEXT NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
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
    PRIMARY KEY (symbol, interval, open_time)
);
```

### Monthly Views

One view per CSV file is created automatically:
- Naming: `{table}_{symbol}_{interval}_{YYYY}_{MM}`
- Example: `klines_1s_solusdt_1s_2020_08`

Views filter by symbol, interval, year, and month for easy querying.

## Key Implementation Details

### Turso Compatibility Quirks

1. **PRAGMA returns rows**: `PRAGMA journal_mode = WAL` returns a row, must use `conn.query()` not `conn.execute()`.
2. **No WITHOUT ROWID**: Turso 0.5.3 does not support `WITHOUT ROWID` tables - creates standard tables only.
3. **Experimental views**: Must enable with `.experimental_materialized_views(true)` in Builder.

### Progress Bar

- Uses `indicatif` crate
- Updates batched every 8192 rows (`PROGRESS_FLUSH_ROWS` constant)
- Total + per-file progress bars via `MultiProgress`

### Resume/Skip Logic

1. **View-based skip**: If monthly view exists and `--recreate` not set, skip entire file
2. **open_time skip**: If file's last `open_time` <= DB max, skip already-imported rows
3. **Conflict mode**: Uses `INSERT OR IGNORE` by default; set `--replace-existing` for `INSERT OR REPLACE`

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
| `collect_file_stats()` | Count rows and find last open_time per file |
| `create_table()` | Create klines table with inferred RSI columns |
| `import_file()` | Import single CSV file with batching |
| `create_monthly_view()` | Create per-month filtering view |
| `monthly_view_exists()` | Check if view exists for skip logic |
| `max_open_time()` | Get max open_time from DB for resume |
| `apply_journal_mode()` | Set WAL mode (query-based workaround) |
| `sql_string_literal()` | Escape single quotes in SQL strings |
| `ident_fragment()` | Sanitize identifiers to ASCII lowercase + underscore |

## Testing

```bash
# Format check
cargo fmt --check

# Build
cargo build --release

# Run CLI help
cargo run --release -- --help

# Quick smoke test (will skip existing data)
cargo run --release -- --dir ../data --db ../db/solusdt.db --table klines_1s
```

## Notes for AI Agents

1. **Never commit secrets**: Don't add API keys, credentials, or secrets to the repo
2. **Preserve terminating newlines**: Ensure `Cargo.toml` and `src/main.rs` end with a newline
3. **Turso 0.5.3 limitations**: No encryption, no WITHOUT ROWID, experimental features need flags
4. **Large imports**: For 100M+ rows, use `--import-mode balanced` (default) - safe but fast
5. **View inspection**: Always use `--experimental-views` flag with tursodb when querying views
