# csv-to-duckdb

Library for importing one directory of Binance Vision kline CSVs into one DuckDB database.

## Use

Add the crate to your Rust project:

```toml
csv-to-duckdb = "1.1.2"
```

Import a folder with `ImportOptions`. The callback runs once for each file imported or skipped:

```rust,ignore
let mut options = csv_to_duckdb::ImportOptions::new(
    "data/BTCUSDT",
    "db/BTCUSDT.duckdb",
    "1s",
);
options.jobs = 4;
options.ignore_extra_columns = true;
csv_to_duckdb::import_folder(options, || {})?;
```

Files must be named `SYMBOL-INTERVAL-YYYY-MM.csv`, contain one symbol and interval per folder, and be ordered by month. CSV rows must be ordered by `open_time`. The importer uses the `klines` table with `open_time` as its primary key. The `ignore` field maps to the database column `ignore_col`; `open_time` and `close` are required. Set `ignore_extra_columns` to discard fields beyond the 12 Binance columns.

When updating an existing database, files ending at or before its maximum timestamp are skipped. The first remaining file replaces overlapping rows and inserts new ones; later files append. This assumes ordered files and does not detect corrections or gaps in skipped history. Selected dropped columns are also removed from the existing table; restoring them requires rebuilding from retained CSVs.
