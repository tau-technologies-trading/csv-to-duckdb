# csv-to-duckdb

Import one directory of Binance Vision kline CSVs into one DuckDB database. CSV files are selected by interval and processed in year-month order. `--jobs` controls how many files are inspected concurrently; database writes remain ordered.

```sh
cargo run --release -- --dir ../data/BTCUSDT --db ../db/BTCUSDT/BTCUSDT.duckdb --interval 1s --jobs 4
```

Files must be named `SYMBOL-INTERVAL-YYYY-MM.csv`, and the selected interval must contain only one symbol. Rows and files must be in strictly increasing timestamp order. The default table is `klines`, with `open_time` as its primary key. `--drop-columns` accepts a comma-separated list of Binance column names to omit; its default is `ignore` (the database column `ignore_col`). Pass `--drop-columns ''` to retain all columns. `open_time` and `close` are required for the pipeline.

On an existing database, the importer reads the last timestamp of each file and skips files ending at or before the database maximum. If the first remaining file starts at or before that maximum, it replaces its overlapping rows and inserts its new rows; later files are imported in order. This shortcut assumes ordered files and does not detect corrections or gaps in skipped history. Use `--replace-existing` to reimport all selected files. Replacement clears existing RSI values on affected rows when importing raw data; run `add-rsi` afterward.

The selected dropped columns are also removed from existing tables. Restoring a previously removed column requires rebuilding the table from the retained CSVs. Only select columns you no longer need in the database.

Use `--ignore-extra-columns` to ignore CSV fields beyond the 12 Binance columns, accepting mixed row widths. Use `--has-header` for headered CSVs and `--auto N` to select only the newest N files.

`--report-file-progress` prints `@@CSV_FILE_IMPORTED@@` after each file is imported or skipped. `--import-mode safe`, `balanced`, and `unsafe` choose the write strategy; `balanced` is the default. Run `--help` for the remaining options. The old multi-folder and wide-database modes have been removed.

## Checks

```sh
cargo fmt --check
cargo test
```
