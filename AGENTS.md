# AGENTS.md

## Project overview

`csv-to-duckdb` is a library crate for importing one Binance Vision CSV folder into one DuckDB database. Its public API is in `src/lib.rs`; this package has no CLI executable.

## Public API

- `ImportOptions` configures one folder import.
- `import_folder` imports or updates one database and calls the callback once per input CSV imported or skipped.

## Behavior

- Files are imported in chronological order.
- File inspection is parallel; writes to a DuckDB database remain sequential.
- Existing rows are preserved except overlapping rows in the first file extending the database range.
- Selected dropped columns are also removed from existing tables.
- Keep source CSVs unchanged.

## Validation

- `cargo fmt --check`
- `cargo build --release`
- `cargo test` when the user asks for tests or verification
