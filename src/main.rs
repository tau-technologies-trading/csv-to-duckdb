use anyhow::{bail, Context, Result};
use clap::{Parser, ValueEnum};
use csv::{ReaderBuilder, StringRecord};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;
use turso::{params_from_iter, Builder, Value};

const BINANCE_COLS: usize = 12;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ImportMode {
    /// Maximum crash safety. Slower.
    Safe,

    /// Good import speed with reasonable durability. Recommended default.
    Balanced,

    /// Fastest import mode. Delete and rebuild the DB if the machine crashes.
    Unsafe,
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Directory containing files like SOLUSDT-1s-2026-04.csv
    #[arg(short, long, default_value = ".")]
    dir: PathBuf,

    /// Output Turso database path
    #[arg(short = 'o', long, default_value = "market_data.turso")]
    db: String,

    /// Symbol to import
    #[arg(short, long, default_value = "SOLUSDT")]
    symbol: String,

    /// Interval per row
    #[arg(short, long, default_value = "1s")]
    interval: String,

    /// SQL table name
    #[arg(short, long, default_value = "candles_1s")]
    table: String,

    /// Commit every N rows
    #[arg(short, long, default_value_t = 250_000)]
    batch_size: usize,

    /// Print progress every N rows
    #[arg(long, default_value_t = 1_000_000)]
    progress_every: usize,

    /// CSV has a header row
    #[arg(long, default_value_t = false)]
    has_header: bool,

    /// Drop and recreate the table before importing
    #[arg(long, default_value_t = false)]
    recreate: bool,

    /// Import durability/speed mode
    #[arg(long, value_enum, default_value = "balanced")]
    import_mode: ImportMode,

    /// Replace existing rows instead of skipping duplicates. Use only when recomputing columns.
    #[arg(long, default_value_t = false)]
    replace_existing: bool,

    /// Use a normal rowid table instead of WITHOUT ROWID
    #[arg(long, default_value_t = false)]
    rowid_table: bool,

    /// Do not fail if CSV open_time values are not strictly increasing
    #[arg(long, default_value_t = false)]
    skip_order_check: bool,
}

#[derive(Debug, Clone)]
struct CsvFile {
    path: PathBuf,
    year: i32,
    month: u32,
}

struct ParsedRow {
    open_time: i64,
    values: Vec<Value>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    validate_args(&args)?;
    validate_ident(&args.table)?;

    let mut files = find_files(&args.dir, &args.symbol, &args.interval)?;
    files.sort_by_key(|f| (f.year, f.month));

    if files.is_empty() {
        bail!(
            "No files found matching {}-{}-YYYY-MM.csv in {}",
            args.symbol,
            args.interval,
            args.dir.display()
        );
    }

    warn_missing_months(&files);

    let rsi_count = infer_max_rsi_columns(&files, args.has_header)?;
    println!("Found {} file(s). Max RSI columns: {}", files.len(), rsi_count);
    println!(
        "Import mode: {:?}. Batch size: {}. Conflict mode: {}.",
        args.import_mode,
        args.batch_size,
        if args.replace_existing { "REPLACE" } else { "IGNORE" }
    );

    let db = Builder::new_local(&args.db).build().await?;
    let conn = db.connect()?;

    apply_import_mode(&conn, args.import_mode).await?;

    if args.recreate {
        conn.execute(&format!("DROP TABLE IF EXISTS {}", args.table), ())
            .await?;
    }

    create_table(&conn, &args.table, rsi_count, !args.rowid_table).await?;

    let insert_sql = build_insert_sql(&args.table, rsi_count, args.replace_existing);
    let mut stmt = conn.prepare(&insert_sql).await?;

    let start = Instant::now();
    let mut total_rows = 0usize;
    let mut last_open_time: Option<i64> = None;

    conn.execute("BEGIN", ()).await?;

    for file in files {
        let imported = import_file(
            &file,
            &args,
            rsi_count,
            &mut stmt,
            &conn,
            &mut total_rows,
            &mut last_open_time,
            start,
        )
        .await?;

        println!(
            "Imported {:>12} rows from {}",
            imported,
            file.path.file_name().unwrap().to_string_lossy()
        );
    }

    conn.execute("COMMIT", ()).await?;

    println!(
        "Done. Imported {} rows into {} in {:.1}s",
        total_rows,
        args.db,
        start.elapsed().as_secs_f64()
    );

    Ok(())
}

fn validate_args(args: &Args) -> Result<()> {
    if args.batch_size == 0 {
        bail!("--batch-size must be greater than 0");
    }

    if args.progress_every == 0 {
        bail!("--progress-every must be greater than 0");
    }

    Ok(())
}

async fn apply_import_mode(conn: &turso::Connection, mode: ImportMode) -> Result<()> {
    match mode {
        ImportMode::Safe => {
            conn.execute("PRAGMA journal_mode = WAL", ()).await?;
            conn.execute("PRAGMA synchronous = FULL", ()).await?;
        }
        ImportMode::Balanced => {
            conn.execute("PRAGMA journal_mode = WAL", ()).await?;
            conn.execute("PRAGMA synchronous = NORMAL", ()).await?;
        }
        ImportMode::Unsafe => {
            conn.execute("PRAGMA journal_mode = WAL", ()).await?;
            conn.execute("PRAGMA synchronous = OFF", ()).await?;
        }
    }

    Ok(())
}

fn find_files(dir: &Path, symbol: &str, interval: &str) -> Result<Vec<CsvFile>> {
    let mut files = Vec::new();

    for entry in fs::read_dir(dir).with_context(|| format!("Cannot read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();

        if !path.is_file() {
            continue;
        }

        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };

        let Some(parsed) = parse_filename(file_name) else {
            continue;
        };

        if parsed.0 == symbol && parsed.1 == interval {
            files.push(CsvFile {
                path,
                year: parsed.2,
                month: parsed.3,
            });
        }
    }

    Ok(files)
}

fn parse_filename(file_name: &str) -> Option<(String, String, i32, u32)> {
    let stem = file_name.strip_suffix(".csv")?;
    let parts: Vec<&str> = stem.rsplitn(4, '-').collect();

    if parts.len() != 4 {
        return None;
    }

    let month = parts[0].parse::<u32>().ok()?;
    let year = parts[1].parse::<i32>().ok()?;
    let interval = parts[2].to_string();
    let symbol = parts[3].to_string();

    if !(1..=12).contains(&month) {
        return None;
    }

    Some((symbol, interval, year, month))
}

fn warn_missing_months(files: &[CsvFile]) {
    if files.len() < 2 {
        return;
    }

    for pair in files.windows(2) {
        let prev = &pair[0];
        let next = &pair[1];
        let mut expected_year = prev.year;
        let mut expected_month = prev.month + 1;

        if expected_month == 13 {
            expected_month = 1;
            expected_year += 1;
        }

        if next.year != expected_year || next.month != expected_month {
            eprintln!(
                "Warning: missing month(s) between {:04}-{:02} and {:04}-{:02}",
                prev.year, prev.month, next.year, next.month
            );
        }
    }
}

fn infer_max_rsi_columns(files: &[CsvFile], has_header: bool) -> Result<usize> {
    let mut max_rsi = 0usize;

    for file in files {
        let mut reader = ReaderBuilder::new()
            .has_headers(has_header)
            .from_path(&file.path)?;

        let mut record = StringRecord::new();

        while reader.read_record(&mut record)? {
            if record.is_empty() {
                continue;
            }

            if record.len() < BINANCE_COLS {
                bail!(
                    "{} has {} columns, expected at least {}",
                    file.path.display(),
                    record.len(),
                    BINANCE_COLS
                );
            }

            max_rsi = max_rsi.max(record.len() - BINANCE_COLS);
            break;
        }
    }

    Ok(max_rsi)
}

async fn create_table(
    conn: &turso::Connection,
    table: &str,
    rsi_count: usize,
    without_rowid: bool,
) -> Result<()> {
    let mut cols = vec![
        "symbol TEXT NOT NULL".to_string(),
        "interval TEXT NOT NULL".to_string(),
        "year INTEGER NOT NULL".to_string(),
        "month INTEGER NOT NULL".to_string(),
        "open_time INTEGER NOT NULL".to_string(),
        "open REAL NOT NULL".to_string(),
        "high REAL NOT NULL".to_string(),
        "low REAL NOT NULL".to_string(),
        "close REAL NOT NULL".to_string(),
        "volume REAL NOT NULL".to_string(),
        "close_time INTEGER NOT NULL".to_string(),
        "quote_asset_volume REAL NOT NULL".to_string(),
        "number_of_trades INTEGER NOT NULL".to_string(),
        "taker_buy_base_asset_volume REAL NOT NULL".to_string(),
        "taker_buy_quote_asset_volume REAL NOT NULL".to_string(),
        "ignore_col TEXT".to_string(),
    ];

    for i in 1..=rsi_count {
        cols.push(format!("rsi_{} REAL", i));
    }

    cols.push("PRIMARY KEY (symbol, interval, open_time)".to_string());

    let suffix = if without_rowid { " WITHOUT ROWID" } else { "" };
    let sql = format!(
        "CREATE TABLE IF NOT EXISTS {} ({}){}",
        table,
        cols.join(", "),
        suffix
    );

    conn.execute(&sql, ()).await?;
    Ok(())
}

fn build_insert_sql(table: &str, rsi_count: usize, replace_existing: bool) -> String {
    let mut cols = vec![
        "symbol",
        "interval",
        "year",
        "month",
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
        "ignore_col",
    ]
    .into_iter()
    .map(String::from)
    .collect::<Vec<_>>();

    for i in 1..=rsi_count {
        cols.push(format!("rsi_{}", i));
    }

    let placeholders = (1..=cols.len())
        .map(|i| format!("?{}", i))
        .collect::<Vec<_>>();

    let conflict_action = if replace_existing { "REPLACE" } else { "IGNORE" };

    format!(
        "INSERT OR {} INTO {} ({}) VALUES ({})",
        conflict_action,
        table,
        cols.join(", "),
        placeholders.join(", ")
    )
}

async fn import_file(
    file: &CsvFile,
    args: &Args,
    rsi_count: usize,
    stmt: &mut turso::Statement,
    conn: &turso::Connection,
    total_rows: &mut usize,
    last_open_time: &mut Option<i64>,
    start: Instant,
) -> Result<usize> {
    let mut reader = ReaderBuilder::new()
        .has_headers(args.has_header)
        .from_path(&file.path)
        .with_context(|| format!("Cannot open {}", file.path.display()))?;

    let mut record = StringRecord::new();
    let mut file_rows = 0usize;

    while reader.read_record(&mut record)? {
        if record.is_empty() {
            continue;
        }

        let row = record_to_row(
            &record,
            &args.symbol,
            &args.interval,
            file.year,
            file.month,
            rsi_count,
        )
        .with_context(|| format!("Bad row in {}", file.path.display()))?;

        if !args.skip_order_check {
            if let Some(prev_open_time) = *last_open_time {
                if row.open_time <= prev_open_time {
                    bail!(
                        "open_time is not strictly increasing: previous={}, current={} in {}",
                        prev_open_time,
                        row.open_time,
                        file.path.display()
                    );
                }
            }
        }

        stmt.execute(params_from_iter(row.values)).await?;
        stmt.reset()?;

        *last_open_time = Some(row.open_time);
        file_rows += 1;
        *total_rows += 1;

        if *total_rows % args.batch_size == 0 {
            conn.execute("COMMIT", ()).await?;
            conn.execute("BEGIN", ()).await?;
        }

        if *total_rows % args.progress_every == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let rows_per_sec = *total_rows as f64 / elapsed.max(0.001);

            println!(
                "Progress: {:>12} rows | {:>10.0} rows/s | {:.1}s elapsed",
                *total_rows,
                rows_per_sec,
                elapsed
            );
        }
    }

    Ok(file_rows)
}

fn record_to_row(
    record: &StringRecord,
    symbol: &str,
    interval: &str,
    year: i32,
    month: u32,
    rsi_count: usize,
) -> Result<ParsedRow> {
    if record.len() < BINANCE_COLS {
        bail!(
            "row has {} columns, expected at least {}",
            record.len(),
            BINANCE_COLS
        );
    }

    let open_time = parse_i64(record, 0, "open_time")?;
    let mut values = Vec::with_capacity(16 + rsi_count);

    values.push(Value::from(symbol.to_string()));
    values.push(Value::from(interval.to_string()));
    values.push(Value::from(year as i64));
    values.push(Value::from(month as i64));

    values.push(Value::from(open_time));
    values.push(Value::from(parse_f64(record, 1, "open")?));
    values.push(Value::from(parse_f64(record, 2, "high")?));
    values.push(Value::from(parse_f64(record, 3, "low")?));
    values.push(Value::from(parse_f64(record, 4, "close")?));
    values.push(Value::from(parse_f64(record, 5, "volume")?));
    values.push(Value::from(parse_i64(record, 6, "close_time")?));
    values.push(Value::from(parse_f64(record, 7, "quote_asset_volume")?));
    values.push(Value::from(parse_i64(record, 8, "number_of_trades")?));
    values.push(Value::from(parse_f64(record, 9, "taker_buy_base_asset_volume")?));
    values.push(Value::from(parse_f64(record, 10, "taker_buy_quote_asset_volume")?));
    values.push(Value::from(record.get(11).unwrap_or("").to_string()));

    for i in 0..rsi_count {
        let idx = BINANCE_COLS + i;

        let value = match record.get(idx) {
            Some(s) if !s.trim().is_empty() => Value::from(s.parse::<f64>()?),
            _ => Value::Null,
        };

        values.push(value);
    }

    Ok(ParsedRow { open_time, values })
}

fn parse_i64(record: &StringRecord, idx: usize, name: &str) -> Result<i64> {
    let raw = record
        .get(idx)
        .with_context(|| format!("missing {}", name))?;

    raw.parse::<i64>()
        .with_context(|| format!("invalid {}: {}", name, raw))
}

fn parse_f64(record: &StringRecord, idx: usize, name: &str) -> Result<f64> {
    let raw = record
        .get(idx)
        .with_context(|| format!("missing {}", name))?;

    raw.parse::<f64>()
        .with_context(|| format!("invalid {}: {}", name, raw))
}

fn validate_ident(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("SQL identifier cannot be empty");
    }

    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        bail!("Unsafe SQL identifier: {}", name);
    }

    Ok(())
}
