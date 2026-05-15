use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use csv::{ReaderBuilder, StringRecord};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use turso::{Builder, Value, params_from_iter};

const BINANCE_COLS: usize = 12;
const PROGRESS_FLUSH_ROWS: u64 = 8192;

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
#[command(
    about = "Import Binance Vision CSV files into a local Turso database",
    long_about = "Import Binance Vision CSV files into a local Turso database.\n\nBy default this reads matching files like SOLUSDT-1s-2026-04.csv from the current directory, creates market_data.turso, and imports rows into klines_1s. Files are processed in year-month order. Existing rows are skipped by default so interrupted imports can be resumed safely. Use --recreate to drop and rebuild the table, --replace-existing to overwrite duplicate primary keys, and --import-mode unsafe only when you are willing to delete and rebuild the database after a crash.",
    version,
    disable_version_flag = true,
    after_help = "Examples:\n  csv-to-turso -d ../data\n  csv-to-turso -d ../data -o sol.turso --recreate\n  csv-to-turso -d ../data --import-mode unsafe --batch-size 500000\n  csv-to-turso -d ../data --has-header --replace-existing\n  csv-to-turso -d ../data --symbol BTCUSDT --interval 1m --table klines_1m\n\nVerification:\n  tursodb --readonly market_data.turso 'SELECT COUNT(*) FROM klines_1s;'\n  tursodb --readonly market_data.turso 'SELECT MIN(open_time), MAX(open_time) FROM klines_1s;'\n\nNotes:\n  Defaults: --dir ., --db market_data.turso, --symbol SOLUSDT, --interval 1s, --table klines_1s.\n  Defaults: --batch-size 250000, --progress-every 1000000, --import-mode balanced.\n  Imports the 12 Binance Vision kline columns plus any extra generated RSI columns.\n  For 120M-row imports, use --import-mode balanced first; use unsafe only if rebuilding after a crash is acceptable.\n  --replace-existing rewrites duplicate primary keys; without it duplicates are ignored.\n  --skip-order-check allows non-increasing open_time values, but only use it when the files are intentionally unordered."
)]
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
    #[arg(short, long, default_value = "klines_1s")]
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

    /// Do not fail if CSV open_time values are not strictly increasing
    #[arg(long, default_value_t = false)]
    skip_order_check: bool,

    /// Print version information and exit.
    #[arg(short = 'v', long = "version", action = clap::ArgAction::SetTrue, required = false)]
    version_flag: bool,
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

struct FileStats {
    row_count: usize,
    last_open_time: Option<i64>,
}

struct ProgressUi {
    multi: MultiProgress,
    total_bar: ProgressBar,
}

struct ProgressSlot {
    file_bar: ProgressBar,
    total_bar: ProgressBar,
    finished: bool,
}

impl ProgressUi {
    fn new(stage: impl Into<String>, total_rows: Option<u64>) -> Arc<Self> {
        let multi = MultiProgress::new();

        let total_bar = match total_rows {
            Some(total_rows) => multi.add(ProgressBar::new(total_rows)),
            None => multi.add(ProgressBar::new_spinner()),
        };
        total_bar.set_prefix(stage.into());
        total_bar.set_message("importing files sequentially");
        total_bar.set_style(total_progress_style(total_rows.is_some()));

        Arc::new(Self { multi, total_bar })
    }

    fn acquire_slot(self: &Arc<Self>, name: &str, total_rows: Option<u64>) -> ProgressSlot {
        let file_bar = match total_rows {
            Some(total_rows) => self.multi.add(ProgressBar::new(total_rows)),
            None => self.multi.add(ProgressBar::new_spinner()),
        };
        file_bar.set_style(file_progress_style(total_rows.is_some()));
        file_bar.set_message(truncate_progress_name(name));

        ProgressSlot {
            file_bar,
            total_bar: self.total_bar.clone(),
            finished: false,
        }
    }

    fn finish(&self) {
        self.total_bar.finish_and_clear();
    }

    fn println(&self, message: impl AsRef<str>) {
        let _ = self.total_bar.println(message.as_ref());
    }
}

fn total_progress_style(has_total: bool) -> ProgressStyle {
    if has_total {
        return ProgressStyle::with_template(
            "{prefix:<7} [{elapsed_precise}] [{bar:40.cyan/blue}] {percent:>3}% {pos}/{len} rows ({per_sec}, ETA {eta}) {msg}",
        )
        .expect("valid total progress-bar template")
        .progress_chars("##-");
    }

    ProgressStyle::with_template(
        "{prefix:<7} [{elapsed_precise}] {spinner:.green} {pos} rows ({per_sec}) {msg}",
    )
    .expect("valid total spinner template")
}

fn file_progress_style(has_total: bool) -> ProgressStyle {
    if has_total {
        return ProgressStyle::with_template(
            "  {msg:<34} [{bar:28.green/black}] {percent:>3}% {pos}/{len}",
        )
        .expect("valid file progress-bar template")
        .progress_chars("##-");
    }

    ProgressStyle::with_template("  {msg:<34} {spinner:.green} {pos} rows")
        .expect("valid file spinner template")
}

impl ProgressSlot {
    fn inc(&self, amount: u64) {
        self.file_bar.inc(amount);
        self.total_bar.inc(amount);
    }

    fn println(&self, message: impl AsRef<str>) {
        let _ = self.total_bar.println(message.as_ref());
    }

    fn finish(&mut self) {
        if self.finished {
            return;
        }

        self.file_bar.finish_and_clear();
        self.finished = true;
    }
}

impl Drop for ProgressSlot {
    fn drop(&mut self) {
        self.finish();
    }
}

fn truncate_progress_name(file_name: &str) -> String {
    const MAX_LEN: usize = 34;
    if file_name.chars().count() <= MAX_LEN {
        return file_name.to_owned();
    }

    let mut shortened = file_name.chars().take(MAX_LEN - 1).collect::<String>();
    shortened.push('~');
    shortened
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::args_os()
        .skip(1)
        .any(|arg| arg == "-v" || arg == "--version")
    {
        print_version();
        return Ok(());
    }

    let args = Args::parse();
    let _ = args.version_flag;

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
    let file_stats = collect_file_stats(&files, args.has_header)?;
    let expected_rows = file_stats
        .iter()
        .map(|stats| stats.row_count)
        .sum::<usize>();
    println!(
        "Found {} file(s). Max RSI columns: {}",
        files.len(),
        rsi_count
    );
    println!(
        "Import mode: {:?}. Batch size: {}. Conflict mode: {}.",
        args.import_mode,
        args.batch_size,
        if args.replace_existing {
            "REPLACE"
        } else {
            "IGNORE"
        }
    );

    let db = Builder::new_local(&args.db)
        .experimental_materialized_views(true)
        .build()
        .await?;
    let conn = db.connect()?;

    apply_import_mode(&conn, args.import_mode).await?;

    if args.recreate {
        drop_monthly_views(&conn, &args, &files).await?;
        conn.execute(&format!("DROP TABLE IF EXISTS {}", args.table), ())
            .await?;
    }

    create_table(&conn, &args.table, rsi_count).await?;
    let resume_open_time = if args.replace_existing {
        None
    } else {
        max_open_time(&conn, &args.table, &args.symbol, &args.interval).await?
    };

    let insert_sql = build_insert_sql(&args.table, rsi_count, args.replace_existing);
    let mut stmt = conn.prepare(&insert_sql).await?;

    let start = Instant::now();
    let mut total_rows = 0usize;
    let mut last_open_time: Option<i64> = None;
    let progress = ProgressUi::new("TOTAL ", Some(expected_rows as u64));

    conn.execute("BEGIN", ()).await?;

    for (file, stats) in files.iter().zip(file_stats.iter()) {
        let file_name = file.path.file_name().unwrap().to_string_lossy().to_string();
        let mut progress_slot = progress.acquire_slot(&file_name, Some(stats.row_count as u64));

        if !args.recreate && monthly_view_exists(&conn, &args, file).await? {
            progress_slot.inc(stats.row_count as u64);
            progress_slot.finish();

            if let Some(file_last_open_time) = stats.last_open_time {
                last_open_time = Some(file_last_open_time);
            }

            progress.println(format!("Imported {:>12} rows from {}", 0, file_name));
            continue;
        }

        if should_skip_file(stats, resume_open_time) {
            progress_slot.inc(stats.row_count as u64);
            progress_slot.finish();

            if let Some(file_last_open_time) = stats.last_open_time {
                last_open_time = Some(file_last_open_time);
            }

            progress.println(format!("Imported {:>12} rows from {}", 0, file_name));
            continue;
        }

        let imported = import_file(
            file,
            &args,
            rsi_count,
            &mut stmt,
            &conn,
            &mut total_rows,
            &mut last_open_time,
            start,
            &progress_slot,
            resume_open_time,
        )
        .await?;
        progress_slot.finish();

        progress.println(format!("Imported {:>12} rows from {}", imported, file_name));
    }

    conn.execute("COMMIT", ()).await?;

    for file in files.iter() {
        create_monthly_view(&conn, &args, file).await?;
    }

    progress.finish();

    let elapsed = start.elapsed().as_secs_f64();
    let average_rows_per_second = total_rows as f64 / elapsed.max(0.001);
    println!(
        "Done. Imported {} rows into {} in {:.1}s (average speed {:.0} rows/second)",
        total_rows, args.db, elapsed, average_rows_per_second
    );

    Ok(())
}

fn print_version() {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
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
    apply_journal_mode(conn).await?;

    match mode {
        ImportMode::Safe => {
            conn.execute("PRAGMA synchronous = FULL", ()).await?;
        }
        ImportMode::Balanced => {
            conn.execute("PRAGMA synchronous = NORMAL", ()).await?;
        }
        ImportMode::Unsafe => {
            conn.execute("PRAGMA synchronous = OFF", ()).await?;
        }
    }

    Ok(())
}

async fn apply_journal_mode(conn: &turso::Connection) -> Result<()> {
    let mut rows = conn.query("PRAGMA journal_mode = WAL", ()).await?;
    while rows.next().await?.is_some() {}
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

fn collect_file_stats(files: &[CsvFile], has_header: bool) -> Result<Vec<FileStats>> {
    files
        .iter()
        .map(|file| {
            let mut reader = ReaderBuilder::new()
                .has_headers(has_header)
                .from_path(&file.path)
                .with_context(|| format!("Cannot open {}", file.path.display()))?;

            let mut record = StringRecord::new();
            let mut row_count = 0usize;
            let mut last_open_time = None;
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

                row_count += 1;
                last_open_time = Some(parse_i64(&record, 0, "open_time")?);
            }

            Ok(FileStats {
                row_count,
                last_open_time,
            })
        })
        .collect()
}

fn should_skip_file(stats: &FileStats, resume_open_time: Option<i64>) -> bool {
    match (stats.last_open_time, resume_open_time) {
        (Some(file_last_open_time), Some(resume_open_time)) => {
            file_last_open_time <= resume_open_time
        }
        _ => false,
    }
}

async fn drop_monthly_views(
    conn: &turso::Connection,
    args: &Args,
    files: &[CsvFile],
) -> Result<()> {
    for file in files {
        let view_name = monthly_view_name(&args.table, &args.symbol, &args.interval, file);
        conn.execute(&format!("DROP VIEW IF EXISTS {}", view_name), ())
            .await?;
    }

    Ok(())
}

async fn create_monthly_view(conn: &turso::Connection, args: &Args, file: &CsvFile) -> Result<()> {
    let view_name = monthly_view_name(&args.table, &args.symbol, &args.interval, file);
    validate_ident(&view_name)?;

    let sql = format!(
        "CREATE VIEW IF NOT EXISTS {} AS SELECT * FROM {} WHERE symbol = '{}' AND interval = '{}' AND year = {} AND month = {}",
        view_name,
        args.table,
        sql_string_literal(&args.symbol),
        sql_string_literal(&args.interval),
        file.year,
        file.month
    );
    conn.execute(&sql, ()).await?;

    Ok(())
}

async fn monthly_view_exists(
    conn: &turso::Connection,
    args: &Args,
    file: &CsvFile,
) -> Result<bool> {
    let view_name = monthly_view_name(&args.table, &args.symbol, &args.interval, file);
    let mut rows = conn
        .query(
            "SELECT 1 FROM sqlite_master WHERE type = 'view' AND name = ?1",
            (view_name,),
        )
        .await?;

    Ok(rows.next().await?.is_some())
}

fn monthly_view_name(table: &str, symbol: &str, interval: &str, file: &CsvFile) -> String {
    format!(
        "{}_{}_{}_{:04}_{:02}",
        table,
        ident_fragment(symbol),
        ident_fragment(interval),
        file.year,
        file.month
    )
}

fn ident_fragment(value: &str) -> String {
    let fragment = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();

    if fragment.is_empty() {
        return "value".to_string();
    }

    fragment
}

fn sql_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

async fn max_open_time(
    conn: &turso::Connection,
    table: &str,
    symbol: &str,
    interval: &str,
) -> Result<Option<i64>> {
    let sql = format!(
        "SELECT MAX(open_time) FROM {} WHERE symbol = ?1 AND interval = ?2",
        table
    );
    let mut rows = conn
        .query(&sql, (symbol.to_string(), interval.to_string()))
        .await?;

    let Some(row) = rows.next().await? else {
        return Ok(None);
    };

    match row.get_value(0)? {
        Value::Integer(open_time) => Ok(Some(open_time)),
        Value::Null => Ok(None),
        other => bail!("Unexpected MAX(open_time) value: {:?}", other),
    }
}

async fn create_table(conn: &turso::Connection, table: &str, rsi_count: usize) -> Result<()> {
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

    let sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", table, cols.join(", "));

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

    let conflict_action = if replace_existing {
        "REPLACE"
    } else {
        "IGNORE"
    };

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
    progress: &ProgressSlot,
    resume_open_time: Option<i64>,
) -> Result<usize> {
    let mut reader = ReaderBuilder::new()
        .has_headers(args.has_header)
        .from_path(&file.path)
        .with_context(|| format!("Cannot open {}", file.path.display()))?;

    let mut record = StringRecord::new();
    let mut file_rows = 0usize;
    let mut pending_progress = 0u64;

    while reader.read_record(&mut record)? {
        if record.is_empty() {
            continue;
        }

        let open_time = parse_i64(&record, 0, "open_time")?;

        if resume_open_time.is_some_and(|resume_open_time| open_time <= resume_open_time) {
            pending_progress += 1;
            if pending_progress >= PROGRESS_FLUSH_ROWS {
                progress.inc(pending_progress);
                pending_progress = 0;
            }
            *last_open_time = Some(open_time);
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
        pending_progress += 1;

        if pending_progress >= PROGRESS_FLUSH_ROWS {
            progress.inc(pending_progress);
            pending_progress = 0;
        }

        if *total_rows % args.batch_size == 0 {
            conn.execute("COMMIT", ()).await?;
            conn.execute("BEGIN", ()).await?;
        }

        if *total_rows % args.progress_every == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let rows_per_sec = *total_rows as f64 / elapsed.max(0.001);

            progress.println(format!(
                "Progress: {:>12} rows | {:>10.0} rows/s | {:.1}s elapsed",
                *total_rows, rows_per_sec, elapsed
            ));
        }
    }

    if pending_progress > 0 {
        progress.inc(pending_progress);
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
    values.push(Value::from(parse_f64(
        record,
        9,
        "taker_buy_base_asset_volume",
    )?));
    values.push(Value::from(parse_f64(
        record,
        10,
        "taker_buy_quote_asset_volume",
    )?));
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
