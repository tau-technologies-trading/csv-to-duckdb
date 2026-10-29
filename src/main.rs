use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use csv::{ReaderBuilder, StringRecord};
use duckdb::{Appender, Connection, appender_params_from_iter, types::ToSql};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const BINANCE_COLS: usize = 12;
const PROGRESS_FLUSH_ROWS: u64 = 8192;
const DEFAULT_SINGLE_DIR: &str = "../data/BTCUSDT/";
const DEFAULT_SINGLE_DB: &str = "../db/BTCUSDT/BTCUSDT.duckdb";

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ImportMode {
    /// Maximum crash safety. Uses row-by-row prepared statements with periodic commits. Slower.
    Safe,

    /// Appender-based bulk insert with periodic flushes. Recommended default.
    Balanced,

    /// Appender-based bulk insert with minimal flushing. Fastest, but rebuild DB if interrupted.
    Unsafe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecreateAction {
    None,
    DeleteDatabaseFiles,
    DropTableOnly,
}

#[derive(Parser, Debug)]
#[command(
    about = "Import Binance Vision CSV files into a local DuckDB database",
    long_about = "Import one folder of Binance Vision CSVs into a DuckDB database. Files are processed chronologically; --jobs controls parallel file inspection. Files ending at or before the database maximum are skipped; the first extending file replaces overlaps.",
    version,
    disable_version_flag = true,
    after_help = "Examples:\n  csv-to-duckdb --dir ../data/BTCUSDT --db ../db/BTCUSDT/BTCUSDT.duckdb --jobs 4\n  csv-to-duckdb --auto 3\n  csv-to-duckdb --ignore-extra-columns\n  csv-to-duckdb --replace-existing"
)]
#[derive(Clone)]
struct Args {
    /// Directory containing files like BTCUSDT-1s-2026-04.csv
    #[arg(short, long, default_value = DEFAULT_SINGLE_DIR)]
    dir: PathBuf,

    /// Output DuckDB database path
    #[arg(short = 'o', long, default_value = DEFAULT_SINGLE_DB)]
    db: String,

    /// Interval per row
    #[arg(short, long, default_value = "1s")]
    interval: String,

    /// SQL table name
    #[arg(short, long, default_value = "klines")]
    table: String,

    /// Commit every N rows (only used in safe mode)
    #[arg(short, long, default_value_t = 250_000)]
    batch_size: usize,

    /// Print progress every N rows
    #[arg(long, default_value_t = 1_000_000)]
    progress_every: usize,

    /// CSV has a header row
    #[arg(long, default_value_t = false)]
    has_header: bool,

    /// Ignore all CSV columns after the 12 standard Binance kline columns.
    #[arg(long, default_value_t = false)]
    ignore_extra_columns: bool,

    /// Comma-separated Binance columns to omit (use an empty value to keep all).
    #[arg(long, default_value = "ignore")]
    drop_columns: String,

    /// Delete DB file and recreate from scratch before importing
    #[arg(long, default_value_t = false)]
    recreate: bool,

    /// Recreate efficiently while preserving other user tables when present
    #[arg(long, default_value_t = false)]
    recreate_pragmatic: bool,

    /// Import durability/speed mode (safe=prepared stmts, balanced=Appender+flush, unsafe=Appender)
    #[arg(long, value_enum, default_value = "balanced")]
    import_mode: ImportMode,

    /// Replace existing rows instead of skipping duplicates. Use only when recomputing columns.
    #[arg(long, default_value_t = false)]
    replace_existing: bool,

    /// Do not fail if CSV open_time values are not strictly increasing
    #[arg(long, default_value_t = false)]
    skip_order_check: bool,

    /// Import only the newest N matching CSV files
    #[arg(long)]
    auto: Option<usize>,

    /// Maximum number of CSV files to inspect concurrently.
    #[arg(long, default_value_t = 4)]
    jobs: usize,

    /// Emit a machine-readable marker after each file is imported or skipped.
    #[arg(long, default_value_t = false)]
    report_file_progress: bool,

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

const COLUMN_NAMES: [&str; 12] = [
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
];

fn dropped_columns(args: &Args) -> Result<Vec<usize>> {
    let mut result = Vec::new();
    for name in args
        .drop_columns
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let name = if name == "ignore" { "ignore_col" } else { name };
        let index = COLUMN_NAMES
            .iter()
            .position(|col| *col == name)
            .with_context(|| format!("unknown column: {name}"))?;
        if index == 0 || index == 4 {
            bail!("{name} is required for importing and RSI");
        }
        if !result.contains(&index) {
            result.push(index);
        }
    }
    Ok(result)
}

struct ParsedRow<'a> {
    open_time: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    close_time: i64,
    quote_asset_volume: f64,
    number_of_trades: i64,
    taker_buy_base_asset_volume: f64,
    taker_buy_quote_asset_volume: f64,
    ignore_col: &'a str,
    rsi_values: Vec<Option<f64>>,
}

struct FileStats {
    row_count: usize,
}

struct ImportJob {
    dir: PathBuf,
    db_path: PathBuf,
    symbol: String,
    interval: String,
    files: Vec<CsvFile>,
}

struct ProgressUi {
    multi: MultiProgress,
}

struct JobProgress {
    ui: Arc<ProgressUi>,
    total_bar: ProgressBar,
    finished: bool,
}

struct ProgressSlot {
    file_bar: ProgressBar,
    total_bar: ProgressBar,
    finished: bool,
}

impl ProgressUi {
    fn new() -> Arc<Self> {
        let multi = MultiProgress::new();
        Arc::new(Self { multi })
    }

    fn start_job(self: &Arc<Self>, name: &str, total_rows: Option<u64>) -> JobProgress {
        let total_bar = match total_rows {
            Some(total_rows) => self.multi.add(ProgressBar::new(total_rows)),
            None => self.multi.add(ProgressBar::new_spinner()),
        };
        total_bar.set_prefix(truncate_progress_prefix(name));
        total_bar.set_message("importing files sequentially");
        total_bar.set_style(total_progress_style(total_rows.is_some()));

        JobProgress {
            ui: Arc::clone(self),
            total_bar,
            finished: false,
        }
    }

    fn println(&self, message: impl AsRef<str>) {
        let message = message.as_ref();
        if self.multi.is_hidden() {
            println!("{message}");
        } else {
            let _ = self.multi.println(message);
        }
    }
}

impl JobProgress {
    fn acquire_slot(&self, name: &str, total_rows: Option<u64>) -> ProgressSlot {
        let file_bar = match total_rows {
            Some(total_rows) => self.ui.multi.add(ProgressBar::new(total_rows)),
            None => self.ui.multi.add(ProgressBar::new_spinner()),
        };
        file_bar.set_style(file_progress_style(total_rows.is_some()));
        file_bar.set_message(truncate_progress_name(name));

        ProgressSlot {
            file_bar,
            total_bar: self.total_bar.clone(),
            finished: false,
        }
    }

    fn println(&self, message: impl AsRef<str>) {
        self.ui.println(message);
    }

    fn finish(&mut self) {
        if self.finished {
            return;
        }

        self.total_bar.finish_and_clear();
        self.finished = true;
    }
}

impl Drop for JobProgress {
    fn drop(&mut self) {
        self.finish();
    }
}

fn total_progress_style(has_total: bool) -> ProgressStyle {
    if has_total {
        return ProgressStyle::with_template(
            "{prefix:<10} [{elapsed_precise}] [{bar:40.cyan/blue}] {percent:>3}% {pos}/{len} rows ({per_sec}, ETA {eta}) {msg}",
        )
        .expect("valid total progress-bar template")
        .progress_chars("##-");
    }

    ProgressStyle::with_template(
        "{prefix:<10} [{elapsed_precise}] {spinner:.green} {pos} rows ({per_sec}) {msg}",
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

fn truncate_progress_prefix(name: &str) -> String {
    const MAX_LEN: usize = 7;
    if name.chars().count() <= MAX_LEN {
        return name.to_owned();
    }

    name.chars().take(MAX_LEN).collect()
}

fn main() -> Result<()> {
    if std::env::args_os()
        .skip(1)
        .any(|arg| arg == "-v" || arg == "--version")
    {
        print_version();
        return Ok(());
    }

    if std::env::args().len() == 1 {
        bail!("no arguments provided; use --help for usage");
    }

    let args = Args::parse();
    validate_args(&args)?;
    validate_ident(&args.table)?;
    let job = import_job_for_single_directory(&args.dir, PathBuf::from(&args.db), &args.interval)?;
    run_import_job(&args, job, ProgressUi::new())
}

fn run_import_job(args: &Args, mut job: ImportJob, progress_ui: Arc<ProgressUi>) -> Result<()> {
    job.files.sort_by_key(|f| (f.year, f.month));
    apply_auto_file_limit(&mut job.files, args.auto);

    if job.files.is_empty() {
        bail!(
            "No files found matching {}-{}-YYYY-MM.csv in {}",
            job.symbol,
            job.interval,
            job.dir.display()
        );
    }

    warn_missing_months(&job.files, &progress_ui);

    let dropped = dropped_columns(args)?;
    let mut rsi_count = if args.ignore_extra_columns {
        0
    } else {
        infer_max_rsi_columns(&job.files, args.has_header)?
    };
    if let Some(parent) = job.db_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Cannot create {}", parent.display()))?;
    }

    let db_path = job.db_path.to_string_lossy().to_string();
    let recreate_action = determine_recreate_action(args, &db_path)?;
    if recreate_action == RecreateAction::DeleteDatabaseFiles {
        remove_database_files(&db_path)?;
    }

    let conn = Connection::open(&db_path)
        .with_context(|| format!("Cannot open DuckDB database at {}", db_path))?;

    if recreate_action == RecreateAction::DropTableOnly {
        conn.execute_batch(&format!("DROP TABLE IF EXISTS {}", args.table))?;
        conn.execute_batch("CHECKPOINT")?;
        conn.execute_batch("VACUUM")?;
        conn.execute_batch("CHECKPOINT")?;
    }

    create_table(&conn, &args.table, rsi_count, &dropped)?;
    let db_max: Option<i64> = conn.query_row(
        &format!("SELECT MAX(open_time) FROM {}", args.table),
        [],
        |row| row.get(0),
    )?;
    if args.skip_order_check && db_max.is_some() && !args.replace_existing {
        bail!(
            "--skip-order-check cannot be used for incremental updates; use --replace-existing to reimport selected files"
        );
    }
    let mut selected = Vec::new();
    for file in &job.files {
        let last = last_file_open_time(file, args.has_header)?;
        if !args.replace_existing
            && db_max.is_some()
            && last.is_none_or(|last| last <= db_max.unwrap())
        {
            progress_ui.println(format!(
                "Skipped {} (already covered by database)",
                file.path.display()
            ));
            if args.report_file_progress {
                println!("@@CSV_FILE_IMPORTED@@");
            }
        } else {
            selected.push(file.clone());
        }
    }
    job.files = selected;
    let first_overlap = if let (Some(max), Some(first)) = (db_max, job.files.first()) {
        let mut reader = ReaderBuilder::new()
            .has_headers(args.has_header)
            .flexible(true)
            .from_path(&first.path)?;
        let mut record = StringRecord::new();
        reader.read_record(&mut record)? && parse_i64(&record, 0, "open_time")? <= max
    } else {
        false
    };
    let file_stats = collect_file_stats(&job.files, args.has_header, args.jobs)?;
    rsi_count = validate_and_extend_row_schema(&conn, &args.table, rsi_count, &dropped)?;
    let expected_rows = file_stats
        .iter()
        .map(|stats| stats.row_count)
        .sum::<usize>();
    progress_ui.println(format!(
        "Importing {} files ({} rows) into {}",
        job.files.len(),
        expected_rows,
        db_path
    ));

    let start = Instant::now();
    let mut total_rows = 0usize;
    let mut last_open_time: Option<i64> = None;
    let mut progress = progress_ui.start_job(&job.symbol, Some(expected_rows as u64));

    let use_appender = matches!(args.import_mode, ImportMode::Balanced | ImportMode::Unsafe);

    if use_appender {
        import_all_files_with_appender(
            &job.files,
            &file_stats,
            args,
            rsi_count,
            &conn,
            &dropped,
            first_overlap,
            &mut total_rows,
            &mut last_open_time,
            start,
            &mut progress,
        )?;
    } else {
        import_all_files_with_prepared_stmts(
            &job.files,
            &file_stats,
            args,
            rsi_count,
            &conn,
            &dropped,
            first_overlap,
            &mut total_rows,
            &mut last_open_time,
            start,
            &mut progress,
        )?;
    }

    progress.finish();

    let elapsed = start.elapsed().as_secs_f64();
    let average_rows_per_second = total_rows as f64 / elapsed.max(0.001);
    progress_ui.println(format!(
        "Done. Processed {} input rows for {} in {:.1}s (average speed {:.0} rows/second)",
        total_rows, db_path, elapsed, average_rows_per_second
    ));

    Ok(())
}

fn import_all_files_with_appender(
    files: &[CsvFile],
    file_stats: &[FileStats],
    args: &Args,
    rsi_count: usize,
    conn: &Connection,
    dropped: &[usize],
    first_overlap: bool,
    total_rows: &mut usize,
    last_open_time: &mut Option<i64>,
    start: Instant,
    progress: &mut JobProgress,
) -> Result<()> {
    conn.execute_batch(&format!(
        "CREATE TEMP TABLE __csv_import_stage AS SELECT * FROM {} WHERE false",
        args.table
    ))?;
    for (file_index, (file, stats)) in files.iter().zip(file_stats.iter()).enumerate() {
        let file_name = file.path.file_name().unwrap().to_string_lossy().to_string();
        let mut progress_slot = progress.acquire_slot(&file_name, Some(stats.row_count as u64));

        let mut appender = conn
            .appender("__csv_import_stage")
            .context("Cannot create staging Appender")?;
        let imported = import_file_with_appender(
            file,
            args,
            rsi_count,
            &mut appender,
            dropped,
            conn,
            total_rows,
            last_open_time,
            start,
            &progress_slot,
        )?;
        appender.flush()?;
        drop(appender);
        conn.execute_batch(&format!(
            "INSERT OR {} INTO {} SELECT * FROM __csv_import_stage",
            if args.replace_existing || (first_overlap && file_index == 0) {
                "REPLACE"
            } else {
                "IGNORE"
            },
            args.table
        ))?;
        conn.execute_batch("DELETE FROM __csv_import_stage")?;
        progress_slot.finish();

        progress.println(format!("Imported {:>12} rows from {}", imported, file_name));
        if args.report_file_progress {
            println!("@@CSV_FILE_IMPORTED@@");
        }
    }

    conn.execute_batch("DROP TABLE __csv_import_stage")?;
    Ok(())
}

fn import_all_files_with_prepared_stmts(
    files: &[CsvFile],
    file_stats: &[FileStats],
    args: &Args,
    rsi_count: usize,
    conn: &Connection,
    dropped: &[usize],
    first_overlap: bool,
    total_rows: &mut usize,
    last_open_time: &mut Option<i64>,
    start: Instant,
    progress: &mut JobProgress,
) -> Result<()> {
    conn.execute_batch("BEGIN")?;

    for (file_index, (file, stats)) in files.iter().zip(file_stats.iter()).enumerate() {
        let file_name = file.path.file_name().unwrap().to_string_lossy().to_string();
        let mut progress_slot = progress.acquire_slot(&file_name, Some(stats.row_count as u64));

        let insert_sql = build_insert_sql(
            &args.table,
            rsi_count,
            args.replace_existing || (first_overlap && file_index == 0),
            dropped,
        );
        let mut stmt = conn.prepare(&insert_sql)?;
        let imported = import_file_with_prepared_stmt(
            file,
            args,
            rsi_count,
            &mut stmt,
            dropped,
            conn,
            total_rows,
            last_open_time,
            start,
            &progress_slot,
        )?;
        progress_slot.finish();

        progress.println(format!("Imported {:>12} rows from {}", imported, file_name));
        conn.execute_batch("COMMIT")?;
        if args.report_file_progress {
            println!("@@CSV_FILE_IMPORTED@@");
        }
        conn.execute_batch("BEGIN")?;
    }

    conn.execute_batch("COMMIT")?;
    Ok(())
}

fn print_version() {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
}

fn validate_args(args: &Args) -> Result<()> {
    dropped_columns(args)?;
    if args.batch_size == 0 {
        bail!("--batch-size must be greater than 0");
    }

    if args.progress_every == 0 {
        bail!("--progress-every must be greater than 0");
    }

    if args.jobs == 0 {
        bail!("--jobs must be greater than 0");
    }

    if args.recreate && args.recreate_pragmatic {
        bail!("--recreate and --recreate-pragmatic cannot be used together");
    }

    if args.auto == Some(0) {
        bail!("--auto must be greater than 0");
    }

    if args.auto.is_some() && (args.recreate || args.recreate_pragmatic) {
        bail!("--auto cannot be combined with recreate options");
    }

    Ok(())
}

fn import_job_for_single_directory(
    dir: &Path,
    db_path: PathBuf,
    interval: &str,
) -> Result<ImportJob> {
    let mut symbol: Option<String> = None;
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

        if !file_name.ends_with(".csv") {
            continue;
        }

        let Some(parsed) = parse_filename(file_name) else {
            eprintln!(
                "Warning: ignoring CSV with unexpected name in {}: {}",
                dir.display(),
                file_name
            );
            continue;
        };

        if parsed.1 != interval {
            continue;
        }

        match &symbol {
            Some(existing) if existing != &parsed.0 => bail!(
                "Mixed symbols for interval {} in {}: found both {} and {}",
                interval,
                dir.display(),
                existing,
                parsed.0
            ),
            None => symbol = Some(parsed.0.clone()),
            _ => {}
        }

        files.push(CsvFile {
            path,
            year: parsed.2,
            month: parsed.3,
        });
    }

    let Some(symbol) = symbol else {
        bail!(
            "No files found matching *-{}-YYYY-MM.csv in {}",
            interval,
            dir.display()
        );
    };

    Ok(ImportJob {
        dir: dir.to_path_buf(),
        db_path,
        symbol,
        interval: interval.to_string(),
        files,
    })
}

fn apply_auto_file_limit(files: &mut Vec<CsvFile>, auto: Option<usize>) {
    let Some(limit) = auto else {
        return;
    };

    if files.len() > limit {
        files.drain(..files.len() - limit);
    }
}

fn determine_recreate_action(args: &Args, db_path: &str) -> Result<RecreateAction> {
    if args.recreate {
        return Ok(RecreateAction::DeleteDatabaseFiles);
    }

    if !args.recreate_pragmatic {
        return Ok(RecreateAction::None);
    }

    let table_names = existing_user_tables(db_path)?;
    if table_names.is_empty() || (table_names.len() == 1 && table_names[0] == args.table) {
        return Ok(RecreateAction::DeleteDatabaseFiles);
    }

    Ok(RecreateAction::DropTableOnly)
}

fn existing_user_tables(db_path: &str) -> Result<Vec<String>> {
    if !Path::new(db_path).exists() {
        return Ok(Vec::new());
    }

    let conn = Connection::open(db_path)
        .with_context(|| format!("Cannot open {} to inspect tables", db_path))?;

    let mut stmt = conn.prepare(
        "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE' AND table_schema = 'main' ORDER BY table_name",
    )?;

    let table_names: Vec<String> = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("Failed to read table names: {}", e))?;

    Ok(table_names)
}

fn remove_database_files(db_path: &str) -> Result<()> {
    match fs::remove_file(db_path) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("Cannot remove {}", db_path));
        }
    }

    Ok(())
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

fn warn_missing_months(files: &[CsvFile], progress_ui: &ProgressUi) {
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
            progress_ui.println(format!(
                "Warning: missing month(s) between {:04}-{:02} and {:04}-{:02}",
                prev.year, prev.month, next.year, next.month
            ));
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

fn collect_file_stats(files: &[CsvFile], has_header: bool, jobs: usize) -> Result<Vec<FileStats>> {
    let workers = jobs.min(files.len()).max(1);
    let results = std::sync::Mutex::new(Vec::with_capacity(files.len()));
    thread::scope(|scope| {
        for index in 0..workers {
            let results = &results;
            scope.spawn(move || {
                for file_index in (index..files.len()).step_by(workers) {
                    let result = scan_file_stats(&files[file_index], has_header);
                    results.lock().unwrap().push((file_index, result));
                }
            });
        }
    });
    let mut results = results.into_inner().unwrap();
    results.sort_by_key(|(index, _)| *index);
    results.into_iter().map(|(_, result)| result).collect()
}

fn scan_file_stats(file: &CsvFile, has_header: bool) -> Result<FileStats> {
    let file_handle =
        File::open(&file.path).with_context(|| format!("Cannot open {}", file.path.display()))?;
    let mut reader = BufReader::new(file_handle);
    let mut buffer = Vec::with_capacity(4096);
    let mut skipped_header = false;
    let mut row_count = 0usize;

    loop {
        buffer.clear();
        let bytes_read = reader
            .read_until(b'\n', &mut buffer)
            .with_context(|| format!("Cannot read {}", file.path.display()))?;

        if bytes_read == 0 {
            break;
        }

        let line = trim_csv_line(&buffer);
        if line.is_empty() {
            continue;
        }

        if has_header && !skipped_header {
            skipped_header = true;
            continue;
        }

        row_count += 1;
        parse_open_time_bytes(line)
            .with_context(|| format!("Bad open_time while scanning {}", file.path.display()))?;
    }

    Ok(FileStats { row_count })
}

// Read backwards in blocks so already covered monthly files do not require a full scan.
fn last_file_open_time(file: &CsvFile, has_header: bool) -> Result<Option<i64>> {
    let mut input =
        File::open(&file.path).with_context(|| format!("Cannot open {}", file.path.display()))?;
    let mut pos = input.metadata()?.len();
    let mut suffix = Vec::new();
    while pos > 0 {
        let size = pos.min(8192) as usize;
        pos -= size as u64;
        input.seek(SeekFrom::Start(pos))?;
        let mut block = vec![0; size];
        input.read_exact(&mut block)?;
        block.extend_from_slice(&suffix);
        let lines: Vec<_> = block.split(|b| *b == b'\n').collect();
        for (i, line) in lines.into_iter().enumerate().rev() {
            if i == 0 && pos > 0 {
                break;
            }
            let line = trim_csv_line(line);
            if !line.is_empty() {
                // A header-only file has no data rows.
                if has_header && pos == 0 && i == 0 {
                    return Ok(None);
                }
                return parse_open_time_bytes(line)
                    .map(Some)
                    .with_context(|| format!("Bad last open_time in {}", file.path.display()));
            }
        }
        suffix = block
            .split(|b| *b == b'\n')
            .next()
            .unwrap_or_default()
            .to_vec();
    }
    Ok(None)
}

fn trim_csv_line(mut line: &[u8]) -> &[u8] {
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line = &line[..line.len() - 1];
    }

    if line.iter().all(|byte| byte.is_ascii_whitespace()) {
        return &[];
    }

    line
}

fn parse_open_time_bytes(line: &[u8]) -> Result<i64> {
    let first_field = line
        .split(|byte| *byte == b',')
        .next()
        .context("missing open_time")?;
    let raw = std::str::from_utf8(first_field)
        .context("open_time is not valid UTF-8")?
        .trim();

    raw.parse::<i64>()
        .with_context(|| format!("invalid open_time: {}", raw))
}

fn create_table(conn: &Connection, table: &str, rsi_count: usize, dropped: &[usize]) -> Result<()> {
    let mut cols = vec![
        "open_time BIGINT NOT NULL".to_string(),
        "open DOUBLE NOT NULL".to_string(),
        "high DOUBLE NOT NULL".to_string(),
        "low DOUBLE NOT NULL".to_string(),
        "close DOUBLE NOT NULL".to_string(),
        "volume DOUBLE NOT NULL".to_string(),
        "close_time BIGINT NOT NULL".to_string(),
        "quote_asset_volume DOUBLE NOT NULL".to_string(),
        "number_of_trades BIGINT NOT NULL".to_string(),
        "taker_buy_base_asset_volume DOUBLE NOT NULL".to_string(),
        "taker_buy_quote_asset_volume DOUBLE NOT NULL".to_string(),
        "ignore_col VARCHAR".to_string(),
    ];

    cols = cols
        .into_iter()
        .enumerate()
        .filter_map(|(i, col)| (!dropped.contains(&i)).then_some(col))
        .collect();
    for i in 1..=rsi_count {
        cols.push(format!("rsi_{} DOUBLE", i));
    }

    cols.push("PRIMARY KEY (open_time)".to_string());

    let sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", table, cols.join(", "));

    conn.execute_batch(&sql)?;
    Ok(())
}

fn validate_and_extend_row_schema(
    conn: &Connection,
    table: &str,
    incoming_rsi: usize,
    dropped: &[usize],
) -> Result<usize> {
    let sql = format!(
        "SELECT name FROM pragma_table_info('{}') ORDER BY cid",
        escape_sql_string(table)
    );
    let mut stmt = conn.prepare(&sql)?;
    let columns = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<duckdb::Result<Vec<_>>>()?;
    let retained: Vec<&str> = COLUMN_NAMES
        .iter()
        .enumerate()
        .filter_map(|(i, name)| (!dropped.contains(&i)).then_some(*name))
        .collect();
    let remaining: Vec<&str> = columns
        .iter()
        .map(String::as_str)
        .filter(|name| !dropped.iter().any(|i| COLUMN_NAMES[*i] == *name))
        .collect();
    if remaining.len() < retained.len() || remaining[..retained.len()] != retained {
        bail!(
            "table {table} has an incompatible column layout; changing retained columns requires a rebuild"
        );
    }
    let existing = remaining.len() - retained.len();
    for (i, name) in remaining[retained.len()..].iter().enumerate() {
        if *name != format!("rsi_{}", i + 1) {
            bail!("table {table} has incompatible RSI columns");
        }
    }
    // Existing databases may contain columns that are now excluded. Drop them explicitly.
    drop(stmt);
    for &index in dropped {
        if columns.iter().any(|col| col == COLUMN_NAMES[index]) {
            conn.execute_batch(&format!(
                "ALTER TABLE {table} DROP COLUMN {}",
                COLUMN_NAMES[index]
            ))?;
        }
    }
    for i in existing + 1..=incoming_rsi {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN rsi_{i} DOUBLE"))?;
    }
    Ok(existing.max(incoming_rsi))
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn build_insert_sql(
    table: &str,
    rsi_count: usize,
    replace_existing: bool,
    dropped: &[usize],
) -> String {
    let mut cols = vec![
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

    cols = cols
        .into_iter()
        .enumerate()
        .filter_map(|(i, col)| (!dropped.contains(&i)).then_some(col))
        .collect();
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

fn import_file_with_appender(
    file: &CsvFile,
    args: &Args,
    rsi_count: usize,
    appender: &mut Appender,
    dropped: &[usize],
    _conn: &Connection,
    total_rows: &mut usize,
    last_open_time: &mut Option<i64>,
    start: Instant,
    progress: &ProgressSlot,
) -> Result<usize> {
    let mut reader = ReaderBuilder::new()
        .has_headers(args.has_header)
        .flexible(args.ignore_extra_columns)
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

        if !args.skip_order_check
            && let Some(prev_open_time) = *last_open_time
            && open_time <= prev_open_time
        {
            bail!(
                "open_time is not strictly increasing: previous={}, current={} in {}",
                prev_open_time,
                open_time,
                file.path.display()
            );
        }

        let row = record_to_row(&record, rsi_count, args.ignore_extra_columns)
            .with_context(|| format!("Bad row in {}", file.path.display()))?;

        append_row_to_appender(appender, &row, dropped)?;

        *last_open_time = Some(row.open_time);
        file_rows += 1;
        *total_rows += 1;
        pending_progress += 1;

        if pending_progress >= PROGRESS_FLUSH_ROWS {
            progress.inc(pending_progress);
            pending_progress = 0;
        }

        if matches!(args.import_mode, ImportMode::Balanced)
            && (*total_rows).is_multiple_of(args.batch_size)
        {
            appender.flush()?;
        }

        if (*total_rows).is_multiple_of(args.progress_every) {
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

fn row_params<'a>(
    row: &'a ParsedRow<'a>,
    dropped: &'a [usize],
) -> impl Iterator<Item = &'a dyn ToSql> + 'a {
    let columns: [&dyn ToSql; 12] = [
        &row.open_time,
        &row.open,
        &row.high,
        &row.low,
        &row.close,
        &row.volume,
        &row.close_time,
        &row.quote_asset_volume,
        &row.number_of_trades,
        &row.taker_buy_base_asset_volume,
        &row.taker_buy_quote_asset_volume,
        &row.ignore_col,
    ];
    columns
        .into_iter()
        .enumerate()
        .filter_map(move |(i, value)| (!dropped.contains(&i)).then_some(value))
        .chain(row.rsi_values.iter().map(|value| value as &dyn ToSql))
}

fn append_row_to_appender(
    appender: &mut Appender,
    row: &ParsedRow,
    dropped: &[usize],
) -> Result<()> {
    appender.append_row(appender_params_from_iter(row_params(row, dropped)))?;
    Ok(())
}

fn import_file_with_prepared_stmt(
    file: &CsvFile,
    args: &Args,
    rsi_count: usize,
    stmt: &mut duckdb::Statement,
    dropped: &[usize],
    conn: &Connection,
    total_rows: &mut usize,
    last_open_time: &mut Option<i64>,
    start: Instant,
    progress: &ProgressSlot,
) -> Result<usize> {
    let mut reader = ReaderBuilder::new()
        .has_headers(args.has_header)
        .flexible(args.ignore_extra_columns)
        .from_path(&file.path)
        .with_context(|| format!("Cannot open {}", file.path.display()))?;

    let mut record = StringRecord::new();
    let mut file_rows = 0usize;
    let mut pending_progress = 0u64;

    while reader.read_record(&mut record)? {
        if record.is_empty() {
            continue;
        }

        let row = record_to_row(&record, rsi_count, args.ignore_extra_columns)
            .with_context(|| format!("Bad row in {}", file.path.display()))?;

        if !args.skip_order_check
            && let Some(prev_open_time) = *last_open_time
            && row.open_time <= prev_open_time
        {
            bail!(
                "open_time is not strictly increasing: previous={}, current={} in {}",
                prev_open_time,
                row.open_time,
                file.path.display()
            );
        }

        stmt.execute(duckdb::params_from_iter(row_params(&row, dropped)))?;

        *last_open_time = Some(row.open_time);
        file_rows += 1;
        *total_rows += 1;
        pending_progress += 1;

        if pending_progress >= PROGRESS_FLUSH_ROWS {
            progress.inc(pending_progress);
            pending_progress = 0;
        }

        if (*total_rows).is_multiple_of(args.batch_size) {
            conn.execute_batch("COMMIT")?;
            conn.execute_batch("BEGIN")?;
        }

        if (*total_rows).is_multiple_of(args.progress_every) {
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

fn record_to_row<'a>(
    record: &'a StringRecord,
    rsi_count: usize,
    ignore_extra_columns: bool,
) -> Result<ParsedRow<'a>> {
    if record.len() < BINANCE_COLS {
        bail!(
            "row has {} columns, expected at least {}",
            record.len(),
            BINANCE_COLS
        );
    }

    let open_time = parse_i64(record, 0, "open_time")?;
    let open = parse_f64(record, 1, "open")?;
    let high = parse_f64(record, 2, "high")?;
    let low = parse_f64(record, 3, "low")?;
    let close = parse_f64(record, 4, "close")?;
    let volume = parse_f64(record, 5, "volume")?;
    let close_time = parse_i64(record, 6, "close_time")?;
    let quote_asset_volume = parse_f64(record, 7, "quote_asset_volume")?;
    let number_of_trades = parse_i64(record, 8, "number_of_trades")?;
    let taker_buy_base_asset_volume = parse_f64(record, 9, "taker_buy_base_asset_volume")?;
    let taker_buy_quote_asset_volume = parse_f64(record, 10, "taker_buy_quote_asset_volume")?;
    let ignore_col = record.get(11).unwrap_or("");

    let mut rsi_values = Vec::with_capacity(rsi_count);
    for i in 0..rsi_count {
        let idx = BINANCE_COLS + i;
        let value = match if ignore_extra_columns {
            None
        } else {
            record.get(idx)
        } {
            Some(s) if !s.trim().is_empty() => Some(s.parse::<f64>()?),
            _ => None,
        };
        rsi_values.push(value);
    }

    Ok(ParsedRow {
        open_time,
        open,
        high,
        low,
        close,
        volume,
        close_time,
        quote_asset_volume,
        number_of_trades,
        taker_buy_base_asset_volume,
        taker_buy_quote_asset_volume,
        ignore_col,
        rsi_values,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_covered_files_and_replaces_first_overlap_then_appends() {
        for mode in ["balanced", "safe"] {
            let root = std::env::temp_dir().join(format!(
                "csv-resume-{}-{}-{mode}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            fs::create_dir_all(&root).unwrap();
            let db = root.join("TEST.duckdb");
            let conn = Connection::open(&db).unwrap();
            create_table(&conn, "klines", 1, &[]).unwrap();
            conn.execute_batch("INSERT INTO klines VALUES (1,1,1,1,1,1,1,1,1,1,1,'0',50), (2,2,2,2,2,2,2,2,2,2,2,'0',60)").unwrap();
            drop(conn);
            let old = root.join("TEST-1s-2024-01.csv");
            let overlap = root.join("TEST-1s-2024-02.csv");
            let next = root.join("TEST-1s-2024-03.csv");
            // The old file has a malformed body, proving it is not scanned during resume.
            fs::write(&old, "bad historical data\n1,1,1,1,1,1,1,1,1,1,1,0\n").unwrap();
            fs::write(
                &overlap,
                "2,9,9,9,9,9,2,9,9,9,9,0\n3,3,3,3,3,3,3,3,3,3,3,0\n",
            )
            .unwrap();
            fs::write(&next, "4,4,4,4,4,4,4,4,4,4,4,0\n").unwrap();
            let args = Args::parse_from([
                "csv-to-duckdb",
                "--dir",
                root.to_str().unwrap(),
                "--ignore-extra-columns",
                "--import-mode",
                mode,
            ]);
            let job = ImportJob {
                dir: root.clone(),
                db_path: db.clone(),
                symbol: "TEST".into(),
                interval: "1s".into(),
                files: vec![
                    CsvFile {
                        path: old.clone(),
                        year: 2024,
                        month: 1,
                    },
                    CsvFile {
                        path: overlap,
                        year: 2024,
                        month: 2,
                    },
                    CsvFile {
                        path: next,
                        year: 2024,
                        month: 3,
                    },
                ],
            };
            run_import_job(&args, job, ProgressUi::new()).unwrap();
            let conn = Connection::open(&db).unwrap();
            assert_eq!(
                conn.query_row("SELECT count(*) FROM klines", [], |r| r.get::<_, i64>(0))
                    .unwrap(),
                4
            );
            assert_eq!(
                conn.query_row("SELECT close FROM klines WHERE open_time=2", [], |r| r
                    .get::<_, f64>(0))
                    .unwrap(),
                9.0
            );
            assert_eq!(
                conn.query_row(
                    "SELECT count(*) FROM pragma_table_info('klines') WHERE name='ignore_col'",
                    [],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
                0
            );
            assert!(old.exists());
            drop(conn);
            fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn validates_column_selection() {
        let args = Args::parse_from(["csv-to-duckdb", "--drop-columns", "ignore,volume"]);
        assert_eq!(dropped_columns(&args).unwrap(), vec![11, 5]);
        let args = Args::parse_from(["csv-to-duckdb", "--drop-columns", "close"]);
        assert!(dropped_columns(&args).is_err());
    }

    #[test]
    fn completed_file_is_committed_if_later_file_fails() {
        let root = std::env::temp_dir().join(format!(
            "csv-partial-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let db = root.join("TEST.duckdb");
        let first = root.join("TEST-1s-2024-01.csv");
        let second = root.join("TEST-1s-2024-02.csv");
        fs::write(&first, "1,1,1,1,1,1,1,1,1,1,1,0\n").unwrap();
        fs::write(&second, "2,invalid\n").unwrap();
        let args = Args::parse_from([
            "csv-to-duckdb",
            "--dir",
            root.to_str().unwrap(),
            "--report-file-progress",
            "--ignore-extra-columns",
        ]);
        let job = ImportJob {
            dir: root.clone(),
            db_path: db.clone(),
            symbol: "TEST".into(),
            interval: "1s".into(),
            files: vec![
                CsvFile {
                    path: first,
                    year: 2024,
                    month: 1,
                },
                CsvFile {
                    path: second,
                    year: 2024,
                    month: 2,
                },
            ],
        };
        assert!(run_import_job(&args, job, ProgressUi::new()).is_err());
        let conn = Connection::open(&db).unwrap();
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM klines", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1
        );
        drop(conn);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn ignore_extra_columns_handles_mixed_width_and_existing_rsi_schema() {
        let root = std::env::temp_dir().join(format!(
            "csv-raw-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let db = root.join("TEST.duckdb");
        let csv = root.join("TEST-1s-2024-01.csv");
        let conn = Connection::open(&db).unwrap();
        create_table(&conn, "klines", 1, &[]).unwrap();
        drop(conn);
        fs::write(
            &csv,
            "1,1,1,1,1,1,1,1,1,1,1,0,not-rsi\n2,2,2,2,2,2,2,2,2,2,2,0\n",
        )
        .unwrap();
        let args = Args::parse_from([
            "csv-to-duckdb",
            "--dir",
            root.to_str().unwrap(),
            "--ignore-extra-columns",
        ]);
        let job = ImportJob {
            dir: root.clone(),
            db_path: db.clone(),
            symbol: "TEST".into(),
            interval: "1s".into(),
            files: vec![CsvFile {
                path: csv.clone(),
                year: 2024,
                month: 1,
            }],
        };
        run_import_job(&args, job, ProgressUi::new()).unwrap();
        let conn = Connection::open(&db).unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM klines WHERE rsi_1 IS NULL",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            2
        );
        drop(conn);
        assert!(fs::read_to_string(csv).unwrap().contains("not-rsi"));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn explicit_replacement_reimports_covered_files() {
        let root = std::env::temp_dir().join(format!(
            "csv-duckdb-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let db = root.join("TEST.duckdb");
        let csv = root.join("TEST-1s-2024-01.csv");
        let conn = Connection::open(&db).unwrap();
        create_table(&conn, "klines", 1, &[]).unwrap();
        conn.execute_batch("INSERT INTO klines VALUES (1,1,1,1,1,1,1,1,1,1,1,'0',50), (3,3,3,3,3,3,3,3,3,3,3,'0',60)").unwrap();
        drop(conn);
        fs::write(
            &csv,
            "1,9,9,9,9,9,1,9,9,9,9,0\n2,2,2,2,2,2,2,2,2,2,2,0\n3,9,9,9,9,9,3,9,9,9,9,0\n",
        )
        .unwrap();
        let args = Args::parse_from(["csv-to-duckdb", "--dir", root.to_str().unwrap()]);
        let job = || ImportJob {
            dir: root.clone(),
            db_path: db.clone(),
            symbol: "TEST".into(),
            interval: "1s".into(),
            files: vec![CsvFile {
                path: csv.clone(),
                year: 2024,
                month: 1,
            }],
        };
        run_import_job(&args, job(), ProgressUi::new()).unwrap();
        let conn = Connection::open(&db).unwrap();
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM klines", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            conn.query_row("SELECT close FROM klines WHERE open_time=1", [], |r| r
                .get::<_, f64>(0))
                .unwrap(),
            1.0
        );
        drop(conn);
        let mut replace = args.clone();
        replace.replace_existing = true;
        run_import_job(&replace, job(), ProgressUi::new()).unwrap();
        let conn = Connection::open(&db).unwrap();
        assert_eq!(
            conn.query_row("SELECT close FROM klines WHERE open_time=1", [], |r| r
                .get::<_, f64>(0))
                .unwrap(),
            9.0
        );
        drop(conn);
        fs::remove_dir_all(root).unwrap();
    }
}
