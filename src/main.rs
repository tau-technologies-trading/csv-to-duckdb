use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use csv::{ReaderBuilder, StringRecord};
use duckdb::{Appender, Connection, appender_params_from_iter, types::ToSql};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const BINANCE_COLS: usize = 12;
const PROGRESS_FLUSH_ROWS: u64 = 8192;
const DEFAULT_SINGLE_DIR: &str = "../data/BTCUSDT/";
const DEFAULT_SINGLE_DB: &str = "../db/BTCUSDT/BTCUSDT.duckdb";
const DEFAULT_ALL_DIR: &str = "../data/";
const DEFAULT_ALL_DB_DIR: &str = "../db/";

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
    long_about = "Import Binance Vision CSV files into local DuckDB databases.\n\nProvide at least one option. The default single-symbol import reads matching files like BTCUSDT-1s-2026-04.csv from ../data/BTCUSDT/, creates ../db/BTCUSDT/BTCUSDT.duckdb, and imports rows into klines. With --all, it scans ../data/ recursively and mirrors every CSV directory under ../db/, creating one {symbol}.duckdb database for each CSV file group. Existing rows are skipped by default so interrupted imports can resume safely. Use --auto N for the newest N files, --recreate to rebuild from scratch, --recreate-pragmatic to rebuild only this table when other user tables exist, and --replace-existing to overwrite duplicate primary keys. Import mode controls the insert path: safe uses row-by-row prepared statements, balanced uses DuckDB's Appender with periodic flushes, and unsafe uses the Appender with minimal flushing.",
    version,
    disable_version_flag = true,
    after_help = "Examples:\n  csv-to-duckdb --auto 3\n  csv-to-duckdb -o ../db/BTCUSDT/BTCUSDT.duckdb --recreate\n  csv-to-duckdb --all\n  csv-to-duckdb --all --jobs 4\n  csv-to-duckdb --all -d ../data/ -o ../db/ --recreate-pragmatic\n  csv-to-duckdb --import-mode unsafe --batch-size 500000\n  csv-to-duckdb --has-header --replace-existing\n  csv-to-duckdb -d ../data/ETHUSDT --db ../db/ETHUSDT/ETHUSDT.duckdb --interval 1m --table eth_klines\n\nVerification:\n  duckdb ../db/BTCUSDT/BTCUSDT.duckdb 'SELECT COUNT(*) FROM klines;'\n  duckdb ../db/BTCUSDT/BTCUSDT.duckdb 'SELECT MIN(open_time), MAX(open_time) FROM klines;'\n\nNotes:\n  At least one option is required; run --auto, --all, --recreate, or an explicit path option to import.\n  Defaults: --dir ../data/BTCUSDT/, --db ../db/BTCUSDT/BTCUSDT.duckdb, --interval 1s, --table klines.\n  With --all and unchanged defaults: --dir ../data/, --db ../db/.\n  Each CSV directory/file group is imported into its own {symbol}.duckdb database.\n  Files must follow SYMBOL-INTERVAL-YYYY-MM.csv; symbols are inferred from filenames.\n  Imports the 12 Binance Vision kline columns plus any extra RSI columns.\n  Import modes: safe (prepared statements), balanced (Appender + periodic flushes), unsafe (Appender + minimal flushing).\n  --replace-existing forces prepared-statement inserts because DuckDB Appender does not support conflict handling."
)]
#[derive(Clone)]
struct Args {
    /// Directory containing files like BTCUSDT-1s-2026-04.csv
    #[arg(short, long, default_value = DEFAULT_SINGLE_DIR)]
    dir: PathBuf,

    /// Output DuckDB database path, or output root directory with --all
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

    /// Recursively import every CSV directory and mirror the directory structure
    #[arg(long, default_value_t = false)]
    all: bool,

    /// Number of CSV directories to process in parallel with --all
    #[arg(long, default_value_t = 1)]
    jobs: usize,

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
    ignore_col: String,
    rsi_values: Vec<Option<f64>>,
}

struct FileStats {
    row_count: usize,
    last_open_time: Option<i64>,
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

    let dir_arg_provided = arg_was_provided("-d", "--dir");
    let db_arg_provided = arg_was_provided("-o", "--db");
    let args = Args::parse();
    let _ = args.version_flag;

    validate_args(&args)?;
    validate_ident(&args.table)?;

    let mut args = args;
    if args.all {
        let cpus = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        if args.jobs > cpus {
            let old = args.jobs;
            args.jobs = cpus;
            eprintln!(
                "Warning: --jobs {} exceeds available system threads ({}); capped to {}",
                old, cpus, args.jobs
            );
        }
    }

    let jobs = build_import_jobs(&args, dir_arg_provided, db_arg_provided)?;
    if args.all {
        let max_parallel = args.jobs.min(jobs.len());
        println!(
            "Found {} import job(s). Processing up to {} folder(s) in parallel.",
            jobs.len(),
            max_parallel
        );
        run_all_import_jobs(args, jobs)?;
    } else {
        let progress_ui = ProgressUi::new();
        for job in jobs {
            run_import_job(&args, job, Arc::clone(&progress_ui))?;
        }
    }

    Ok(())
}

fn run_all_import_jobs(args: Args, jobs: Vec<ImportJob>) -> Result<()> {
    let max_parallel = args.jobs.min(jobs.len());
    let args = Arc::new(args);
    let progress_ui = ProgressUi::new();
    let mut handles = Vec::new();
    let mut job_iter = jobs.into_iter();

    for _ in 0..max_parallel {
        let Some(job) = job_iter.next() else {
            break;
        };
        let args = Arc::clone(&args);
        let ui = Arc::clone(&progress_ui);
        handles.push(thread::spawn(move || run_import_job(&args, job, ui)));
    }

    while let Some(handle) = handles.pop() {
        handle
            .join()
            .map_err(|e| anyhow::anyhow!("import thread panicked: {:?}", e))??;

        if let Some(job) = job_iter.next() {
            let args = Arc::clone(&args);
            let ui = Arc::clone(&progress_ui);
            handles.push(thread::spawn(move || run_import_job(&args, job, ui)));
        }
    }

    Ok(())
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

    let rsi_count = infer_max_rsi_columns(&job.files, args.has_header)?;
    let file_stats = collect_file_stats(&job.files, args.has_header)?;
    let expected_rows = file_stats
        .iter()
        .map(|stats| stats.row_count)
        .sum::<usize>();

    progress_ui.println(format!(
        "Importing {} {} from {} into {}",
        job.symbol,
        job.interval,
        job.dir.display(),
        job.db_path.display()
    ));
    progress_ui.println(format!(
        "Found {} file(s). Max RSI columns: {}. Expected rows: {}",
        job.files.len(),
        rsi_count,
        expected_rows
    ));
    progress_ui.println(format!(
        "Import mode: {:?}. Batch size: {}. Conflict mode: {}.",
        args.import_mode,
        args.batch_size,
        if args.replace_existing {
            "REPLACE"
        } else {
            "IGNORE"
        }
    ));

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

    apply_import_mode(&conn, args.import_mode)?;

    if recreate_action == RecreateAction::DropTableOnly {
        conn.execute_batch(&format!("DROP TABLE IF EXISTS {}", args.table))?;
        conn.execute_batch("CHECKPOINT")?;
        conn.execute_batch("VACUUM")?;
        conn.execute_batch("CHECKPOINT")?;
    }

    create_table(&conn, &args.table, rsi_count)?;
    let resume_open_time = if args.replace_existing {
        None
    } else {
        max_open_time(&conn, &args.table)?
    };

    let start = Instant::now();
    let mut total_rows = 0usize;
    let mut last_open_time: Option<i64> = None;
    let mut progress = progress_ui.start_job(&job.symbol, Some(expected_rows as u64));

    let use_appender = matches!(args.import_mode, ImportMode::Balanced | ImportMode::Unsafe)
        && !args.replace_existing;

    if use_appender {
        import_all_files_with_appender(
            &job.files,
            &file_stats,
            args,
            rsi_count,
            &conn,
            &mut total_rows,
            &mut last_open_time,
            start,
            &mut progress,
            resume_open_time,
        )?;
    } else {
        import_all_files_with_prepared_stmts(
            &job.files,
            &file_stats,
            args,
            rsi_count,
            &conn,
            &mut total_rows,
            &mut last_open_time,
            start,
            &mut progress,
            resume_open_time,
        )?;
    }

    progress.finish();

    let elapsed = start.elapsed().as_secs_f64();
    let average_rows_per_second = total_rows as f64 / elapsed.max(0.001);
    progress_ui.println(format!(
        "Done. Imported {} rows into {} in {:.1}s (average speed {:.0} rows/second)",
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
    total_rows: &mut usize,
    last_open_time: &mut Option<i64>,
    start: Instant,
    progress: &mut JobProgress,
    resume_open_time: Option<i64>,
) -> Result<()> {
    let mut appender = conn
        .appender(&args.table)
        .with_context(|| format!("Cannot create Appender for table {}", args.table))?;

    for (file, stats) in files.iter().zip(file_stats.iter()) {
        let file_name = file.path.file_name().unwrap().to_string_lossy().to_string();
        let mut progress_slot = progress.acquire_slot(&file_name, Some(stats.row_count as u64));

        if should_skip_file(stats, resume_open_time) {
            progress_slot.inc(stats.row_count as u64);
            progress_slot.finish();

            if let Some(file_last_open_time) = stats.last_open_time {
                *last_open_time = Some(file_last_open_time);
            }

            progress.println(format!("Imported {:>12} rows from {}", 0, file_name));
            continue;
        }

        let imported = import_file_with_appender(
            file,
            args,
            rsi_count,
            &mut appender,
            conn,
            total_rows,
            last_open_time,
            start,
            &progress_slot,
            resume_open_time,
        )?;
        progress_slot.finish();

        progress.println(format!("Imported {:>12} rows from {}", imported, file_name));
    }

    appender.flush()?;
    Ok(())
}

fn import_all_files_with_prepared_stmts(
    files: &[CsvFile],
    file_stats: &[FileStats],
    args: &Args,
    rsi_count: usize,
    conn: &Connection,
    total_rows: &mut usize,
    last_open_time: &mut Option<i64>,
    start: Instant,
    progress: &mut JobProgress,
    resume_open_time: Option<i64>,
) -> Result<()> {
    let insert_sql = build_insert_sql(&args.table, rsi_count, args.replace_existing);
    let mut stmt = conn.prepare(&insert_sql)?;

    conn.execute_batch("BEGIN")?;

    for (file, stats) in files.iter().zip(file_stats.iter()) {
        let file_name = file.path.file_name().unwrap().to_string_lossy().to_string();
        let mut progress_slot = progress.acquire_slot(&file_name, Some(stats.row_count as u64));

        if should_skip_file(stats, resume_open_time) {
            progress_slot.inc(stats.row_count as u64);
            progress_slot.finish();

            if let Some(file_last_open_time) = stats.last_open_time {
                *last_open_time = Some(file_last_open_time);
            }

            progress.println(format!("Imported {:>12} rows from {}", 0, file_name));
            continue;
        }

        let imported = import_file_with_prepared_stmt(
            file,
            args,
            rsi_count,
            &mut stmt,
            conn,
            total_rows,
            last_open_time,
            start,
            &progress_slot,
            resume_open_time,
        )?;
        progress_slot.finish();

        progress.println(format!("Imported {:>12} rows from {}", imported, file_name));
    }

    conn.execute_batch("COMMIT")?;
    Ok(())
}

fn print_version() {
    println!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
}

fn arg_was_provided(short: &str, long: &str) -> bool {
    std::env::args_os().skip(1).any(|arg| {
        arg == short || arg == long || arg.to_string_lossy().starts_with(&format!("{}=", long))
    })
}

fn validate_args(args: &Args) -> Result<()> {
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

fn build_import_jobs(
    args: &Args,
    dir_arg_provided: bool,
    db_arg_provided: bool,
) -> Result<Vec<ImportJob>> {
    if args.all {
        let dir = if dir_arg_provided {
            args.dir.clone()
        } else {
            PathBuf::from(DEFAULT_ALL_DIR)
        };
        let db_root = if db_arg_provided {
            PathBuf::from(&args.db)
        } else {
            PathBuf::from(DEFAULT_ALL_DB_DIR)
        };

        let mut jobs = Vec::new();
        collect_all_import_jobs(&dir, &dir, &db_root, &mut jobs)?;
        jobs.sort_by(|a, b| a.dir.cmp(&b.dir));

        if jobs.is_empty() {
            bail!("No CSV directories found in {}", dir.display());
        }

        return Ok(jobs);
    }

    Ok(vec![import_job_for_single_directory(
        &args.dir,
        PathBuf::from(&args.db),
        &args.interval,
    )?])
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

fn collect_all_import_jobs(
    root: &Path,
    dir: &Path,
    db_root: &Path,
    jobs: &mut Vec<ImportJob>,
) -> Result<()> {
    if let Some(job) = import_job_for_directory(root, dir, db_root)? {
        jobs.push(job);
    }

    for entry in fs::read_dir(dir).with_context(|| format!("Cannot read {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            collect_all_import_jobs(root, &path, db_root, jobs)?;
        }
    }

    Ok(())
}

fn import_job_for_directory(root: &Path, dir: &Path, db_root: &Path) -> Result<Option<ImportJob>> {
    let mut symbol: Option<String> = None;
    let mut interval: Option<String> = None;
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

        match &symbol {
            Some(existing) if existing != &parsed.0 => bail!(
                "Mixed symbols in {}: found both {} and {}",
                dir.display(),
                existing,
                parsed.0
            ),
            None => symbol = Some(parsed.0.clone()),
            _ => {}
        }

        match &interval {
            Some(existing) if existing != &parsed.1 => bail!(
                "Mixed intervals in {}: found both {} and {}",
                dir.display(),
                existing,
                parsed.1
            ),
            None => interval = Some(parsed.1.clone()),
            _ => {}
        }

        files.push(CsvFile {
            path,
            year: parsed.2,
            month: parsed.3,
        });
    }

    let Some(symbol) = symbol else {
        return Ok(None);
    };
    let interval = interval.context("missing interval for discovered CSV directory")?;
    let relative_dir = dir.strip_prefix(root).with_context(|| {
        format!(
            "Cannot map {} relative to input root {}",
            dir.display(),
            root.display()
        )
    })?;
    let db_path = db_root
        .join(relative_dir)
        .join(format!("{}.duckdb", symbol));

    Ok(Some(ImportJob {
        dir: dir.to_path_buf(),
        db_path,
        symbol,
        interval,
        files,
    }))
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

fn apply_import_mode(conn: &Connection, mode: ImportMode) -> Result<()> {
    match mode {
        ImportMode::Safe => {
            conn.execute_batch("PRAGMA synchronous = FULL")?;
        }
        ImportMode::Balanced => {
            conn.execute_batch("PRAGMA synchronous = NORMAL")?;
        }
        ImportMode::Unsafe => {
            conn.execute_batch("PRAGMA synchronous = OFF")?;
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

fn collect_file_stats(files: &[CsvFile], has_header: bool) -> Result<Vec<FileStats>> {
    files
        .iter()
        .map(|file| scan_file_stats(file, has_header))
        .collect()
}

fn scan_file_stats(file: &CsvFile, has_header: bool) -> Result<FileStats> {
    let file_handle =
        File::open(&file.path).with_context(|| format!("Cannot open {}", file.path.display()))?;
    let mut reader = BufReader::new(file_handle);
    let mut buffer = Vec::with_capacity(4096);
    let mut skipped_header = false;
    let mut row_count = 0usize;
    let mut last_open_time = None;

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
        last_open_time =
            Some(parse_open_time_bytes(line).with_context(|| {
                format!("Bad open_time while scanning {}", file.path.display())
            })?);
    }

    Ok(FileStats {
        row_count,
        last_open_time,
    })
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

fn should_skip_file(stats: &FileStats, resume_open_time: Option<i64>) -> bool {
    match (stats.last_open_time, resume_open_time) {
        (Some(file_last_open_time), Some(resume_open_time)) => {
            file_last_open_time <= resume_open_time
        }
        _ => false,
    }
}

fn max_open_time(conn: &Connection, table: &str) -> Result<Option<i64>> {
    let sql = format!("SELECT MAX(open_time) FROM {}", table);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query_map([], |row| row.get::<_, Option<i64>>(0))?;

    match rows.next() {
        Some(Ok(Some(open_time))) => Ok(Some(open_time)),
        Some(Ok(None)) => Ok(None),
        Some(Err(e)) => Err(anyhow::anyhow!("Failed to read MAX(open_time): {}", e)),
        None => Ok(None),
    }
}

fn create_table(conn: &Connection, table: &str, rsi_count: usize) -> Result<()> {
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

    for i in 1..=rsi_count {
        cols.push(format!("rsi_{} DOUBLE", i));
    }

    cols.push("PRIMARY KEY (open_time)".to_string());

    let sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", table, cols.join(", "));

    conn.execute_batch(&sql)?;
    Ok(())
}

fn build_insert_sql(table: &str, rsi_count: usize, replace_existing: bool) -> String {
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
    _conn: &Connection,
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

        if !args.skip_order_check {
            if let Some(prev_open_time) = *last_open_time {
                if open_time <= prev_open_time {
                    bail!(
                        "open_time is not strictly increasing: previous={}, current={} in {}",
                        prev_open_time,
                        open_time,
                        file.path.display()
                    );
                }
            }
        }

        let row = record_to_row(&record, rsi_count)
            .with_context(|| format!("Bad row in {}", file.path.display()))?;

        append_row_to_appender(appender, &row)?;

        *last_open_time = Some(row.open_time);
        file_rows += 1;
        *total_rows += 1;
        pending_progress += 1;

        if pending_progress >= PROGRESS_FLUSH_ROWS {
            progress.inc(pending_progress);
            pending_progress = 0;
        }

        if matches!(args.import_mode, ImportMode::Balanced) && *total_rows % args.batch_size == 0 {
            appender.flush()?;
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

fn append_row_to_appender(appender: &mut Appender, row: &ParsedRow) -> Result<()> {
    let mut params: Vec<Box<dyn ToSql>> = Vec::with_capacity(12 + row.rsi_values.len());
    params.push(Box::new(row.open_time) as Box<dyn ToSql>);
    params.push(Box::new(row.open) as Box<dyn ToSql>);
    params.push(Box::new(row.high) as Box<dyn ToSql>);
    params.push(Box::new(row.low) as Box<dyn ToSql>);
    params.push(Box::new(row.close) as Box<dyn ToSql>);
    params.push(Box::new(row.volume) as Box<dyn ToSql>);
    params.push(Box::new(row.close_time) as Box<dyn ToSql>);
    params.push(Box::new(row.quote_asset_volume) as Box<dyn ToSql>);
    params.push(Box::new(row.number_of_trades) as Box<dyn ToSql>);
    params.push(Box::new(row.taker_buy_base_asset_volume) as Box<dyn ToSql>);
    params.push(Box::new(row.taker_buy_quote_asset_volume) as Box<dyn ToSql>);
    params.push(Box::new(row.ignore_col.clone()) as Box<dyn ToSql>);

    for rsi in &row.rsi_values {
        params.push(Box::new(*rsi) as Box<dyn ToSql>);
    }

    let refs: Vec<&dyn ToSql> = params.iter().map(|v| v.as_ref()).collect();
    appender.append_row(appender_params_from_iter(refs.iter().copied()))?;
    Ok(())
}

fn import_file_with_prepared_stmt(
    file: &CsvFile,
    args: &Args,
    rsi_count: usize,
    stmt: &mut duckdb::Statement,
    conn: &Connection,
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

        let row = record_to_row(&record, rsi_count)
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

        let params = row_to_params(&row);
        let refs: Vec<&dyn ToSql> = params.iter().map(|v| v.as_ref()).collect();
        stmt.execute(duckdb::params_from_iter(refs.iter().copied()))?;

        *last_open_time = Some(row.open_time);
        file_rows += 1;
        *total_rows += 1;
        pending_progress += 1;

        if pending_progress >= PROGRESS_FLUSH_ROWS {
            progress.inc(pending_progress);
            pending_progress = 0;
        }

        if *total_rows % args.batch_size == 0 {
            conn.execute_batch("COMMIT")?;
            conn.execute_batch("BEGIN")?;
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

fn record_to_row(record: &StringRecord, rsi_count: usize) -> Result<ParsedRow> {
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
    let ignore_col = record.get(11).unwrap_or("").to_string();

    let mut rsi_values = Vec::with_capacity(rsi_count);
    for i in 0..rsi_count {
        let idx = BINANCE_COLS + i;
        let value = match record.get(idx) {
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

fn row_to_params(row: &ParsedRow) -> Vec<Box<dyn ToSql>> {
    let mut params: Vec<Box<dyn ToSql>> = Vec::with_capacity(12 + row.rsi_values.len());
    params.push(Box::new(row.open_time));
    params.push(Box::new(row.open));
    params.push(Box::new(row.high));
    params.push(Box::new(row.low));
    params.push(Box::new(row.close));
    params.push(Box::new(row.volume));
    params.push(Box::new(row.close_time));
    params.push(Box::new(row.quote_asset_volume));
    params.push(Box::new(row.number_of_trades));
    params.push(Box::new(row.taker_buy_base_asset_volume));
    params.push(Box::new(row.taker_buy_quote_asset_volume));
    params.push(Box::new(row.ignore_col.clone()));

    for rsi in &row.rsi_values {
        params.push(Box::new(*rsi));
    }

    params
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
