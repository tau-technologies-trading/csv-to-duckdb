use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use csv::{ReaderBuilder, StringRecord};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use turso::{Builder, Value, params_from_iter};

const BINANCE_COLS: usize = 12;
const PROGRESS_FLUSH_ROWS: u64 = 8192;
const DEFAULT_SINGLE_DIR: &str = "../data/BTCUSDT/";
const DEFAULT_SINGLE_DB: &str = "../db/BTCUSDT/BTCUSDT.db";
const DEFAULT_ALL_DIR: &str = "../data/";
const DEFAULT_ALL_DB_DIR: &str = "../db/";

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ImportMode {
    /// Maximum crash safety. Slower.
    Safe,

    /// Good import speed with reasonable durability. Recommended default.
    Balanced,

    /// Fastest import mode. Delete and rebuild the DB if the machine crashes.
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
    about = "Import Binance Vision CSV files into a local Turso database",
    long_about = "Import Binance Vision CSV files into local Turso databases.\n\nBy default this reads matching files like BTCUSDT-1s-2026-04.csv from ../data/BTCUSDT/, creates ../db/BTCUSDT/BTCUSDT.db, and imports rows into klines. With --all, it scans ../data/ recursively and mirrors every directory containing CSVs into ../db/, creating one {symbol}.db per CSV directory. Use --jobs N with --all to process up to N folders in parallel. Files within each folder are processed in year-month order. Existing rows are skipped by default so interrupted imports can be resumed safely. Use --auto N to import only the newest N matching CSV files per job, --recreate to delete DB file families and rebuild from scratch, --recreate-pragmatic to rebuild only this table unless it is the DB's only user table, --replace-existing to overwrite duplicate primary keys, and --import-mode unsafe only when you are willing to delete and rebuild databases after a crash.",
    version,
    disable_version_flag = true,
    after_help = "Examples:\n  csv-to-turso\n  csv-to-turso --auto 3\n  csv-to-turso -o ../db/BTCUSDT/BTCUSDT.db --recreate\n  csv-to-turso --all\n  csv-to-turso --all --jobs 4\n  csv-to-turso --all -d ../data/ -o ../db/ --recreate-pragmatic\n  csv-to-turso --import-mode unsafe --batch-size 500000\n  csv-to-turso --has-header --replace-existing\n  csv-to-turso -d ../data/ETHUSDT --db ../db/ETHUSDT/ETHUSDT.db --interval 1m --table eth_klines\n\nVerification:\n  tursodb --readonly ../db/BTCUSDT/BTCUSDT.db 'SELECT COUNT(*) FROM klines;'\n  tursodb --readonly ../db/BTCUSDT/BTCUSDT.db 'SELECT MIN(open_time), MAX(open_time) FROM klines;'\n\nNotes:\n  Defaults: --dir ../data/BTCUSDT/, --db ../db/BTCUSDT/BTCUSDT.db, --interval 1s, --table klines.\n  Symbols are inferred from CSV filenames.\n  With --all and unchanged defaults: --dir ../data/, --db ../db/.\n  Defaults: --batch-size 250000, --progress-every 1000000, --import-mode balanced, --jobs 1.\n  Imports the 12 Binance Vision kline columns plus any extra generated RSI columns.\n  --all recursively mirrors the input directory structure and creates one {symbol}.db per directory containing CSVs.\n  --jobs N only affects --all and processes up to N CSV directories in parallel; files inside each directory remain sequential.\n  --auto N imports only the newest N matching CSV files per job and cannot be combined with recreate options.\n  For 120M-row imports, use --import-mode balanced first; use unsafe only if rebuilding after a crash is acceptable.\n  --recreate deletes the DB, WAL, and SHM files before importing.\n  --recreate-pragmatic deletes the DB file family only when the requested table is the only user table; otherwise it drops that table, vacuums, and truncates WAL.\n  --replace-existing rewrites duplicate primary keys; without it duplicates are ignored.\n  --skip-order-check allows non-increasing open_time values, but only use it when the files are intentionally unordered."
)]
#[derive(Clone)]
struct Args {
    /// Directory containing files like BTCUSDT-1s-2026-04.csv
    #[arg(short, long, default_value = DEFAULT_SINGLE_DIR)]
    dir: PathBuf,

    /// Output Turso database path, or output root directory with --all
    #[arg(short = 'o', long, default_value = DEFAULT_SINGLE_DB)]
    db: String,

    /// Interval per row
    #[arg(short, long, default_value = "1s")]
    interval: String,

    /// SQL table name
    #[arg(short, long, default_value = "klines")]
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

    /// Delete DB/WAL/SHM files and recreate from scratch before importing
    #[arg(long, default_value_t = false)]
    recreate: bool,

    /// Recreate efficiently while preserving other user tables when present
    #[arg(long, default_value_t = false)]
    recreate_pragmatic: bool,

    /// Import durability/speed mode
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
    values: Vec<Value>,
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

fn truncate_progress_prefix(name: &str) -> String {
    const MAX_LEN: usize = 7;
    if name.chars().count() <= MAX_LEN {
        return name.to_owned();
    }

    name.chars().take(MAX_LEN).collect()
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

    let dir_arg_provided = arg_was_provided("-d", "--dir");
    let db_arg_provided = arg_was_provided("-o", "--db");
    let args = Args::parse();
    let _ = args.version_flag;

    validate_args(&args)?;
    validate_ident(&args.table)?;

    let jobs = build_import_jobs(&args, dir_arg_provided, db_arg_provided)?;
    if args.all {
        let max_parallel = args.jobs.min(jobs.len());
        println!(
            "Found {} import job(s). Processing up to {} folder(s) in parallel.",
            jobs.len(),
            max_parallel
        );
        run_all_import_jobs(args, jobs).await?;
    } else {
        let progress_ui = ProgressUi::new();
        for job in jobs {
            run_import_job(&args, job, Arc::clone(&progress_ui)).await?;
        }
    }

    Ok(())
}

async fn run_all_import_jobs(args: Args, jobs: Vec<ImportJob>) -> Result<()> {
    let max_parallel = args.jobs.min(jobs.len());
    let args = Arc::new(args);
    let progress_ui = ProgressUi::new();
    let mut jobs = jobs.into_iter();
    let mut running = tokio::task::JoinSet::new();

    for _ in 0..max_parallel {
        let Some(job) = jobs.next() else {
            break;
        };
        spawn_import_job(
            &mut running,
            Arc::clone(&args),
            job,
            Arc::clone(&progress_ui),
        );
    }

    while let Some(result) = running.join_next().await {
        result.context("import task failed to complete")??;

        if let Some(job) = jobs.next() {
            spawn_import_job(
                &mut running,
                Arc::clone(&args),
                job,
                Arc::clone(&progress_ui),
            );
        }
    }

    Ok(())
}

fn spawn_import_job(
    running: &mut tokio::task::JoinSet<Result<()>>,
    args: Arc<Args>,
    job: ImportJob,
    progress_ui: Arc<ProgressUi>,
) {
    running.spawn(async move { run_import_job(&args, job, progress_ui).await });
}

async fn run_import_job(
    args: &Args,
    mut job: ImportJob,
    progress_ui: Arc<ProgressUi>,
) -> Result<()> {
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
    let recreate_action = determine_recreate_action(args, &db_path).await?;
    if recreate_action == RecreateAction::DeleteDatabaseFiles {
        remove_database_files(&db_path)?;
    }

    let db = Builder::new_local(&db_path).build().await?;
    let conn = db.connect()?;

    apply_import_mode(&conn, args.import_mode).await?;

    if recreate_action == RecreateAction::DropTableOnly {
        conn.execute(&format!("DROP TABLE IF EXISTS {}", args.table), ())
            .await?;
        apply_wal_checkpoint_truncate(&conn).await?;
        conn.execute("VACUUM", ()).await?;
        apply_wal_checkpoint_truncate(&conn).await?;
    }

    create_table(&conn, &args.table, rsi_count).await?;
    let resume_open_time = if args.replace_existing {
        None
    } else {
        max_open_time(&conn, &args.table).await?
    };

    let insert_sql = build_insert_sql(&args.table, rsi_count, args.replace_existing);
    let mut stmt = conn.prepare(&insert_sql).await?;

    let start = Instant::now();
    let mut total_rows = 0usize;
    let mut last_open_time: Option<i64> = None;
    let mut progress = progress_ui.start_job(&job.symbol, Some(expected_rows as u64));

    conn.execute("BEGIN", ()).await?;

    for (file, stats) in job.files.iter().zip(file_stats.iter()) {
        let file_name = file.path.file_name().unwrap().to_string_lossy().to_string();
        let mut progress_slot = progress.acquire_slot(&file_name, Some(stats.row_count as u64));

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
            args,
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

    progress.finish();

    let elapsed = start.elapsed().as_secs_f64();
    let average_rows_per_second = total_rows as f64 / elapsed.max(0.001);
    progress_ui.println(format!(
        "Done. Imported {} rows into {} in {:.1}s (average speed {:.0} rows/second)",
        total_rows, db_path, elapsed, average_rows_per_second
    ));

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
    let db_path = db_root.join(relative_dir).join(format!("{}.db", symbol));

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

async fn determine_recreate_action(args: &Args, db_path: &str) -> Result<RecreateAction> {
    if args.recreate {
        return Ok(RecreateAction::DeleteDatabaseFiles);
    }

    if !args.recreate_pragmatic {
        return Ok(RecreateAction::None);
    }

    let table_names = existing_user_tables(db_path).await?;
    if table_names.is_empty() || (table_names.len() == 1 && table_names[0] == args.table) {
        return Ok(RecreateAction::DeleteDatabaseFiles);
    }

    Ok(RecreateAction::DropTableOnly)
}

async fn existing_user_tables(db_path: &str) -> Result<Vec<String>> {
    if !Path::new(db_path).exists() {
        return Ok(Vec::new());
    }

    let db = Builder::new_local(db_path).build().await?;
    let conn = db.connect()?;
    let mut rows = conn
        .query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
            (),
        )
        .await?;

    let mut table_names = Vec::new();
    while let Some(row) = rows.next().await? {
        match row.get_value(0)? {
            Value::Text(table_name) => table_names.push(table_name),
            other => bail!("Unexpected sqlite_master table name value: {:?}", other),
        }
    }

    Ok(table_names)
}

fn remove_database_files(db_path: &str) -> Result<()> {
    for path in database_file_family(db_path) {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => {
                return Err(err).with_context(|| format!("Cannot remove {}", path.display()));
            }
        }
    }

    Ok(())
}

fn database_file_family(db_path: &str) -> [PathBuf; 3] {
    [
        PathBuf::from(db_path),
        PathBuf::from(format!("{}-wal", db_path)),
        PathBuf::from(format!("{}-shm", db_path)),
    ]
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

async fn apply_wal_checkpoint_truncate(conn: &turso::Connection) -> Result<()> {
    let mut rows = conn.query("PRAGMA wal_checkpoint(TRUNCATE)", ()).await?;
    while rows.next().await?.is_some() {}
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

async fn max_open_time(conn: &turso::Connection, table: &str) -> Result<Option<i64>> {
    let sql = format!("SELECT MAX(open_time) FROM {}", table);
    let mut rows = conn.query(&sql, ()).await?;

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

    cols.push("PRIMARY KEY (open_time)".to_string());

    let sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", table, cols.join(", "));

    conn.execute(&sql, ()).await?;
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

fn record_to_row(record: &StringRecord, rsi_count: usize) -> Result<ParsedRow> {
    if record.len() < BINANCE_COLS {
        bail!(
            "row has {} columns, expected at least {}",
            record.len(),
            BINANCE_COLS
        );
    }

    let open_time = parse_i64(record, 0, "open_time")?;
    // 12 is the Binance Vision kline column count; RSI columns are optional extras.
    let mut values = Vec::with_capacity(BINANCE_COLS + rsi_count);

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
