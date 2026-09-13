//! Download engine: probe, worker pool, verified Range I/O, resume, merge, cancel.

use std::collections::VecDeque;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use indicatif::{MultiProgress, ProgressBar};
use reqwest::header::{
    ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, ETAG, IF_RANGE, LAST_MODIFIED, RANGE, RETRY_AFTER,
};
use reqwest::{Client, StatusCode};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{watch, Mutex};
use tracing::warn;

use crate::disk::check_disk_space;
use crate::progress::progress_line;
use crate::ranges::{
    apply_key_to_url, chunk_file_name, has_gzip_magic, looks_like_blockchair_addresses_tsv,
    merge_temp_path, parse_content_range, parts_dir, range_len, redact_url, split_ranges,
};
use crate::retry::{
    backoff_delay, classify_status, jitter_seed, parse_retry_after, ClassifiedStatus,
    DEFAULT_INITIAL_DELAY_MS, DEFAULT_MAX_402_RETRIES, DEFAULT_MAX_DELAY_MS,
};
use crate::ui;
use crate::validate::{
    validate_chunk_mib, validate_connections, validate_output, validate_retries, validate_url,
};

/// Raised on Ctrl+C / SIGTERM. Partial `.parts` are preserved.
#[derive(Debug)]
pub struct Interrupted;

impl std::fmt::Display for Interrupted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "interrupted; partial files preserved for resume. Run the same command again to continue."
        )
    }
}

impl std::error::Error for Interrupted {}

pub fn is_interrupted(err: &anyhow::Error) -> bool {
    err.downcast_ref::<Interrupted>().is_some()
}

pub fn payment_required_error() -> anyhow::Error {
    anyhow!(
        "HTTP 402 Payment Required.\n\
         The remote server/CDN rejected this request.\n\
         Check whether a valid Blockchair API/download key is required.\n\
         Set BLOCKCHAIR_KEY (environment / GitHub Actions secret) or pass --key.\n\
         HTTP 402 is a server-side access/payment/policy response; lowering --connections \
         does not magically fix a genuine 402.\n\
         Stopping instead of endlessly retrying."
    )
}

#[derive(Debug, Clone)]
pub struct DownloadConfig {
    pub url: String,
    pub output: PathBuf,
    pub connections: u32,
    pub chunk_mib: u64,
    /// Optional explicit chunk size in bytes (tests). Overrides `chunk_mib` when `Some`.
    pub chunk_bytes: Option<u64>,
    pub retries: u32,
    pub timeout_secs: u64,
    pub key: String,
    pub force: bool,
    pub no_resume: bool,
    pub verify_gzip: bool,
    pub decompress: bool,
    pub no_progress: bool,
}

#[derive(Debug, Clone, Default)]
struct Probe {
    total: Option<u64>,
    supports_range: bool,
    etag: Option<String>,
    last_modified: Option<String>,
}

struct Shared {
    downloaded: AtomicU64,
    stop: AtomicBool,
}

pub fn build_client(read_timeout_secs: u64, connections: usize) -> Result<Client> {
    let version = env!("CARGO_PKG_VERSION");
    let mut builder = Client::builder()
        .user_agent(format!(
            "bcdownloader/{version} (+https://github.com/merajal746-oss/bcdwonloader)"
        ))
        .connect_timeout(Duration::from_secs(30))
        .tcp_keepalive(Duration::from_secs(30))
        .tcp_nodelay(true)
        .pool_max_idle_per_host(connections.max(2))
        .http1_only()
        .redirect(reqwest::redirect::Policy::limited(10));
    // Never set an overall request timeout: a 32 MiB chunk at ~10 kB/s needs ~55 minutes.
    if read_timeout_secs > 0 {
        builder = builder.read_timeout(Duration::from_secs(read_timeout_secs));
    }
    builder.build().context("building HTTP client")
}

pub fn spawn_signal_handler(tx: watch::Sender<bool>) {
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        eprintln!();
        eprintln!("Received interrupt (Ctrl+C / SIGTERM). Stopping; .parts preserved.");
        let _ = tx.send(true);
    });
}

async fn wait_for_shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(s) => s,
                Err(_) => {
                    let _ = ctrl_c.await;
                    return;
                }
            };
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}

fn stopped(cancel: &watch::Receiver<bool>, stop: &AtomicBool) -> bool {
    stop.load(Ordering::Relaxed) || *cancel.borrow()
}

pub async fn download(cfg: DownloadConfig, cancel: watch::Receiver<bool>) -> Result<()> {
    validate_url(&cfg.url)?;
    validate_output(&cfg.output)?;
    let connections = validate_connections(cfg.connections)?;
    let chunk_size = match cfg.chunk_bytes {
        Some(n) if n >= 1 => n,
        Some(n) => {
            return Err(anyhow!("chunk size must be >= 1 byte (got {n})"));
        }
        None => validate_chunk_mib(cfg.chunk_mib)?,
    };
    let retries = validate_retries(i64::from(cfg.retries))?;

    let url = apply_key_to_url(&cfg.url, &cfg.key)?;
    let display_url = redact_url(&url);
    let has_key = !cfg.key.trim().is_empty()
        || url::Url::parse(&cfg.url)
            .ok()
            .map(|u| u.query_pairs().any(|(k, _)| k == "key"))
            .unwrap_or(false);

    if let Some(parent) = cfg.output.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("create parent directory {}", parent.display()))?;
        }
    }

    let client = build_client(cfg.timeout_secs, connections)?;
    let probe = probe(&client, &url, &cancel).await?;
    let total = probe.total.unwrap_or(0);

    println!("Starting download");
    println!("URL: {display_url}");
    if total > 0 {
        println!(
            "Total size: {:.2} MiB ({total} bytes)",
            crate::progress::bytes_as_mib(total)
        );
    } else {
        println!("Total size: unknown");
    }
    println!(
        "Chunk size: {:.2} MiB ({chunk_size} bytes)",
        crate::progress::bytes_as_mib(chunk_size)
    );
    println!("Connections: {connections}");
    println!("Output: {}", cfg.output.display());
    if has_key {
        println!("Key: yes (value hidden)");
    } else {
        println!("Key: no (Blockchair may throttle ~10 kB/s per connection)");
    }

    if stopped(&cancel, &AtomicBool::new(false)) {
        return Err(Interrupted.into());
    }

    if !cfg.force {
        if let Ok(m) = fs::metadata(&cfg.output).await {
            if let Some(t) = probe.total {
                if m.len() == t {
                    println!("already complete: {} ({t} bytes)", cfg.output.display());
                    return Ok(());
                }
            } else if m.len() > 0 {
                println!(
                    "output exists ({} bytes, upstream size unknown) — use --force to re-download",
                    m.len()
                );
                return Ok(());
            }
        }
    } else if cfg.output.exists() {
        warn!("--force: existing output will be replaced on success");
    }

    if total == 0 && probe.supports_range {
        tokio::fs::File::create(&cfg.output)
            .await
            .with_context(|| format!("create empty {}", cfg.output.display()))?;
        println!("upstream file is empty; created {}", cfg.output.display());
        return Ok(());
    }

    let started = Instant::now();
    let interactive = !cfg.no_progress && std::io::stderr().is_terminal();
    let mp = MultiProgress::new();
    if !interactive {
        mp.set_draw_target(indicatif::ProgressDrawTarget::hidden());
    }
    let overall = ui::add_overall(&mp, total);
    overall.set_message("downloading");

    let use_parallel = probe.supports_range && probe.total.is_some();
    if !probe.supports_range && connections > 1 {
        eprintln!("server does not advertise byte ranges; falling back to a single stream");
    }

    let result = if use_parallel {
        parallel_download(
            &client,
            &url,
            &cfg,
            &probe,
            connections,
            chunk_size,
            retries,
            &cancel,
            &mp,
            overall.clone(),
            interactive,
        )
        .await
    } else {
        single_stream(
            &client,
            &url,
            &cfg,
            &probe,
            retries,
            &cancel,
            overall.clone(),
        )
        .await
    };

    if let Err(e) = result {
        overall.abandon_with_message("failed");
        return Err(e);
    }

    overall.finish_with_message("done");
    let final_len = fs::metadata(&cfg.output).await?.len();
    ui::print_summary(
        &cfg.output.to_string_lossy(),
        final_len,
        started.elapsed().as_secs(),
    );

    if cfg.verify_gzip {
        println!("verifying gzip integrity (full read)...");
        match verify_gzip_full(cfg.output.clone()).await {
            Ok(n) => println!("gzip OK, decompressed ~{n} bytes"),
            Err(e) => {
                return Err(anyhow!(
                    "gzip verification failed: {e:#}\nFile left at {} for diagnosis",
                    cfg.output.display()
                ));
            }
        }
    }

    if cfg.decompress {
        decompress_output(&cfg.output).await?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn parallel_download(
    client: &Client,
    url: &str,
    cfg: &DownloadConfig,
    probe: &Probe,
    connections: usize,
    chunk_size: u64,
    retries: u32,
    cancel: &watch::Receiver<bool>,
    mp: &MultiProgress,
    overall: ProgressBar,
    interactive: bool,
) -> Result<()> {
    let total = probe.total.unwrap();
    let ranges = split_ranges(total, chunk_size);
    let parts = parts_dir(&cfg.output);
    let tmp = merge_temp_path(&cfg.output);

    if cfg.force || cfg.no_resume {
        if parts.exists() {
            fs::remove_dir_all(&parts).await.ok();
        }
        if tmp.exists() {
            fs::remove_file(&tmp).await.ok();
        }
    }
    fs::create_dir_all(&parts)
        .await
        .with_context(|| format!("create parts dir {}", parts.display()))?;

    let mut already = 0u64;
    let mut pending: VecDeque<(u64, u64)> = VecDeque::new();
    for (start, end) in &ranges {
        let dest = parts.join(chunk_file_name(*start));
        let expected = range_len(*start, *end);
        match fs::metadata(&dest).await {
            Ok(m) if m.len() == expected => already += expected,
            Ok(m) if m.len() > expected => {
                eprintln!(
                    "chunk {} is oversized ({} > {expected}); deleting and re-downloading",
                    dest.display(),
                    m.len()
                );
                fs::remove_file(&dest).await.ok();
                pending.push_back((*start, *end));
            }
            Ok(m) => {
                already += m.len();
                pending.push_back((*start, *end));
            }
            Err(_) => pending.push_back((*start, *end)),
        }
    }

    check_disk_space(&cfg.output, total, already)?;
    overall.set_length(total);
    overall.set_position(already);

    let shared = Arc::new(Shared {
        downloaded: AtomicU64::new(already),
        stop: AtomicBool::new(false),
    });

    let printer = spawn_progress_printer(
        Arc::clone(&shared),
        total,
        mp.clone(),
        overall.clone(),
        cancel.clone(),
    );

    if pending.is_empty() {
        println!("all chunks already complete; merging");
    } else {
        println!(
            "Downloading {} remaining chunk(s) of {} with {connections} worker(s)",
            pending.len(),
            ranges.len()
        );
    }

    let jobs = Arc::new(Mutex::new(pending));
    let mut set = tokio::task::JoinSet::new();
    let workers = connections.min(ranges.len()).max(1);
    for worker_id in 0..workers {
        let client = client.clone();
        let url = url.to_string();
        let jobs = Arc::clone(&jobs);
        let parts = parts.clone();
        let shared = Arc::clone(&shared);
        let cancel = cancel.clone();
        let overall = overall.clone();
        let etag = probe.etag.clone();
        let last_modified = probe.last_modified.clone();
        // Beyond MAX_CHUNK_BARS workers run headless; TOTAL still tracks them.
        let chunk_pb = if interactive && worker_id < ui::MAX_CHUNK_BARS {
            let pb = ui::add_chunk(mp, format!("w{worker_id}"));
            Some(pb)
        } else {
            None
        };
        set.spawn(async move {
            worker_loop(WorkerCtx {
                worker_id,
                client,
                url,
                jobs,
                parts,
                retries,
                total,
                shared,
                cancel,
                overall,
                chunk_pb,
                etag,
                last_modified,
            })
            .await
        });
    }

    let mut first_err: Option<anyhow::Error> = None;
    while let Some(joined) = set.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                shared.stop.store(true, Ordering::Relaxed);
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(e) => {
                shared.stop.store(true, Ordering::Relaxed);
                if first_err.is_none() {
                    first_err = Some(anyhow!("worker task panicked: {e}"));
                }
            }
        }
    }
    let _ = printer.send(());
    if let Some(e) = first_err {
        return Err(e);
    }
    if *cancel.borrow() {
        return Err(Interrupted.into());
    }

    for (start, end) in &ranges {
        let dest = parts.join(chunk_file_name(*start));
        let expected = range_len(*start, *end);
        let got = fs::metadata(&dest)
            .await
            .with_context(|| format!("missing chunk {}", dest.display()))?
            .len();
        if got != expected {
            return Err(anyhow!(
                "chunk {} incomplete: {got} vs {expected} bytes (range {start}-{end})",
                dest.display()
            ));
        }
    }

    overall.set_message("merging");
    println!("Merging {} chunks into {} ...", ranges.len(), tmp.display());
    if tmp.exists() {
        fs::remove_file(&tmp).await.ok();
    }
    merge_chunks(&ranges, &parts, &tmp, cancel).await?;
    let merged = fs::metadata(&tmp).await?.len();
    if merged != total {
        return Err(anyhow!(
            "merged size {merged} != upstream {total}; temp file left at {}",
            tmp.display()
        ));
    }

    overall.set_message("verifying");
    if wants_gzip(&cfg.output) {
        if let Err(e) = verify_gzip_magic_quick(&tmp) {
            return Err(anyhow!(
                "{e:#}\nMerged file left at {}; parts left at {}",
                tmp.display(),
                parts.display()
            ));
        }
    }

    atomic_rename(&tmp, &cfg.output).await?;
    fs::remove_dir_all(&parts).await.ok();
    Ok(())
}

struct WorkerCtx {
    worker_id: usize,
    client: Client,
    url: String,
    jobs: Arc<Mutex<VecDeque<(u64, u64)>>>,
    parts: PathBuf,
    retries: u32,
    total: u64,
    shared: Arc<Shared>,
    cancel: watch::Receiver<bool>,
    overall: ProgressBar,
    chunk_pb: Option<ProgressBar>,
    etag: Option<String>,
    last_modified: Option<String>,
}

async fn worker_loop(ctx: WorkerCtx) -> Result<()> {
    loop {
        if stopped(&ctx.cancel, &ctx.shared.stop) {
            return if *ctx.cancel.borrow() {
                Err(Interrupted.into())
            } else {
                Ok(())
            };
        }
        let job = {
            let mut q = ctx.jobs.lock().await;
            q.pop_front()
        };
        let Some((start, end)) = job else {
            if let Some(pb) = &ctx.chunk_pb {
                pb.finish_and_clear();
            }
            return Ok(());
        };
        let dest = ctx.parts.join(chunk_file_name(start));
        println!("Downloading chunk {start}-{end} (worker {})", ctx.worker_id);
        let _ = std::io::Write::flush(&mut std::io::stdout());
        if let Some(pb) = &ctx.chunk_pb {
            pb.set_length(range_len(start, end));
            pb.set_message(format!("{start}-{end}"));
        }
        let job = RangeJob {
            client: ctx.client.clone(),
            url: ctx.url.clone(),
            start,
            end,
            dest,
            retries: ctx.retries,
            total: ctx.total,
            shared: Arc::clone(&ctx.shared),
            cancel: ctx.cancel.clone(),
            overall: ctx.overall.clone(),
            chunk_pb: ctx.chunk_pb.clone(),
            etag: ctx.etag.clone(),
            last_modified: ctx.last_modified.clone(),
        };
        if let Err(e) = job.run().await {
            ctx.shared.stop.store(true, Ordering::Relaxed);
            return Err(e.context(format!("chunk {start}-{end}")));
        }
    }
}

struct RangeJob {
    client: Client,
    url: String,
    start: u64,
    end: u64,
    dest: PathBuf,
    retries: u32,
    total: u64,
    shared: Arc<Shared>,
    cancel: watch::Receiver<bool>,
    overall: ProgressBar,
    chunk_pb: Option<ProgressBar>,
    etag: Option<String>,
    last_modified: Option<String>,
}

impl RangeJob {
    async fn run(self) -> Result<()> {
        let expected = range_len(self.start, self.end);
        let mut attempt: u32 = 0;
        let mut hits_402: u32 = 0;

        loop {
            if *self.cancel.borrow() {
                return Err(Interrupted.into());
            }
            if self.shared.stop.load(Ordering::Relaxed) {
                return Ok(());
            }

            let have = match fs::metadata(&self.dest).await {
                Ok(m) => m.len(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
                Err(e) => return Err(anyhow!("stat {}: {e}", self.dest.display())),
            };

            if have == expected {
                if let Some(c) = &self.chunk_pb {
                    c.set_length(expected);
                    c.set_position(expected);
                    c.finish_with_message("cached");
                }
                return Ok(());
            }
            if have > expected {
                warn!(
                    "{} larger than expected ({have} > {expected}), deleting",
                    self.dest.display()
                );
                fs::remove_file(&self.dest).await.ok();
                continue;
            }

            let from = self.start + have;
            let range_header = format!("bytes={from}-{}", self.end);

            let mut req = self
                .client
                .get(&self.url)
                .header(RANGE, range_header.clone());
            if let Some(etag) = &self.etag {
                req = req.header(IF_RANGE, etag);
            } else if let Some(lm) = &self.last_modified {
                req = req.header(IF_RANGE, lm);
            }

            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    if *self.cancel.borrow() {
                        return Err(Interrupted.into());
                    }
                    if self.shared.stop.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    if attempt >= self.retries {
                        return Err(anyhow!(
                            "GET {range_header} failed after {} retries: {e:#}",
                            self.retries
                        ));
                    }
                    let delay = backoff_delay(
                        attempt,
                        DEFAULT_INITIAL_DELAY_MS,
                        DEFAULT_MAX_DELAY_MS,
                        jitter_seed(),
                    );
                    log_chunk_failure(
                        "network error",
                        None,
                        &range_header,
                        attempt,
                        self.retries,
                        delay,
                    );
                    eprintln!("  detail: {e:#}");
                    sleep_or_cancel(delay, &self.cancel, &self.shared.stop).await?;
                    attempt += 1;
                    continue;
                }
            };

            let status = resp.status();
            let headers = resp.headers().clone();

            match classify_status(status) {
                ClassifiedStatus::RangeNotSatisfiable416 => {
                    drop(resp);
                    if have == expected {
                        return Ok(());
                    }
                    let cr_total = headers
                        .get(CONTENT_RANGE)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.rsplit('/').next())
                        .and_then(|t| t.trim().parse::<u64>().ok());
                    if let Some(t) = cr_total {
                        if from >= t && have > 0 {
                            return Err(anyhow!(
                                "HTTP 416 Range Not Satisfiable for {range_header}; \
                                 requested bytes are past upstream size {t}. Delete .parts and retry."
                            ));
                        }
                    }
                    return Err(anyhow!(
                        "HTTP 416 Range Not Satisfiable for {range_header}; \
                         local chunk has {have}/{expected} bytes. Delete the incomplete part and retry."
                    ));
                }
                ClassifiedStatus::Precondition412 => {
                    drop(resp);
                    return Err(anyhow!(
                        "HTTP 412 Precondition Failed for {range_header}. \
                         The remote file changed (ETag / Last-Modified). Delete .parts and retry."
                    ));
                }
                ClassifiedStatus::Payment402 => {
                    drop(resp);
                    hits_402 += 1;
                    eprintln!("HTTP 402 Payment Required");
                    eprintln!("The remote server rejected the request.");
                    eprintln!("Check Blockchair access credentials/key.");
                    eprintln!("Range: {range_header}");
                    if hits_402 > DEFAULT_MAX_402_RETRIES {
                        eprintln!("Stopping instead of endlessly retrying.");
                        return Err(payment_required_error());
                    }
                    let delay = Duration::from_secs(15 * u64::from(hits_402));
                    eprintln!("Retry: {hits_402}/{DEFAULT_MAX_402_RETRIES}");
                    eprintln!("Backoff: {}s", delay.as_secs());
                    sleep_or_cancel(delay, &self.cancel, &self.shared.stop).await?;
                    continue;
                }
                ClassifiedStatus::RateLimit429 => {
                    let ra = headers
                        .get(RETRY_AFTER)
                        .and_then(|v| v.to_str().ok())
                        .and_then(parse_retry_after);
                    drop(resp);
                    if attempt >= self.retries {
                        return Err(anyhow!(
                            "HTTP 429 Too Many Requests for {range_header} after {} retries",
                            self.retries
                        ));
                    }
                    let delay = ra.unwrap_or_else(|| {
                        backoff_delay(
                            attempt,
                            DEFAULT_INITIAL_DELAY_MS,
                            DEFAULT_MAX_DELAY_MS,
                            jitter_seed(),
                        )
                    });
                    log_chunk_failure(
                        "429",
                        Some(status),
                        &range_header,
                        attempt,
                        self.retries,
                        delay,
                    );
                    sleep_or_cancel(delay, &self.cancel, &self.shared.stop).await?;
                    attempt += 1;
                    continue;
                }
                ClassifiedStatus::Fatal(code) => {
                    drop(resp);
                    if code == 401 || code == 403 {
                        return Err(anyhow!(
                            "HTTP {code} for {range_header}. Access denied. \
                             Check BLOCKCHAIR_KEY / --key if the dump requires authentication."
                        ));
                    }
                    return Err(anyhow!("HTTP {code} for {range_header} (not retrying)"));
                }
                ClassifiedStatus::Retryable(code) => {
                    drop(resp);
                    if attempt >= self.retries {
                        return Err(anyhow!(
                            "HTTP {code} for {range_header} after {} retries",
                            self.retries
                        ));
                    }
                    let delay = backoff_delay(
                        attempt,
                        DEFAULT_INITIAL_DELAY_MS,
                        DEFAULT_MAX_DELAY_MS,
                        jitter_seed(),
                    );
                    log_chunk_failure(
                        "retryable HTTP",
                        Some(StatusCode::from_u16(code).unwrap_or(status)),
                        &range_header,
                        attempt,
                        self.retries,
                        delay,
                    );
                    sleep_or_cancel(delay, &self.cancel, &self.shared.stop).await?;
                    attempt += 1;
                    continue;
                }
                ClassifiedStatus::Success200 => {
                    let clen = headers
                        .get(CONTENT_LENGTH)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.trim().parse::<u64>().ok());
                    let remaining = expected - have;
                    let whole_file = from == 0 && self.start == 0 && self.end + 1 == self.total;
                    let matches_remaining = clen == Some(remaining);
                    if !matches_remaining && !whole_file {
                        drop(resp);
                        return Err(anyhow!(
                            "HTTP 200 for ranged GET {range_header}; server ignored Range \
                             (Content-Length={clen:?}, expected {remaining}). \
                             Refusing to write the wrong bytes into this chunk."
                        ));
                    }
                    if whole_file && have > 0 {
                        drop(resp);
                        return Err(anyhow!(
                            "HTTP 200 full-body response while resuming; cannot splice into a partial chunk"
                        ));
                    }
                }
                ClassifiedStatus::Partial206 => {
                    let cr = headers
                        .get(CONTENT_RANGE)
                        .and_then(|v| v.to_str().ok())
                        .and_then(parse_content_range);
                    let Some((cr_start, cr_end, cr_total)) = cr else {
                        drop(resp);
                        if attempt >= self.retries {
                            return Err(anyhow!(
                                "HTTP 206 for {range_header} missing/invalid Content-Range"
                            ));
                        }
                        let delay = backoff_delay(
                            attempt,
                            DEFAULT_INITIAL_DELAY_MS,
                            DEFAULT_MAX_DELAY_MS,
                            jitter_seed(),
                        );
                        log_chunk_failure(
                            "invalid Content-Range",
                            Some(status),
                            &range_header,
                            attempt,
                            self.retries,
                            delay,
                        );
                        sleep_or_cancel(delay, &self.cancel, &self.shared.stop).await?;
                        attempt += 1;
                        continue;
                    };
                    if cr_start != from {
                        drop(resp);
                        return Err(anyhow!(
                            "HTTP 206 Content-Range start {cr_start} != requested {from} ({range_header})"
                        ));
                    }
                    if cr_end < from || cr_end > self.end {
                        drop(resp);
                        return Err(anyhow!(
                            "HTTP 206 Content-Range end {cr_end} outside requested {from}-{} ({range_header})",
                            self.end
                        ));
                    }
                    if cr_total > 0 && self.total > 0 && cr_total != self.total {
                        drop(resp);
                        return Err(anyhow!(
                            "HTTP 206 Content-Range total {cr_total} != probed {}. \
                             The remote file likely changed mid-download (e.g. a new daily \
                             snapshot). Delete the .parts directory and retry to avoid mixing \
                             two different snapshots.",
                            self.total
                        ));
                    }
                }
            }

            if let Some(c) = &self.chunk_pb {
                c.set_length(expected);
                c.set_position(have);
                c.set_message(format!("{}-{}", self.start, self.end));
            }

            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(have > 0)
                .truncate(have == 0)
                .open(&self.dest)
                .await
                .with_context(|| format!("open {}", self.dest.display()))?;
            if have > 0 {
                file.seek(std::io::SeekFrom::End(0)).await?;
            }

            let mut stream = resp.bytes_stream();
            let remaining = expected - have;
            let mut got_this = 0u64;
            let mut stream_error: Option<String> = None;
            let mut made_progress = false;

            loop {
                if *self.cancel.borrow() {
                    file.flush().await.ok();
                    drop(file);
                    return Err(Interrupted.into());
                }
                if self.shared.stop.load(Ordering::Relaxed) {
                    file.flush().await.ok();
                    drop(file);
                    return Ok(());
                }
                if got_this >= remaining {
                    break;
                }
                let item = stream.next().await;
                match item {
                    None => break,
                    Some(Ok(bytes)) => {
                        if bytes.is_empty() {
                            continue;
                        }
                        let room = remaining.saturating_sub(got_this);
                        let n = (bytes.len() as u64).min(room) as usize;
                        file.write_all(&bytes[..n])
                            .await
                            .with_context(|| format!("write {}", self.dest.display()))?;
                        got_this += n as u64;
                        made_progress = true;
                        let now = self
                            .shared
                            .downloaded
                            .fetch_add(n as u64, Ordering::Relaxed)
                            + n as u64;
                        self.overall.set_position(now.min(self.total));
                        if let Some(c) = &self.chunk_pb {
                            c.set_position(have + got_this);
                        }
                    }
                    Some(Err(e)) => {
                        stream_error = Some(format!("{e:#}"));
                        break;
                    }
                }
            }
            file.flush().await.ok();
            file.sync_all().await.ok();
            drop(file);

            let final_len = fs::metadata(&self.dest).await.map(|m| m.len()).unwrap_or(0);
            if final_len == expected {
                if let Some(c) = &self.chunk_pb {
                    c.set_position(expected);
                    c.finish_with_message("done");
                }
                return Ok(());
            }

            if let Some(msg) = stream_error {
                eprintln!("Stream error");
                eprintln!("Chunk: {}-{}", self.start, self.end);
                eprintln!(
                    "Downloaded: {got_this} bytes this attempt ({final_len}/{expected} on disk)"
                );
                eprintln!("Resuming from byte: {}", self.start + final_len);
                eprintln!("  detail: {msg}");
            } else {
                eprintln!(
                    "Chunk incomplete ({final_len}/{expected}); resuming from byte {}",
                    self.start + final_len
                );
            }

            if made_progress {
                attempt = 0;
            }
            if attempt >= self.retries {
                return Err(anyhow!(
                    "chunk {} failed after {} retries ({final_len}/{expected} bytes)",
                    self.dest.display(),
                    self.retries
                ));
            }
            let delay = backoff_delay(
                attempt,
                DEFAULT_INITIAL_DELAY_MS,
                DEFAULT_MAX_DELAY_MS,
                jitter_seed(),
            );
            eprintln!("Retry: {}/{}", attempt + 1, self.retries);
            eprintln!("Backoff: {:.1}s", delay.as_secs_f64());
            sleep_or_cancel(delay, &self.cancel, &self.shared.stop).await?;
            attempt += 1;
        }
    }
}

fn log_chunk_failure(
    why: &str,
    status: Option<StatusCode>,
    range_header: &str,
    attempt: u32,
    retries: u32,
    delay: Duration,
) {
    eprintln!("Chunk failed");
    eprintln!("  reason: {why}");
    if let Some(s) = status {
        eprintln!("HTTP status: {s}");
    }
    eprintln!("Range: {range_header}");
    eprintln!("Retry: {}/{retries}", attempt + 1);
    eprintln!("Backoff: {:.1}s", delay.as_secs_f64());
}

async fn sleep_or_cancel(
    delay: Duration,
    cancel: &watch::Receiver<bool>,
    stop: &AtomicBool,
) -> Result<()> {
    let _ = stop;
    if *cancel.borrow() {
        return Err(Interrupted.into());
    }
    if delay.is_zero() {
        return Ok(());
    }
    let mut cancel = cancel.clone();
    tokio::select! {
        _ = tokio::time::sleep(delay) => Ok(()),
        _ = cancel.changed() => Err(Interrupted.into()),
    }
}

fn spawn_progress_printer(
    shared: Arc<Shared>,
    total: u64,
    mp: MultiProgress,
    overall: ProgressBar,
    cancel: watch::Receiver<bool>,
) -> tokio::sync::oneshot::Sender<()> {
    let (tx, mut rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let emit = |n: u64| {
            let line = progress_line(n, total);
            let _ = mp.println(&line);
            overall.set_position(n.min(total));
            let _ = std::io::Write::flush(&mut std::io::stdout());
        };
        emit(shared.downloaded.load(Ordering::Relaxed));
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval.tick().await;
        loop {
            tokio::select! {
                _ = &mut rx => break,
                _ = interval.tick() => {
                    if *cancel.borrow() || shared.stop.load(Ordering::Relaxed) {
                        emit(shared.downloaded.load(Ordering::Relaxed));
                        break;
                    }
                    emit(shared.downloaded.load(Ordering::Relaxed));
                }
            }
        }
    });
    tx
}

async fn merge_chunks(
    ranges: &[(u64, u64)],
    parts_dir: &Path,
    dest: &Path,
    cancel: &watch::Receiver<bool>,
) -> Result<()> {
    let mut out = fs::File::create(dest)
        .await
        .with_context(|| format!("create {}", dest.display()))?;
    for (start, end) in ranges {
        if *cancel.borrow() {
            drop(out);
            fs::remove_file(dest).await.ok();
            return Err(Interrupted.into());
        }
        let p = parts_dir.join(chunk_file_name(*start));
        let mut f = fs::File::open(&p)
            .await
            .with_context(|| format!("open {}", p.display()))?;
        tokio::io::copy(&mut f, &mut out)
            .await
            .with_context(|| format!("merge {} ({}-{})", p.display(), start, end))?;
    }
    out.flush().await?;
    out.sync_all().await.ok();
    Ok(())
}

async fn atomic_rename(from: &Path, to: &Path) -> Result<()> {
    match fs::rename(from, to).await {
        Ok(()) => Ok(()),
        Err(_) if to.exists() => {
            fs::remove_file(to)
                .await
                .with_context(|| format!("remove existing {}", to.display()))?;
            fs::rename(from, to)
                .await
                .with_context(|| format!("rename {} -> {}", from.display(), to.display()))
        }
        Err(e) => Err(e).with_context(|| format!("rename {} -> {}", from.display(), to.display())),
    }
}

async fn probe(client: &Client, url: &str, cancel: &watch::Receiver<bool>) -> Result<Probe> {
    if *cancel.borrow() {
        return Err(Interrupted.into());
    }
    match client.head(url).send().await {
        Ok(resp) => {
            let status = resp.status();
            fail_probe_auth(status)?;
            if status.is_success() {
                let headers = resp.headers().clone();
                drop(resp);
                let total = headers
                    .get(CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.trim().parse::<u64>().ok())
                    .filter(|t| *t > 0);
                let supports_range = headers
                    .get(ACCEPT_RANGES)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_ascii_lowercase().contains("bytes"))
                    .unwrap_or(false);
                if total.is_some() {
                    return Ok(Probe {
                        total,
                        supports_range,
                        etag: header_string(&headers, ETAG),
                        last_modified: header_string(&headers, LAST_MODIFIED),
                    });
                }
            } else {
                warn!("HEAD returned {status}, falling back to ranged GET probe");
            }
        }
        Err(e) => warn!("HEAD failed ({e:#}), falling back to ranged GET probe"),
    }

    if *cancel.borrow() {
        return Err(Interrupted.into());
    }
    let resp = client
        .get(url)
        .header(RANGE, "bytes=0-0")
        .send()
        .await
        .context("ranged GET probe failed")?;
    let status = resp.status();
    let headers = resp.headers().clone();
    let _ = resp.bytes().await;
    fail_probe_auth(status)?;

    if status == StatusCode::PARTIAL_CONTENT {
        let total = headers
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_content_range)
            .map(|(_, _, t)| t)
            .filter(|t| *t > 0)
            .or_else(|| {
                headers
                    .get(CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.trim().parse::<u64>().ok())
            });
        return Ok(Probe {
            total,
            supports_range: true,
            etag: header_string(&headers, ETAG),
            last_modified: header_string(&headers, LAST_MODIFIED),
        });
    }
    if status == StatusCode::RANGE_NOT_SATISFIABLE {
        let total = headers
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.rsplit('/').next())
            .and_then(|t| t.trim().parse::<u64>().ok());
        return Ok(Probe {
            total,
            supports_range: true,
            etag: None,
            last_modified: None,
        });
    }
    let total = headers
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok());
    Ok(Probe {
        total,
        supports_range: false,
        etag: None,
        last_modified: None,
    })
}

fn fail_probe_auth(status: StatusCode) -> Result<()> {
    match status.as_u16() {
        402 => Err(payment_required_error()),
        401 | 403 => Err(anyhow!(
            "HTTP {status} during probe. Access denied. \
             Check BLOCKCHAIR_KEY / --key if the dump requires authentication."
        )),
        _ => Ok(()),
    }
}

fn header_string(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

async fn single_stream(
    client: &Client,
    url: &str,
    cfg: &DownloadConfig,
    probe: &Probe,
    retries: u32,
    cancel: &watch::Receiver<bool>,
    overall: ProgressBar,
) -> Result<()> {
    let tmp = merge_temp_path(&cfg.output);
    if cfg.no_resume || cfg.force {
        fs::remove_file(&tmp).await.ok();
    }
    let mut have = fs::metadata(&tmp).await.map(|m| m.len()).unwrap_or(0);
    if let Some(total) = probe.total {
        if !probe.supports_range && have > 0 && have != total {
            // Byte resume is impossible against a server that ignores Range;
            // restarting is the only correct action (splicing a full 200 body
            // onto a partial file would corrupt it).
            eprintln!(
                "server does not support byte ranges; discarding {have}-byte partial and restarting"
            );
            fs::remove_file(&tmp).await.ok();
            have = 0;
        }
        check_disk_space(&cfg.output, total, have)?;
        overall.set_length(total);
        overall.set_position(have.min(total));
        if have == total && total > 0 {
            if wants_gzip(&cfg.output) {
                verify_gzip_magic_quick(&tmp)?;
            }
            atomic_rename(&tmp, &cfg.output).await?;
            return Ok(());
        }
        let shared = Arc::new(Shared {
            downloaded: AtomicU64::new(have.min(total)),
            stop: AtomicBool::new(false),
        });
        let printer = spawn_progress_printer(
            Arc::clone(&shared),
            total,
            MultiProgress::new(),
            overall.clone(),
            cancel.clone(),
        );
        let job = RangeJob {
            client: client.clone(),
            url: url.to_string(),
            start: 0,
            end: total.saturating_sub(1),
            dest: tmp.clone(),
            retries,
            total,
            shared,
            cancel: cancel.clone(),
            overall: overall.clone(),
            chunk_pb: None,
            etag: probe.etag.clone(),
            last_modified: probe.last_modified.clone(),
        };
        let r = job.run().await;
        let _ = printer.send(());
        r?;
        if wants_gzip(&cfg.output) {
            verify_gzip_magic_quick(&tmp)?;
        }
        let got = fs::metadata(&tmp).await?.len();
        if got != total {
            return Err(anyhow!("size mismatch: got {got}, want {total}"));
        }
        atomic_rename(&tmp, &cfg.output).await?;
        return Ok(());
    }

    println!("single-stream download (upstream size unknown; resume from {have} bytes)");
    let mut req = client.get(url);
    if have > 0 {
        req = req.header(RANGE, format!("bytes={have}-"));
    }
    let resp = req.send().await.context("GET failed")?;
    fail_probe_auth(resp.status())?;
    let append = resp.status() == StatusCode::PARTIAL_CONTENT;
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(append)
        .truncate(!append)
        .open(&tmp)
        .await?;
    if append {
        file.seek(std::io::SeekFrom::End(0)).await?;
    }
    let mut stream = resp.bytes_stream();
    while let Some(item) = stream.next().await {
        if *cancel.borrow() {
            file.flush().await.ok();
            return Err(Interrupted.into());
        }
        let bytes = item.context("stream error")?;
        file.write_all(&bytes).await?;
        overall.inc(bytes.len() as u64);
    }
    file.flush().await?;
    if wants_gzip(&cfg.output) {
        verify_gzip_magic_quick(&tmp)?;
    }
    atomic_rename(&tmp, &cfg.output).await?;
    Ok(())
}

fn wants_gzip(path: &Path) -> bool {
    path.as_os_str()
        .to_string_lossy()
        .to_ascii_lowercase()
        .contains(".gz")
}

fn verify_gzip_magic_quick(path: &Path) -> Result<()> {
    use std::io::Read as _;
    let mut f = std::fs::File::open(path)
        .with_context(|| format!("open {} for magic check", path.display()))?;
    let mut magic = [0u8; 2];
    let n = f.read(&mut magic).unwrap_or(0);
    if n < 2 || !has_gzip_magic(&magic) {
        return Err(anyhow!(
            "{} does not start with gzip magic (1F 8B); download may be truncated or an HTML error page",
            path.display()
        ));
    }
    Ok(())
}

async fn verify_gzip_full(path: PathBuf) -> Result<u64> {
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let f = std::fs::File::open(&path)
            .with_context(|| format!("open {} for gzip test", path.display()))?;
        let mut dec = flate2::read::GzDecoder::new(f);
        let mut buf = [0u8; 128 * 1024];
        let mut total: u64 = 0;
        let mut first_line = String::new();
        let mut first_done = false;
        let mut line_buf: Vec<u8> = Vec::with_capacity(4096);
        loop {
            let n = dec.read(&mut buf)?;
            if n == 0 {
                break;
            }
            total += n as u64;
            if !first_done {
                for b in &buf[..n] {
                    if *b == b'\n' {
                        first_done = true;
                        break;
                    }
                    if line_buf.len() < 8192 {
                        line_buf.push(*b);
                    }
                }
                if first_done {
                    first_line = String::from_utf8_lossy(&line_buf).to_string();
                }
            }
        }
        if !first_done {
            first_line = String::from_utf8_lossy(&line_buf).to_string();
        }
        if !first_line.is_empty() && !looks_like_blockchair_addresses_tsv(&first_line) {
            warn!("first TSV line does not look like Blockchair addresses header: {first_line:?}");
        }
        Ok(total)
    })
    .await
    .context("gzip verify task panicked")?
}

async fn decompress_output(output: &Path) -> Result<()> {
    let out: PathBuf = if output.extension().and_then(|e| e.to_str()) == Some("gz") {
        let s = output.to_string_lossy();
        PathBuf::from(s.trim_end_matches(".gz").to_owned())
    } else {
        output.with_extension("tsv")
    };
    println!("decompressing to {} ...", out.display());
    let src = output.to_path_buf();
    tokio::task::spawn_blocking(move || -> Result<()> {
        use std::io::{BufReader, BufWriter};
        let f = std::fs::File::open(&src)?;
        let mut dec = flate2::read::GzDecoder::new(BufReader::new(f));
        let o = std::fs::File::create(&out)?;
        let mut w = BufWriter::new(o);
        std::io::copy(&mut dec, &mut w)?;
        Ok(())
    })
    .await
    .context("decompress task panicked")??;
    println!("decompressed OK");
    Ok(())
}
