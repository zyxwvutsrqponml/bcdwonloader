use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use bcdownloader::{
    apply_key_to_url, chunk_file_name, has_gzip_magic, range_len, split_ranges, DEFAULT_URL,
};
use clap::Parser;
use indicatif::{MultiProgress, ProgressBar};
use reqwest::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, ETAG, LAST_MODIFIED, RANGE};
use reqwest::{Client, StatusCode};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};

mod ui;

/// Perfect parallel resumable downloader for Blockchair database dumps.
///
/// Optimized for:
///   https://gz.blockchair.com/bitcoin/addresses/blockchair_bitcoin_addresses_latest.tsv.gz
/// (~1.77 GB, Accept-Ranges: bytes, throttled to ~10 kB/s per connection
///  without ?key=SECRETKEY — hence parallel connections).
#[derive(Parser, Debug)]
#[command(name = "bcdownloader", version, about, long_about = None)]
struct Cli {
    /// URL to download (defaults to latest Bitcoin addresses dump).
    #[arg(default_value = DEFAULT_URL)]
    url: String,

    /// Output file path.
    #[arg(
        short,
        long,
        default_value = "blockchair_bitcoin_addresses_latest.tsv.gz"
    )]
    output: PathBuf,

    /// Parallel connections (HTTP Range chunks). 1 = single stream.
    #[arg(short, long, default_value_t = 16, value_parser = clap::value_parser!(u8).range(1..=64))]
    connections: u8,

    /// Max retries per chunk / request.
    #[arg(long, default_value_t = 10)]
    retries: u32,

    /// Per-request timeout in seconds.
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,

    /// Blockchair speed-unlock key (appended as ?key=). Env: BLOCKCHAIR_KEY.
    #[arg(long, env = "BLOCKCHAIR_KEY", default_value = "")]
    key: String,

    /// Re-download even if output already exists and size matches.
    #[arg(long, default_value_t = false)]
    force: bool,

    /// Discard partial chunks and start from scratch.
    #[arg(long, default_value_t = false)]
    no_resume: bool,

    /// Gunzip to .tsv after successful download (keeps .gz by default).
    #[arg(long, default_value_t = false)]
    decompress: bool,

    /// Full streaming gzip integrity test after download (reads whole file).
    #[arg(long, default_value_t = false)]
    verify_gzip: bool,

    /// Disable progress bars (for CI logs).
    #[arg(long, default_value_t = false)]
    no_progress: bool,

    /// Verbose logging (-v, -vv).
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(Debug, Clone, Default)]
struct Probe {
    total: Option<u64>,
    supports_range: bool,
    etag: Option<String>,
    last_modified: Option<String>,
}

fn build_client(timeout_secs: u64) -> Result<Client> {
    let version = env!("CARGO_PKG_VERSION");
    Client::builder()
        .user_agent(format!(
            "bcdownloader/{version} (+blockchair-dump-downloader)"
        ))
        .timeout(Duration::from_secs(timeout_secs))
        .connect_timeout(Duration::from_secs(15))
        .tcp_keepalive(Duration::from_secs(30))
        .pool_max_idle_per_host(32)
        .build()
        .context("building HTTP client")
}

async fn probe(client: &Client, url: &str) -> Result<Probe> {
    // Preferred: HEAD.
    match client.head(url).send().await {
        Ok(resp) => {
            let status = resp.status();
            if status.is_success() {
                let headers = resp.headers().clone();
                let total = headers
                    .get(CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.trim().parse::<u64>().ok())
                    .filter(|t| *t > 0);
                let supports_range = headers
                    .get(ACCEPT_RANGES)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_lowercase().contains("bytes"))
                    .unwrap_or(false);
                return Ok(Probe {
                    total,
                    supports_range,
                    etag: headers
                        .get(ETAG)
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string()),
                    last_modified: headers
                        .get(LAST_MODIFIED)
                        .and_then(|v| v.to_str().ok())
                        .map(|s| s.to_string()),
                });
            }
            warn!("HEAD returned {status}, falling back to ranged GET probe");
        }
        Err(e) => warn!("HEAD failed ({e:#}), falling back to ranged GET probe"),
    }

    // Fallback: GET bytes=0-0, expect 206 + Content-Range: bytes 0-0/TOTAL.
    let resp = client
        .get(url)
        .header(RANGE, "bytes=0-0")
        .send()
        .await
        .context("ranged GET probe failed")?;
    let status = resp.status();
    let headers = resp.headers().clone();
    // Drain body.
    let _ = resp.bytes().await;
    if status == StatusCode::PARTIAL_CONTENT {
        let total = headers
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(bcdownloader::parse_content_range)
            .map(|(_, _, t)| t)
            .or_else(|| {
                headers
                    .get(CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.trim().parse::<u64>().ok())
            });
        return Ok(Probe {
            total,
            supports_range: true,
            etag: headers
                .get(ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string()),
            last_modified: headers
                .get(LAST_MODIFIED)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string()),
        });
    }
    // Server ignored Range: single-stream only.
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

async fn sleep_backoff(attempt: u32) {
    // 500ms * 2^attempt capped at 15s. No extra rand dep: cheap jitter via nanos.
    let exp = 500u64.saturating_mul(1u64 << attempt.min(5));
    let capped = exp.min(15_000);
    let jitter = (std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() % 250)
        .unwrap_or(0)) as u64;
    tokio::time::sleep(Duration::from_millis(capped + jitter)).await;
}

/// Download one byte-range into `dest`, resuming from existing partial length.
///
/// `overall` is always updated; `chunk` (per-chunk live bar) is updated when present.
#[allow(clippy::too_many_arguments)]
async fn download_range(
    client: &Client,
    url: &str,
    start: u64,
    end: u64,
    dest: &Path,
    overall: ProgressBar,
    chunk: Option<ProgressBar>,
    retries: u32,
    etag: Option<&str>,
    last_modified: Option<&str>,
) -> Result<()> {
    let expected = range_len(start, end);
    let mut attempt: u32 = 0;
    loop {
        // Resume offset = already-written bytes for this chunk.
        let have: u64 = match fs::metadata(dest).await {
            Ok(m) => m.len(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(anyhow!("stat {}: {e}", dest.display())),
        };
        if have == expected {
            // Already counted by the caller; mark live chunk bar complete.
            if let Some(c) = chunk.as_ref() {
                c.set_length(expected);
                c.set_position(expected);
                c.finish_with_message("cached");
            }
            return Ok(());
        }
        if have > expected {
            warn!(
                "{} larger than expected ({have} > {expected}), truncating",
                dest.display()
            );
            fs::remove_file(dest)
                .await
                .with_context(|| format!("removing oversized {}", dest.display()))?;
            // loop again from 0
            continue;
        }
        let from = start + have;
        let range_header = format!("bytes={from}-{end}");

        let mut req = client.get(url).header(RANGE, range_header.clone());
        if let Some(e) = etag {
            req = req.header(reqwest::header::IF_MATCH, e);
        } else if let Some(lm) = last_modified {
            req = req.header(reqwest::header::IF_UNMODIFIED_SINCE, lm);
        }

        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                if attempt >= retries {
                    if let Some(c) = chunk.as_ref() {
                        c.abandon_with_message("failed");
                    }
                    return Err(anyhow!(
                        "GET {range_header} failed after {retries} retries: {e:#}"
                    ));
                }
                warn!(
                    "GET {range_header} error ({e:#}), retry {}/{}",
                    attempt + 1,
                    retries
                );
                if let Some(c) = chunk.as_ref() {
                    c.set_message(format!("retry {}/{}", attempt + 1, retries));
                }
                sleep_backoff(attempt).await;
                attempt += 1;
                continue;
            }
        };
        let status = resp.status();
        if status == StatusCode::RANGE_NOT_SATISFIABLE {
            // Already complete (race) or file shrank upstream.
            if have == expected {
                return Ok(());
            }
            return Err(anyhow!(
                "server returned 416 for {range_header}; upstream file may have changed. Delete partials and retry."
            ));
        }
        if status == StatusCode::PRECONDITION_FAILED {
            return Err(anyhow!(
                "upstream file changed during download (412). Delete partials and retry."
            ));
        }
        if !(status == StatusCode::PARTIAL_CONTENT
            || (status == StatusCode::OK && have == 0 && from == start))
        {
            if attempt >= retries {
                if let Some(c) = chunk.as_ref() {
                    c.abandon_with_message(format!("HTTP {status}"));
                }
                return Err(anyhow!("unexpected HTTP {status} for {range_header}"));
            }
            warn!(
                "unexpected HTTP {status} for {range_header}, retry {}/{}",
                attempt + 1,
                retries
            );
            if let Some(c) = chunk.as_ref() {
                c.set_message(format!("retry {}/{}", attempt + 1, retries));
            }
            sleep_backoff(attempt).await;
            attempt += 1;
            continue;
        }
        if status == StatusCode::OK && expected != 0 && from != start {
            return Err(anyhow!(
                "server ignored Range request (200 instead of 206); cannot resume multi-chunk. Use --connections 1 or a server with Accept-Ranges."
            ));
        }

        // Stream to file (append if resuming).
        if let Some(c) = chunk.as_ref() {
            c.set_length(expected);
            c.set_position(have);
            c.set_message(format!("{}-{}", start, end));
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(have > 0)
            .truncate(have == 0)
            .open(dest)
            .await
            .with_context(|| format!("open {}", dest.display()))?;
        if have > 0 {
            // Ensure cursor at end for append mode on all platforms.
            use tokio::io::AsyncSeekExt;
            file.seek(std::io::SeekFrom::End(0)).await?;
        }

        let mut stream = resp.bytes_stream();
        use futures::StreamExt;
        let mut ok = true;
        while let Some(item) = stream.next().await {
            match item {
                Ok(bytes) => {
                    file.write_all(&bytes)
                        .await
                        .with_context(|| format!("write {}", dest.display()))?;
                    let n = bytes.len() as u64;
                    overall.inc(n);
                    if let Some(c) = chunk.as_ref() {
                        c.inc(n);
                    }
                }
                Err(e) => {
                    warn!("stream error on {range_header}: {e:#}");
                    if let Some(c) = chunk.as_ref() {
                        c.set_message("reconnecting");
                    }
                    ok = false;
                    break;
                }
            }
        }
        file.flush().await.ok();
        drop(file);

        if ok {
            let final_len = fs::metadata(dest).await.map(|m| m.len()).unwrap_or(0);
            if final_len == expected {
                if let Some(c) = chunk.as_ref() {
                    c.set_position(expected);
                    c.finish_with_message("done");
                }
                return Ok(());
            }
            warn!(
                "chunk {} incomplete ({final_len}/{expected}), retrying",
                dest.display()
            );
        }
        if attempt >= retries {
            return Err(anyhow!(
                "chunk {} failed after {retries} retries",
                dest.display()
            ));
        }
        attempt += 1;
        sleep_backoff(attempt).await;
    }
}

async fn merge_chunks(parts_dir: &Path, count: usize, final_part: &Path) -> Result<()> {
    let mut out = fs::File::create(final_part)
        .await
        .with_context(|| format!("create {}", final_part.display()))?;
    for i in 0..count {
        let p = parts_dir.join(chunk_file_name(i, count));
        let mut f = fs::File::open(&p)
            .await
            .with_context(|| format!("open {}", p.display()))?;
        tokio::io::copy(&mut f, &mut out)
            .await
            .with_context(|| format!("merge {}", p.display()))?;
    }
    out.flush().await?;
    out.sync_all().await.ok();
    Ok(())
}

fn verify_gzip_magic_quick(path: &Path) -> Result<()> {
    use std::io::Read as _;
    let mut f = std::fs::File::open(path)
        .with_context(|| format!("open {} for magic check", path.display()))?;
    let mut magic = [0u8; 2];
    let _ = f.read(&mut magic);
    if !has_gzip_magic(&magic) {
        return Err(anyhow!(
            "{} does not start with gzip magic (1F 8B); download may be truncated or an HTML error page",
            path.display()
        ));
    }
    Ok(())
}

async fn verify_gzip_full(path: PathBuf) -> Result<u64> {
    // Blocking gunzip test (reads whole file). Returns decompressed bytes.
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let f = std::fs::File::open(&path)
            .with_context(|| format!("open {} for gzip test", path.display()))?;
        let mut dec = flate2::read::GzDecoder::new(f);
        let mut buf = [0u8; 128 * 1024];
        let mut total: u64 = 0;
        let mut first_line = String::new();
        let mut first_done = false;
        // We count bytes and capture the first line for TSV validation.
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
        if !first_line.is_empty() && !bcdownloader::looks_like_blockchair_addresses_tsv(&first_line)
        {
            warn!("first TSV line does not look like Blockchair addresses header: {first_line:?}");
        }
        Ok(total)
    })
    .await
    .context("gzip verify task panicked")?
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let filter = match cli.verbose {
        0 => "bcdownloader=info",
        1 => "bcdownloader=debug",
        _ => "debug",
    };
    tracing_subscriber::fmt()
        .with_env_filter(format!(
            "{},{}",
            std::env::var("RUST_LOG").unwrap_or_default(),
            filter
        ))
        .with_target(false)
        .init();

    let url = apply_key_to_url(&cli.url, &cli.key)?;
    if !cli.key.is_empty() {
        info!("using Blockchair key (hidden), full speed expected");
    } else {
        info!("no --key provided: expect ~10 kB/s per connection; parallel connections compensate");
    }

    let client = build_client(cli.timeout_secs)?;
    let probe = probe(&client, &url).await?;
    info!(
        "probe: total={} range={} etag={:?} modified={:?}",
        probe
            .total
            .map(|t| format!("{t} ({:.2} GiB)", t as f64 / 1024.0 / 1024.0 / 1024.0))
            .unwrap_or_else(|| "unknown".to_string()),
        probe.supports_range,
        probe.etag,
        probe.last_modified
    );

    // Fast path: output already complete.
    if !cli.force {
        if let Ok(m) = fs::metadata(&cli.output).await {
            if let Some(total) = probe.total {
                if m.len() == total {
                    println!("already complete: {} ({total} bytes)", cli.output.display());
                    return Ok(());
                }
                info!(
                    "existing output {} bytes vs upstream {total} bytes, resuming",
                    m.len()
                );
            } else if m.len() > 0 {
                println!(
                    "output exists ({} bytes, upstream size unknown) — use --force to re-download",
                    m.len()
                );
                return Ok(());
            }
        }
    } else if cli.output.exists() {
        warn!("--force: existing output will be replaced on success");
    }

    let connections = cli.connections as usize;
    let use_parallel = probe.supports_range && probe.total.is_some() && connections > 1;

    // ---- Realtime CLI UI ----
    let started = Instant::now();
    ui::print_header(
        env!("CARGO_PKG_VERSION"),
        &ui::short_url(&url),
        &cli.output.to_string_lossy(),
        probe.total,
        connections,
        !cli.no_resume,
        !cli.key.is_empty(),
    );
    let mp = MultiProgress::new();
    if cli.no_progress {
        mp.set_draw_target(indicatif::ProgressDrawTarget::hidden());
    }
    let total = probe.total.unwrap_or(0);
    let overall = ui::add_overall(&mp, total);
    overall.set_message("downloading");

    if use_parallel {
        let total = probe.total.unwrap();
        let ranges = split_ranges(total, connections);
        let parts_dir = {
            let mut s = cli.output.as_os_str().to_owned();
            s.push(".parts");
            PathBuf::from(s)
        };
        if cli.no_resume && parts_dir.exists() {
            fs::remove_dir_all(&parts_dir).await.ok();
        }
        fs::create_dir_all(&parts_dir)
            .await
            .with_context(|| format!("create parts dir {}", parts_dir.display()))?;

        // If a previous run used a different chunk count/total, stale chunks
        // would corrupt the merge — detect expected sizes and wipe on mismatch.
        // (Cheap heuristic: any chunk bigger than its expected range.)
        for (i, (s, e)) in ranges.iter().enumerate() {
            let p = parts_dir.join(chunk_file_name(i, ranges.len()));
            if let Ok(m) = fs::metadata(&p).await {
                if m.len() > range_len(*s, *e) {
                    warn!(
                        "stale parts detected (upstream changed?), wiping {}",
                        parts_dir.display()
                    );
                    fs::remove_dir_all(&parts_dir).await.ok();
                    fs::create_dir_all(&parts_dir).await?;
                    break;
                }
            }
        }

        // Account already-downloaded bytes in progress bar.
        let mut have: u64 = 0;
        for (i, (s, e)) in ranges.iter().enumerate() {
            let p = parts_dir.join(chunk_file_name(i, ranges.len()));
            if let Ok(m) = fs::metadata(&p).await {
                have += m.len().min(range_len(*s, *e));
            }
        }
        overall.inc(have);
        overall.set_length(total);

        info!(
            "parallel download: {} chunks, {:.2} MiB each (avg)",
            ranges.len(),
            total as f64 / ranges.len() as f64 / 1024.0 / 1024.0
        );

        let chunk_count = ranges.len();
        let show_chunks = !cli.no_progress && chunk_count <= ui::MAX_CHUNK_BARS;
        let mut set = tokio::task::JoinSet::new();
        for (i, (s, e)) in ranges.into_iter().enumerate() {
            let client = client.clone();
            let url = url.clone();
            let dest = parts_dir.join(chunk_file_name(i, chunk_count));
            let overall = overall.clone();
            let etag = probe.etag.clone();
            let lm = probe.last_modified.clone();
            let retries = cli.retries;
            // Pre-check: skip spawning work if already done (still need overall accounting, done above).
            let expected = range_len(s, e);
            let already = fs::metadata(&dest)
                .await
                .map(|m| m.len() == expected)
                .unwrap_or(false);
            // Live per-chunk bar (realtime UI).
            let chunk_pb: Option<ProgressBar> = if show_chunks {
                let c = ui::add_chunk(&mp, format!("#{i}"));
                c.set_length(expected);
                c.set_position(if already {
                    expected
                } else {
                    fs::metadata(&dest)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0)
                        .min(expected)
                });
                if already {
                    c.finish_with_message("cached");
                }
                Some(c)
            } else {
                None
            };
            if already {
                continue;
            }
            let label = format!("#{i}");
            set.spawn(async move {
                let r = download_range(
                    &client,
                    &url,
                    s,
                    e,
                    &dest,
                    overall,
                    chunk_pb.clone(),
                    retries,
                    etag.as_deref(),
                    lm.as_deref(),
                )
                .await;
                if let Some(c) = chunk_pb {
                    if r.is_err() {
                        c.abandon_with_message(format!("{label} failed"));
                    }
                }
                r.with_context(|| format!("chunk {i} bytes={s}-{e}"))
            });
        }
        while let Some(r) = set.join_next().await {
            r.context("chunk task panicked")??;
        }
        if !show_chunks {
            overall.set_message(format!("{chunk_count} workers"));
        }

        // Verify all chunks then merge atomically.
        let count = chunk_count;
        let expected_ranges = split_ranges(total, connections);
        for (i, (s, e)) in expected_ranges.iter().enumerate() {
            let (s, e) = (*s, *e);
            let p = parts_dir.join(chunk_file_name(i, count));
            let m = fs::metadata(&p)
                .await
                .with_context(|| format!("missing {}", p.display()))?;
            // Expected size from authoritative ranges.
            if m.len() != range_len(s, e) {
                return Err(anyhow!(
                    "chunk {i} incomplete: {} vs {} bytes",
                    m.len(),
                    range_len(s, e)
                ));
            }
        }
        overall.set_message("merging");
        let tmp = cli.output.with_extension("gz.part");
        merge_chunks(&parts_dir, count, &tmp).await?;
        let merged = fs::metadata(&tmp).await?.len();
        if merged != total {
            overall.abandon_with_message("size mismatch");
            return Err(anyhow!("merged size {merged} != upstream {total}"));
        }
        overall.set_message("verifying");
        verify_gzip_magic_quick(&tmp)?;
        fs::rename(&tmp, &cli.output)
            .await
            .context("atomic rename to final output")?;
        fs::remove_dir_all(&parts_dir).await.ok();
        overall.finish_with_message("done");
    } else {
        // Single-stream (with resume if server honors Range).
        if connections > 1 && !probe.supports_range {
            warn!("server does not support Range; falling back to single connection");
        }
        let tmp = cli.output.with_extension("gz.part");
        if cli.no_resume {
            fs::remove_file(&tmp).await.ok();
        }
        let have = fs::metadata(&tmp).await.map(|m| m.len()).unwrap_or(0);
        if let Some(total) = probe.total {
            overall.set_length(total);
            overall.set_position(have.min(total));
            if have == total && total > 0 {
                fs::rename(&tmp, &cli.output).await.ok();
                overall.finish_with_message("done");
                println!("already complete: {} ({total} bytes)", cli.output.display());
                return Ok(());
            }
        }
        info!("single-stream download (resume from {have} bytes)");
        overall.set_message("downloading");
        // Reuse download_range for the whole file when total known, else plain GET.
        if let Some(total) = probe.total {
            download_range(
                &client,
                &url,
                0,
                total - 1,
                &tmp,
                overall.clone(),
                None,
                cli.retries,
                probe.etag.as_deref(),
                probe.last_modified.as_deref(),
            )
            .await?;
        } else {
            // Unknown size: plain streaming GET (append if resuming without Range is unsafe,
            // so start fresh unless server confirms 206).
            let mut req = client.get(&url);
            if have > 0 {
                req = req.header(RANGE, format!("bytes={have}-"));
            }
            let resp = req.send().await.context("GET failed")?;
            let append = resp.status() == StatusCode::PARTIAL_CONTENT;
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(append)
                .truncate(!append)
                .open(&tmp)
                .await?;
            if append {
                use tokio::io::AsyncSeekExt;
                file.seek(std::io::SeekFrom::End(0)).await?;
            }
            let mut stream = resp.bytes_stream();
            use futures::StreamExt;
            while let Some(item) = stream.next().await {
                let bytes = item.context("stream error")?;
                file.write_all(&bytes).await?;
                overall.inc(bytes.len() as u64);
            }
            file.flush().await?;
        }
        overall.set_message("verifying");
        verify_gzip_magic_quick(&tmp)?;
        if let Some(total) = probe.total {
            let got = fs::metadata(&tmp).await?.len();
            if got != total {
                overall.abandon_with_message("size mismatch");
                return Err(anyhow!("size mismatch: got {got}, want {total}"));
            }
        }
        fs::rename(&tmp, &cli.output).await?;
        overall.finish_with_message("done");
    }

    let final_len = fs::metadata(&cli.output).await?.len();
    ui::print_summary(
        &cli.output.to_string_lossy(),
        final_len,
        started.elapsed().as_secs(),
    );

    if cli.verify_gzip {
        println!("verifying gzip integrity (full read)...");
        let n = verify_gzip_full(cli.output.clone()).await?;
        println!("gzip OK, decompressed ~{n} bytes");
    }

    if cli.decompress {
        let out: PathBuf = if cli.output.extension().and_then(|e| e.to_str()) == Some("gz") {
            // foo.tsv.gz -> foo.tsv (strip only the final .gz)
            let s = cli.output.to_string_lossy();
            PathBuf::from(s.trim_end_matches(".gz").to_owned())
        } else {
            cli.output.with_extension("tsv")
        };
        println!("decompressing to {} ...", out.display());
        let src = cli.output.clone();
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
    }

    Ok(())
}
