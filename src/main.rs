use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::Result;
use bcdownloader::{
    download, is_interrupted, spawn_signal_handler, validate_chunk_mib, validate_connections,
    validate_output, validate_retries, validate_url, DownloadConfig, DEFAULT_URL,
};
use clap::Parser;
use tokio::sync::watch;

/// Parallel, resumable downloader for Blockchair database dumps.
///
/// Optimized for:
///   https://gz.blockchair.com/bitcoin/addresses/blockchair_bitcoin_addresses_latest.tsv.gz
/// (~1.65 GiB / 1_772_480_833 bytes). Uses a worker pool and verified HTTP Range requests.
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

    /// Parallel HTTP connections (worker pool). In-flight requests never exceed this.
    #[arg(short, long, default_value_t = 4)]
    connections: u32,

    /// Chunk size in MiB (default 32). Each chunk is one Range request.
    #[arg(long, default_value_t = 32)]
    chunk_mib: u64,

    /// Max retries per chunk for network failures (not used for genuine HTTP 402).
    #[arg(long, default_value_t = 20)]
    retries: u32,

    /// Idle read timeout in seconds (0 = none). Not a total-transfer timeout.
    #[arg(long, default_value_t = 120)]
    timeout_secs: u64,

    /// Blockchair speed-unlock key (appended as ?key=). Env: BLOCKCHAIR_KEY.
    #[arg(
        long,
        env = "BLOCKCHAIR_KEY",
        default_value = "",
        hide_env_values = true
    )]
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

    /// Disable indicatif progress bars. CI still prints a `[progress]` line every 10s.
    #[arg(long, default_value_t = false)]
    no_progress: bool,

    /// Verbose logging (-v, -vv).
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

fn validate_cli(cli: &Cli) -> Result<()> {
    validate_url(&cli.url)?;
    validate_output(&cli.output)?;
    validate_connections(cli.connections)?;
    validate_chunk_mib(cli.chunk_mib)?;
    validate_retries(i64::from(cli.retries))?;
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match real_main().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) if is_interrupted(&e) => {
            eprintln!("{e}");
            ExitCode::from(130)
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn real_main() -> Result<()> {
    let cli = Cli::parse();

    let filter = match cli.verbose {
        0 => "bcdownloader=warn",
        1 => "bcdownloader=info",
        _ => "debug",
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(format!(
            "{},{}",
            std::env::var("RUST_LOG").unwrap_or_default(),
            filter
        ))
        .with_target(false)
        .try_init();

    validate_cli(&cli)?;

    let cfg = DownloadConfig {
        url: cli.url,
        output: cli.output,
        connections: cli.connections,
        chunk_mib: cli.chunk_mib,
        chunk_bytes: None,
        retries: cli.retries,
        timeout_secs: cli.timeout_secs,
        key: cli.key,
        force: cli.force,
        no_resume: cli.no_resume,
        verify_gzip: cli.verify_gzip,
        decompress: cli.decompress,
        no_progress: cli.no_progress,
    };

    let (tx, rx) = watch::channel(false);
    spawn_signal_handler(tx);
    download(cfg, rx).await
}
