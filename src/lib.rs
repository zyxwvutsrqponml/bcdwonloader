//! Core logic for bcdownloader: range splitting, retries, HTTP helpers, engine.
//!
//! Network-independent helpers are unit-tested without touching the ~1.65 GiB dump.

pub mod disk;
pub mod engine;
pub mod progress;
pub mod ranges;
pub mod retry;
pub mod ui;
pub mod validate;

pub use engine::{
    build_client, download, is_interrupted, payment_required_error, spawn_signal_handler,
    DownloadConfig, Interrupted,
};
pub use progress::{bytes_as_mib, progress_line, progress_pct};
pub use ranges::{
    apply_key_to_url, chunk_file_name, expected_byte_count, has_gzip_magic,
    looks_like_blockchair_addresses_tsv, merge_temp_path, parse_content_range, parts_dir,
    range_len, redact_url, split_ranges, DEFAULT_URL,
};
pub use retry::{
    backoff_delay, classify_status, jitter_seed, parse_retry_after, ClassifiedStatus,
    DEFAULT_INITIAL_DELAY_MS, DEFAULT_MAX_402_RETRIES, DEFAULT_MAX_DELAY_MS, DEFAULT_MAX_RETRIES,
};
pub use validate::{
    validate_chunk_mib, validate_connections, validate_output, validate_retries, validate_url,
};
