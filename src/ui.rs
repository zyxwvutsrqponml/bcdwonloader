//! Realtime CLI UI: header, live multi-bar progress, summary.
//!
//! Design goals:
//! - One overall bar (always) + one live bar per chunk (when it fits on screen).
//! - Constant 8 Hz refresh so speed / ETA / % feel realtime.
//! - Zero extra dependencies (only `indicatif`).
//! - `--no-progress` hides everything for CI logs.

use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::time::Duration;

/// Refresh interval for spinners / speed estimates.
pub const TICK: Duration = Duration::from_millis(120);

/// Show per-chunk bars only up to this many connections (beyond that the
/// screen would scroll; overall bar + worker count is enough).
pub const MAX_CHUNK_BARS: usize = 16;

pub fn overall_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{spinner:.green} TOTAL [{elapsed_precise}] [{wide_bar:.cyan/blue}] \
         {bytes}/{total_bytes} ({percent}%) {bytes_per_sec} ETA {eta} {msg}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar())
    .progress_chars("#>-")
}

pub fn chunk_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "  {spinner:.cyan} chunk {msg} [{bar:28.cyan/blue}] {bytes}/{total_bytes} {bytes_per_sec}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar())
    .progress_chars("#>-")
}

/// Overall bar, registered on `mp` and ticking.
pub fn add_overall(mp: &MultiProgress, total: u64) -> ProgressBar {
    let pb = mp.add(ProgressBar::new(total));
    pb.set_style(overall_style());
    pb.set_message("starting");
    pb.enable_steady_tick(TICK);
    pb
}

/// One bar per chunk. Caller sets length/position afterwards.
pub fn add_chunk(mp: &MultiProgress, label: String) -> ProgressBar {
    let pb = mp.add(ProgressBar::new(0));
    pb.set_style(chunk_style());
    pb.set_message(label);
    pb.enable_steady_tick(TICK);
    pb
}

pub fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n} B")
    } else {
        format!("{v:.2} {}", UNITS[u])
    }
}

pub fn human_duration(secs: u64) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h:02}:{m:02}:{s:02}")
    } else {
        format!("{m:02}:{s:02}")
    }
}

/// Static header printed once before any bar ticks.
pub fn print_header(
    version: &str,
    url_short: &str,
    output: &str,
    total: Option<u64>,
    connections: usize,
    resume: bool,
    has_key: bool,
) {
    let size = total
        .map(|t| format!("{} ({t} bytes)", human_bytes(t)))
        .unwrap_or_else(|| "unknown".to_string());
    println!("== bcdownloader v{version} ==");
    println!("   URL    : {url_short}");
    println!("   output : {output}");
    println!("   size   : {size}");
    println!(
        "   conns  : {connections} | resume : {} | key : {}",
        if resume { "on" } else { "off" },
        if has_key {
            "yes"
        } else {
            "no (10 kB/s per conn)"
        },
    );
    println!("------------------------------------------------------------");
}

/// One-line live footer summary.
pub fn print_summary(saved_as: &str, bytes: u64, elapsed_secs: u64) {
    let avg = if elapsed_secs > 0 {
        human_bytes(bytes / elapsed_secs) + "/s"
    } else {
        "-".to_string()
    };
    println!("------------------------------------------------------------");
    println!(
        "[done] {} in {} | avg {} | saved {}",
        human_bytes(bytes),
        human_duration(elapsed_secs),
        avg,
        saved_as
    );
}

/// Shorten a long URL for the header (keep host + last segment + key hidden).
pub fn short_url(url: &str) -> String {
    const KEEP: usize = 72;
    let mut s = url.to_string();
    if let Some(pos) = s.find("key=") {
        s.replace_range(pos.., "key=***");
    }
    if s.len() > KEEP {
        let tail: String = s
            .chars()
            .rev()
            .take(KEEP - 3)
            .collect::<String>()
            .chars()
            .rev()
            .collect();
        format!("...{tail}")
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_format() {
        assert_eq!(human_bytes(0), "0 B");
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(2048), "2.00 KiB");
        assert_eq!(human_bytes(1_772_480_833), "1.65 GiB");
    }

    #[test]
    fn duration_format() {
        assert_eq!(human_duration(65), "01:05");
        assert_eq!(human_duration(3725), "01:02:05");
    }

    #[test]
    fn short_hides_key() {
        let s = short_url("https://example.com/f.tsv.gz?key=SECRET");
        assert!(s.contains("key=***"));
        assert!(!s.contains("SECRET"));
    }
}
