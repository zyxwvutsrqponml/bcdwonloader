//! Core logic for bcdownloader: range splitting, URL handling, verification.
//!
//! All network-independent helpers live here so they are unit-testable in CI
//! without touching the 1.7 GB Blockchair dump.

use anyhow::{anyhow, Result};

/// Default Blockchair dump analyzed:
/// `https://gz.blockchair.com/bitcoin/addresses/blockchair_bitcoin_addresses_latest.tsv.gz`
/// - ~1_772_480_833 bytes (shows as 2G), daily updated (`Latest` symlink)
/// - `Accept-Ranges: bytes`, `ETag` + `Last-Modified` present
/// - Default throttle 10 kB/s per connection without `?key=SECRETKEY`
pub const DEFAULT_URL: &str =
    "https://gz.blockchair.com/bitcoin/addresses/blockchair_bitcoin_addresses_latest.tsv.gz";

/// Append `?key=...` (or `&key=...`) unless the URL already carries a key
/// or the supplied key is empty.
pub fn apply_key_to_url(url: &str, key: &str) -> Result<String> {
    let key = key.trim();
    if key.is_empty() {
        return Ok(url.to_string());
    }
    let mut parsed =
        url::Url::parse(url).map_err(|e| anyhow!("invalid URL {url:?}: {e}"))?;
    // Don't duplicate if user already embedded ?key= in the URL.
    if parsed
        .query_pairs()
        .any(|(k, _)| k == "key")
    {
        return Ok(parsed.to_string());
    }
    parsed.query_pairs_mut().append_pair("key", key);
    Ok(parsed.to_string())
}

/// Split `total` bytes into `n` inclusive `bytes=start-end` ranges.
///
/// Guarantees:
/// - ranges are contiguous, non-overlapping, cover `[0, total)`
/// - `ranges.len() == min(n, total)` (no empty ranges)
/// - last range ends at `total - 1`
pub fn split_ranges(total: u64, n: usize) -> Vec<(u64, u64)> {
    assert!(n > 0, "connection count must be > 0");
    if total == 0 {
        return Vec::new();
    }
    let n = (n as u64).min(total) as usize;
    let base = total / n as u64;
    let rem = total % n as u64;
    let mut ranges = Vec::with_capacity(n);
    let mut start = 0u64;
    for i in 0..n {
        // Distribute remainder one byte each to first `rem` chunks.
        let len = base + u64::from((i as u64) < rem);
        let end = start + len - 1;
        ranges.push((start, end));
        start = end + 1;
    }
    ranges
}

/// Expected length of an inclusive range.
pub fn range_len(start: u64, end: u64) -> u64 {
    end - start + 1
}

/// Parse a `Content-Range: bytes start-end/total` header.
pub fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    // Example: "bytes 0-1023/1772480833" or "bytes */1772480833"
    let value = value.trim();
    let rest = value.strip_prefix("bytes ")?;
    let (range_part, total_part) = rest.split_once('/')?;
    let total: u64 = total_part.trim().parse().ok()?;
    if range_part.trim() == "*" {
        return None;
    }
    let (s, e) = range_part.split_once('-')?;
    let start: u64 = s.trim().parse().ok()?;
    let end: u64 = e.trim().parse().ok()?;
    if end < start || end >= total {
        return None;
    }
    Some((start, end, total))
}

/// Chunk file name inside the sidecar parts dir: `chunk-00003-of-00016.part`
pub fn chunk_file_name(index: usize, total_chunks: usize) -> String {
    let width = total_chunks.to_string().len().max(2);
    format!("chunk-{index:0width$}-of-{total_chunks:0width$}.part")
}

/// Check gzip magic bytes `1F 8B`.
pub fn has_gzip_magic(header: &[u8]) -> bool {
    header.len() >= 2 && header[0] == 0x1f && header[1] == 0x8b
}

/// Validate TSV header for Blockchair address dumps.
/// Known to start with `address\t` (first column is always the address).
pub fn looks_like_blockchair_addresses_tsv(first_line: &str) -> bool {
    let first = first_line.trim_start_matches('\u{feff}').to_lowercase();
    first.starts_with("address\t") || first.starts_with("address,")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_even() {
        assert_eq!(split_ranges(100, 4), vec![(0, 24), (25, 49), (50, 74), (75, 99)]);
    }

    #[test]
    fn split_with_remainder_covers_all() {
        let ranges = split_ranges(10, 3);
        assert_eq!(ranges, vec![(0, 3), (4, 6), (7, 9)]);
        // contiguous + full coverage
        assert_eq!(ranges[0].0, 0);
        assert_eq!(ranges.last().unwrap().1, 9);
        for w in ranges.windows(2) {
            assert_eq!(w[0].1 + 1, w[1].0);
        }
        let sum: u64 = ranges.iter().map(|(s, e)| range_len(*s, *e)).sum();
        assert_eq!(sum, 10);
    }

    #[test]
    fn split_more_connections_than_bytes() {
        let ranges = split_ranges(3, 16);
        assert_eq!(ranges, vec![(0, 0), (1, 1), (2, 2)]);
    }

    #[test]
    fn split_single() {
        assert_eq!(split_ranges(1772480833, 1), vec![(0, 1772480832)]);
    }

    #[test]
    fn split_sums_to_total_large() {
        let total = 1772480833u64;
        for n in [1, 2, 8, 16, 32] {
            let ranges = split_ranges(total, n);
            assert_eq!(ranges.len(), n);
            assert_eq!(ranges.last().unwrap().1, total - 1);
            let sum: u64 = ranges.iter().map(|(s, e)| range_len(*s, *e)).sum();
            assert_eq!(sum, total);
        }
    }

    #[test]
    fn key_appending() {
        assert_eq!(
            apply_key_to_url("https://example.com/f.tsv.gz", "").unwrap(),
            "https://example.com/f.tsv.gz"
        );
        assert_eq!(
            apply_key_to_url("https://example.com/f.tsv.gz", "SECRET").unwrap(),
            "https://example.com/f.tsv.gz?key=SECRET"
        );
        assert_eq!(
            apply_key_to_url("https://example.com/f.tsv.gz?a=b", "K").unwrap(),
            "https://example.com/f.tsv.gz?a=b&key=K"
        );
        // Do not duplicate existing key.
        let already = "https://example.com/f.tsv.gz?key=OLD";
        assert_eq!(apply_key_to_url(already, "NEW").unwrap(), already);
    }

    #[test]
    fn content_range_parsing() {
        assert_eq!(
            parse_content_range("bytes 0-1023/1772480833"),
            Some((0, 1023, 1772480833))
        );
        assert_eq!(parse_content_range("bytes */1772480833"), None);
        assert_eq!(parse_content_range("garbage"), None);
        assert_eq!(parse_content_range("bytes 5-3/10"), None);
    }

    #[test]
    fn gzip_magic() {
        assert!(has_gzip_magic(&[0x1f, 0x8b, 0x08, 0x00]));
        assert!(!has_gzip_magic(&[0x00, 0x01]));
        assert!(!has_gzip_magic(&[]));
    }

    #[test]
    fn tsv_header_check() {
        assert!(looks_like_blockchair_addresses_tsv(
            "address\tbalance\treceived\n"
        ));
        assert!(!looks_like_blockchair_addresses_tsv("hash\tvalue\n"));
    }

    #[test]
    fn chunk_naming_is_orderable() {
        let mut names: Vec<_> = (0..16).map(|i| chunk_file_name(i, 16)).collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
    }
}
