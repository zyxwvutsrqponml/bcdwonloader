//! Range splitting, Content-Range parsing, URL helpers, sidecar paths.

use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};

/// Default Blockchair Bitcoin addresses dump.
pub const DEFAULT_URL: &str =
    "https://gz.blockchair.com/bitcoin/addresses/blockchair_bitcoin_addresses_latest.tsv.gz";

/// Append `?key=` / `&key=` unless the URL already carries a key or `key` is empty.
pub fn apply_key_to_url(url: &str, key: &str) -> Result<String> {
    let key = key.trim();
    if key.is_empty() {
        return Ok(url.to_string());
    }
    let mut parsed = url::Url::parse(url).map_err(|e| anyhow!("invalid URL {url:?}: {e}"))?;
    if parsed.query_pairs().any(|(k, _)| k == "key") {
        return Ok(parsed.to_string());
    }
    parsed.query_pairs_mut().append_pair("key", key);
    Ok(parsed.to_string())
}

/// Hide `key=` query values so logs never print credentials.
pub fn redact_url(url: &str) -> String {
    let Ok(mut parsed) = url::Url::parse(url) else {
        return redact_raw_url(url);
    };
    let pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(k, v)| {
            if k == "key" {
                (k.into_owned(), "***".to_string())
            } else {
                (k.into_owned(), v.into_owned())
            }
        })
        .collect();
    if pairs.is_empty() {
        return parsed.to_string();
    }
    parsed.set_query(None);
    parsed.query_pairs_mut().clear();
    for (k, v) in &pairs {
        parsed.query_pairs_mut().append_pair(k, v);
    }
    parsed.to_string()
}

/// Best-effort redaction for URLs that fail to parse: replace every
/// `key=<value>` (up to `&`, `#`, whitespace or end) with `key=***`.
fn redact_raw_url(url: &str) -> String {
    let mut out = String::with_capacity(url.len());
    let bytes = url.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if url[i..].starts_with("key=") {
            out.push_str("key=***");
            i += 4;
            while i < bytes.len() && !matches!(bytes[i], b'&' | b'#' | b' ' | b'\t' | b'\n') {
                i += 1;
            }
        } else {
            out.push(bytes[i] as char);
            i += 1;
        }
    }
    out
}

/// Split `total` bytes into inclusive `start-end` ranges of at most `chunk_size` bytes.
///
/// For `total = 100`, `chunk_size = 32`:
/// `0-31`, `32-63`, `64-95`, `96-99`.
///
/// Guarantees: contiguous, non-overlapping, cover `[0, total)`. No empty ranges.
pub fn split_ranges(total: u64, chunk_size: u64) -> Vec<(u64, u64)> {
    if total == 0 || chunk_size == 0 {
        return Vec::new();
    }
    let mut ranges = Vec::new();
    let mut start = 0u64;
    while start < total {
        let end = start
            .saturating_add(chunk_size)
            .saturating_sub(1)
            .min(total - 1);
        ranges.push((start, end));
        if end == u64::MAX {
            break;
        }
        start = end.saturating_add(1);
        if start == 0 {
            break;
        }
    }
    ranges
}

/// Inclusive range length. Returns 0 if `end < start`.
pub fn range_len(start: u64, end: u64) -> u64 {
    if end < start {
        0
    } else {
        end - start + 1
    }
}

/// Expected number of bytes for an inclusive range (alias of [`range_len`]).
pub fn expected_byte_count(start: u64, end: u64) -> u64 {
    range_len(start, end)
}

/// Parse `Content-Range: bytes start-end/total` (total may be `*`).
///
/// Returns `(start, end, total)` where `total == 0` means unknown (`*`).
pub fn parse_content_range(value: &str) -> Option<(u64, u64, u64)> {
    let value = value.trim();
    let rest = value
        .strip_prefix("bytes ")
        .or_else(|| value.strip_prefix("bytes="))?;
    let (range_part, total_part) = rest.split_once('/')?;
    let total_part = total_part.trim();
    let total: u64 = if total_part == "*" {
        0
    } else {
        total_part.parse().ok()?
    };
    if range_part.trim() == "*" {
        return None;
    }
    let (s, e) = range_part.split_once('-')?;
    let start: u64 = s.trim().parse().ok()?;
    let end: u64 = e.trim().parse().ok()?;
    if end < start {
        return None;
    }
    if total > 0 && end >= total {
        return None;
    }
    Some((start, end, total))
}

/// Sidecar directory: `output.gz` → `output.gz.parts/`.
pub fn parts_dir(output: &Path) -> PathBuf {
    let mut s = output.as_os_str().to_owned();
    s.push(".parts");
    PathBuf::from(s)
}

/// Merge temp file: `output.gz` → `output.gz.part`.
pub fn merge_temp_path(output: &Path) -> PathBuf {
    let mut s = output.as_os_str().to_owned();
    s.push(".part");
    PathBuf::from(s)
}

/// Chunk file named by start offset: `00000000000000000000.part`.
pub fn chunk_file_name(start: u64) -> String {
    format!("{start:020}.part")
}

/// Gzip magic `1F 8B`.
pub fn has_gzip_magic(header: &[u8]) -> bool {
    header.len() >= 2 && header[0] == 0x1f && header[1] == 0x8b
}

/// Blockchair address dumps start with `address\t` (or CSV `address,`).
pub fn looks_like_blockchair_addresses_tsv(first_line: &str) -> bool {
    let first = first_line.trim_start_matches('\u{feff}').to_lowercase();
    first.starts_with("address\t") || first.starts_with("address,")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_example_total_100_chunk_32() {
        let ranges = split_ranges(100, 32);
        assert_eq!(ranges, vec![(0, 31), (32, 63), (64, 95), (96, 99)]);
        assert_no_gaps_or_overlap(&ranges, 100);
    }

    #[test]
    fn final_chunk_smaller_than_normal() {
        let ranges = split_ranges(100, 32);
        assert_eq!(range_len(ranges[0].0, ranges[0].1), 32);
        assert_eq!(range_len(ranges[3].0, ranges[3].1), 4);
    }

    #[test]
    fn zero_byte_file() {
        assert!(split_ranges(0, 32).is_empty());
        assert!(split_ranges(100, 0).is_empty());
    }

    #[test]
    fn exact_chunk_size_file() {
        let ranges = split_ranges(32, 32);
        assert_eq!(ranges, vec![(0, 31)]);
        assert_eq!(range_len(0, 31), 32);
    }

    #[test]
    fn single_byte() {
        assert_eq!(split_ranges(1, 32), vec![(0, 0)]);
    }

    #[test]
    fn real_dump_32_mib() {
        let total = 1_772_480_833u64;
        let chunk = 32 * 1024 * 1024;
        let ranges = split_ranges(total, chunk);
        assert_eq!(ranges[0], (0, chunk - 1));
        assert_eq!(ranges.last().unwrap().1, total - 1);
        assert_no_gaps_or_overlap(&ranges, total);
        let last_len = range_len(ranges.last().unwrap().0, ranges.last().unwrap().1);
        assert!(last_len > 0 && last_len <= chunk);
    }

    #[test]
    fn chunk_names_are_offset_based_and_sorted() {
        let names: Vec<_> = [0u64, 33_554_432, 67_108_864]
            .into_iter()
            .map(chunk_file_name)
            .collect();
        assert_eq!(names[0], "00000000000000000000.part");
        assert_eq!(names[1], "00000000000033554432.part");
        assert_eq!(names[2], "00000000000067108864.part");
        for n in &names {
            assert_eq!(n.len(), "00000000000000000000.part".len(), "{n}");
        }
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
    }

    fn assert_no_gaps_or_overlap(ranges: &[(u64, u64)], total: u64) {
        assert_eq!(ranges.first().map(|r| r.0), Some(0));
        assert_eq!(ranges.last().map(|r| r.1), Some(total - 1));
        for w in ranges.windows(2) {
            assert_eq!(w[0].1 + 1, w[1].0, "gap or overlap between {w:?}");
        }
        let sum: u64 = ranges.iter().map(|(s, e)| range_len(*s, *e)).sum();
        assert_eq!(sum, total);
    }

    #[test]
    fn content_range_parsing() {
        assert_eq!(
            parse_content_range("bytes 0-1023/1772480833"),
            Some((0, 1023, 1_772_480_833))
        );
        assert_eq!(
            parse_content_range("bytes 100-199/200"),
            Some((100, 199, 200))
        );
        assert_eq!(parse_content_range("bytes */1772480833"), None);
        assert_eq!(parse_content_range("bytes 0-0/*"), Some((0, 0, 0)));
        assert_eq!(parse_content_range("garbage"), None);
        assert_eq!(parse_content_range("bytes 5-3/10"), None);
        assert_eq!(parse_content_range("bytes 5-9/9"), None);
        assert_eq!(parse_content_range("bytes=0-31/100"), Some((0, 31, 100)));
    }

    #[test]
    fn expected_byte_count_matches_range() {
        assert_eq!(expected_byte_count(0, 31), 32);
        assert_eq!(expected_byte_count(96, 99), 4);
        assert_eq!(expected_byte_count(5, 4), 0);
    }

    #[test]
    fn key_appending_and_redaction() {
        assert_eq!(
            apply_key_to_url("https://example.com/f.tsv.gz", "").unwrap(),
            "https://example.com/f.tsv.gz"
        );
        let with = apply_key_to_url("https://example.com/f.tsv.gz", "SECRET").unwrap();
        assert_eq!(with, "https://example.com/f.tsv.gz?key=SECRET");
        let redacted = redact_url(&with);
        assert!(redacted.contains("key=***"), "{redacted}");
        assert!(!redacted.contains("SECRET"), "{redacted}");
        let already = "https://example.com/f.tsv.gz?key=OLD";
        assert_eq!(apply_key_to_url(already, "NEW").unwrap(), already);
    }

    #[test]
    fn redaction_fallback_for_unparseable_url() {
        // Must actually hide the secret even when `Url::parse` fails.
        let redacted = redact_url("not a url?key=SUPERSECRET&x=1");
        assert!(redacted.contains("key=***"), "{redacted}");
        assert!(!redacted.contains("SUPERSECRET"), "{redacted}");
        let redacted = redact_url("http://[::1/named?key=ABC");
        assert!(!redacted.contains("key=ABC"), "{redacted}");
    }

    #[test]
    fn sidecar_paths() {
        let out = Path::new("dump/blockchair_bitcoin_addresses_latest.tsv.gz");
        assert_eq!(
            parts_dir(out).as_os_str(),
            "dump/blockchair_bitcoin_addresses_latest.tsv.gz.parts"
        );
        assert_eq!(
            merge_temp_path(out).as_os_str(),
            "dump/blockchair_bitcoin_addresses_latest.tsv.gz.part"
        );
    }
}
