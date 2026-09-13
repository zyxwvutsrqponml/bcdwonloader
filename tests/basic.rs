use bcdownloader::{
    apply_key_to_url, backoff_delay, bytes_as_mib, chunk_file_name, expected_byte_count,
    parse_content_range, progress_line, progress_pct, range_len, split_ranges, validate_chunk_mib,
    validate_connections, DEFAULT_URL,
};
use std::time::Duration;

#[test]
fn default_url_points_to_bitcoin_addresses_latest() {
    assert!(DEFAULT_URL.contains("gz.blockchair.com/bitcoin/addresses/"));
    assert!(DEFAULT_URL.ends_with(".tsv.gz"));
}

#[test]
fn ranges_cover_real_dump_size_32_mib() {
    let total = 1_772_480_833u64;
    let chunk = 32 * 1024 * 1024;
    let ranges = split_ranges(total, chunk);
    assert_eq!(ranges[0].0, 0);
    assert_eq!(ranges.last().unwrap().1, total - 1);
    let sum: u64 = ranges.iter().map(|(s, e)| range_len(*s, *e)).sum();
    assert_eq!(sum, total);
    for w in ranges.windows(2) {
        assert_eq!(w[0].1 + 1, w[1].0);
    }
}

#[test]
fn key_env_handling() {
    let with_key = apply_key_to_url(DEFAULT_URL, "SECRET123").unwrap();
    assert!(with_key.contains("key=SECRET123"));
    let redacted = bcdownloader::redact_url(&with_key);
    assert!(!redacted.contains("SECRET123"));
    assert!(redacted.contains("key=***"));
}

#[test]
fn chunk_boundaries_user_example() {
    assert_eq!(
        split_ranges(100, 32),
        vec![(0, 31), (32, 63), (64, 95), (96, 99)]
    );
}

#[test]
fn invalid_cli_values() {
    assert!(validate_connections(0).is_err());
    assert!(validate_chunk_mib(0).is_err());
}

#[test]
fn progress_percentage_and_line() {
    let total = 1_772_480_833u64;
    assert_eq!(format!("{:.2}", bytes_as_mib(total)), "1690.37");
    let line = progress_line(38_596_608, total);
    assert!(line.starts_with("[progress] "));
    assert!(line.contains("/ 1690.37 MiB"));
    assert!((progress_pct(1, 4) - 25.0).abs() < f64::EPSILON);
}

#[test]
fn retry_backoff_calculation() {
    assert_eq!(
        backoff_delay(0, 1000, 30_000, 0),
        Duration::from_millis(1000)
    );
    assert_eq!(
        backoff_delay(1, 1000, 30_000, 0),
        Duration::from_millis(2000)
    );
    assert_eq!(
        backoff_delay(8, 1000, 30_000, 0),
        Duration::from_millis(30_000)
    );
}

#[test]
fn content_range_and_expected_bytes() {
    assert_eq!(
        parse_content_range("bytes 0-33554431/1772480833"),
        Some((0, 33_554_431, 1_772_480_833))
    );
    assert_eq!(expected_byte_count(0, 33_554_431), 33_554_432);
    assert_eq!(chunk_file_name(33_554_432), "00000000000033554432.part");
}
