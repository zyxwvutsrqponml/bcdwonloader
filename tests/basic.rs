use bcdownloader::{apply_key_to_url, split_ranges, DEFAULT_URL};

#[test]
fn default_url_points_to_bitcoin_addresses_latest() {
    assert!(DEFAULT_URL.contains("gz.blockchair.com/bitcoin/addresses/"));
    assert!(DEFAULT_URL.ends_with(".tsv.gz"));
}

#[test]
fn ranges_cover_real_dump_size() {
    // Real size observed 2026-09-13: 1772480833 bytes.
    let total = 1_772_480_833u64;
    let ranges = split_ranges(total, 16);
    assert_eq!(ranges.len(), 16);
    assert_eq!(ranges[0].0, 0);
    assert_eq!(ranges.last().unwrap().1, total - 1);
}

#[test]
fn key_env_handling() {
    let with_key = apply_key_to_url(DEFAULT_URL, "SECRET123").unwrap();
    assert!(with_key.contains("key=SECRET123"));
}
