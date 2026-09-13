//! CLI / config validation. Returns helpful errors instead of panicking.

use anyhow::{anyhow, Result};
use std::path::Path;

pub fn validate_connections(n: u32) -> Result<usize> {
    if n < 1 {
        return Err(anyhow!("connections must be >= 1 (got {n})"));
    }
    if n > 64 {
        return Err(anyhow!(
            "connections must be <= 64 (got {n}); excessive concurrency can trigger HTTP 429/402"
        ));
    }
    Ok(n as usize)
}

pub fn validate_chunk_mib(n: u64) -> Result<u64> {
    if n < 1 {
        return Err(anyhow!("--chunk-mib must be >= 1 (got {n})"));
    }
    if n > 2048 {
        return Err(anyhow!("--chunk-mib must be <= 2048 (got {n})"));
    }
    Ok(n.saturating_mul(1024 * 1024))
}

pub fn validate_retries(n: i64) -> Result<u32> {
    if n < 0 {
        return Err(anyhow!("retries must be >= 0 (got {n})"));
    }
    if n > 10_000 {
        return Err(anyhow!("retries is unreasonably large ({n})"));
    }
    Ok(n as u32)
}

pub fn validate_url(url: &str) -> Result<url::Url> {
    let url = url.trim();
    if url.is_empty() {
        return Err(anyhow!("URL is empty"));
    }
    let parsed = url::Url::parse(url).map_err(|e| anyhow!("invalid URL {url:?}: {e}"))?;
    match parsed.scheme() {
        "http" | "https" => Ok(parsed),
        other => Err(anyhow!(
            "URL scheme must be http or https, not {other:?} ({url})"
        )),
    }
}

pub fn validate_output(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() {
        return Err(anyhow!("output path is empty"));
    }
    if path.is_dir() {
        return Err(anyhow!(
            "output path {} is a directory; pass a file path",
            path.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn invalid_connection_count() {
        assert!(validate_connections(0).is_err());
        assert!(validate_connections(1).is_ok());
        assert!(validate_connections(4).is_ok());
        assert!(validate_connections(65).is_err());
    }

    #[test]
    fn invalid_chunk_size() {
        assert!(validate_chunk_mib(0).is_err());
        assert_eq!(validate_chunk_mib(1).unwrap(), 1024 * 1024);
        assert_eq!(validate_chunk_mib(32).unwrap(), 32 * 1024 * 1024);
        assert!(validate_chunk_mib(3000).is_err());
    }

    #[test]
    fn retries_validation() {
        assert!(validate_retries(-1).is_err());
        assert_eq!(validate_retries(0).unwrap(), 0);
        assert_eq!(validate_retries(20).unwrap(), 20);
    }

    #[test]
    fn url_validation() {
        assert!(validate_url("https://gz.blockchair.com/file.gz").is_ok());
        assert!(validate_url("http://127.0.0.1:9/x").is_ok());
        assert!(validate_url("ftp://example.com/x").is_err());
        assert!(validate_url("not a url").is_err());
        assert!(validate_url("").is_err());
    }

    #[test]
    fn output_validation() {
        assert!(validate_output(Path::new("")).is_err());
        assert!(validate_output(Path::new("out.tsv.gz")).is_ok());
        let dir = PathBuf::from(".");
        assert!(validate_output(&dir).is_err());
    }
}
