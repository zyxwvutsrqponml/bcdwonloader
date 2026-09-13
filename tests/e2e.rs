//! End-to-end tests against a local HTTP/1.1 server that supports Range.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use bcdownloader::{download, is_interrupted, DownloadConfig};
use tokio::sync::watch;

#[derive(Default)]
struct Behavior {
    remain_429: u32,
    always_402: bool,
    always_416: bool,
    reset_once_after: Option<usize>,
    wrong_content_range: bool,
    ignore_range: bool,
    /// HEAD omits `Accept-Ranges`, routing the client into single-stream mode.
    no_ranges: bool,
    hits: u32,
}

struct TestServer {
    url: String,
    behavior: Arc<Mutex<Behavior>>,
    /// Kept so the accept thread stays alive for the test.
    _hold: Arc<AtomicU64>,
}

impl TestServer {
    fn start(body: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        listener.set_nonblocking(true).ok();
        let addr = listener.local_addr().unwrap();
        let body = Arc::new(body);
        let behavior = Arc::new(Mutex::new(Behavior::default()));
        let hold = Arc::new(AtomicU64::new(1));
        let hold2 = Arc::clone(&hold);
        let behavior2 = Arc::clone(&behavior);
        thread::spawn(move || {
            while hold2.load(Ordering::Relaxed) == 1 {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let body = Arc::clone(&body);
                        let behavior = Arc::clone(&behavior2);
                        thread::spawn(move || {
                            let _ = handle_conn(stream, &body, &behavior);
                        });
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            url: format!("http://{addr}/dump.tsv.gz"),
            behavior,
            _hold: hold,
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self._hold.store(0, Ordering::Relaxed);
    }
}

fn handle_conn(
    mut stream: TcpStream,
    body: &[u8],
    behavior: &Mutex<Behavior>,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(5))).ok();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        let n = match stream.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => return Err(e),
        };
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 64 * 1024 {
            break;
        }
    }
    let req = String::from_utf8_lossy(&buf);
    let is_head = req.starts_with("HEAD ");
    let is_get = req.starts_with("GET ");
    if !is_head && !is_get {
        write_http(&mut stream, 400, &[], None)?;
        return Ok(());
    }

    let range = parse_range_header(&req);
    let total = body.len() as u64;

    let mut b = behavior.lock().unwrap();
    b.hits += 1;
    if b.always_402 {
        drop(b);
        write_http(&mut stream, 402, b"payment required", None)?;
        return Ok(());
    }
    if b.always_416 && is_get {
        drop(b);
        let extra = format!("Content-Range: bytes */{total}\r\n");
        write_status(&mut stream, 416, &extra, &[])?;
        return Ok(());
    }
    if b.remain_429 > 0 && is_get && range.is_some() {
        b.remain_429 -= 1;
        drop(b);
        write_status(&mut stream, 429, "Retry-After: 0\r\n", b"slow down")?;
        return Ok(());
    }
    let reset_after = if is_get {
        b.reset_once_after.take()
    } else {
        None
    };
    let wrong_cr = b.wrong_content_range;
    let ignore_range = b.ignore_range;
    let no_ranges = b.no_ranges;
    drop(b);

    if is_head {
        let headers = if no_ranges {
            format!("Content-Length: {total}\r\nETag: \"test\"\r\n")
        } else {
            format!("Content-Length: {total}\r\nAccept-Ranges: bytes\r\nETag: \"test\"\r\n")
        };
        write_status(&mut stream, 200, &headers, &[])?;
        return Ok(());
    }

    if ignore_range {
        write_status(
            &mut stream,
            200,
            &format!("Content-Length: {total}\r\n"),
            body,
        )?;
        return Ok(());
    }

    let (start, end) = match range {
        Some((s, e)) => (s, e.min(total.saturating_sub(1))),
        None => (0, total.saturating_sub(1)),
    };
    if start >= total {
        write_status(
            &mut stream,
            416,
            &format!("Content-Range: bytes */{total}\r\n"),
            &[],
        )?;
        return Ok(());
    }
    let end = end.max(start);
    let slice = &body[start as usize..=end as usize];
    if wrong_cr {
        write_status(
            &mut stream,
            206,
            &format!(
                "Content-Range: bytes {}-{}/{total}\r\nContent-Length: {}\r\n",
                start.saturating_add(99),
                end,
                slice.len()
            ),
            slice,
        )?;
        return Ok(());
    }
    let headers = format!(
        "Content-Range: bytes {start}-{end}/{total}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n",
        slice.len()
    );
    if let Some(n) = reset_after {
        let n = n.min(slice.len());
        let head = format!("HTTP/1.1 206 Partial Content\r\n{headers}Connection: close\r\n\r\n");
        stream.write_all(head.as_bytes())?;
        stream.write_all(&slice[..n])?;
        stream.flush()?;
        return Ok(());
    }
    write_status(&mut stream, 206, &headers, slice)?;
    Ok(())
}

fn parse_range_header(req: &str) -> Option<(u64, u64)> {
    for line in req.lines() {
        let line = line.trim();
        let lower = line.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("range:") {
            let orig = &line[line.len() - rest.len()..];
            let rest = orig.trim();
            let rest = rest
                .strip_prefix("bytes=")
                .or_else(|| rest.strip_prefix("bytes:"))?;
            let (s, e) = rest.split_once('-')?;
            let start: u64 = s.trim().parse().ok()?;
            let end: u64 = e.trim().parse().ok()?;
            return Some((start, end));
        }
    }
    None
}

fn write_http(
    stream: &mut TcpStream,
    code: u16,
    body: &[u8],
    extra: Option<&str>,
) -> std::io::Result<()> {
    let extra = extra.unwrap_or("");
    write_status(stream, code, extra, body)
}

fn write_status(
    stream: &mut TcpStream,
    code: u16,
    extra_headers: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let reason = match code {
        200 => "OK",
        206 => "Partial Content",
        402 => "Payment Required",
        416 => "Range Not Satisfiable",
        429 => "Too Many Requests",
        400 => "Bad Request",
        _ => "Error",
    };
    let cl = if extra_headers
        .to_ascii_lowercase()
        .contains("content-length:")
    {
        String::new()
    } else {
        format!("Content-Length: {}\r\n", body.len())
    };
    let head = format!("HTTP/1.1 {code} {reason}\r\n{cl}Connection: close\r\n{extra_headers}\r\n");
    stream.write_all(head.as_bytes())?;
    if !body.is_empty() {
        stream.write_all(body)?;
    }
    stream.flush()
}

fn gzip_payload(plain: &[u8]) -> Vec<u8> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    let mut enc = GzEncoder::new(Vec::new(), Compression::default());
    enc.write_all(plain).unwrap();
    enc.finish().unwrap()
}

/// Deterministic pseudo-random bytes (splitmix64) that do NOT compress well.
/// Tests that used `(i % 251)` or `vec![7u8; N]` patterns produced gzip bodies
/// of only a few hundred bytes, breaking every multi-chunk assumption.
fn incompressible(len: usize, seed: u64) -> Vec<u8> {
    let mut state = seed.max(1);
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        state = state
            .wrapping_add(0x9E3779B97F4A7C15)
            .wrapping_mul(0xBF58476D1CE4E5B9);
        state ^= state >> 29;
        out.push((state >> 33) as u8);
    }
    out
}

fn cfg(url: &str, output: PathBuf) -> DownloadConfig {
    DownloadConfig {
        url: url.to_string(),
        output,
        connections: 2,
        chunk_mib: 1,
        chunk_bytes: Some(16_384),
        retries: 8,
        timeout_secs: 5,
        key: String::new(),
        force: false,
        no_resume: false,
        verify_gzip: false,
        decompress: false,
        no_progress: true,
    }
}

fn never_cancel() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

fn unique_out(tmp: &tempfile::TempDir, name: &str) -> PathBuf {
    tmp.path().join(name)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn downloads_with_valid_206_and_merges() {
    let plain = incompressible(120_000, 0x1234);
    let body = gzip_payload(&plain);
    assert!(
        body.len() > 32_768,
        "payload should span multiple 16 KiB chunks"
    );
    let expected = body.clone();
    let server = TestServer::start(body);
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "out.tsv.gz");
    let mut c = cfg(&server.url, out.clone());
    c.connections = 2;
    let (_tx, rx) = never_cancel();
    download(c, rx).await.unwrap();
    let got = std::fs::read(&out).unwrap();
    assert_eq!(got, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiple_chunks_cover_small_file() {
    let plain = incompressible(90_000, 0x5678);
    let body = gzip_payload(&plain);
    let expected_len = body.len() as u64;
    let n_chunks = bcdownloader::split_ranges(expected_len, 16_384).len();
    assert!(
        n_chunks >= 2,
        "need multiple chunks, got {n_chunks} for {expected_len} bytes"
    );
    let server = TestServer::start(body.clone());
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "multi.tsv.gz");
    let mut c = cfg(&server.url, out.clone());
    c.connections = 4;
    let (_tx, rx) = never_cancel();
    download(c, rx).await.unwrap();
    assert_eq!(std::fs::metadata(&out).unwrap().len(), expected_len);
    assert_eq!(std::fs::read(&out).unwrap(), body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumes_after_partial_part_file() {
    let plain = incompressible(80_000, 0x9ABC);
    let body = gzip_payload(&plain);
    let expected = body.clone();
    let server = TestServer::start(body);
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "resume.tsv.gz");
    let parts = {
        let mut s = out.as_os_str().to_owned();
        s.push(".parts");
        PathBuf::from(s)
    };
    std::fs::create_dir_all(&parts).unwrap();
    let partial = parts.join("00000000000000000000.part");
    let cut = 1000.min(expected.len().saturating_sub(1).max(1));
    std::fs::write(&partial, &expected[..cut]).unwrap();

    let mut c = cfg(&server.url, out.clone());
    c.connections = 1;
    let (_tx, rx) = never_cancel();
    download(c, rx).await.unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_reset_then_resume() {
    let plain = vec![9u8; 60_000];
    let body = gzip_payload(&plain);
    let expected = body.clone();
    let server = TestServer::start(body);
    server.behavior.lock().unwrap().reset_once_after = Some(1500);
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "reset.tsv.gz");
    let mut c = cfg(&server.url, out.clone());
    c.connections = 1;
    c.retries = 10;
    let (_tx, rx) = never_cancel();
    download(c, rx).await.unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_429_then_success() {
    let plain = b"address\tok\n".as_slice();
    let body = gzip_payload(plain);
    let server = TestServer::start(body.clone());
    server.behavior.lock().unwrap().remain_429 = 2;
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "r429.tsv.gz");
    let mut c = cfg(&server.url, out.clone());
    c.connections = 1;
    c.retries = 10;
    let (_tx, rx) = never_cancel();
    download(c, rx).await.unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_402_stops_with_clear_error() {
    let body = gzip_payload(b"nope");
    let server = TestServer::start(body);
    server.behavior.lock().unwrap().always_402 = true;
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "r402.tsv.gz");
    let c = cfg(&server.url, out);
    let (_tx, rx) = never_cancel();
    let err = download(c, rx).await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("402"), "{msg}");
    assert!(
        msg.to_ascii_lowercase().contains("key") || msg.contains("Payment Required"),
        "{msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_416_on_incomplete_chunk_is_error() {
    let body = gzip_payload(&vec![1u8; 8000]);
    let server = TestServer::start(body);
    server.behavior.lock().unwrap().always_416 = true;
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "r416.tsv.gz");
    let mut c = cfg(&server.url, out);
    c.connections = 1;
    let (_tx, rx) = never_cancel();
    let err = download(c, rx).await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("416"), "{msg}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_content_range_is_rejected() {
    let body = gzip_payload(&vec![2u8; 4000]);
    let server = TestServer::start(body);
    server.behavior.lock().unwrap().wrong_content_range = true;
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "wrongcr.tsv.gz");
    let mut c = cfg(&server.url, out);
    c.connections = 1;
    let (_tx, rx) = never_cancel();
    let err = download(c, rx).await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("Content-Range") || msg.contains("206"),
        "{msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ignore_range_full_body_rejected_for_chunk() {
    let plain = incompressible(80_000, 0xDEF0);
    let body = gzip_payload(&plain);
    assert!(body.len() > 16_384);
    let server = TestServer::start(body);
    server.behavior.lock().unwrap().ignore_range = true;
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "norange.tsv.gz");
    let mut c = cfg(&server.url, out);
    c.connections = 2;
    let (_tx, rx) = never_cancel();
    let err = download(c, rx).await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("ignored Range") || msg.contains("HTTP 200") || msg.contains("wrong bytes"),
        "{msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_stream_restarts_when_server_ignores_range() {
    // Server advertises no Accept-Ranges and answers every GET with 200 + full
    // body. A stale `.part` temp file must be discarded and re-downloaded from
    // scratch (splicing a full body onto a partial file would corrupt it).
    let plain = incompressible(40_000, 0xBEEF);
    let body = gzip_payload(&plain);
    let expected = body.clone();
    let server = TestServer::start(body);
    {
        let mut b = server.behavior.lock().unwrap();
        b.no_ranges = true;
        b.ignore_range = true;
    }
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "stream.tsv.gz");
    let stale: PathBuf = {
        let mut s = out.as_os_str().to_owned();
        s.push(".part");
        PathBuf::from(s)
    };
    std::fs::write(&stale, vec![0xAAu8; 5_000]).unwrap();
    let mut c = cfg(&server.url, out.clone());
    c.connections = 2;
    let (_tx, rx) = never_cancel();
    download(c, rx).await.unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_part_is_truncated() {
    let plain = vec![4u8; 20_000];
    let body = gzip_payload(&plain);
    let expected = body.clone();
    let server = TestServer::start(body);
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "oversize.tsv.gz");
    let parts = {
        let mut s = out.as_os_str().to_owned();
        s.push(".parts");
        PathBuf::from(s)
    };
    std::fs::create_dir_all(&parts).unwrap();
    let garbage = vec![0xFFu8; expected.len() + 500];
    std::fs::write(parts.join("00000000000000000000.part"), garbage).unwrap();
    let mut c = cfg(&server.url, out.clone());
    c.connections = 1;
    let (_tx, rx) = never_cancel();
    download(c, rx).await.unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_preserves_parts_and_resume_completes() {
    let plain = vec![5u8; 100_000];
    let body = gzip_payload(&plain);
    let expected = body.clone();
    let server = TestServer::start(body);
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "intr.tsv.gz");

    let (tx, rx) = watch::channel(false);
    let mut c = cfg(&server.url, out.clone());
    c.connections = 1;
    // Cancel immediately so the first run is interrupted (parts may or may not exist).
    let _ = tx.send(true);
    let err = download(c.clone(), rx).await.unwrap_err();
    assert!(is_interrupted(&err), "{err:#}");
    assert!(
        !out.exists(),
        "final output must not be replaced on interrupt"
    );

    let (_tx2, rx2) = never_cancel();
    c.force = false;
    download(c, rx2).await.unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn verify_gzip_succeeds_on_valid_payload() {
    let plain = b"address\tbalance\nzz\t0\n";
    let body = gzip_payload(plain);
    let server = TestServer::start(body);
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "vg.tsv.gz");
    let mut c = cfg(&server.url, out);
    c.verify_gzip = true;
    c.connections = 1;
    let (_tx, rx) = never_cancel();
    download(c, rx).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_help_and_invalid_args() {
    let bin = env!("CARGO_BIN_EXE_bcdownloader");
    let out = std::process::Command::new(bin)
        .arg("--help")
        .output()
        .expect("run --help");
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.to_ascii_lowercase().contains("blockchair"));
    assert!(stdout.contains("chunk-mib"));
    assert!(stdout.contains("connections"));

    let out = std::process::Command::new(bin)
        .args(["--connections", "0", "-o", "x.gz", "http://127.0.0.1:1/x"])
        .output()
        .expect("run invalid connections");
    assert!(!out.status.success());
    let err = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    assert!(
        err.contains("connections") || err.contains("invalid") || err.contains("error"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_downloads_from_range_server() {
    let body = gzip_payload(b"address\tbalance\nok\t1\n");
    let server = TestServer::start(body.clone());
    let tmp = tempfile::tempdir().unwrap();
    let out = unique_out(&tmp, "bin.tsv.gz");
    let bin = env!("CARGO_BIN_EXE_bcdownloader");
    let status = std::process::Command::new(bin)
        .args([
            &server.url,
            "-c",
            "2",
            "--chunk-mib",
            "1",
            "-o",
            out.to_str().unwrap(),
            "--no-progress",
        ])
        .status()
        .expect("spawn binary");
    assert!(status.success());
    assert_eq!(std::fs::read(&out).unwrap(), body);
}

// Silence unused import if Result is not needed in every cfg.
#[allow(dead_code)]
fn _keep_result(_: Result<()>) {}
