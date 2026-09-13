# bcdownloader — perfect Blockchair dump downloader (Rust)

Fast, accurate, resumable parallel downloader optimized for Blockchair database dumps, in particular:

```text
https://gz.blockchair.com/bitcoin/addresses/blockchair_bitcoin_addresses_latest.tsv.gz
```

## 1. Source analysis (verified 2026-09-13)

| Property | Value |
|---|---|
| URL | `https://gz.blockchair.com/bitcoin/addresses/blockchair_bitcoin_addresses_latest.tsv.gz` |
| Index | `https://gz.blockchair.com/bitcoin/addresses/` — single `latest` file (addresses are a snapshot, not date-sharded like outputs/transactions) |
| Size | `Content-Length: 1772480833` (~1.65 GiB, listed as `2G`) |
| `Last-Modified` | `Sun, 13 Sep 2026 01:21:44 GMT` — updated daily |
| `ETag` | `"6aa5faa8-69a5e941"` — present, used for `If-Match` resume validation |
| `Accept-Ranges` | `bytes` — resume + parallel `Range` supported |
| `Content-Type` | `application/octet-stream` |
| Format | gzip-compressed TSV (`.tsv.gz`, magic `1F 8B`). First column `address`. Decompressed is many GB — do not decompress unless you have disk space. |
| Throttle | **10 kB/s per connection by default**. Unlock with `?key=SECRETKEY` from `info@blockchair.com` (see `https://gz.blockchair.com/README.html`). This tool compensates with N parallel Range connections and documents the key path. |
| Total Blockchair data | >1 TB compressed, TSV, daily (`https://blockchair.com/dumps`) |

Why parallel? At 10 kB/s, 1.77 GB single-stream takes ~49 hours. 16 connections ≈ 160 kB/s ≈ ~3 h. With a key, full speed.

Existing prior art (`blockchair-dl`) uses the same Range-parallel trick; `bcdownloader` adds resume, ETag validation, atomic merge, gzip verification, and strict CI.

## 2. Features (perfect & accurate)

- **Parallel Range download** (`--connections 1..64`, default 16) with contiguous non-overlapping `bytes=start-end` split covering `[0, total)`.
- **Resumable**: sidecar `*.parts/` chunk files, per-chunk resume, `--no-resume` to start fresh, stale-part detection on upstream change.
- **Accurate**: `HEAD` + `GET bytes=0-0` probe, `Content-Length` / `Content-Range` parsing, `ETag` / `Last-Modified` conditional resume (`If-Match` / `If-Unmodified-Since`), final size check, gzip-magic check, optional full streaming gzip test (`--verify-gzip`) + TSV header sniff.
- **Robust**: per-chunk exponential backoff retries (`--retries`, default 10), timeouts, `416`/`412` handling (upstream changed), atomic `*.gz.part` → final rename, merge verification.
- **Key support**: `--key` / `BLOCKCHAIR_KEY` env / `?key=` in URL (no duplication).
- **Progress**: `indicatif` bar with bytes, speed, ETA; `--no-progress` for CI.
- **Decompress**: `--decompress` streams `foo.tsv.gz` → `foo.tsv` after verified download.
- **Cross-platform**: Linux / Windows / macOS, tested in CI.

## 3. Install

### From source (requires Rust 1.75+)

```powershell
cargo build --release
./target/release/bcdownloader --help
```

### From GitHub Release

Download `bcdownloader-<target>.tar.gz/.zip` from Releases (built by `release.yml`), verify `sha256`, unpack.

## 4. Usage

```bash
# Default: latest Bitcoin addresses, 16 connections
bcdownloader

# Custom output + 32 connections + full gzip verify
bcdownloader -o ./data/addrs.tsv.gz -c 32 --verify-gzip

# With speed-unlock key (env preferred so key isn't in shell history)
export BLOCKCHAIR_KEY=SECRETKEY
bcdownloader -o addrs.tsv.gz

# Or explicit flag / URL-embedded key
bcdownloader --key SECRETKEY -o addrs.tsv.gz
bcdownloader "https://gz.blockchair.com/bitcoin/addresses/blockchair_bitcoin_addresses_latest.tsv.gz?key=SECRETKEY"

# Single-stream (debug / servers without Range)
bcdownloader -c 1 -o addrs.tsv.gz

# Resume is automatic; force redownload:
bcdownloader --force -o addrs.tsv.gz

# Discard partials:
bcdownloader --no-resume -o addrs.tsv.gz

# Download + decompress to .tsv (needs ~10 GB+ free):
bcdownloader -o addrs.tsv.gz --decompress

# Quiet for CI logs:
bcdownloader --no-progress -o addrs.tsv.gz
```

Full help:

```text
bcdownloader --help
```

### Realtime CLI UI

Every run prints a static header, then a live dashboard at 8 Hz:

```text
== bcdownloader v0.1.0 ==
   URL    : .../blockchair_bitcoin_addresses_latest.tsv.gz
   output : blockchair_bitcoin_addresses_latest.tsv.gz
   size   : 1.65 GiB (1772480833 bytes)
   conns  : 16 | resume : on | key : no (10 kB/s per conn)
------------------------------------------------------------
⠁ TOTAL [00:04:12] [###########>----------------] 412.5 MiB/1.65 GiB (25%) 1.62 MiB/s ETA 12:40 downloading
  ⠁ chunk #0  [########>-------------------] 28.1 MiB/110.8 MiB 102.4 KiB/s
  ⠉ chunk #1  [#########>------------------] 30.4 MiB/110.8 MiB 101.8 KiB/s
  ...
------------------------------------------------------------
[done] 1.65 GiB in 12:40 | avg 2.21 MiB/s | saved blockchair_bitcoin_addresses_latest.tsv.gz
```

- Top `TOTAL` bar: elapsed, wide bar, bytes, percent, live speed, ETA, phase (`downloading` → `merging` → `verifying` → `done`).
- One `chunk #i` bar per connection (up to 16; beyond that workers run headless and `TOTAL` shows the worker count).
- Per-chunk states: byte range → `retry k/n` → `reconnecting` → `done` / `cached` / `failed`.
- `--no-progress` hides all bars for CI logs; header + summary still print.

## 5. How it works

1. `apply_key_to_url()` appends key safely.
2. `probe()`: `HEAD` → `total`, `Accept-Ranges`, `ETag`, `Last-Modified`; fallback `GET Range: bytes=0-0` → parse `Content-Range`.
3. If output exists and `len == total` → skip (unless `--force`).
4. If `supports_range && total && connections>1`: `split_ranges(total, n)` → `N` chunk files in `output.parts/` → concurrent `download_range()` with resume offset `start+have` → progress → verify each chunk → `merge_chunks()` → size + gzip-magic check → atomic rename.
5. Else single-stream with resume.
6. Optional `--verify-gzip` (full `flate2` streaming decode + first-line TSV check) and `--decompress`.

Pure helpers (`split_ranges`, `parse_content_range`, `apply_key_to_url`, gzip/TSV sniff) are unit-tested without network (`cargo test`).

## 6. GitHub CI (perfect & accurate)

- `.github/workflows/ci.yml` (push/PR):
  - `cargo fmt --check`
  - `cargo clippy --all-targets -- -D warnings`
  - `cargo test --verbose`
  - Matrix `cargo build` (debug+release) on `ubuntu/windows/macos` + `--help` smoke test, `Swatinem/rust-cache`, concurrency cancel, minimal `contents: read`.
- `.github/workflows/release.yml` (tag `v*`):
  - Builds `--locked --release` for `x86_64-linux`, `x86_64-windows`, `x86_64-macos`, `aarch64-macos`, packages `tar.gz`/`zip` + `sha256`, uploads artifacts, publishes GitHub Release with notes.

```bash
git tag v0.1.0
git push origin v0.1.0
```

## 7. Accuracy notes / limitations

- Blockchair publishes no checksum; accuracy = `Content-Length` + `ETag`/`Last-Modified` guards + gzip integrity + TSV sniff. For research-grade assurance, run `--verify-gzip`.
- `latest` is a daily snapshot; `ETag` changes daily. If download spans the midnight UTC refresh, you get `412/416` with a clear message — delete partials and retry (by design, to avoid mixing two snapshots).
- Without a key you are still throttled per-connection; more connections help but a key is the only full-speed path. Contact `info@blockchair.com`.

## License

MIT OR Apache-2.0
