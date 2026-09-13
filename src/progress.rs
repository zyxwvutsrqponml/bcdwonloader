//! CI-friendly progress line: `[progress] XX.XX MiB / XXXX.XX MiB (XX.XX%)`.

const MIB: f64 = 1_048_576.0;

pub fn bytes_as_mib(n: u64) -> f64 {
    n as f64 / MIB
}

/// `downloaded / total * 100`. Zero-length files are 100% at 0 bytes.
pub fn progress_pct(downloaded: u64, total: u64) -> f64 {
    if total == 0 {
        if downloaded == 0 {
            100.0
        } else {
            0.0
        }
    } else {
        (downloaded as f64 / total as f64) * 100.0
    }
}

pub fn progress_line(downloaded: u64, total: u64) -> String {
    format!(
        "[progress] {:.2} MiB / {:.2} MiB ({:.2}%)",
        bytes_as_mib(downloaded),
        bytes_as_mib(total),
        progress_pct(downloaded, total)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_dump_examples() {
        let total = 1_772_480_833u64;
        // 1772480833 / 1048576 = 1690.3694... -> "1690.37" (not "1690.00").
        assert_eq!(format!("{:.2}", bytes_as_mib(total)), "1690.37");
        let line = progress_line(38_596_608, total); // 36.81 MiB
        assert!(line.starts_with("[progress] "), "{line}");
        assert!(line.contains("36.81 MiB / 1690.37 MiB"), "{line}");
        assert!(line.contains("(2.18%)"), "{line}");
        assert!(line.contains("%"));
    }

    #[test]
    fn percentage_uses_actual_bytes() {
        assert!((progress_pct(50, 100) - 50.0).abs() < f64::EPSILON);
        assert!((progress_pct(0, 100) - 0.0).abs() < f64::EPSILON);
        assert!((progress_pct(100, 100) - 100.0).abs() < f64::EPSILON);
        assert!((progress_pct(0, 0) - 100.0).abs() < f64::EPSILON);
        let p = progress_pct(36_818_944, 1_771_929_600);
        assert!((p - 2.078).abs() < 0.01, "{p}");
    }

    #[test]
    fn line_format() {
        assert_eq!(
            progress_line(0, 1_048_576),
            "[progress] 0.00 MiB / 1.00 MiB (0.00%)"
        );
        assert_eq!(
            progress_line(1_048_576, 1_048_576),
            "[progress] 1.00 MiB / 1.00 MiB (100.00%)"
        );
    }
}
