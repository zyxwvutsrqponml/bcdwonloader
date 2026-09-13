//! Disk-space checks (Unix `statvfs`; other platforms warn and continue).

use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};

use crate::ui::human_bytes;

/// Peak space: remaining bytes to fetch + a full merge copy + 64 MiB margin.
pub fn required_space(total: u64, already: u64) -> u64 {
    let remaining = total.saturating_sub(already);
    remaining
        .saturating_add(total)
        .saturating_add(64 * 1024 * 1024)
}

pub fn existing_ancestor(path: &Path) -> PathBuf {
    let mut p = if path.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        path.to_path_buf()
    };
    loop {
        if p.exists() {
            if p.is_dir() {
                return p;
            }
            if let Some(parent) = p.parent() {
                return if parent.as_os_str().is_empty() {
                    PathBuf::from(".")
                } else {
                    parent.to_path_buf()
                };
            }
            return PathBuf::from(".");
        }
        match p.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => p = parent.to_path_buf(),
            _ => return PathBuf::from("."),
        }
    }
}

#[cfg(unix)]
fn statvfs_available(path: &Path) -> Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let dir = existing_ancestor(path);
    let cstr = std::ffi::CString::new(dir.as_os_str().as_bytes())
        .map_err(|_| anyhow!("path contains interior NUL: {}", dir.display()))?;
    unsafe {
        let mut s: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(cstr.as_ptr(), &mut s) != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok((s.f_bavail as u64).saturating_mul(s.f_frsize as u64))
    }
}

pub fn available_space(path: &Path) -> Result<Option<u64>> {
    #[cfg(unix)]
    {
        Ok(Some(statvfs_available(path)?))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(None)
    }
}

pub fn check_disk_space(path: &Path, total: u64, already: u64) -> Result<()> {
    let required = required_space(total, already);
    println!(
        "Required space: {} ({required} bytes)",
        human_bytes(required)
    );
    match available_space(path)? {
        Some(avail) => {
            println!("Available space: {} ({avail} bytes)", human_bytes(avail));
            if avail < required {
                return Err(anyhow!(
                    "insufficient disk space: need {} ({required} bytes), have {} ({avail} bytes). \
                     Chunks plus the merged copy need about 2× the dump size. Free space and retry.",
                    human_bytes(required),
                    human_bytes(avail)
                ));
            }
        }
        None => {
            println!(
                "Available space: unknown (disk check not supported on this platform); continuing"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_space_accounts_for_merge_copy() {
        let total = 1_000u64;
        let r = required_space(total, 0);
        assert!(r >= 2 * total);
        let r2 = required_space(total, total);
        assert!(r2 >= total);
        assert!(r2 < r);
    }

    #[test]
    fn ancestor_of_cwd() {
        let a = existing_ancestor(Path::new("."));
        assert!(a.exists());
    }

    #[cfg(unix)]
    #[test]
    fn available_space_on_cwd() {
        let n = available_space(Path::new(".")).unwrap();
        assert!(n.unwrap() > 0);
    }
}
