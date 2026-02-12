//! # PID File Management
//!
//! Provides atomic PID file creation with advisory locking.
//! The PID file is deleted on clean shutdown (via `Drop`).

use anyhow::{Result, bail};
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

/// A PID file that is atomically written, advisory-locked, and cleaned up on drop.
#[derive(Debug)]
pub struct PidFile {
    path: PathBuf,
    /// Hold the locked file descriptor for the lifetime of the struct.
    /// The lock is released implicitly when the file is closed on drop.
    _file: fs::File,
}

impl PidFile {
    /// Create a PID file at `path`.
    ///
    /// 1. Rejects symlinks at the target path.
    /// 2. Writes the current PID to a temp file in the same directory.
    /// 3. Atomically renames the temp file to the target.
    /// 4. Opens the target and acquires an exclusive advisory lock (`flock`).
    /// 5. Sets permissions to 0644.
    pub fn create(path: &Path) -> Result<PidFile> {
        // Reject symlinks
        if let Ok(meta) = fs::symlink_metadata(path) {
            if meta.file_type().is_symlink() {
                bail!("PID file path is a symlink: {:?}", path);
            }
        }

        let parent = path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("PID file path has no parent directory: {:?}", path))?;

        if !parent.exists() {
            bail!("Parent directory for PID file does not exist: {:?}", parent);
        }

        let pid = std::process::id();

        // Write to a temp file in the same directory, then rename atomically
        let tmp_path = parent.join(format!(".bigtent_pid_{}.tmp", pid));
        {
            let mut tmp = fs::File::create(&tmp_path)?;
            write!(tmp, "{}", pid)?;
            tmp.flush()?;
        }

        fs::rename(&tmp_path, path)?;

        // Open the file and acquire an exclusive advisory lock
        let file = fs::File::open(path)?;
        use std::os::unix::io::AsRawFd;
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            let _ = fs::remove_file(path);
            bail!(
                "Failed to acquire exclusive lock on PID file {:?}: another instance may be running",
                path
            );
        }

        // Set permissions to 0644
        fs::set_permissions(path, fs::Permissions::from_mode(0o644))?;

        Ok(PidFile {
            path: path.to_path_buf(),
            _file: file,
        })
    }
}

impl Drop for PidFile {
    fn drop(&mut self) {
        // Lock is released when _file is dropped (fd closed).
        // Delete the PID file.
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pid_file_create_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("test.pid");

        {
            let pf = PidFile::create(&pid_path).unwrap();
            // File should exist and contain our PID
            let contents = fs::read_to_string(&pid_path).unwrap();
            assert_eq!(contents, format!("{}", std::process::id()));
            // Keep pf alive in this scope
            let _ = &pf;
        }
        // After drop, file should be deleted
        assert!(!pid_path.exists());
    }

    #[test]
    fn test_pid_file_symlink_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let real_path = dir.path().join("real.pid");
        let link_path = dir.path().join("link.pid");

        fs::write(&real_path, "old").unwrap();
        std::os::unix::fs::symlink(&real_path, &link_path).unwrap();

        let result = PidFile::create(&link_path);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("symlink")
        );
    }

    #[test]
    fn test_pid_file_missing_dir() {
        let path = PathBuf::from("/tmp/nonexistent_bigtent_test_dir_12345/test.pid");
        let result = PidFile::create(&path);
        assert!(result.is_err());
    }

    #[test]
    fn test_pid_file_overwrite_stale() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("test.pid");

        // Write a stale PID file
        fs::write(&pid_path, "99999").unwrap();

        // PidFile::create should overwrite it
        let pf = PidFile::create(&pid_path).unwrap();
        let contents = fs::read_to_string(&pid_path).unwrap();
        assert_eq!(contents, format!("{}", std::process::id()));
        drop(pf);
    }
}
