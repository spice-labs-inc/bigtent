//! # Cluster List Loading
//!
//! Loads and validates a JSON file containing a list of directories
//! that hold `.grc` cluster files. Used with the `--cluster-list` flag.

use anyhow::{Result, bail};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(not(test))]
use log::{error, info, warn};

#[cfg(test)]
use std::println as info;
#[cfg(test)]
use std::eprintln as warn;
#[cfg(test)]
use std::eprintln as error;

use crate::rodeo::{
    goat::GoatRodeoCluster,
    member::{HerdMember, member_core},
    robo_goat::ClusterRoboMember,
};

const MAX_ENTRIES: usize = 100;
const MAX_PATH_LEN: usize = 4096;

/// Read a JSON file at `path` and return a validated list of absolute directory paths.
///
/// Validation rules:
/// - Maximum 100 entries
/// - No path longer than 4096 characters
/// - No `..` path components (prevents directory traversal)
/// - Paths are resolved to absolute paths
/// - Warns if the file is world-writable
pub fn load_cluster_list(path: &Path) -> Result<Vec<PathBuf>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("Failed to read cluster list {:?}: {}", path, e))?;

    let entries: Vec<String> = serde_json::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Failed to parse cluster list {:?} as JSON: {}", path, e))?;

    if entries.len() > MAX_ENTRIES {
        bail!(
            "Cluster list has {} entries, maximum is {}",
            entries.len(),
            MAX_ENTRIES
        );
    }

    // Check file permissions (warn if world-writable)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(path) {
            let mode = meta.permissions().mode();
            if mode & 0o002 != 0 {
                warn!(
                    "Cluster list file {:?} is world-writable (mode {:o})",
                    path, mode
                );
            }
        }
    }

    let mut result = Vec::with_capacity(entries.len());
    for entry in &entries {
        if entry.len() > MAX_PATH_LEN {
            bail!(
                "Path exceeds {} character limit: {}...",
                MAX_PATH_LEN,
                &entry[..64]
            );
        }

        let p = Path::new(entry);
        for component in p.components() {
            if let std::path::Component::ParentDir = component {
                bail!("Path contains '..': {}", entry);
            }
        }

        // Resolve to absolute path
        let abs = if p.is_absolute() {
            p.to_path_buf()
        } else {
            std::env::current_dir()?.join(p)
        };

        result.push(abs);
    }

    Ok(result)
}

/// Load cluster members from the given directories.
///
/// For each directory that exists, calls `GoatRodeoCluster::cluster_files_in_dir()`.
/// Missing directories are skipped with a warning. Malformed `.grc` files are
/// logged as errors and skipped.
pub async fn load_clusters_from_dirs(
    dirs: &[PathBuf],
    cache_index: bool,
) -> Result<Vec<Arc<HerdMember>>> {
    let mut clusters: Vec<Arc<HerdMember>> = Vec::new();

    for dir in dirs {
        if !dir.exists() {
            warn!("Cluster directory does not exist, skipping: {:?}", dir);
            continue;
        }

        match GoatRodeoCluster::cluster_files_in_dir(dir.clone(), cache_index, vec![]).await {
            Ok(loaded) => {
                for b in loaded {
                    info!("Loaded cluster {}", b.name());
                    clusters.push(member_core(b));
                }
            }
            Err(e) => {
                error!(
                    "Failed to load clusters from {:?}: {}, skipping directory",
                    dir, e
                );
            }
        }
    }

    Ok(clusters)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_valid_cluster_list() {
        let dir = tempfile::tempdir().unwrap();
        let list_path = dir.path().join("clusters.json");
        std::fs::write(&list_path, r#"["/tmp/cluster_a", "/tmp/cluster_b"]"#).unwrap();

        let result = load_cluster_list(&list_path).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], PathBuf::from("/tmp/cluster_a"));
        assert_eq!(result[1], PathBuf::from("/tmp/cluster_b"));
    }

    #[test]
    fn test_load_empty_list() {
        let dir = tempfile::tempdir().unwrap();
        let list_path = dir.path().join("clusters.json");
        std::fs::write(&list_path, "[]").unwrap();

        let result = load_cluster_list(&list_path).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_reject_dot_dot_paths() {
        let dir = tempfile::tempdir().unwrap();
        let list_path = dir.path().join("clusters.json");
        std::fs::write(&list_path, r#"["/tmp/../etc/passwd"]"#).unwrap();

        let result = load_cluster_list(&list_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains(".."));
    }

    #[test]
    fn test_reject_too_many_entries() {
        let dir = tempfile::tempdir().unwrap();
        let list_path = dir.path().join("clusters.json");
        let entries: Vec<String> = (0..101).map(|i| format!("/tmp/cluster_{}", i)).collect();
        std::fs::write(&list_path, serde_json::to_string(&entries).unwrap()).unwrap();

        let result = load_cluster_list(&list_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("maximum"));
    }

    #[test]
    fn test_reject_long_paths() {
        let dir = tempfile::tempdir().unwrap();
        let list_path = dir.path().join("clusters.json");
        let long_path = format!("/{}", "a".repeat(MAX_PATH_LEN + 1));
        std::fs::write(&list_path, serde_json::to_string(&vec![long_path]).unwrap()).unwrap();

        let result = load_cluster_list(&list_path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("limit"));
    }

    #[test]
    fn test_invalid_json() {
        let dir = tempfile::tempdir().unwrap();
        let list_path = dir.path().join("clusters.json");
        std::fs::write(&list_path, "not json").unwrap();

        let result = load_cluster_list(&list_path);
        assert!(result.is_err());
    }
}
