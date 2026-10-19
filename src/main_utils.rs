//! CLI mode routing: decide what a `bigtent` invocation does, and refuse
//! invalid combinations with clear messages.
//!
//! Modes:
//! * `--convert-to-v4 <dirs...> --dest <dir>` — convert V3 clusters to
//!   V4/BLAKE3[0..16] clusters (permanent output).
//! * `--compare <left> <right>` — compare two clusters (or directories
//!   of clusters) for item equality; exits nonzero on inequality.
//! * `--fresh-merge <dirs...> --dest <dir>` — merge clusters.
//! * `--lookup` (+ `--rodeo`) — batch lookup.
//! * `--rodeo` — serve.

use anyhow::{Result, bail};
use std::path::PathBuf;

use crate::config::Args;

/// What this invocation does.
#[derive(Debug, Clone, PartialEq)]
pub enum Mode {
    /// Serve clusters over HTTP (`--rodeo`, optionally `--lookup`).
    Serve,
    /// Batch lookup (`--lookup` with `--rodeo`).
    Lookup,
    /// Fresh merge (`--fresh-merge`).
    Merge(Vec<PathBuf>),
    /// Convert V3 clusters to V4/BLAKE3 clusters (`--convert-to-v4`).
    ConvertToV4 {
        /// Input cluster directories.
        inputs: Vec<PathBuf>,
        /// Output root; each input's converted chunks land in
        /// `dest/<input-dir-name>/`.
        dest: PathBuf,
    },
    /// Compare two clusters or cluster directories for item equality
    /// (`--compare`).
    Compare { left: PathBuf, right: PathBuf },
}

/// Decide the mode from the parsed arguments, validating combinations.
pub fn mode_from_args(args: &Args) -> Result<Mode> {
    if !args.compare.is_empty() {
        if args.compare.len() != 2 {
            bail!(
                "The --compare option requires exactly two paths (got {}); \
                 each is a cluster directory or a .grc file",
                args.compare.len()
            );
        }
        if !args.convert_to_v4.is_empty() || !args.fresh_merge.is_empty() || args.rodeo.is_some() {
            bail!(
                "The --compare option is mutually exclusive with --convert-to-v4, --fresh-merge, and --rodeo"
            );
        }
        return Ok(Mode::Compare {
            left: args.compare[0].clone(),
            right: args.compare[1].clone(),
        });
    }

    if !args.convert_to_v4.is_empty() {
        let dest = match &args.dest {
            Some(d) => d.clone(),
            None => bail!("The --convert-to-v4 option requires a --dest directory"),
        };
        if !args.fresh_merge.is_empty() || args.rodeo.is_some() {
            bail!(
                "The --convert-to-v4 option is mutually exclusive with --fresh-merge and --rodeo"
            );
        }
        return Ok(Mode::ConvertToV4 {
            inputs: args.convert_to_v4.clone(),
            dest,
        });
    }

    if !args.fresh_merge.is_empty() {
        return Ok(Mode::Merge(args.fresh_merge.clone()));
    }

    if args.lookup.is_some() {
        if args.rodeo.is_none() {
            bail!("The --lookup option requires --rodeo");
        }
        return Ok(Mode::Lookup);
    }

    // Both --rodeo and --cluster-list serve over HTTP (mutually exclusive,
    // enforced by Args::cluster_source); the dispatch in main.rs routes via
    // the cluster source, but it only runs when a mode is decided here, so
    // a cluster-list-only invocation must also resolve to Serve.
    if args.rodeo.is_some() || args.cluster_list.is_some() {
        return Ok(Mode::Serve);
    }

    bail!("No operation specified; see --help")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Args;
    use std::path::PathBuf;

    fn args_with(cluster_list: Option<PathBuf>, rodeo: Option<Vec<PathBuf>>) -> Args {
        Args {
            host: vec![],
            port: None,
            rodeo,
            fresh_merge: vec![],
            threaded: None,
            dest: None,
            cache_index: None,
            buffer_limit: 10_000,
            merge_buffer_size: 15,
            merge_worker_count: None,
            merge_temp_dir: None,
            force_temp_dir: false,
            convert_to_v4: vec![],
            compare: vec![],
            block_list: None,
            lookup: None,
            output: None,
            cluster_list,
            pid_file: None,
            check: false,
        }
    }

    #[test]
    fn cluster_list_routes_to_serve() {
        let args = args_with(Some(PathBuf::from("/tmp/dirs.json")), None);
        assert_eq!(mode_from_args(&args).unwrap(), Mode::Serve);
    }

    #[test]
    fn rodeo_routes_to_serve() {
        let args = args_with(None, Some(vec![PathBuf::from("/tmp/clusters")]));
        assert_eq!(mode_from_args(&args).unwrap(), Mode::Serve);
    }

    #[test]
    fn both_rodeo_and_cluster_list_still_serve_here() {
        // cluster_source() enforces the mutual exclusion for the actual
        // source; the mode decision treats either as Serve.
        let args = args_with(
            Some(PathBuf::from("/tmp/dirs.json")),
            Some(vec![PathBuf::from("/tmp/clusters")]),
        );
        assert_eq!(mode_from_args(&args).unwrap(), Mode::Serve);
    }

    #[test]
    fn no_operation_bails() {
        let args = args_with(None, None);
        let err = mode_from_args(&args).unwrap_err();
        assert!(err.to_string().contains("No operation specified"), "{err}");
    }
}
