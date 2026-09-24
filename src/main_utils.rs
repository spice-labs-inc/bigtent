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

    if args.rodeo.is_some() {
        return Ok(Mode::Serve);
    }

    bail!("No operation specified; see --help")
}
