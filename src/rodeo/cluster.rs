//! # Cluster File Format
//!
//! This module defines the `.grc` (Goat Rodeo Cluster) file format which serves
//! as the root metadata file for a BigTent cluster.
//!
//! ## File Structure
//!
//! ```text
//! ┌────────────────────────────────────┐
//! │ Magic Number (4 bytes): 0xba4a4a   │  "Banana" - identifies file type
//! ├────────────────────────────────────┤
//! │ Length (2 bytes, u16 big-endian)   │  Size of CBOR payload
//! ├────────────────────────────────────┤
//! │ CBOR Payload (ClusterFileEnvelope) │  Serialized metadata
//! └────────────────────────────────────┘
//! ```
//!
//! The u16 length prefix imposes an implicit 64 KiB safety bound on the CBOR payload.
//!
//! ## ClusterFileEnvelope Fields
//!
//! - `version` - Format version (currently 3)
//! - `magic` - Magic number for validation
//! - `data_files` - SHA256 hashes of referenced `.grd` data files
//! - `index_files` - SHA256 hashes of referenced `.gri` index files
//! - `info` - Key-value metadata (creation time, source info, etc.)
//!
//! ## Magic Number Convention
//!
//! BigTent uses food-themed magic numbers for file identification:
//! - `.grc` (Cluster): `0xba4a4a` - "Banana"
//! - `.gri` (Index): `0x54154170` - "Shishitō" (pepper)
//! - `.grd` (Data): `0x00be1100` - "Bell" (pepper)

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Metadata envelope stored in .grc cluster files.
///
/// This structure is CBOR-serialized and stored at the beginning of each
/// cluster file (after the magic number and length prefix).
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterFileEnvelope {
    /// File format version (3 = legacy with per-DataFile aliasMap, 4 =
    /// cluster-wide alias-map sidecar referenced by [`Self::alias_map_file`])
    pub version: u32,

    /// Magic number for file type validation (should be `ClusterFileMagicNumber`)
    pub magic: u32,

    /// SHA256 hashes (truncated to u64) of referenced .grd data files
    pub data_files: Vec<u64>,

    /// SHA256 hashes (truncated to u64) of referenced .gri index files
    pub index_files: Vec<u64>,

    /// Arbitrary metadata (creation time, source info, build details, etc.)
    pub info: BTreeMap<String, String>,

    /// SHA256-derived hash (truncated to u64) of the cluster-wide
    /// alias-map sidecar file (`<hash>.gra`), if one was written. v3
    /// envelopes omit the field; v4 envelopes set it to `Some(_)` when
    /// the cluster has any aliases and `None` (encoded as CBOR null)
    /// when it has none.
    #[serde(default, rename = "alias_map_file")]
    pub alias_map_file: Option<u64>,
}

/// Magic number identifying cluster (.grc) files: 0xba4a4a ("Banana")
///
/// BigTent uses food-themed magic numbers for file identification.
/// This allows quick validation that a file is the expected type.
#[allow(non_upper_case_globals)]
pub const ClusterFileMagicNumber: u32 = 0xba4a4a; // Banana

/// Magic number identifying alias-map sidecar (.gra) files: 0x0a11a5ed ("Aliased").
///
/// Written by goatrodeo's v4 writer when the cluster's alias map is
/// non-empty. The file's CBOR payload is a `BTreeMap<String,
/// BTreeSet<String>>` mapping canonical identifiers to their alternates.
#[allow(non_upper_case_globals)]
pub const AliasMapFileMagicNumber: u32 = 0x0a11a5ed; // Aliased

/// Lowest supported cluster file format version. v3 is accepted for
/// backward compatibility (alias map in DataFile envelope, broken at
/// scale); v4 is preferred (alias map in sidecar).
#[allow(non_upper_case_globals)]
pub const MinClusterVersion: u32 = 3;

/// Highest supported cluster file format version.
#[allow(non_upper_case_globals)]
pub const MaxClusterVersion: u32 = 4;

/// Current cluster file format version used when writing new clusters.
pub const CLUSTER_VERSION: u32 = MaxClusterVersion;

impl ClusterFileEnvelope {
    pub fn validate(&self) -> Result<()> {
        if self.magic != ClusterFileMagicNumber {
            bail!("Loaded a cluster with an invalid magic number: {:?}", self);
        }

        if self.version < MinClusterVersion || self.version > MaxClusterVersion {
            bail!(
                "Loaded a Cluster with version {} but this code only supports versions {}..={} Clusters",
                self.version,
                MinClusterVersion,
                MaxClusterVersion
            );
        }

        Ok(())
    }
}

impl std::fmt::Display for ClusterFileEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClusterFileEnvelope {{v: {}, data_files: {:?}, index_files: {:?}, info: {:?}}}",
            self.version,
            self.data_files
                .iter()
                .map(|h| format!("{:016x}", h))
                .collect::<Vec<String>>(),
            self.index_files
                .iter()
                .map(|h| format!("{:016x}", h))
                .collect::<Vec<String>>(),
            self.info,
        )
    }
}
