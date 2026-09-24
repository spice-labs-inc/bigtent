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
//! - `version` - Format version (4 is current; 3 stays readable)
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
    /// File format version (version 4 is current; version 3 stays readable)
    pub version: u32,

    /// Magic number for file type validation (should be `ClusterFileMagicNumber`)
    pub magic: u32,

    /// SHA256 hashes (truncated to u64) of referenced .grd data files
    pub data_files: Vec<u64>,

    /// SHA256 hashes (truncated to u64) of referenced .gri index files
    pub index_files: Vec<u64>,

    /// Arbitrary metadata (creation time, source info, build details, etc.)
    pub info: BTreeMap<String, String>,

    /// The cluster's index key algorithm declaration (ADR 0002).
    ///
    /// Version 4 clusters written by BigTent always carry
    /// [`V4_CLUSTER_ENCODING`]. Version 3 clusters have no declaration
    /// (the field is absent and reads as `None`); for them the reader
    /// falls back to the first `.gri` file's `encoding` description.
    /// The declared algorithm is honored regardless of the version
    /// number: readers use the algorithm the files declare.
    #[serde(default)]
    pub encoding: Option<String>,
}

/// Magic number identifying cluster (.grc) files: 0xba4a4a ("Banana")
///
/// BigTent uses food-themed magic numbers for file identification.
/// This allows quick validation that a file is the expected type.
#[allow(non_upper_case_globals)]
pub const ClusterFileMagicNumber: u32 = 0xba4a4a; // Banana

/// Minimum supported cluster file format version.
///
/// Files with versions below this cannot be read by this version of BigTent.
#[allow(non_upper_case_globals)]
pub const MinClusterVersion: u32 = 3;

/// Current cluster file format version used when writing new clusters.
pub const CLUSTER_VERSION: u32 = 4;

/// Index key algorithm declaration for version 4 clusters (ADR 0002):
/// the first 16 bytes (128 bits) of the BLAKE3 digest over the
/// identifier's UTF-8 bytes, followed by the two big-endian u64 fields
/// of the index entry.
pub const V4_CLUSTER_ENCODING: &str = "BLAKE3[0..16]/Long/Long";

/// Index key algorithm declaration used by version 3 clusters
/// (carried in the `.gri` envelopes).
pub const V3_CLUSTER_ENCODING: &str = "MD5/Long/Long";

/// Parse an index key algorithm declaration into its [`KeyAlg`].
///
/// An unknown declaration fails closed: the reader cannot honor an
/// algorithm it does not know.
pub fn parse_encoding(encoding: &str) -> Result<crate::util::KeyAlg> {
    match encoding {
        V3_CLUSTER_ENCODING => Ok(crate::util::KeyAlg::Md5),
        V4_CLUSTER_ENCODING => Ok(crate::util::KeyAlg::Blake3Truncated128),
        other => bail!(
            "Unknown index key encoding {:?} (known: {:?}, {:?})",
            other,
            V3_CLUSTER_ENCODING,
            V4_CLUSTER_ENCODING
        ),
    }
}

impl ClusterFileEnvelope {
    pub fn validate(&self) -> Result<()> {
        if self.magic != ClusterFileMagicNumber {
            bail!("Loaded a cluster with an invalid magic number: {:?}", self);
        }

        // readers accept versions 3 and 4; everything else is rejected
        if self.version < MinClusterVersion || self.version > CLUSTER_VERSION {
            bail!(
                "Loaded a Cluster with version {} but this code only supports versions {} through {}",
                self.version,
                MinClusterVersion,
                CLUSTER_VERSION
            );
        }

        // an unknown declaration cannot be honored; fail closed naming the
        // constant. A known declaration is accepted regardless of version
        // (the algorithm is what the files declare, per ADR 0002).
        if let Some(encoding) = &self.encoding {
            parse_encoding(encoding)?;
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
