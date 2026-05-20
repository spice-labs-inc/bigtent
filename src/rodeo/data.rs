//! # Data File Format (.grd)
//!
//! This module handles `.grd` (Goat Rodeo Data) files which store the actual
//! Item records in CBOR format.
//!
//! ## File Structure
//!
//! ```text
//! ┌─────────────────────────────────────┐
//! │ Magic Number (4 bytes): 0x00be1100  │  "Bell" - identifies data file
//! ├─────────────────────────────────────┤
//! │ Envelope Length (4 bytes)           │
//! ├─────────────────────────────────────┤
//! │ CBOR Envelope (DataFileEnvelope)    │  File metadata
//! ├─────────────────────────────────────┤
//! │ Item 1: [length][CBOR Item]         │
//! │ Item 2: [length][CBOR Item]         │
//! │ ...                                 │
//! │ Item N: [length][CBOR Item]         │
//! └─────────────────────────────────────┘
//! ```
//!
//! ## Item Storage
//!
//! Each Item is stored as:
//! - 4-byte length prefix (little-endian u32)
//! - CBOR-encoded Item payload
//!
//! Items are accessed by byte offset (provided by the index file).
//!
//! ## Memory Mapping
//!
//! Data files are memory-mapped for efficient random access without
//! loading the entire file into memory.

use crate::{
    item::{Item, WireItem},
    util::{read_cbor_sync, read_len_and_cbor_sync, read_u32_sync},
};
use anyhow::{Result, bail};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File as SyncFile,
    io::{BufReader as SyncBufReader, Read, Seek},
    path::PathBuf,
    sync::Arc,
};
use tracing::error;

use super::goat::GoatRodeoCluster;

/// Metadata envelope stored at the beginning of .grd data files.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct DataFileEnvelope {
    /// File format version.
    ///
    /// - v1 stored connections as `(String, String)` tuples directly.
    /// - v2 switched to the streamed `WireEdge` encoding.
    /// - v3 added an `alias_map` field — withdrawn: the 16-bit
    ///   envelope-length prefix overflows once the embedded alias map
    ///   exceeds 64 KiB.
    /// - v4 has no `alias_map` field; the alias map lives in a
    ///   cluster-wide `.gra` sidecar referenced from the cluster
    ///   envelope's `alias_map_file`.
    pub version: u32,

    /// Magic number for validation (should be `DataFileMagicNumber`)
    pub magic: u32,

    /// Hash of the previous data file in the chain (for incremental updates)
    pub previous: u64,

    /// Hashes of data files this file depends on
    pub depends_on: BTreeSet<u64>,

    /// True if this file was created by a merge operation
    pub built_from_merge: bool,

    /// Arbitrary metadata
    pub info: BTreeMap<String, String>,
}

pub trait DataReader: Read + Seek + Unpin + Send + Sync + std::fmt::Debug {}

impl DataReader for SyncBufReader<SyncFile> {}

#[derive(Debug, Clone)]
pub struct DataFile {
    pub envelope: DataFileEnvelope,
    pub file: Arc<Mmap>,
    pub data_offset: usize,
    /// Position of this DataFile in the cluster envelope's `data_files`
    /// vector. Used to resolve v3 [`crate::item::WireEdge`] back-references.
    /// Saturates at [`u8::MAX`] for clusters with more than 128
    /// DataFiles — past that, cross-file edges are written as
    /// [`crate::item::WireEdge::External`] by goatrodeo.
    pub file_ordinal: u8,
}

pub const GOAT_RODEO_DATA_FILE_SUFFIX: &str = "grd";
pub const GOAT_RODEO_INDEX_FILE_SUFFIX: &str = "gri";
pub const GOAT_RODEO_CLUSTER_FILE_SUFFIX: &str = "grc";

impl DataFile {
    pub async fn new(dir: &PathBuf, hash: u64, file_ordinal: u8) -> Result<DataFile> {
        let mut data_file = GoatRodeoCluster::find_data_or_index_file_from_sha256(
            dir,
            hash,
            GOAT_RODEO_DATA_FILE_SUFFIX,
        )
        .await?;

        let dfp = &mut data_file;
        let magic = read_u32_sync(dfp)?;
        if magic != DataFileMagicNumber {
            bail!(
                "Unexpected magic number {:x}, expecting {:x} for data file {:016x}.{}",
                magic,
                DataFileMagicNumber,
                hash,
                GOAT_RODEO_DATA_FILE_SUFFIX
            );
        }

        let env: DataFileEnvelope = read_len_and_cbor_sync(dfp)?;

        let cur_pos: u64 = data_file.stream_position()?;
        // FIXME do additional validation of the envelope

        let mmap: Mmap = unsafe { Mmap::map(&data_file)? };

        Ok(DataFile {
            envelope: env,
            file: Arc::new(mmap),
            data_offset: cur_pos as usize,
            file_ordinal,
        })
    }

    /// Read an Item at the given byte offset in this data file.
    ///
    /// For envelopes at version 1 the legacy CBOR `Item` codec is used.
    /// For version >= 2 the on-disk form is the streamed [`WireItem`]
    /// shape — callers (in `goat.rs`) handle WireEdge resolution and
    /// alias:from synthesis against the cluster state and assemble the
    /// final [`Item`].
    ///
    /// This is a mixture of synchronous and async code. Why?
    /// Turns out the async BufReader is freakin' slow, so we're doing synchronous
    /// BufReader. Ideally, we'd put this on a blocking Tokio thread, but, sigh
    /// async closures are not in mainline Rust right now, so "no thread-friendly soup for you!"
    pub fn read_item_at(&self, pos: usize) -> Option<Item> {
        if self.envelope.version >= 2 {
            // v3 callers should be using `read_wire_item_at` and
            // resolving against the cluster-level back-reference map.
            // We don't have access to that map here, so we surface an
            // error via the existing `None`-on-failure contract.
            error!(
                "DataFile::read_item_at called on v{} file at offset {} without a cluster resolver — \
                 use GoatRodeoCluster::item_for_file_and_offset for v >= 2 data",
                self.envelope.version, pos
            );
            return None;
        }
        let mut my_reader: &[u8] = &self.file[pos..];

        let item_len = match read_u32_sync(&mut my_reader) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to read at offset {} err {:?}", pos, e);
                return None;
            }
        };
        let item = match read_cbor_sync(&mut my_reader, item_len as usize) {
            Ok(i) => i,
            Err(e) => {
                error!("Failed to read CBOR at offset {} error {:?}", pos, e);
                return None;
            }
        };
        Some(item)
    }

    /// Read a [`WireItem`] from this v >= 2 data file. Returns `None`
    /// for v1 files (callers should use [`read_item_at`] for those).
    pub fn read_wire_item_at(&self, pos: usize) -> Option<WireItem> {
        if self.envelope.version < 2 {
            return None;
        }
        let mut my_reader: &[u8] = &self.file[pos..];
        let item_len = match read_u32_sync(&mut my_reader) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to read length at offset {} err {:?}", pos, e);
                return None;
            }
        };
        let wi = match read_cbor_sync::<WireItem, _>(&mut my_reader, item_len as usize) {
            Ok(i) => i,
            Err(e) => {
                error!("Failed to read WireItem at offset {} error {:?}", pos, e);
                return None;
            }
        };
        Some(wi)
    }

    /// Read just the `identifier` of the v >= 2 item at this offset,
    /// without paying the cost of resolving its connections. Used for
    /// lazily populating the cluster's back-reference cache.
    pub fn read_identifier_at(&self, pos: usize) -> Option<String> {
        self.read_wire_item_at(pos).map(|wi| wi.identifier)
    }
}

/// Magic number identifying data (.grd) files: 0x00be1100 ("Bell" pepper)
///
/// BigTent uses food-themed magic numbers for file identification.
/// Data files contain CBOR-encoded Items at specific byte offsets.
#[allow(non_upper_case_globals)]
pub const DataFileMagicNumber: u32 = 0x00be1100; // Bell
