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
    item::Item,
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
    /// File format version
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
    /// The truncated-SHA256 name hash of this data file (for error naming)
    pub hash: u64,
}

pub const GOAT_RODEO_DATA_FILE_SUFFIX: &str = "grd";
pub const GOAT_RODEO_INDEX_FILE_SUFFIX: &str = "gri";
pub const GOAT_RODEO_CLUSTER_FILE_SUFFIX: &str = "grc";

impl DataFile {
    pub async fn new(dir: &PathBuf, hash: u64, expected_envelope_version: u32) -> Result<DataFile> {
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

        // the envelope repeats its magic inside; validate it (H3)
        if env.magic != DataFileMagicNumber {
            bail!(
                "Data file envelope for {:016x}.{} has invalid magic {:x}",
                hash,
                GOAT_RODEO_DATA_FILE_SUFFIX,
                env.magic
            );
        }

        // the data envelope version is checked against the cluster version:
        // version 3 clusters carry data envelope 1, version 4 carries 2
        if env.version != expected_envelope_version {
            bail!(
                "Data file envelope for {:016x}.{} has version {} but the cluster requires {}",
                hash,
                GOAT_RODEO_DATA_FILE_SUFFIX,
                env.version,
                expected_envelope_version
            );
        }

        let cur_pos: u64 = data_file.stream_position()?;

        let mmap: Mmap = unsafe { Mmap::map(&data_file)? };

        Ok(DataFile {
            envelope: env,
            file: Arc::new(mmap),
            data_offset: cur_pos as usize,
            hash,
        })
    }

    /// Read the item at the given offset.
    ///
    /// Failures — an unreadable length, a length that claims more bytes
    /// than the mapped file has remaining (which would otherwise permit a
    /// multi-gigabyte allocation per lookup), or a payload that does not
    /// deserialize — return an `Err` naming the file and offset (H3).
    /// The lookup boundary logs the error and reports the item as absent.
    pub fn read_item_at(&self, pos: usize) -> Result<Item> {
        let file_len = self.file.len();
        if pos >= file_len || file_len - pos < 4 {
            bail!(
                "Offset {} is past the end of data file {:016x}.{} ({} bytes)",
                pos,
                self.hash,
                GOAT_RODEO_DATA_FILE_SUFFIX,
                file_len
            );
        }
        let mut my_reader: &[u8] = &self.file[pos..];

        let item_len = read_u32_sync(&mut my_reader)?;

        // H3: reject lengths beyond the remaining mapped bytes BEFORE the
        // allocation in read_cbor_sync, so a corrupt or malicious length
        // cannot trigger a multi-gigabyte allocation.
        if item_len as usize > my_reader.len() {
            bail!(
                "Item length {} at offset {} in data file {:016x}.{} exceeds the remaining {} bytes",
                item_len,
                pos,
                self.hash,
                GOAT_RODEO_DATA_FILE_SUFFIX,
                my_reader.len()
            );
        }

        read_cbor_sync(&mut my_reader, item_len as usize).map_err(|e| {
            error!("Failed to read CBOR at offset {} error {:?}", pos, e);
            e
        })
    }
}

/// Magic number identifying data (.grd) files: 0x00be1100 ("Bell" pepper)
///
/// BigTent uses food-themed magic numbers for file identification.
/// Data files contain CBOR-encoded Items at specific byte offsets.
#[allow(non_upper_case_globals)]
pub const DataFileMagicNumber: u32 = 0x00be1100; // Bell
