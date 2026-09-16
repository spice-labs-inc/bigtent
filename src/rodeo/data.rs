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
}

pub const GOAT_RODEO_DATA_FILE_SUFFIX: &str = "grd";
pub const GOAT_RODEO_INDEX_FILE_SUFFIX: &str = "gri";
pub const GOAT_RODEO_CLUSTER_FILE_SUFFIX: &str = "grc";

/// The raw byte span of a length-prefixed CBOR item at `pos` within a
/// data-file byte slice: `[u32 LE length][CBOR item]`. Pure byte-level
/// (no decode, no I/O) so it is testable without a mapped file.
pub fn read_item_bytes_at(data: &[u8], pos: usize) -> Option<&[u8]> {
    if pos + 4 > data.len() {
        return None;
    }
    let length = u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]])
        as usize;
    let start = pos + 4;
    let end = start.checked_add(length)?;
    if end > data.len() {
        return None;
    }
    Some(&data[start..end])
}

impl DataFile {
    /// The raw byte span of the length-prefixed item at `pos`, WITHOUT
    /// decoding it: the Sansho integration seam's no-decode accessor —
    /// the engine walks the bytes selectively, so the host must not
    /// decode first (that would defeat selective materialization).
    ///
    /// Returns the CBOR bytes of the item itself (without the length
    /// prefix), or `None` when the position/length is out of range.
    pub fn read_item_bytes_at(&self, pos: usize) -> Option<&[u8]> {
        read_item_bytes_at(self.file.as_ref(), pos)
    }

    pub async fn new(dir: &PathBuf, hash: u64) -> Result<DataFile> {
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
        })
    }

    /// read the item. This is a mixture of synchronous and async code. Why?
    /// Turns out the async BufReader is freakin' slow, so we're doing synchronous
    /// BufReader. Ideally, we'd put this on a blocking Tokio thread, but, sigh
    /// async closures are not in mainline Rust right now, so "no thread-friendly soup for you!"
    pub fn read_item_at(&self, pos: usize) -> Option<Item> {
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
}

/// Magic number identifying data (.grd) files: 0x00be1100 ("Bell" pepper)
///
/// BigTent uses food-themed magic numbers for file identification.
/// Data files contain CBOR-encoded Items at specific byte offsets.
#[allow(non_upper_case_globals)]
pub const DataFileMagicNumber: u32 = 0x00be1100; // Bell
