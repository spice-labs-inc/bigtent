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
//! ## The versioned item-shape contract
//!
//! The data envelope's `version` declares the item format the file
//! carries, and reads DISPATCH on it — each version's deserializer reads
//! ONLY its own shape: version 2 (version 4 clusters) deserializes the
//! item as `Item` (map-shaped connections, the ONLY shape `Item`
//! reads); version 1 (version 3 clusters) deserializes the item as
//! `ItemV3` (the legacy pair array, the ONLY shape `ItemV3` reads) and
//! upgrades it through `From<ItemV3> for Item`. A payload of the wrong
//! shape fails its deserializer: a loud read error naming the file,
//! offset, and declared version — never a silent dual-shape read
//! (external readers deserialize strictly by the declared version and
//! would answer wrong-shaped items with empty edges).
//! Tests: `test_read_item_at_enforces_version_shape`.
//!
//! ## Memory Mapping
//!
//! Data files are memory-mapped for efficient random access without
//! loading the entire file into memory.

use crate::{
    item::{Item, ItemV3},
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

/// The raw byte span of a length-prefixed CBOR item at `pos` within a
/// data-file byte slice: `[u32 LE length][CBOR item]`. Pure byte-level
/// (no decode, no I/O) so it is testable without a mapped file.
pub fn read_item_bytes_at(data: &[u8], pos: usize) -> Option<&[u8]> {
    if pos + 4 > data.len() {
        return None;
    }
    let length =
        u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
    let start = pos + 4;
    let end = start.checked_add(length)?;
    if end > data.len() {
        return None;
    }
    Some(&data[start..end])
}

impl DataFile {
    /// The raw byte span of the length-prefixed item at `pos`, WITHOUT
    /// decoding it: the Sansho integration interface's no-decode accessor —
    /// the engine walks the bytes selectively, so the host must not
    /// decode first (that would defeat selective materialization).
    ///
    /// Returns the CBOR bytes of the item itself (without the length
    /// prefix), or `None` when the position/length is out of range.
    pub fn read_item_bytes_at(&self, pos: usize) -> Option<&[u8]> {
        read_item_bytes_at(self.file.as_ref(), pos)
    }

    /// Read the raw byte span of the length-prefixed item at `pos`,
    /// WITHOUT deserializing it. This is the byte path's read: the Sansho
    /// integration walks the returned span selectively, so nothing here
    /// may decode.
    ///
    /// An unreadable length, or a length that claims more bytes than the
    /// mapped file has remaining (which would otherwise permit a
    /// multi-gigabyte allocation per lookup), is an `Err` naming the file
    /// and offset (H3). The lookup boundary logs the error and reports
    /// the item as absent.
    pub fn read_bytes_at<'a>(&'a self, pos: usize) -> Result<&'a [u8]> {
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
        // allocation, so a corrupt or malicious length cannot trigger a
        // multi-gigabyte allocation.
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

        Ok(&self.file[(pos + 4)..(pos + 4 + item_len as usize)])
    }

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
    /// The read DISPATCHES on the declared data-envelope version, and
    /// each version reads ONLY its own item format (owner directive
    /// 2026-09-30):
    ///
    /// * version 2 (version 4 clusters): deserializes an [`Item`] — the
    ///   connections map is the ONLY shape it reads;
    /// * version 1 (version 3 clusters): deserializes an [`ItemV3`] (the
    ///   legacy pair set is the ONLY shape it reads) and upgrades it
    ///   through the destructive `From<ItemV3> for Item`.
    ///
    /// A payload of the OTHER version's shape fails its deserializer, so
    /// a file that lies about its content is a loud error naming the
    /// file and offset (H3) — never a silent dual-shape read.
    /// Tests: `test_read_item_at_enforces_version_shape`.
    ///
    /// Other failures — an unreadable length, a length that claims more
    /// bytes than the mapped file has remaining (which would otherwise
    /// permit a multi-gigabyte allocation per lookup) — also return an
    /// `Err` naming the file and offset. The lookup boundary logs the
    /// error and reports the item as absent.
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

        // the version dispatch: the envelope version is authoritative,
        // and each version's deserializer is strict to its own shape —
        // no probe, no second parse, no shape capture needed
        let item = match self.envelope.version {
            2 => {
                let item: Item = read_cbor_sync(&mut my_reader, item_len as usize)
                    .map_err(|e| format_item_read_error(self, pos, e))?;
                item
            }
            1 => {
                let v3: ItemV3 = read_cbor_sync(&mut my_reader, item_len as usize)
                    .map_err(|e| format_item_read_error(self, pos, e))?;
                Item::from(v3)
            }
            other => bail!(
                "Data file {:016x}.{} declares data envelope version {} — \
                 no item format is defined for it",
                self.hash,
                GOAT_RODEO_DATA_FILE_SUFFIX,
                other
            ),
        };
        Ok(item)
    }
}

/// The H3 item-read failure: name the file, the offset, the declared
/// format version, and the deserializer's reason (which is the version
/// -shape violation when the payload carries the other version's item
/// shape). The deserializer already logged the escaped payload bytes;
/// the lookup boundary logs the returned error and reports the item as
/// absent.
fn format_item_read_error(df: &DataFile, pos: usize, e: anyhow::Error) -> anyhow::Error {
    anyhow::anyhow!(
        "Item at offset {} in data file {:016x}.{} does not match its \
         declared format (data envelope version {}): {}",
        pos,
        df.hash,
        GOAT_RODEO_DATA_FILE_SUFFIX,
        df.envelope.version,
        e
    )
}

/// Magic number identifying data (.grd) files: 0x00be1100 ("Bell" pepper)
///
/// BigTent uses food-themed magic numbers for file identification.
/// Data files contain CBOR-encoded Items at specific byte offsets.
#[allow(non_upper_case_globals)]
pub const DataFileMagicNumber: u32 = 0x00be1100; // Bell

#[cfg(test)]
mod tests {
    use super::*;
    use crate::item::Item;
    use std::collections::BTreeMap;

    /// Write a minimal `.grd` with the given data-envelope version and
    /// framed item payloads, then return its name hash (the file name is
    /// the hash, which is how `DataFile::new` finds it).
    fn write_grd(
        dir: &std::path::Path,
        envelope_version: u32,
        payloads: &[Vec<u8>],
    ) -> Result<u64> {
        use std::io::Write;
        let mut grd: Vec<u8> = vec![];
        grd.write_all(&DataFileMagicNumber.to_be_bytes())?;
        let env = DataFileEnvelope {
            version: envelope_version,
            magic: DataFileMagicNumber,
            previous: 0,
            depends_on: BTreeSet::new(),
            built_from_merge: false,
            info: BTreeMap::new(),
        };
        let env_bytes = serde_cbor::to_vec(&env)?;
        grd.write_all(&(env_bytes.len() as u16).to_be_bytes())?;
        grd.write_all(&env_bytes)?;
        for payload in payloads {
            grd.write_all(&(payload.len() as u32).to_be_bytes())?;
            grd.write_all(payload)?;
        }
        let hash = crate::util::byte_slice_to_u63(&crate::util::sha256_for_slice(&grd))?;
        std::fs::write(dir.join(format!("{hash:016x}.grd")), &grd)?;
        Ok(hash)
    }

    /// An item serialized in the VERSION 4 shape: connections as the
    /// ordered map (what the merge and the conversion write).
    fn v4_item_bytes(identifier: &str) -> Vec<u8> {
        let mut connections: BTreeMap<String, BTreeSet<String>> = Default::default();
        connections
            .entry("contained:up".to_string())
            .or_default()
            .insert("pkg:npm/x@1".to_string());
        let item = Item {
            identifier: identifier.to_string(),
            connections,
            body_mime_type: None,
            body: None,
        };
        serde_cbor::to_vec(&item).expect("the v4 item serializes")
    }

    /// The same item serialized in the VERSION 3 shape: connections as
    /// the legacy pair array (what version 3 clusters hold).
    fn v3_item_bytes(identifier: &str) -> Vec<u8> {
        let v4 = serde_cbor::from_slice::<Item>(&v4_item_bytes(identifier)).expect("round trip");
        serde_cbor::to_vec(&v4.to_v3()).expect("the v3 item serializes")
    }

    /// An item with NO connections member at all (the documented
    /// `#[serde(default)]` tolerance).
    fn item_bytes_without_connections(identifier: &str) -> Vec<u8> {
        let mut map = BTreeMap::new();
        map.insert(
            serde_cbor::Value::Text("identifier".to_string()),
            serde_cbor::Value::Text(identifier.to_string()),
        );
        serde_cbor::to_vec(&serde_cbor::Value::Map(map)).expect("serializes")
    }

    fn open(dir: &std::path::Path, hash: u64, envelope_version: u32) -> DataFile {
        tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(DataFile::new(&dir.to_path_buf(), hash, envelope_version))
            .expect("the data file loads")
    }

    /// The versioned-format contract, enforced at the read boundary: the
    /// read DISPATCHES on the declared envelope version, and each
    /// version's deserializer reads ONLY its own item shape — version 2
    /// (version 4 clusters) the ordered-map connections, version 1
    /// (version 3 clusters) the legacy pair array (up-converted through
    /// `From<ItemV3> for Item`). A file that lies about its content (the
    /// byte-copy conversion used to write version 3 pair bytes into
    /// version 4 -declared files) fails its deserializer and is a loud
    /// read error naming the file, offset, and declared version — not a
    /// silent dual-shape read that version-specific readers answer with
    /// empty edges. A missing `connections` member is tolerated in both
    /// versions (the documented `#[serde(default)]`).
    #[test]
    fn test_read_item_at_enforces_version_shape() {
        let dir = tempfile::TempDir::new().unwrap();

        // version 2 + the version 4 map shape: reads
        let hash = write_grd(dir.path(), 2, &[v4_item_bytes("gitoid:blob:sha256:ok_v4")]).unwrap();
        let df = open(dir.path(), hash, 2);
        let item = df.read_item_at(df.data_offset).expect("v4 shape reads");
        assert_eq!(item.identifier, "gitoid:blob:sha256:ok_v4");

        // version 2 + the version 3 pair-array shape: the misdeclared
        // file is a LOUD error (the version 4 item deserializer refuses
        // the pair array)
        let hash = write_grd(dir.path(), 2, &[v3_item_bytes("gitoid:blob:sha256:bad_v4")]).unwrap();
        let df = open(dir.path(), hash, 2);
        let err = df
            .read_item_at(df.data_offset)
            .expect_err("pair-array bytes must not read from a version 2 data file");
        assert!(
            err.to_string()
                .contains("does not match its declared format"),
            "the error names the file, offset, and declared version: {err:#}"
        );

        // version 1 + the version 3 pair-array shape: reads, and the
        // ItemV3 is up-converted through the destructive From
        let hash = write_grd(dir.path(), 1, &[v3_item_bytes("gitoid:blob:sha256:ok_v3")]).unwrap();
        let df = open(dir.path(), hash, 1);
        let item = df.read_item_at(df.data_offset).expect("v3 shape reads");
        assert_eq!(item.identifier, "gitoid:blob:sha256:ok_v3");
        assert_eq!(
            item.connections.get("contained:up").map(|t| t.len()),
            Some(1),
            "the version 3 pair set folded into the version 4 map"
        );

        // version 1 + the version 4 map shape: equally misdeclared (the
        // version 3 item deserializer refuses the map)
        let hash = write_grd(dir.path(), 1, &[v4_item_bytes("gitoid:blob:sha256:bad_v3")]).unwrap();
        let df = open(dir.path(), hash, 1);
        let err = df
            .read_item_at(df.data_offset)
            .expect_err("map-shaped bytes must not read from a version 1 data file");
        assert!(
            err.to_string()
                .contains("does not match its declared format"),
            "the error names the file, offset, and declared version: {err:#}"
        );

        // a missing connections member is tolerated in both versions
        let no_conn = item_bytes_without_connections("gitoid:blob:sha256:noconn");
        let hash = write_grd(dir.path(), 2, &[no_conn.clone()]).unwrap();
        let df = open(dir.path(), hash, 2);
        assert!(df.read_item_at(df.data_offset).is_ok());
        let hash = write_grd(dir.path(), 1, &[no_conn]).unwrap();
        let df = open(dir.path(), hash, 1);
        assert!(df.read_item_at(df.data_offset).is_ok());
    }
}
