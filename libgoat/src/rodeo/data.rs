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

use super::goat::GoatRodeoCluster;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct DataFileEnvelope {
  pub version: u32,
  pub magic: u32,
  pub previous: u64,
  pub depends_on: BTreeSet<u64>,
  pub built_from_merge: bool,
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

impl DataFile {
  pub async fn new(dir: &PathBuf, hash: u64) -> Result<DataFile> {
    let mut data_file =
      GoatRodeoCluster::find_data_or_index_file_from_sha256(dir, hash, GOAT_RODEO_DATA_FILE_SUFFIX)
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
  pub fn read_item_at(&self, pos: usize) -> Result<Item> {
    let mut my_reader: &[u8] = &self.file[pos..];

    let item_len = read_u32_sync(&mut my_reader)?;
    let item = read_cbor_sync(&mut my_reader, item_len as usize)?;
    Ok(item)
  }
}

#[allow(non_upper_case_globals)]
pub const DataFileMagicNumber: u32 = 0x00be1100; // Bell
