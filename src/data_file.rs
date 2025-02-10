use crate::{
  rodeo::{DataFileMagicNumber, GoatRodeoCluster},
  structs::Item,
  util::{read_cbor_sync, read_len_and_cbor, read_u32, read_u32_sync},
};
use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use std::{
  collections::{BTreeMap, BTreeSet},
  fs::File as SyncFile,
  io::{BufReader as SyncBufReader, Read, Seek},
  path::PathBuf,
  sync::Arc,
};
use tokio::{fs::File, io::AsyncSeekExt, sync::Mutex};

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
  pub file: Arc<Mutex<Box<dyn DataReader>>>,
  pub data_offset: u64,
}

pub const GOAT_RODEO_DATA_FILE_SUFFIX: &str = "grd";
pub const GOAT_RODEO_INDEX_FILE_SUFFIX: &str = "gri";
pub const GOAT_RODEO_CLUSTER_FILE_SUFFIX: &str = "grc";

impl DataFile {
  pub async fn new(dir: &PathBuf, hash: u64) -> Result<DataFile> {
    let mut data_file = GoatRodeoCluster::find_file(dir, hash, GOAT_RODEO_DATA_FILE_SUFFIX).await?;

    let dfp: &mut File = &mut data_file;
    let magic = read_u32(dfp).await?;
    if magic != DataFileMagicNumber {
      bail!(
        "Unexpected magic number {:x}, expecting {:x} for data file {:016x}.{}",
        magic,
        DataFileMagicNumber,
        hash,
        GOAT_RODEO_DATA_FILE_SUFFIX
      );
    }

    let env: DataFileEnvelope = read_len_and_cbor(dfp).await?;

    let cur_pos: u64 = data_file.stream_position().await?;
    // FIXME do additional validation of the envelope
    let data_file = data_file
      .try_into_std()
      .map_err(|e| anyhow!(format!("Couldn't convert file {:?}", e)))?;
    Ok(DataFile {
      envelope: env,
      file: Arc::new(Mutex::new(Box::new(SyncBufReader::with_capacity(
        16384, data_file,
      )))),
      data_offset: cur_pos,
    })
  }

  /// Seek to a position in the DataReader which should be a SyncBufReader<SyncFile>
  /// Avoid absolute seeking which always blows away cache, but do relative seeking
  /// which may preserve cache
  fn seek_to(file: &mut Box<dyn DataReader>, desired_pos: u64) -> Result<()> {
    let pos = file.stream_position()?;
    if pos == desired_pos {
      return Ok(());
    }

    let rel_seek = (desired_pos as i64) - (pos as i64);

    file.seek(std::io::SeekFrom::Current(rel_seek))?;
    Ok(())
  }

  /// read the item. This is a mixture of synchronous and async code. Why?
  /// Turns out the async BufReader is freakin' slow, so we're doing synchronous
  /// BufReader. Ideally, we'd put this on a blocking Tokio thread, but, sigh
  /// async closures are not in mainline Rust right now, so "no thread-friendly soup for you!"
  pub async fn read_item_at(&self, pos: u64) -> Result<Item> {
    let mut my_file = self.file.lock().await;
    let my_reader: &mut Box<dyn DataReader> = &mut my_file;

    tokio::task::block_in_place(move || {
      DataFile::seek_to(my_reader, pos)?;

      let item_len = read_u32_sync(my_reader)?;
      let item = read_cbor_sync(my_reader, item_len as usize)?;
      Ok(item)
    })
  }
}
