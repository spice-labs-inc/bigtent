use crate::{
  rodeo::{DataFileMagicNumber, GoatRodeoCluster},
  structs::Item,
  util::{read_cbor, read_len_and_cbor, read_u32},
};
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::{
  collections::{BTreeMap, BTreeSet},
  path::PathBuf,
  sync::Arc,
};
use tokio::{
  fs::File,
  io::{AsyncRead, AsyncSeek, AsyncSeekExt, BufReader},
  sync::Mutex,
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct DataFileEnvelope {
  pub version: u32,
  pub magic: u32,
  pub previous: u64,
  pub depends_on: BTreeSet<u64>,
  pub built_from_merge: bool,
  pub info: BTreeMap<String, String>,
}

pub trait DataAsyncReader: AsyncRead + AsyncSeek + Unpin + Send+ Sync + std::fmt::Debug {}

impl DataAsyncReader for BufReader<File> {}

impl DataAsyncReader for File {}

#[derive(Debug, Clone)]
pub struct DataFile {
  pub envelope: DataFileEnvelope,
  pub file: Arc<Mutex<Box<dyn DataAsyncReader>>>,
  pub data_offset: u64,
}

impl DataFile {
  pub async fn new(dir: &PathBuf, hash: u64) -> Result<DataFile> {
    let mut data_file = GoatRodeoCluster::find_file(dir, hash, "grd").await?;
    let dfp: &mut File = &mut data_file;
    let magic = read_u32(dfp).await?;
    if magic != DataFileMagicNumber {
      bail!(
        "Unexpected magic number {:x}, expecting {:x} for data file {:016x}.grd",
        magic,
        DataFileMagicNumber,
        hash
      );
    }

    let env: DataFileEnvelope = read_len_and_cbor(dfp).await?;

    let cur_pos: u64 = data_file.stream_position().await?;
    // FIXME do additional validation of the envelope
    Ok(DataFile {
      envelope: env,
      //file: Arc::new(Mutex::new(Box::new(BufReader::with_capacity(4096, data_file)))),
      file: Arc::new(Mutex::new(Box::new(data_file))),
      data_offset: cur_pos,
    })
  }

  async fn seek_to(file: &mut Box<dyn DataAsyncReader>, desired_pos: u64) -> Result<()> {
    let pos = file.stream_position().await?;
    if pos == desired_pos {
      return Ok(());
    }

    // let rel_seek = (desired_pos as i64) - (pos as i64);

    file.seek(std::io::SeekFrom::Start(desired_pos)).await?;
    Ok(())
  }

  pub async fn read_item_at(&self, pos: u64) -> Result<Item> {
    let mut my_file = self.file.lock().await;
    let my_reader: &mut Box<dyn DataAsyncReader> = &mut my_file;
    DataFile::seek_to(my_reader, pos).await?;
    
    let item_len = read_u32(my_reader).await?;
    let item = read_cbor(&mut *my_file, item_len as usize).await?;
    Ok(item)
  }
}
