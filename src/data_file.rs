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
  io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, BufReader},
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

#[derive(Debug, Clone)]
pub struct DataFile<F: AsyncRead + AsyncReadExt + AsyncSeek + AsyncSeekExt + Unpin> {
  pub envelope: DataFileEnvelope,
  pub file: Arc<Mutex<BufReader<F>>>,
  pub data_offset: u64,
}

impl DataFile<File> {
  pub async fn new(dir: &PathBuf, hash: u64) -> Result<DataFile<File>> {
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
      file: Arc::new(Mutex::new(BufReader::with_capacity(4096, data_file))),
      data_offset: cur_pos,
    })
  }

  async fn seek_to(file: &mut BufReader<File>, desired_pos: u64) -> Result<()> {
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
    DataFile::seek_to(&mut my_file, pos).await?;
    let my_reader: &mut BufReader<File> = &mut my_file;
    let item_len = read_u32(my_reader).await?;
    let item = read_cbor(&mut *my_file, item_len as usize).await?;
    Ok(item)
  }
}
