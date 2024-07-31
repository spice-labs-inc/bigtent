use crate::{
  rodeo::GoatRodeoCluster,
  structs::Item,
  util::{read_cbor, read_len_and_cbor, read_u32},
};
use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use std::{
  collections::{BTreeMap, BTreeSet},
  fs::File,
  io::{BufReader, Seek},
  path::PathBuf,
  sync::{Arc, Mutex},
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
pub struct DataFile {
  pub envelope: DataFileEnvelope,
  pub file: Arc<Mutex<BufReader<File>>>,
  pub data_offset: u64,
}

impl DataFile {
  pub fn new(dir: &PathBuf, hash: u64) -> Result<DataFile> {
    let mut data_file = GoatRodeoCluster::find_file(dir, hash, "grd")?;
    let dfp: &mut File = &mut data_file;
    let magic = read_u32(dfp)?;
    if magic != GoatRodeoCluster::DataFileMagicNumber {
      bail!(
        "Unexpected magic number {:x}, expecting {:x} for data file {:016x}.grd",
        magic,
        GoatRodeoCluster::DataFileMagicNumber,
        hash
      );
    }

    let env: DataFileEnvelope = read_len_and_cbor(dfp)?;

    let cur_pos: u64 = data_file.stream_position()?;
    // FIXME do additional validation of the envelope
    Ok(DataFile {
      envelope: env,
      file: Arc::new(Mutex::new(BufReader::with_capacity(4096, data_file))),
      data_offset: cur_pos,
    })
  }

  fn seek_to(file: &mut BufReader<File>, desired_pos: u64) -> Result<()> {
    let pos = file.stream_position()?;
    if pos == desired_pos {
      return Ok(());
    }

    let rel_seek = (desired_pos as i64) - (pos as i64);

    file.seek_relative(rel_seek)?;
    Ok(())
  }

  pub fn read_item_at(&self, pos: u64) -> Result<Item> {
    let mut my_file = self
      .file
      .lock()
      .map_err(|e| anyhow!("Failed to lock {:?}", e))?;
    DataFile::seek_to(&mut my_file, pos)?;
    let my_reader: &mut BufReader<File> = &mut my_file;
    let item_len = read_u32(my_reader)?;
    let item = read_cbor(&mut *my_file, item_len as usize)?;
    Ok(item)
  }
}
