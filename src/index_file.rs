use crate::{
  data_file::GOAT_RODEO_INDEX_FILE_SUFFIX,
  rodeo::{GoatRodeoCluster, IndexFileMagicNumber},
  tokio::io::AsyncSeekExt,
  util::{byte_slice_to_u63, read_len_and_cbor, read_u32, sha256_for_reader},
};
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::{
  collections::{BTreeMap, HashSet},
  io::SeekFrom,
  path::PathBuf,
  sync::Arc,
};
use tokio::{
  fs::File,
  io::{AsyncReadExt, BufReader},
  sync::Mutex,
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct IndexEnvelope {
  pub version: u32,
  pub magic: u32,
  pub size: u32,
  pub data_files: HashSet<u64>,
  pub encoding: String,
  pub info: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct IndexFile {
  pub envelope: IndexEnvelope,
  pub file: Arc<Mutex<BufReader<File>>>,
  pub data_offset: u64,
}

impl IndexFile {
  pub async fn new(dir: &PathBuf, hash: u64, check_hash: bool) -> Result<IndexFile> {
    // ensure we close `file` after computing the hash
    if check_hash {
      let mut file = GoatRodeoCluster::find_file(&dir, hash, GOAT_RODEO_INDEX_FILE_SUFFIX).await?;

      let tested_hash = byte_slice_to_u63(&sha256_for_reader(&mut file).await?)?;
      if tested_hash != hash {
        bail!(
          "Index file for {:016x} does not match {:016x}",
          hash,
          tested_hash
        );
      }
    }

    let mut file = GoatRodeoCluster::find_file(&dir, hash, GOAT_RODEO_INDEX_FILE_SUFFIX).await?;

    let ifp = &mut file;
    let magic = read_u32(ifp).await?;
    if magic != IndexFileMagicNumber {
      bail!(
        "Unexpected magic number {:x}, expecting {:x} for data file {:016x}.{}",
        magic,
        IndexFileMagicNumber,
        hash,
        GOAT_RODEO_INDEX_FILE_SUFFIX
      );
    }

    let idx_env: IndexEnvelope = read_len_and_cbor(ifp).await?;

    let idx_pos: u64 = file.seek(SeekFrom::Current(0)).await?;

    Ok(IndexFile {
      envelope: idx_env,
      file: Arc::new(Mutex::new(BufReader::new(file))),
      data_offset: idx_pos,
    })
  }

  pub async fn read_index(&self) -> Result<Vec<ItemOffset>> {
    let mut ret = Vec::with_capacity(self.envelope.size as usize);
    let mut last = [0u8; 16];
    // let mut not_sorted = false;

    let mut my_file = self.file.lock().await;
    //      .map_err(|e| anyhow!("Failed to lock {:?}", e))?;
    let fp: &mut BufReader<File> = &mut my_file;
    fp.seek(SeekFrom::Start(self.data_offset)).await?;
    let mut buf = vec![];
    fp.read_to_end(&mut buf).await?;

    let mut info: &[u8] = &buf;
    for _ in 0..self.envelope.size {
      let eo = ItemOffset::read(&mut info).await?;
      if eo.hash < last {
        // not_sorted = true;
        bail!("Not sorted!!! last {:?} eo.hash {:?}", last, eo.hash);
      }
      last = eo.hash;
      ret.push(eo);
    }

    // if not_sorted {
    //     ret.sort_by(|a, b| a.hash.cmp(&b.hash))
    // }

    Ok(ret)
  }
}

#[repr(u8)]
#[derive(Debug, Clone, PartialEq)]
pub enum IndexLoc {
  Loc { offset: u64, file_hash: u64 },
  Chain(Vec<IndexLoc>),
}

impl IndexLoc {
  pub fn as_vec(&self) -> Vec<IndexLoc> {
    match self {
      IndexLoc::Chain(v) => v.clone(),
      il @ IndexLoc::Loc { file_hash: _, .. } => vec![il.clone()],
    }
  }

  #[inline(always)]
  pub fn offset(&self) -> u64 {
    match self {
      IndexLoc::Loc {
        offset: off,
        file_hash: _,
      } => *off & 0xffffffffffffff, // lop off the top 8 bits
      _ => u64::MAX,
    }
  }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ItemOffset {
  pub hash: [u8; 16],
  pub loc: IndexLoc,
}

impl ItemOffset {
  pub async fn read<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<ItemOffset> {
    let mut hash_bytes = u128::default().to_be_bytes();
    let mut file_bytes = u64::default().to_be_bytes();
    let mut loc_bytes = u64::default().to_be_bytes();
    let hl = reader.read(&mut hash_bytes).await?;
    let fl = reader.read(&mut file_bytes).await?;
    let ll = reader.read(&mut loc_bytes).await?;
    if hl != hash_bytes.len() || ll != loc_bytes.len() || fl != file_bytes.len() {
      bail!("Failed to read enough bytes for EntryOffset")
    }
    Ok(ItemOffset {
      hash: hash_bytes,
      loc: IndexLoc::Loc {
        offset: u64::from_be_bytes(loc_bytes),
        file_hash: u64::from_be_bytes(file_bytes),
      },
    })
  }

  pub async fn build_from_index_file(file_name: &str) -> Result<Vec<ItemOffset>> {
    // make sure the buffer is a multiple of 24 (the length of the u128 + u64 + u64)
    let mut reader = BufReader::with_capacity(32 * 4096, File::open(file_name).await?);
    let mut stuff = vec![];

    loop {
      match ItemOffset::read(&mut reader).await {
        Ok(eo) => {
          stuff.push(eo);
        }
        Err(e) => {
          if stuff.len() > 0 {
            break;
          }
          return Err(e);
        }
      }
    }

    Ok(stuff)
  }
}
