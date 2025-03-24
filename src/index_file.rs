use crate::{
  data_file::GOAT_RODEO_INDEX_FILE_SUFFIX,
  rodeo::{GoatRodeoCluster, IndexFileMagicNumber},
  tokio::io::AsyncSeekExt,
  util::{byte_slice_to_u63, read_len_and_cbor, read_u32, sha256_for_reader, MD5Hash},
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
      let mut file = GoatRodeoCluster::find_data_or_index_file_from_sha256(
        &dir,
        hash,
        GOAT_RODEO_INDEX_FILE_SUFFIX,
      )
      .await?;

      let tested_hash = byte_slice_to_u63(&sha256_for_reader(&mut file).await?)?;
      if tested_hash != hash {
        bail!(
          "Index file for {:016x} does not match {:016x}",
          hash,
          tested_hash
        );
      }
    }

    let mut file = GoatRodeoCluster::find_data_or_index_file_from_sha256(
      &dir,
      hash,
      GOAT_RODEO_INDEX_FILE_SUFFIX,
    )
    .await?;

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

  pub async fn read_index(&self) -> Result<Vec<ItemOffset<ItemLoc>>> {
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
  Loc(ItemLoc),
  Chain(Vec<IndexLoc>),
}

impl IndexLoc {
  pub fn as_vec(&self) -> Vec<IndexLoc> {
    match self {
      IndexLoc::Chain(v) => v.clone(),
      il @ IndexLoc::Loc(_) => vec![il.clone()],
    }
  }
}

impl GetOffset for IndexLoc {
  #[inline(always)]
  fn get_offset(&self) -> u64 {
    match self {
      IndexLoc::Loc((off, _)) => *off,
      _ => u64::MAX,
    }
  }

  #[inline(always)]
  fn get_file_hash(&self) -> u64 {
    match self {
      IndexLoc::Loc((_, file_hash)) => *file_hash, // & 0xffffffffffffff, // lop off the top 8 bits
      _ => u64::MAX,
    }
  }
}

pub trait GetOffset {
  fn get_offset(&self) -> u64;
  fn get_file_hash(&self) -> u64;
}

/// a location
pub type ItemLoc = (u64, u64);

impl Into<IndexLoc> for ItemLoc {
  #[inline(always)]
  fn into(self) -> IndexLoc {
    IndexLoc::Loc(self)
  }
}

impl GetOffset for ItemLoc {
  #[inline(always)]
  fn get_file_hash(&self) -> u64 {
    self.1 // & 0xffffffffffffff // lop off the top 8 bits
  }

  #[inline(always)]
  fn get_offset(&self) -> u64 {
    self.0
  }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ItemOffset<
  IL: Into<IndexLoc> + GetOffset + Clone + std::fmt::Debug + PartialEq + Send + Sync,
> {
  pub hash: [u8; 16],
  pub loc: IL,
}

pub trait HasHash {
  fn hash<'a>(&'a self) -> &'a MD5Hash;
}

#[derive(Debug, Clone, PartialEq)]
pub enum EitherItemOffset {
  Smol(ItemOffset<ItemLoc>),
  Big(ItemOffset<IndexLoc>),
}
impl Into<ItemOffset<IndexLoc>> for EitherItemOffset {
  fn into(self) -> ItemOffset<IndexLoc> {
    match self {
      EitherItemOffset::Smol(item_offset) => ItemOffset {
        hash: item_offset.hash,
        loc: IndexLoc::Loc(item_offset.loc),
      },
      EitherItemOffset::Big(item_offset) => item_offset,
    }
  }
}
impl EitherItemOffset {
  pub fn index_loc(&self) -> Vec<IndexLoc> {
    match self {
      EitherItemOffset::Smol(item_offset) => vec![IndexLoc::Loc(item_offset.loc)],
      EitherItemOffset::Big(item_offset) => item_offset.loc.as_vec(),
    }
  }
  #[inline(always)]
  pub fn loc(&self) -> IndexLoc {
    match self {
      EitherItemOffset::Smol(item_offset) => IndexLoc::Loc(item_offset.loc),
      EitherItemOffset::Big(item_offset) => item_offset.loc.clone(),
    }
  }
  #[inline(always)]
  pub fn hash(&self) -> &[u8; 16] {
    match self {
      EitherItemOffset::Smol(item_offset) => &item_offset.hash,
      EitherItemOffset::Big(item_offset) => &item_offset.hash,
    }
  }
}

impl From<ItemOffset<ItemLoc>> for EitherItemOffset {
  fn from(value: ItemOffset<ItemLoc>) -> Self {
    EitherItemOffset::Smol(value)
  }
}

impl From<ItemOffset<IndexLoc>> for EitherItemOffset {
  fn from(value: ItemOffset<IndexLoc>) -> Self {
    EitherItemOffset::Big(value)
  }
}

impl HasHash for ItemOffset<ItemLoc> {
  fn hash<'a>(&'a self) -> &'a MD5Hash {
    &self.hash
  }
}

impl HasHash for ItemOffset<IndexLoc> {
  fn hash<'a>(&'a self) -> &'a MD5Hash {
    &self.hash
  }
}

/// we have to reduce ItemOffset to a concrete type without
/// type parameters on `GoatRodeoCluster`, so we have this
/// enum that captures the two types we could use
#[derive(Debug, Clone, PartialEq)]
pub enum EitherItemOffsetVec {
  Smol(Vec<ItemOffset<ItemLoc>>),
  Big(Vec<ItemOffset<IndexLoc>>),
}

impl EitherItemOffsetVec {
  #[cfg(test)]
  pub fn flatten(&self) -> Vec<EitherItemOffset> {
    match self {
      EitherItemOffsetVec::Smol(item_offsets) => {
        item_offsets.iter().map(|i| i.clone().into()).collect()
      }
      EitherItemOffsetVec::Big(item_offsets) => {
        item_offsets.iter().map(|i| i.clone().into()).collect()
      }
    }
  }
  pub fn item_at(&self, pos: usize) -> EitherItemOffset {
    match self {
      EitherItemOffsetVec::Smol(item_offsets) => item_offsets[pos].clone().into(),
      EitherItemOffsetVec::Big(item_offsets) => item_offsets[pos].clone().into(),
    }
  }
  pub fn len(&self) -> usize {
    match self {
      EitherItemOffsetVec::Smol(item_offsets) => item_offsets.len(),
      EitherItemOffsetVec::Big(item_offsets) => item_offsets.len(),
    }
  }

  pub fn find(&self, hash: MD5Hash) -> Option<EitherItemOffset> {
    let ret = match self {
      EitherItemOffsetVec::Smol(item_offsets) => {
        EitherItemOffsetVec::find_item::<ItemOffset<ItemLoc>>(hash, item_offsets).map(|x| x.into())
      }
      EitherItemOffsetVec::Big(item_offsets) => {
        EitherItemOffsetVec::find_item::<ItemOffset<IndexLoc>>(hash, item_offsets).map(|x| x.into())
      }
    };
    ret
  }

  fn find_item<H: HasHash + Clone>(to_find: [u8; 16], offsets: &[H]) -> Option<H> {
    if offsets.len() == 0 {
      return None;
    }
    let mut low = 0;
    let mut hi = offsets.len() - 1;

    while low <= hi {
      let mid = low + (hi - low) / 2;
      match offsets.get(mid) {
        Some(entry) => {
          if entry.hash() == &to_find {
            return Some(entry.clone());
          } else if entry.hash() > &to_find {
            hi = mid - 1;
          } else {
            low = mid + 1;
          }
        }
        None => return None,
      }
    }

    None
  }
}

impl Into<EitherItemOffsetVec> for Vec<ItemOffset<ItemLoc>> {
  fn into(self) -> EitherItemOffsetVec {
    EitherItemOffsetVec::Smol(self)
  }
}

impl Into<EitherItemOffsetVec> for Vec<ItemOffset<IndexLoc>> {
  fn into(self) -> EitherItemOffsetVec {
    EitherItemOffsetVec::Big(self)
  }
}

impl ItemOffset<ItemLoc> {
  pub async fn read<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<ItemOffset<ItemLoc>> {
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
      loc: (
        u64::from_be_bytes(loc_bytes),
        u64::from_be_bytes(file_bytes),
      ),
    })
  }

  pub async fn build_from_index_file(file_name: &str) -> Result<Vec<ItemOffset<ItemLoc>>> {
    // make sure the buffer is a multiple of 24 (the length of the u128 + u64 + u64)
    let mut reader = BufReader::with_capacity(32 * 4096, File::open(file_name).await?);
    let mut stuff = vec![];

    loop {
      match ItemOffset::<ItemLoc>::read(&mut reader).await {
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
