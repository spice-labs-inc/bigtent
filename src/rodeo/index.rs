use crate::util::{
  byte_slice_to_u63, read_len_and_cbor_sync, read_u32_sync, sha256_for_reader_sync, MD5Hash,
};
use anyhow::{bail, Result};
use memmap2::Mmap;
use serde::{Deserialize, Serialize};
use std::{
  collections::{BTreeMap, HashMap, HashSet},
  io::Read,
  path::PathBuf,
  sync::Arc,
};

use super::{data::GOAT_RODEO_INDEX_FILE_SUFFIX, goat::GoatRodeoCluster};

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
  file: Arc<Mmap>,
  pub data_offset: usize,
  pub hash_for_file_lookup: u64,
}

impl IndexFile {
  /// given a vec of index files, make a lookup table
  pub fn vec_of_index_files_to_hash_lookup(
    the_vec: &Vec<Arc<IndexFile>>,
  ) -> HashMap<u64, Arc<IndexFile>> {
    let mut ret = HashMap::new();
    for idx_f in the_vec {
      ret.insert(idx_f.hash_for_file_lookup, idx_f.clone());
    }
    ret
  }
  pub async fn new(dir: &PathBuf, hash: u64, check_hash: bool) -> Result<IndexFile> {
    // ensure we close `file` after computing the hash
    if check_hash {
      let mut file = GoatRodeoCluster::find_data_or_index_file_from_sha256(
        &dir,
        hash,
        GOAT_RODEO_INDEX_FILE_SUFFIX,
      )
      .await?;

      let tested_hash = byte_slice_to_u63(&sha256_for_reader_sync(&mut file)?)?;
      if tested_hash != hash {
        bail!(
          "Index file for {:016x} does not match {:016x}",
          hash,
          tested_hash
        );
      }
    }

    let file = GoatRodeoCluster::find_data_or_index_file_from_sha256(
      &dir,
      hash,
      GOAT_RODEO_INDEX_FILE_SUFFIX,
    )
    .await?;

    let bytes = unsafe { Mmap::map(&file)? };
    let mut ifp = &bytes[0..];
    let start_len = ifp.len();
    let magic = read_u32_sync(&mut ifp)?;
    if magic != IndexFileMagicNumber {
      bail!(
        "Unexpected magic number {:x}, expecting {:x} for data file {:016x}.{}",
        magic,
        IndexFileMagicNumber,
        hash,
        GOAT_RODEO_INDEX_FILE_SUFFIX
      );
    }

    let idx_env: IndexEnvelope = read_len_and_cbor_sync(&mut ifp)?;

    // the "position" is the starting length of the byte array less the
    // current length... that's the number of bytes we read
    let idx_pos = start_len - ifp.len();

    Ok(IndexFile {
      envelope: idx_env,
      file: Arc::new(bytes),
      data_offset: idx_pos,
      hash_for_file_lookup: hash,
    })
  }

  /// Compute the number of bytes in the file less the
  /// offset for the header/envelope
  pub fn data_len(&self) -> usize {
    self.file.len() - self.data_offset
  }

  pub fn read_index(&self) -> Result<Vec<ItemOffset>> {
    let mut ret = Vec::with_capacity(self.envelope.size as usize);
    let mut last = [0u8; 16];

    let mut info: &[u8] = &self.file[self.data_offset..];
    for _ in 0..self.envelope.size {
      let eo = ItemOffset::read(&mut info)?;
      if eo.hash < last {
        bail!("Not sorted!!! last {:?} eo.hash {:?}", last, eo.hash);
      }
      last = eo.hash;
      ret.push(eo);
    }

    Ok(ret)
  }

  pub fn read_index_at_byte_offset(&self, pos: usize) -> Result<ItemOffset> {
    let mut info: &[u8] = &self.file[(self.data_offset + pos)..];
    Ok(ItemOffset::read(&mut info)?)
  }
}

// #[repr(u8)]
// #[derive(Debug, Clone, PartialEq)]
// pub enum IndexLoc {
//   Loc(ItemLoc),
//   Chain(Vec<IndexLoc>),
// }

// impl IndexLoc {
//   pub fn as_vec(&self) -> Vec<IndexLoc> {
//     match self {
//       IndexLoc::Chain(v) => v.clone(),
//       il @ IndexLoc::Loc(_) => vec![il.clone()],
//     }
//   }
// }

// impl GetOffset for IndexLoc {

// }

pub trait GetOffset {
  fn get_offset(&self) -> usize;
  fn get_file_hash(&self) -> u64;
}

/// a location
pub type ItemLoc = (usize, u64);

impl GetOffset for ItemLoc {
  #[inline(always)]
  fn get_file_hash(&self) -> u64 {
    self.1 // & 0xffffffffffffff // lop off the top 8 bits
  }

  #[inline(always)]
  fn get_offset(&self) -> usize {
    self.0
  }
}

#[derive(Debug, Clone, PartialEq, Hash, Copy)]
pub struct ItemOffset {
  pub hash: [u8; 16],
  pub loc: ItemLoc,
}

pub trait HasHash {
  fn hash<'a>(&'a self) -> &'a MD5Hash;
}

// #[derive(Debug, Clone, PartialEq)]
// pub enum EitherItemOffset {
//   Smol(ItemOffset<ItemLoc>),
//   Big(ItemOffset<IndexLoc>),
// }
// impl Into<ItemOffset<IndexLoc>> for EitherItemOffset {
//   fn into(self) -> ItemOffset<IndexLoc> {
//     match self {
//       EitherItemOffset::Smol(item_offset) => ItemOffset {
//         hash: item_offset.hash,
//         loc: IndexLoc::Loc(item_offset.loc),
//       },
//       EitherItemOffset::Big(item_offset) => item_offset,
//     }
//   }
// }
// impl EitherItemOffset {
//   pub fn index_loc(&self) -> Vec<IndexLoc> {
//     match self {
//       EitherItemOffset::Smol(item_offset) => vec![IndexLoc::Loc(item_offset.loc)],
//       EitherItemOffset::Big(item_offset) => item_offset.loc.as_vec(),
//     }
//   }
//   #[inline(always)]
//   pub fn loc(&self) -> IndexLoc {
//     match self {
//       EitherItemOffset::Smol(item_offset) => IndexLoc::Loc(item_offset.loc),
//       EitherItemOffset::Big(item_offset) => item_offset.loc.clone(),
//     }
//   }
//   #[inline(always)]
//   pub fn hash(&self) -> &MD5Hash {
//     match self {
//       EitherItemOffset::Smol(item_offset) => &item_offset.hash,
//       EitherItemOffset::Big(item_offset) => &item_offset.hash,
//     }
//   }
// }

// impl From<ItemOffset<ItemLoc>> for EitherItemOffset {
//   fn from(value: ItemOffset<ItemLoc>) -> Self {
//     EitherItemOffset::Smol(value)
//   }
// }

// impl From<ItemOffset<IndexLoc>> for EitherItemOffset {
//   fn from(value: ItemOffset<IndexLoc>) -> Self {
//     EitherItemOffset::Big(value)
//   }
// }

impl HasHash for ItemOffset {
  fn hash<'a>(&'a self) -> &'a MD5Hash {
    &self.hash
  }
}

// impl HasHash for ItemOffset<IndexLoc> {
//   fn hash<'a>(&'a self) -> &'a MD5Hash {
//     &self.hash
//   }
// }

/// we have to reduce ItemOffset to a concrete type without
/// type parameters on `GoatRodeoCluster`, so we have this
/// enum that captures the two types we could use
// #[derive(Debug, Clone, PartialEq)]
// pub enum EitherItemOffsetVec {
//   Smol(Vec<ItemOffset<ItemLoc>>),
//   Big(Vec<ItemOffset<IndexLoc>>),
// }

// impl EitherItemOffsetVec {
//   #[cfg(test)]
//   pub fn flatten(&self) -> Vec<EitherItemOffset> {
//     match self {
//       EitherItemOffsetVec::Smol(item_offsets) => {
//         item_offsets.iter().map(|i| i.clone().into()).collect()
//       }
//       EitherItemOffsetVec::Big(item_offsets) => {
//         item_offsets.iter().map(|i| i.clone().into()).collect()
//       }
//     }
//   }
//   pub fn item_at(&self, pos: usize) -> EitherItemOffset {
//     match self {
//       EitherItemOffsetVec::Smol(item_offsets) => item_offsets[pos].clone().into(),
//       EitherItemOffsetVec::Big(item_offsets) => item_offsets[pos].clone().into(),
//     }
//   }
//   pub fn len(&self) -> usize {
//     match self {
//       EitherItemOffsetVec::Smol(item_offsets) => item_offsets.len(),
//       EitherItemOffsetVec::Big(item_offsets) => item_offsets.len(),
//     }
//   }

//   pub fn find(&self, hash: MD5Hash) -> Option<EitherItemOffset> {
//     let ret = match self {
//       EitherItemOffsetVec::Smol(item_offsets) => {
//         EitherItemOffsetVec::find_item::<ItemOffset<ItemLoc>>(hash, item_offsets).map(|x| x.into())
//       }
//       EitherItemOffsetVec::Big(item_offsets) => {
//         EitherItemOffsetVec::find_item::<ItemOffset<IndexLoc>>(hash, item_offsets).map(|x| x.into())
//       }
//     };
//     ret
//   }

//   fn find_item<H: HasHash + Clone>(to_find: [u8; 16], offsets: &[H]) -> Option<H> {
//     if offsets.len() == 0 {
//       return None;
//     }
//     let mut low = 0;
//     let mut hi = offsets.len() - 1;

//     while low <= hi {
//       let mid = low + (hi - low) / 2;
//       match offsets.get(mid) {
//         Some(entry) => {
//           if entry.hash() == &to_find {
//             return Some(entry.clone());
//           } else if entry.hash() > &to_find {
//             hi = mid - 1;
//           } else {
//             low = mid + 1;
//           }
//         }
//         None => return None,
//       }
//     }

//     None
//   }
// }

// impl Into<EitherItemOffsetVec> for Vec<ItemOffset<ItemLoc>> {
//   fn into(self) -> EitherItemOffsetVec {
//     EitherItemOffsetVec::Smol(self)
//   }
// }

// impl Into<EitherItemOffsetVec> for Vec<ItemOffset<IndexLoc>> {
//   fn into(self) -> EitherItemOffsetVec {
//     EitherItemOffsetVec::Big(self)
//   }
// }

pub(crate) fn find_item_offset(to_find: [u8; 16], offsets: &[ItemOffset]) -> Option<ItemOffset> {
  if offsets.len() == 0 {
    return None;
  }
  let mut low = 0;
  let mut hi = offsets.len() - 1;

  while low <= hi {
    let mid = low + (hi - low) / 2;
    match offsets.get(mid) {
      Some(entry) => {
        if entry.hash == to_find {
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

impl ItemOffset {
  //   #[inline(always)]
  //   fn get_offset(&self) -> usize {
  //     self.loc.get_offset()
  //   }

  //   #[inline(always)]
  //   fn get_file_hash(&self) -> u64 {
  //     self.loc.get_file_hash()
  //   }

  pub fn read<R: Read>(reader: &mut R) -> Result<ItemOffset> {
    let mut hash_bytes = u128::default().to_be_bytes();
    let mut file_bytes = u64::default().to_be_bytes();
    let mut loc_bytes = u64::default().to_be_bytes();
    let hl = reader.read(&mut hash_bytes)?;
    let fl = reader.read(&mut file_bytes)?;
    let ll = reader.read(&mut loc_bytes)?;
    if hl != hash_bytes.len() || ll != loc_bytes.len() || fl != file_bytes.len() {
      bail!("Failed to read enough bytes for EntryOffset")
    }
    Ok(ItemOffset {
      hash: hash_bytes,
      loc: (
        usize::from_be_bytes(loc_bytes),
        u64::from_be_bytes(file_bytes),
      ),
    })
  }
}

#[allow(non_upper_case_globals)]
pub const IndexFileMagicNumber: u32 = 0x54154170; // Shishitō
