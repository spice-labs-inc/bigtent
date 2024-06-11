use crate::{index_file::ItemOffset, rodeo_server::MD5Hash};
use anyhow::{bail, Result};
#[cfg(not(test))]
use log::info;
use serde::{de::DeserializeOwned, Serialize};
use serde_cbor::Value;
use std::{
  collections::{BTreeMap, HashSet},
  ffi::OsStr,
  io::{Read, Write},
  path::PathBuf,
  time::{Duration, SystemTime},
}; // Use log crate when building application

#[cfg(test)]
use std::println as info;
const BYTE_BUFFER_SIZE: usize = 4096;

pub fn hex_to_md5bytes(it: &str) -> Option<MD5Hash> {
  hex::decode(it)
    .map(|bytes| {
      if bytes.len() < std::mem::size_of::<MD5Hash>() {
        None
      } else {
        let (int_bytes, _) = bytes.split_at(std::mem::size_of::<MD5Hash>());
        let slice: [u8; 16] = int_bytes.try_into().ok()?;
        Some(slice)
      }
    })
    .ok()
    .flatten()
}

pub fn hex_to_u64(it: &str) -> Option<u64> {
  hex::decode(it)
    .map(|bytes| {
      if bytes.len() < std::mem::size_of::<u64>() {
        None
      } else {
        let (int_bytes, _) = bytes.split_at(std::mem::size_of::<u64>());
        let slice: [u8; 8] = int_bytes.try_into().ok()?;
        Some(u64::from_be_bytes(slice))
      }
    })
    .ok()
    .flatten()
}

pub fn byte_slice_to_u63(it: &[u8]) -> Result<u64> {
  let mut buff = [0u8; 8];
  if it.len() < 8 {
    bail!(
      "The byte slice for a 64 bit number must have at least 8 bytes, this has {} bytes",
      it.len()
    );
  }

  for x in 0..8 {
    buff[x] = it[x];
  }
  Ok(u64::from_be_bytes(buff) & 0x7fffffffffffffff)
}

pub fn check_md5(it: Option<&String>) -> Option<MD5Hash> {
  match it {
    Some(v) if v.len() == 32 => hex_to_md5bytes(v),
    Some(v) if v.len() == 37 && v.ends_with(".json") => hex_to_md5bytes(&v[0..32]),
    _ => None,
  }
}

pub fn millis_now() -> i64 {
  SystemTime::now()
    .duration_since(SystemTime::UNIX_EPOCH)
    .unwrap()
    .as_millis() as i64
}

pub fn find_item(to_find: [u8; 16], offsets: &[ItemOffset]) -> Option<ItemOffset> {
  let mut low = 0;
  let mut hi = offsets.len() - 1;

  while low <= hi {
    let mid = low + (hi - low) / 2;
    match offsets.get(mid) {
      Some(entry) => {
        if entry.hash == to_find {
          return Some(entry.clone());
        } else if entry.hash > to_find {
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

pub fn md5hash_str(st: &str) -> MD5Hash {
  let res = md5::compute(st);

  res.into()
}

pub fn sha256_for_slice(r: &[u8]) -> [u8; 32] {
  // create a Sha256 object
  use sha2::{Digest, Sha256};
  let mut hasher = Sha256::new();

  hasher.update(r);

  hasher.finalize().into()
}

#[test]
fn test_big_sha() {
  use rand::Rng;

  let mut rng = rand::rngs::ThreadRng::default();
  let mut buf = [0u8; 4096];
  let mut v: Vec<u8> = vec![];
  for _ in 1..1_000 {
    rng.fill(&mut buf);
    v.extend_from_slice(&buf);
  }

  sha256_for_slice(v.as_slice());
}

pub fn sha256_for_reader<R: Read>(r: &mut R) -> Result<[u8; 32]> {
  use sha2::{Digest, Sha256};
  // create a Sha256 object
  let mut hasher = Sha256::new();
  let mut buffer = [0u8; BYTE_BUFFER_SIZE];
  loop {
    let read = r.read(&mut buffer)?;
    if read == 0 {
      break;
    }
    hasher.update(&buffer[0..read]);
  }
  Ok(hasher.finalize().into())
}

#[test]
fn test_sha256() {
  use bytes::Buf;
  use hex_literal::hex;
  let res = sha256_for_reader(&mut b"hello world".reader()).unwrap();
  assert_eq!(
    res,
    hex!("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
  );

  let res = sha256_for_slice(b"hello world");
  assert_eq!(
    res,
    hex!("b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9")
  );
}

pub fn is_child_dir(root: &PathBuf, potential_child: &PathBuf) -> Result<bool> {
  let full_root = root.canonicalize()?;

  let full_kid = potential_child.canonicalize()?;

  let mut kid_parts = HashSet::new();
  for i in full_kid.iter() {
    kid_parts.insert(i);
  }

  for i in full_root.iter() {
    if !kid_parts.contains(i) {
      return Ok(false);
    }
  }

  Ok(true)
}

#[test]
fn test_child_dir() {
  let root = PathBuf::from(".");

  assert!(is_child_dir(&root, &PathBuf::from("./src")).unwrap());

  assert!(!is_child_dir(&root, &PathBuf::from("/tmp")).unwrap());
}

pub fn find_common_root_dir(from: Vec<PathBuf>) -> Result<PathBuf> {
  let len = from.len();
  if len == 0 {
    bail!("Must have at least 1 directory to find common");
  } else if len == 1 {
    let ret = from[0].canonicalize()?;
    if ret.is_dir() {
      return Ok(ret);
    } else {
      return Ok(match ret.parent() {
        Some(p) => p.to_path_buf(),
        _ => bail!("{:?} is not a directory and does not have a parent", ret),
      });
    }
  }

  let mut all = vec![];

  for p in &from {
    all.push(p.canonicalize()?);
  }

  let mut with_parents = vec![];
  for p in &all {
    let mut parents = HashSet::new();
    for j in p.clone().iter() {
      parents.insert(os_str_to_string(j)?);
    }
    with_parents.push((p, parents));
  }

  let mut root = with_parents[0].1.clone();
  for j in &with_parents {
    root = root.intersection(&j.1).map(|v| v.clone()).collect();
  }

  if root.len() <= 1 {
    bail!("Couldn't find a common root");
  }

  let mut thing = all[0].clone();
  loop {
    let mut bad = false;
    for j in &thing {
      if !root.contains(&os_str_to_string(j)?) {
        thing = match thing.parent() {
          Some(v) => v.to_path_buf(),
          None => bail!("Couldn't get parent for {:?}", thing),
        };
        bad = true;
        break;
      }
    }

    if !bad {
      return Ok(thing);
    }
  }
}

#[test]
fn test_parents() {
  assert_eq!(
    find_common_root_dir(vec![PathBuf::from("."), PathBuf::from("./..")]).unwrap(),
    PathBuf::from("./..").canonicalize().unwrap()
  );
  assert_eq!(
    find_common_root_dir(vec![PathBuf::from("."), PathBuf::from("./../..")]).unwrap(),
    PathBuf::from("./../..").canonicalize().unwrap()
  );
  assert!(find_common_root_dir(vec![PathBuf::from("."), PathBuf::from("/tmp")]).is_err());
}

pub fn os_str_to_string(oss: &OsStr) -> Result<String> {
  match oss.to_str() {
    Some(s) => Ok(s.to_string()),
    None => bail!("Unable to convert {:?} to a String", oss),
  }
}

pub fn path_plus_timed(root: &PathBuf, suffix: &str) -> PathBuf {
  let mut ret = root.clone();
  ret.push(timed_filename(suffix));
  ret
}

pub fn timed_filename(suffix: &str) -> String {
  use chrono::prelude::*;

  let now: DateTime<Utc> = Utc::now();

  format!(
    "{:04}_{:02}_{:02}_{:02}_{:02}_{:02}_{}",
    now.year(),
    now.month(),
    now.day(),
    now.hour(),
    now.minute(),
    now.second(),
    suffix
  )
}

pub fn read_all<R: Read>(r: &mut R, max: usize) -> Result<Vec<u8>> {
  let mut ret = vec![];
  let mut buf = [0u8; BYTE_BUFFER_SIZE];
  let mut read: usize = 0;
  loop {
    let to_read = BYTE_BUFFER_SIZE.min(max - read);
    if to_read <= 0 {
      break;
    }
    match r.read(&mut buf[0..to_read]) {
      Ok(0) => {
        break;
      }
      Ok(n) => {
        read += n;
        let mut to_str = vec![];
        to_str.extend_from_slice(&buf[..n]);
        let qq = &buf[..n];
        ret.extend_from_slice(qq);
      }
      Err(e) => return Err(e.into()),
    }
  }

  Ok(ret)
}

pub fn read_u16<R: Read>(r: &mut R) -> Result<u16> {
  let mut buf = [0u8; 2];
  r.read_exact(&mut buf)?;
  Ok(u16::from_be_bytes(buf))
}

pub fn read_u32<R: Read>(r: &mut R) -> Result<u32> {
  let mut buf = [0u8; 4];
  r.read_exact(&mut buf)?;

  Ok(u32::from_be_bytes(buf))
}

pub fn read_u64<R: Read>(r: &mut R) -> Result<u64> {
  let mut buf = [0u8; 8];
  r.read_exact(&mut buf)?;

  Ok(u64::from_be_bytes(buf))
}

pub fn read_len_and_cbor<T: DeserializeOwned, R: Read>(file: &mut R) -> Result<T> {
  let len = read_u16(file)? as usize;
  let mut buffer = Vec::with_capacity(len);
  for _ in 0..len {
    buffer.push(0u8);
  }
  file.read_exact(&mut buffer)?;

  serde_cbor::from_reader(&*buffer).map_err(|e| e.into())
}

pub fn read_cbor<T: DeserializeOwned, R: Read>(file: &mut R, len: usize) -> Result<T> {
  let mut buffer = Vec::with_capacity(len);
  for _ in 0..len {
    buffer.push(0u8);
  }
  file.read_exact(&mut buffer)?;

  match serde_cbor::from_slice(&*buffer) {
    Ok(v) => Ok(v),
    Err(e) => {
      match serde_cbor::from_slice::<Value>(&*&buffer) {
        Ok(v) => {
          info!("Deserialized value {:?}", v);
        }
        Err(_) => {}
      }
      bail!("Failed to deserialize with error {}", e);
    }
  }
}

pub fn write_int<W: Write>(target: &mut W, val: u32) -> Result<()> {
  target.write_all(&val.to_be_bytes())?;
  Ok(())
}

pub fn write_short<W: Write>(target: &mut W, val: u16) -> Result<()> {
  target.write_all(&val.to_be_bytes())?;
  Ok(())
}

pub fn write_short_signed<W: Write>(target: &mut W, val: i16) -> Result<()> {
  target.write_all(&val.to_be_bytes())?;
  Ok(())
}

pub fn write_long<W: Write>(target: &mut W, val: u64) -> Result<()> {
  target.write_all(&val.to_be_bytes())?;
  Ok(())
}

pub fn write_envelope<W: Write, T: Serialize>(target: &mut W, envelope: &T) -> Result<()> {
  let bytes = serde_cbor::to_vec(envelope)?;
  write_short(target, bytes.len() as u16)?;
  target.write_all(&bytes)?;
  Ok(())
}

pub fn write_envelope_and_payload<W: Write, T: Serialize, T2: Serialize>(
  target: &mut W,
  envelope: &T,
  payload: &T2,
) -> Result<()> {
  let env_bytes = serde_cbor::to_vec(envelope)?;
  let payload_bytes = serde_cbor::to_vec(payload)?;
  write_short(target, env_bytes.len() as u16)?;
  write_int(target, payload_bytes.len() as u32)?;
  target.write_all(&env_bytes)?;
  target.write_all(&payload_bytes)?;
  Ok(())
}

pub fn traverse_value(v: &Value, path: Vec<&str>) -> Option<Value> {
  let mut first = v;
  for p in path {
    match first {
      Value::Map(m) => match m.get(&Value::Text(p.to_string())) {
        Some(v) => first = v,
        None => return None,
      },
      _ => return None,
    }
  }
  Some(first.clone())
}

pub fn as_obj<'a>(v: &'a Value) -> Option<&'a BTreeMap<Value, Value>> {
  match v {
    Value::Map(m) => Some(m),
    _ => None,
  }
}

pub fn as_array<'a>(v: &'a Value) -> Option<&'a Vec<Value>> {
  match v {
    Value::Array(m) => Some(m),
    _ => None,
  }
}

pub fn as_str<'a>(v: &'a Value) -> Option<&'a String> {
  match v {
    Value::Text(s) => Some(s),
    _ => None,
  }
}

pub fn cbor_to_json_str<T: Serialize>(input: &T) -> Result<String> {
  let json: serde_json::Value = serde_json::to_value(input)?;
  let ret = serde_json::to_string_pretty(&json)?;
  Ok(ret)
}

pub struct NiceDurationDisplay {
  pub d: u64,
  pub h: u64,
  pub m: u64,
  pub s: u64,
}
// impl Into<NiceDurationDisplay> for Duration {
//     fn into(self) -> NiceDurationDisplay {
//       NiceDurationDisplay {
//         d: self.num_days(),
//         h: self.num_hours() % 24,
//         m: self.num_minutes() % 60,
//         s: self.num_seconds() % 60,
//       }
//     }
// }
impl From<Duration> for NiceDurationDisplay {
  fn from(value: Duration) -> Self {
    let secs = value.as_secs();
    NiceDurationDisplay {
      d: secs / (24 * 60 * 60),
      h: (secs / (60 * 60)) % 24,
      m: (secs / 60) % 60,
      s: secs % 60,
    }
  }
}

impl From<f64> for NiceDurationDisplay {
  fn from(value: f64) -> Self {
    let secs = value as u64;
    NiceDurationDisplay {
      d: secs / (24 * 60 * 60),
      h: (secs / (60 * 60)) % 24,
      m: (secs / 60) % 60,
      s: secs % 60,
    }
  }
}

impl std::fmt::Display for NiceDurationDisplay {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    if self.d == 0 {
      if self.h == 0 {
        if self.m == 0 {
          write!(f, "{}s", self.s)
        } else {
          write!(f, "{}m{}s", self.m, self.s)
        }
      } else {
        write!(f, "{}h{}m{}s", self.h, self.m, self.s)
      }
    } else {
      write!(f, "{}d{}h{}m{}s", self.d, self.h, self.m, self.d)
    }
  }
}
