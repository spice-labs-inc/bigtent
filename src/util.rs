use std::io::Read;
use anyhow::Result;
use crate::index::EntryOffset;

pub fn hex_to_u128(it: &str) -> Option<u128> {
    hex::decode(it)
        .map(|bytes| {
            if bytes.len() < std::mem::size_of::<u128>() {
                None
            } else {
                let (int_bytes, _) = bytes.split_at(std::mem::size_of::<u128>());
                let slice: [u8; 16] = int_bytes.try_into().ok()?;
                Some(u128::from_be_bytes(slice))
            }
        })
        .ok()
        .flatten()
}

pub fn check_md5(it: Option<&String>) -> Option<u128> {
    match it {
        Some(v) if v.len() == 32 => hex_to_u128(v),
        Some(v) if v.len() == 37 && v.ends_with(".json") => hex_to_u128(&v[0..32]),
        _ => None,
    }
}

pub fn find_entry(to_find: u128, offsets: &[EntryOffset]) -> Option<EntryOffset> {
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

pub fn md5hash_str(st: &str) -> u128 {
  let res = md5::compute(st);

  u128::from_be_bytes(res.into())
}

pub fn read_all<R: Read>(r: &mut R, max: usize) -> Result<Vec<u8>> {
  let mut ret = vec![];
  let mut buf: [u8; 4096] = [0u8; 4096];
  let mut read: usize = 0;
  loop {
    let to_read = 4096.min(max - read);
    if to_read <= 0 {
      break;
    }
    match r.read(&mut buf[0 .. to_read]) {
      Ok(0) => {break;}
      Ok(n) => {
        read += n;
        let mut to_str = vec![];
        to_str.extend_from_slice(&buf[..n]);
        let qq = &buf[..n];
        ret.extend_from_slice(qq);
      }
      Err(e) => return Err(e.into())
    }
  }

  Ok(ret)
}
