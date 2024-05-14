use crate::index::{EntryOffset, MD5Hash};
use anyhow::{bail, Result};

use serde::de::DeserializeOwned;
use serde_cbor::Value;
use serde_json::json;
use sha2::Sha256;
use std::{
    collections::{BTreeMap, HashMap},
    io::Read,
};

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
        bail!("The byte slice for a 64 bit number must have at least 8 bytes, this has {} bytes", it.len());
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

pub fn find_entry(to_find: [u8; 16], offsets: &[EntryOffset]) -> Option<EntryOffset> {
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

pub fn sha256_for_reader<R: Read>(r: &mut R) -> Result<[u8; 32]> {
    use sha2::Digest;
    // create a Sha256 object
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 4096];
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
    )
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
    let len = r.read(&mut buf)?;
    if len != buf.len() {
        bail!("Tried to read {} and only read {}", buf.len(), len);
    }
    Ok(u16::from_be_bytes(buf))
}

pub fn read_u32<R: Read>(r: &mut R) -> Result<u32> {
    let mut buf = [0u8; 4];
    let len = r.read(&mut buf)?;
    if len != buf.len() {
        bail!("Tried to read {} and only read {}", buf.len(), len);
    }
    Ok(u32::from_be_bytes(buf))
}

pub fn read_u64<R: Read>(r: &mut R) -> Result<u64> {
    let mut buf = [0u8; 8];
    let len = r.read(&mut buf)?;
    if len != buf.len() {
        bail!("Tried to read {} and only read {}", buf.len(), len);
    }
    Ok(u64::from_be_bytes(buf))
}

pub fn read_len_and_cbor<T: DeserializeOwned, R: Read>(file: &mut R) -> Result<T> {
    let len = read_u16(file)? as usize;
    let mut buffer = Vec::with_capacity(len);
    for _ in 0..len {
        buffer.push(0u8);
    }
    let read_len = file.read(&mut buffer)?;
    if read_len != len {
        bail!("Wanted to read {} bytes, but only got {}", len, read_len);
    }

    serde_cbor::from_reader(&*buffer).map_err(|e| e.into())
}

pub fn read_cbor<T: DeserializeOwned, R: Read>(file: &mut R, len: usize) -> Result<T> {
    let mut buffer = Vec::with_capacity(len);
    for _ in 0..len {
        buffer.push(0u8);
    }
    let read_len = file.read(&mut buffer)?;
    if read_len != len {
        bail!("Wanted to read {} bytes, but only got {}", len, read_len);
    }

    serde_cbor::from_reader(&*buffer).map_err(|e| e.into())
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

pub fn cbor_to_json(input: &Value) -> serde_json::Value {
    match input {
        Value::Null => serde_json::Value::Null,
        Value::Bool(v) => serde_json::Value::Bool(*v),
        Value::Integer(n) => json!(n),
        Value::Float(f) => json!(f),
        Value::Bytes(b) => json!(b),
        Value::Text(s) => json!(s),
        Value::Array(a) => {
            let mut ra = vec![];
            for v in a {
                ra.push(cbor_to_json(v))
            }
            json!(ra)
        }
        Value::Map(m) => {
            let mut ma: HashMap<String, serde_json::Value> = HashMap::new();
            for (k, v) in m {
                match k {
                    Value::Text(s) => {
                        ma.insert(s.to_string(), cbor_to_json(v));
                    }
                    _ => todo!("Maybe we should handle other cases {:?}", k),
                }
            }
            json!(ma)
        }
        Value::Tag(_, _) => todo!(),
        _ => serde_json::Value::Null,
    }
}

pub fn cbor_to_json_str(input: &Value) -> String {
    let json: serde_json::Value = cbor_to_json(input);
    serde_json::to_string_pretty(&json).unwrap() // FIXME is this smart?
}
