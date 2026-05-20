//! # Utility Functions
//!
//! This module provides shared utility functions used throughout BigTent,
//! organized into several categories:
//!
//! ## Hashing Functions
//! - [`md5hash_str`] - Compute MD5 hash of a string (used for index keys)
//! - [`sha256_for_slice`], [`sha256_for_reader`] - SHA256 hashing (file integrity)
//! - [`hex_to_md5bytes`], [`hex_to_u64`] - Parse hex strings to bytes
//!
//! ## Binary I/O
//! - [`read_u32`], [`read_u32_sync`] - Read 32-bit integers
//! - [`write_int`], [`write_long`] - Write integers in various sizes
//! - [`read_len_and_cbor`], [`write_envelope`] - CBOR envelope handling
//!
//! ## Path Utilities
//! - [`find_common_root_dir`] - Find common parent directory
//! - [`path_plus_timed`] - Generate timestamped filenames
//! - [`is_child_dir`] - Check path containment
//!
//! ## Time Utilities
//! - [`iso8601_now`] - Current time in ISO 8601 format
//! - [`NiceDurationDisplay`] - Human-readable duration formatting
//!
//! ## CBOR Utilities
//! - [`traverse_value`] - Walk CBOR value trees
//! - [`read_cbor_sync`] - Deserialize CBOR from bytes

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use serde_cbor::Value;
use std::{
    collections::{BTreeMap, HashSet},
    ffi::OsStr,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(not(test))]
use tracing::info;

#[cfg(test)]
use std::println as info;

/// Buffer size for streaming hash operations (4 KB)
const BYTE_BUFFER_SIZE: usize = 4096;

/// Parse a hex string into an MD5 hash (16 bytes).
///
/// # Arguments
/// * `it` - A 32-character hex string representing the MD5 hash
///
/// # Returns
/// * `Some(MD5Hash)` - The parsed 16-byte hash
/// * `None` - If the string is not valid hex or too short
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

/// Parse a hex string into a u64.
///
/// # Arguments
/// * `it` - A 16-character hex string representing the u64
///
/// # Returns
/// * `Some(u64)` - The parsed value (big-endian)
/// * `None` - If the string is not valid hex or too short
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

/// Convert the first 8 bytes of a slice to a u63 (63-bit unsigned integer).
///
/// The high bit is masked off to ensure the result fits in a signed i64
/// when needed for compatibility with systems that don't support u64.
///
/// # Arguments
/// * `it` - A byte slice with at least 8 bytes
///
/// # Returns
/// * `Ok(u64)` - The 63-bit value (high bit always 0)
/// * `Err` - If the slice has fewer than 8 bytes
pub fn byte_slice_to_u63(it: &[u8]) -> Result<u64> {
    let mut buff = [0u8; 8];
    if it.len() < 8 {
        bail!(
            "The byte slice for a 64 bit number must have at least 8 bytes, this has {} bytes",
            it.len()
        );
    }

    buff.copy_from_slice(&it[..8]);

    // Mask off high bit to get 63-bit value
    Ok(u64::from_be_bytes(buff) & 0x7fffffffffffffff)
}

/// Check if a string is a valid MD5 hash and parse it.
///
/// Handles both raw 32-char hex and filenames like "abc123...def.json".
pub fn check_md5(it: Option<&String>) -> Option<MD5Hash> {
    match it {
        Some(v) if v.len() == 32 => hex_to_md5bytes(v),
        Some(v) if v.len() == 37 && v.ends_with(".json") => hex_to_md5bytes(&v[0..32]),
        _ => None,
    }
}

/// Get current time as milliseconds since Unix epoch.
pub fn millis_now() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Compute MD5 hash of a string for use as a primary `.gri` index key.
///
/// Goatrodeo's v4 writer (and BigTent, going forward) keys index entries
/// by MD5 of the *binary* `Identifier` form (header byte + raw hash
/// bytes for gitoid/raw-hash, header byte + UTF-8 bytes for
/// PackageUrl/sentinel). See [`identifier_bytes`] for the layout.
///
/// MD5 is used for lookup efficiency, not security; data integrity
/// uses SHA256.
pub fn md5hash_str(st: &str) -> MD5Hash {
    let bytes = identifier_bytes(st);
    let res = md5::compute(&bytes);
    res.into()
}

/// Legacy MD5 hash form — MD5 of the canonical UTF-8 string.
///
/// Pre-v4 goatrodeo (and earlier BigTent writers) keyed `.gri` entries
/// by this. Kept for backward-compatible lookups against existing v1
/// clusters, used as a fallback when the primary [`md5hash_str`] lookup
/// misses.
pub fn md5hash_str_canonical(st: &str) -> MD5Hash {
    let res = md5::compute(st);
    res.into()
}

/// Encode a canonical-form identifier string into the binary representation
/// that goatrodeo's `Identifier` opaque type stores on disk.
///
/// Layout — byte 0 packs the kind into the high nibble and a kind-specific
/// sub-encoding into the low nibble:
///
/// ```text
///   high nibble (bits 7..4)        low nibble (bits 3..0)
///   0 = Gitoid                     (ObjectType.ordinal << 2) | HashAlgorithm.ordinal
///   1 = RawHash                    RawHashAlgorithm.ordinal
///   2 = PackageUrl                 0
///   3 = Sentinel                   0
/// ```
///
/// Bytes 1.. are the raw hash bytes (for Gitoid / RawHash) or the UTF-8
/// bytes of the canonical text form (for PackageUrl / Sentinel).
///
/// Object-type ordinals: Blob=0, Tree=1, Commit=2, Tag=3.
/// HashAlgorithm ordinals (Gitoid): Sha1=0, Sha256=1.
/// RawHashAlgorithm ordinals: Md5=0, Sha1=1, Sha256=2, Sha512=3.
///
/// Falls back to the sentinel encoding for malformed gitoid / raw-hash
/// strings, matching goatrodeo's tolerance for short test fixtures.
pub fn identifier_bytes(canonical: &str) -> Vec<u8> {
    const KIND_GITOID: u8 = 0;
    const KIND_RAW_HASH: u8 = 1;
    const KIND_PACKAGE_URL: u8 = 2;
    const KIND_SENTINEL: u8 = 3;

    fn header(kind: u8, sub: u8) -> u8 {
        ((kind & 0xf) << 4) | (sub & 0xf)
    }

    fn sentinel(s: &str) -> Vec<u8> {
        let utf8 = s.as_bytes();
        let mut out = Vec::with_capacity(1 + utf8.len());
        out.push(header(KIND_SENTINEL, 0));
        out.extend_from_slice(utf8);
        out
    }

    fn hex_to_bytes(hex: &str, out: &mut Vec<u8>) -> bool {
        if !hex.len().is_multiple_of(2) {
            return false;
        }
        for chunk in hex.as_bytes().chunks(2) {
            let hi = match (chunk[0] as char).to_digit(16) {
                Some(v) => v as u8,
                None => return false,
            };
            let lo = match (chunk[1] as char).to_digit(16) {
                Some(v) => v as u8,
                None => return false,
            };
            out.push((hi << 4) | lo);
        }
        true
    }

    // gitoid:<objectType>:<algo>:<hex>
    if let Some(rest) = canonical.strip_prefix("gitoid:") {
        let parts: Vec<&str> = rest.splitn(3, ':').collect();
        if parts.len() == 3 {
            let object_ordinal: Option<u8> = match parts[0] {
                "blob" => Some(0),
                "tree" => Some(1),
                "commit" => Some(2),
                "tag" => Some(3),
                _ => None,
            };
            let algo: Option<(u8, usize)> = match parts[1] {
                "sha1" => Some((0, 20)),
                "sha256" => Some((1, 32)),
                _ => None,
            };
            if let (Some(obj), Some((algo_ord, byte_len))) = (object_ordinal, algo) {
                if parts[2].len() == byte_len * 2 {
                    let mut out = Vec::with_capacity(1 + byte_len);
                    out.push(header(KIND_GITOID, ((obj & 0x3) << 2) | (algo_ord & 0x3)));
                    if hex_to_bytes(parts[2], &mut out) {
                        return out;
                    }
                }
            }
        }
        return sentinel(canonical);
    }

    // pkg:...
    if let Some(_rest) = canonical.strip_prefix("pkg:") {
        let utf8 = canonical.as_bytes();
        let mut out = Vec::with_capacity(1 + utf8.len());
        out.push(header(KIND_PACKAGE_URL, 0));
        out.extend_from_slice(utf8);
        return out;
    }

    // <algo>:<hex>
    let raw_algo: Option<(&str, u8, usize)> = if canonical.starts_with("md5:") {
        Some(("md5:", 0, 16))
    } else if canonical.starts_with("sha1:") {
        Some(("sha1:", 1, 20))
    } else if canonical.starts_with("sha256:") {
        Some(("sha256:", 2, 32))
    } else if canonical.starts_with("sha512:") {
        Some(("sha512:", 3, 64))
    } else {
        None
    };
    if let Some((prefix, algo_ord, byte_len)) = raw_algo {
        let hex = &canonical[prefix.len()..];
        if hex.len() == byte_len * 2 {
            let mut out = Vec::with_capacity(1 + byte_len);
            out.push(header(KIND_RAW_HASH, algo_ord));
            if hex_to_bytes(hex, &mut out) {
                return out;
            }
        }
        return sentinel(canonical);
    }

    sentinel(canonical)
}

/// Compute SHA256 hash of a byte slice.
///
/// Returns a 32-byte hash used for file integrity and content addressing.
pub fn sha256_for_slice(r: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();

    hasher.update(r);

    hasher.finalize().into()
}

#[test]
fn test_md5hash_str_matches_goatrodeo_identifier_form() {
    // Pinned against the index files goatrodeo's v4 writer produces.
    // The MD5 keys cover the binary Identifier layout, not the canonical
    // text form — see identifier_bytes for the layout.
    let cases: &[(&str, &str)] = &[
        (
            "gitoid:blob:sha256:bac938b7bc5d3af6ffc1e83d3ae26219d10660315f6f5481d97251bbfc8560a6",
            "d58fa99dbbb64eac9ed11ff56440cec2",
        ),
        (
            "gitoid:blob:sha256:4ffa7795396697e3de38d20cae12be8d81b6da1cebb61c214a332747e726ba86",
            "b68691f79d534a3086626d83e0675c97",
        ),
        ("tags", "7d71edd9af3c27c8fae771e8811d524e"),
    ];
    for (id, expected) in cases {
        let bytes = md5hash_str(id);
        let hex: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();
        assert_eq!(
            hex, *expected,
            "md5hash_str({}) produced {} but expected {}",
            id, hex, expected
        );
    }
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

pub async fn sha256_for_reader<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};
    // create a Sha256 object
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; BYTE_BUFFER_SIZE];
    loop {
        let read = r.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[0..read]);
    }
    Ok(hasher.finalize().into())
}

pub fn sha256_for_reader_sync<R: Read>(r: &mut R) -> Result<[u8; 32]> {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_sha256() {
    use hex_literal::hex;
    let mut to_hash: &[u8] = b"hello world";
    let res = sha256_for_reader(&mut to_hash).await.unwrap();
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

pub fn iso8601_now() -> String {
    let dt: DateTime<Utc> = SystemTime::now().into();
    format!("{}", dt.format("%+"))
    // formats like "2001-07-08T00:34:60.026490+09:30"
}

pub fn current_date_string() -> String {
    format!("{:?}", chrono::offset::Utc::now())
}

pub fn is_child_dir(root: &Path, potential_child: &PathBuf) -> Result<bool> {
    let full_root = root.canonicalize()?;

    let full_kid = potential_child
        .canonicalize()
        .with_context(|| format!("child directory {:?}", potential_child))?;

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
        root = root.intersection(&j.1).cloned().collect();
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

// #[test]
// fn test_parents() {
//   assert_eq!(
//     find_common_root_dir(vec![PathBuf::from("."), PathBuf::from("./..")]).unwrap(),
//     PathBuf::from("./..").canonicalize().unwrap()
//   );
//   assert_eq!(
//     find_common_root_dir(vec![PathBuf::from("."), PathBuf::from("./../..")]).unwrap(),
//     PathBuf::from("./../..").canonicalize().unwrap()
//   );
//   assert!(find_common_root_dir(vec![PathBuf::from("."), PathBuf::from("/tmp")]).is_err());
// }

pub fn os_str_to_string(oss: &OsStr) -> Result<String> {
    match oss.to_str() {
        Some(s) => Ok(s.to_string()),
        None => bail!("Unable to convert {:?} to a String", oss),
    }
}

pub fn path_plus_timed(root: &Path, suffix: &str) -> PathBuf {
    let mut ret = root.to_path_buf();
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

pub async fn read_all<R: AsyncReadExt + Unpin>(r: &mut R, max: usize) -> Result<Vec<u8>> {
    let mut ret = vec![];
    let mut buf = [0u8; BYTE_BUFFER_SIZE];
    let mut read: usize = 0;
    loop {
        let to_read = BYTE_BUFFER_SIZE.min(max - read);
        if to_read == 0 {
            break;
        }
        match r.read(&mut buf[0..to_read]).await {
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

pub async fn read_u16<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<u16> {
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf).await?;
    Ok(u16::from_be_bytes(buf))
}

pub fn read_u16_sync<R: Read>(r: &mut R) -> Result<u16> {
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf)?;
    Ok(u16::from_be_bytes(buf))
}

pub async fn read_u32<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf).await?;

    Ok(u32::from_be_bytes(buf))
}

pub fn read_u32_sync<R: Read>(r: &mut R) -> Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;

    Ok(u32::from_be_bytes(buf))
}

pub async fn read_u64<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<u64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf).await?;

    Ok(u64::from_be_bytes(buf))
}

pub async fn read_len_and_cbor<T: DeserializeOwned, R: AsyncReadExt + Unpin>(
    file: &mut R,
) -> Result<T> {
    let len = read_u16(file).await? as usize;
    let mut buffer = vec![0u8; len];

    file.read_exact(&mut buffer).await?;

    serde_cbor::from_reader(&*buffer).map_err(|e| e.into())
}

pub fn read_len_and_cbor_sync<T: DeserializeOwned, R: Read>(file: &mut R) -> Result<T> {
    let len = read_u16_sync(file)? as usize;
    let mut buffer = vec![0u8; len];

    file.read_exact(&mut buffer)?;

    serde_cbor::from_reader(&*buffer).map_err(|e| e.into())
}

pub async fn read_cbor<T: DeserializeOwned, R: AsyncReadExt + Unpin>(
    file: &mut R,
    len: usize,
) -> Result<T> {
    let mut buffer = vec![0u8; len];

    file.read_exact(&mut buffer).await?;

    match serde_cbor::from_slice(&buffer) {
        Ok(v) => Ok(v),
        Err(e) => {
            match serde_cbor::from_slice::<Value>(&buffer) {
                Ok(v) => {
                    info!("Deserialized value {:?} but got error {}", v, e);
                }
                Err(e2) => {
                    info!(
                        "Failed to do basic deserialization of {} with errors e {} and e2 {}",
                        unsafe { String::from_utf8_unchecked(buffer) },
                        e,
                        e2
                    )
                }
            }
            bail!("Failed to deserialize with error {}", e);
        }
    }
}

pub fn read_cbor_sync<T: DeserializeOwned, R: Read + Unpin>(file: &mut R, len: usize) -> Result<T> {
    let mut buffer = vec![0u8; len];

    file.read_exact(&mut buffer)?;

    match serde_cbor::from_slice(&buffer) {
        Ok(v) => Ok(v),
        Err(e) => {
            match serde_cbor::from_slice::<Value>(&buffer) {
                Ok(v) => {
                    info!("Deserialized value {:?} but got error {}", v, e);
                }
                Err(e2) => {
                    info!(
                        "Failed to do basic deserialization of {} with errors e {} and e2 {}",
                        unsafe { String::from_utf8_unchecked(buffer) },
                        e,
                        e2
                    )
                }
            }
            bail!("Failed to deserialize with error {}", e);
        }
    }
}

pub async fn write_int<W: AsyncWriteExt + Unpin>(target: &mut W, val: u32) -> Result<()> {
    target.write_all(&val.to_be_bytes()).await?;
    Ok(())
}

pub async fn write_short<W: AsyncWriteExt + Unpin>(target: &mut W, val: u16) -> Result<()> {
    target.write_all(&val.to_be_bytes()).await?;
    Ok(())
}

pub async fn write_short_signed<W: AsyncWriteExt + Unpin>(target: &mut W, val: i16) -> Result<()> {
    target.write_all(&val.to_be_bytes()).await?;
    Ok(())
}

pub async fn write_long<W: AsyncWriteExt + Unpin>(target: &mut W, val: u64) -> Result<()> {
    target.write_all(&val.to_be_bytes()).await?;
    Ok(())
}

pub fn write_usize_sync<W: Write>(target: &mut W, val: usize) -> Result<()> {
    target.write_all(&val.to_be_bytes())?;
    Ok(())
}

pub async fn write_envelope<W: AsyncWriteExt + Unpin, T: Serialize>(
    target: &mut W,
    envelope: &T,
) -> Result<()> {
    let bytes = serde_cbor::to_vec(envelope)?;
    write_short(target, bytes.len() as u16).await?;
    target.write_all(&bytes).await?;
    Ok(())
}

pub async fn write_envelope_and_payload<W: AsyncWriteExt + Unpin, T: Serialize, T2: Serialize>(
    target: &mut W,
    envelope: &T,
    payload: &T2,
) -> Result<()> {
    let env_bytes = serde_cbor::to_vec(envelope)?;
    let payload_bytes = serde_cbor::to_vec(payload)?;
    write_short(target, env_bytes.len() as u16).await?;
    write_int(target, payload_bytes.len() as u32).await?;
    target.write_all(&env_bytes).await?;
    target.write_all(&payload_bytes).await?;
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

pub fn as_obj(v: &Value) -> Option<&BTreeMap<Value, Value>> {
    match v {
        Value::Map(m) => Some(m),
        _ => None,
    }
}

pub fn as_array(v: &Value) -> Option<&Vec<Value>> {
    match v {
        Value::Array(m) => Some(m),
        _ => None,
    }
}

pub fn as_str(v: &Value) -> Option<&String> {
    match v {
        Value::Text(s) => Some(s),
        _ => None,
    }
}

pub struct NiceDurationDisplay {
    pub d: u64,
    pub h: u64,
    pub m: u64,
    pub s: u64,
}

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

/// A 16-byte MD5 hash used as index keys.
///
/// MD5 is used for index lookups due to its compact size (16 bytes vs 32 for SHA256).
/// This is purely for efficiency - data integrity uses SHA256.
///
/// Note: MD5 is cryptographically broken and should never be used for security.
/// Here it's used only as a fast hash function for identifier lookup.
pub type MD5Hash = [u8; 16];
