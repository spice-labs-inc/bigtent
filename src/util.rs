//! # Utility Functions
//!
//! This module provides shared utility functions used throughout BigTent,
//! organized into several categories:
//!
//! ## Hashing Functions
//! - [`md5hash_str`], [`blake3hash_str`] - Index key derivation (version 3
//!   and version 4 key spaces)
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
//! - [`read_cbor_sync`] - Deserialize CBOR from bytes

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use serde_cbor::Value;
use std::{
    collections::HashSet,
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

/// Parse a hex string into a 16-byte index key.
///
/// # Arguments
/// * `it` - A 32-character hex string representing the key
///
/// # Returns
/// * `Some(KeyHash)` - The parsed 16-byte key
/// * `None` - If the string is not valid hex or too short
pub fn hex_to_md5bytes(it: &str) -> Option<KeyHash> {
    hex::decode(it)
        .map(|bytes| {
            if bytes.len() < std::mem::size_of::<KeyHash>() {
                None
            } else {
                let (int_bytes, _) = bytes.split_at(std::mem::size_of::<KeyHash>());
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

/// Compute MD5 hash of a string.
///
/// Used for index key generation. Note: MD5 is used for lookup efficiency,
/// not security. Data integrity uses SHA256.
pub fn md5hash_str(st: &str) -> KeyHash {
    let res = md5::compute(st);

    res.into()
}

/// Compute the version 4 index key: BLAKE3 over the UTF-8 bytes of the
/// identifier, truncated to the first 16 bytes of the 32-byte digest.
///
/// Compared as unsigned byte strings, with the existing big-endian binary
/// search unchanged (ADR 0002). Data integrity uses SHA256; BLAKE3 here is
/// only the index key derivation.
pub fn blake3hash_str(st: &str) -> KeyHash {
    let digest = blake3::hash(st.as_bytes());
    let mut key = [0u8; 16];
    key.copy_from_slice(&digest.as_bytes()[0..16]);
    key
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
                        escape_bytes_for_log(&buffer),
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
                        escape_bytes_for_log(&buffer),
                        e,
                        e2
                    )
                }
            }
            bail!("Failed to deserialize with error {}", e);
        }
    }
}

/// Render raw bytes safely for a log line.
///
/// Every byte outside printable ASCII (0x20..=0x7e) is hex-escaped as
/// `\xNN`, so attacker-controlled payload bytes cannot forge log lines
/// with control characters (newlines, backspaces) or invalid UTF-8
/// sequences. Printable ASCII is passed through readably. This replaces
/// the former `String::from_utf8_unchecked` diagnostic, which was
/// undefined behavior on non-UTF-8 payloads (phase 1, H1).
fn escape_bytes_for_log(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 4);
    for &b in bytes {
        if (0x20..=0x7e).contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("\\x{:02x}", b));
        }
    }
    out
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

/// A 16-byte index key (the output of a key-space algorithm).
///
/// The 16-byte width keeps the on-disk index entry (16 key + 8 data-file
/// hash + 8 offset = 32 bytes) and the big-endian binary-search rules
/// unchanged across algorithms (ADR 0002).
///
/// Data integrity uses SHA256; this type is only the identifier index key.
/// It is not used for security.
pub type KeyHash = [u8; 16];

/// The key-space algorithm used by a file format version.
///
/// Version 3 clusters use MD5 keys; version 4 clusters use BLAKE3
/// truncated to the first 16 bytes of the 32-byte digest (ADR 0002).
/// The algorithm a reader uses is the one the cluster's files declare.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyAlg {
    /// MD5 over the identifier (version 3 key space).
    Md5,
    /// BLAKE3 truncated to the first 16 bytes (version 4 key space).
    Blake3Truncated128,
}

impl KeyAlg {
    /// Hash an identifier into this algorithm's 16-byte key space.
    ///
    /// Both algorithms consume the UTF-8 bytes of the identifier and are
    /// deterministic for a given input.
    pub fn hash_identifier(&self, identifier: &str) -> KeyHash {
        match self {
            KeyAlg::Md5 => md5hash_str(identifier),
            KeyAlg::Blake3Truncated128 => blake3hash_str(identifier),
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 1 tests (plan: plans/2026_09_16_connection_map_and_blake3/
// phase_1_hash_primitives_and_removals.md).
//
// Requirement provenance and test theory are documented per project rule 3.
// ---------------------------------------------------------------------------

/// Requirement: phase 1, deliverable 1 (D2).
///
/// What this test tests: the `KeyAlg::Blake3Truncated128` key derivation is
/// unkeyed BLAKE3 over the UTF-8 bytes of the identifier, truncated to the
/// first 16 bytes (128 bits) of the 32-byte digest.
///
/// Why this test tests it: it uses the official BLAKE3 test vectors
/// (BLAKE3-team/BLAKE3 test_vectors.json) at four input lengths — empty,
/// short (1 byte), one-block boundary (64 bytes), and multi-block (128
/// bytes) — and asserts the first 16 bytes of the official digest. Multiple
/// lengths are required because a single short input cannot distinguish
/// truncated BLAKE3 from BLAKE2 or a folded digest; the block and multi-block
/// cases pin the construction across BLAKE3's chunk structure. The official
/// vector inputs are the repeating byte pattern 0, 1, 2, ...; for lengths
/// up to 128 every byte is ASCII and therefore a valid UTF-8 `String`, so
/// the vectors exercise the same UTF-8 path production identifiers use.
#[test]
fn test_blake3_known_answer_vectors() {
    use crate::util::KeyAlg;
    use hex_literal::hex;

    // Official BLAKE3 test vectors: input is bytes [0, 1, ..., len-1]
    // (the 251-byte repeating pattern starts with 0,1,2,... which for
    // len <= 128 is exactly the byte sequence itself, all valid UTF-8).
    let cases: Vec<(usize, [u8; 16])> = vec![
        // input_len 0: digest af1349b9... first 16 bytes
        (0, hex!("af1349b9f5f9a1a6a0404dea36dcc949")),
        // input_len 1: input [0x00]; digest 2d3adedf... first 16 bytes
        (1, hex!("2d3adedff11b61f14c886e35afa03673")),
        // input_len 64 (one block); digest 4eed7141... first 16 bytes
        (
            64,
            hex!("4eed7141ea4a5cd4b788606bd23f46e2"),
        ),
        // input_len 128 (two blocks); digest f17e5705... first 16 bytes
        (
            128,
            hex!("f17e570564b26578c33bb7f44643f539"),
        ),
    ];

    for (len, expected) in cases {
        let input_bytes: Vec<u8> = (0..len as u8).collect();
        let input = String::from_utf8(input_bytes).expect("vector input is valid UTF-8");
        let key = KeyAlg::Blake3Truncated128.hash_identifier(&input);
        assert_eq!(
            key, expected,
            "BLAKE3[0..16] KAT mismatch at input length {}",
            len
        );
    }
}

/// Requirement: phase 1, deliverable 1 (D2).
///
/// What this test tests: `KeyAlg::Md5` dispatch still produces the standard
/// MD5 digest for the version 3 key space.
///
/// Why this test tests it: version 3 clusters keep MD5 keys, so the MD5
/// path must be provably unchanged. A fixed known-answer vector (MD5 of
/// "abc", the classic RFC 1321-adjacent reference digest) proves the
/// dispatch selects MD5 and that the primitive is untouched by the
/// introduction of the BLAKE3 path.
#[test]
fn test_md5_known_answer_vector() {
    use crate::util::KeyAlg;
    use hex_literal::hex;

    let key = KeyAlg::Md5.hash_identifier("abc");
    assert_eq!(key, hex!("900150983cd24fb0d6963f7d28e17f72"));
}

/// Requirement: phase 1, H1 (owner-accepted).
///
/// What this test tests: rendering raw payload bytes for a log line is safe
/// — control characters (including newlines) and non-printable bytes are
/// hex-escaped so attacker-controlled bytes cannot forge log lines, and the
/// CBOR error path never converts non-UTF-8 bytes with
/// `String::from_utf8_unchecked` (undefined behavior).
///
/// Why this test tests it: the pre-phase-1 code logged raw payload bytes
/// with `String::from_utf8_unchecked`, which is UB on non-UTF-8 input. The
/// substantive property of the fix is that no byte outside printable ASCII
/// is ever emitted raw. The helper is asserted directly over a crafted
/// payload, and `read_cbor_sync` is driven with a crafted non-UTF-8,
/// non-CBOR buffer to prove the failure path returns an error rather than
/// panicking or invoking UB. Miri, where available, checks the UB
/// elimination; the execution state records whether Miri ran.
#[test]
fn test_cbor_error_logging_does_not_panic_on_non_utf8() {
    use crate::util::escape_bytes_for_log;

    // Crafted payload: valid neither as CBOR nor as UTF-8, with control
    // characters and non-ASCII bytes a log-forger would want to abuse.
    let payload: Vec<u8> = vec![0xbf, 0x78, 0xff, 0xfe, 0x0a, 0x01, 0x1b];

    // The escaped rendering contains no raw control/non-printable bytes
    // and explicitly escapes the ones we injected.
    let rendered = escape_bytes_for_log(&payload);
    for b in rendered.bytes() {
        assert!(
            (0x20..0x7f).contains(&b),
            "log rendering emitted raw byte {:#04x}",
            b
        );
    }
    assert!(rendered.contains("\\xbf"));
    assert!(rendered.contains("\\x0a"), "newline must be escaped");

    // Drive the real failure path: the typed parse and the generic parse
    // both fail on this buffer, so the (formerly unsafe) diagnostic branch
    // runs. It must return an Err, not panic and not invoke UB.
    let buffer: &[u8] = &payload;
    let result: Result<String> = crate::util::read_cbor_sync(&mut &buffer[..], payload.len());
    assert!(result.is_err(), "crafted garbage must fail to deserialize");
}

/// Requirement: phase 1, deliverable 1 (D2).
///
/// What this test tests: hashing is deterministic — repeated hashing with
/// the same algorithm returns identical keys, and the two algorithms do not
/// return the same key for the same input.
///
/// Why this test tests it: index keys and file names are content-addressed,
/// so the same identifier must always produce the same key within an
/// algorithm. The cross-algorithm inequality is a sanity property of the
/// dispatch (MD5 and BLAKE3 are different functions). Distribution
/// properties are not asserted by tests; they follow from the BLAKE3 and
/// MD5 designs as recorded in ADR 0002.
#[cfg(test)]
mod prop_hash_determinism {
    use crate::util::{KeyAlg, blake3hash_str, md5hash_str};
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_hash_is_deterministic(s in ".*") {
            let m1 = KeyAlg::Md5.hash_identifier(&s);
            let m2 = KeyAlg::Md5.hash_identifier(&s);
            prop_assert_eq!(m1, m2);
            prop_assert_eq!(m1, md5hash_str(&s));

            let b1 = KeyAlg::Blake3Truncated128.hash_identifier(&s);
            let b2 = KeyAlg::Blake3Truncated128.hash_identifier(&s);
            prop_assert_eq!(b1, b2);
            prop_assert_eq!(b1, blake3hash_str(&s));

            prop_assert_ne!(m1, b1);
        }
    }
}
