//! # `.gra` v2 sorted alias sidecar — mmap'd reader
//!
//! Pairs with goatrodeo's [`GraphManager.writeAliasMapFile`][grm] writer.
//!
//! The earlier `.gra` layout was a single CBOR-encoded
//! `TreeMap[String, TreeSet[String]]` that bigtent had to deserialise
//! into a `BTreeMap<String, BTreeSet<String>>` at cluster-open time.
//! For the ADG corpus that cost ~100 ms of CPU + ~180 MB of resident
//! set just to load the alias mapping. Scaled linearly with cluster
//! size and became infeasible at >100× the current corpus.
//!
//! v2 replaces the deserialisation with a sorted on-disk format that
//! supports binary search over the memory-mapped file:
//!
//! ```text
//! Header (32 B, big-endian):
//!   +0   magic              u32 = 0x0a11a5ed
//!   +4   version            u8  = 2
//!   +5   padding            u8 × 3
//!   +8   n_alt              u32
//!   +12  n_canon            u32
//!   +16  alt_index_off      u32   (byte offset within the file)
//!   +20  canon_index_off    u32
//!   +24  alt_list_off       u32
//!   +28  strings_off        u32
//!
//! alt_index (n_alt × 20 B, sorted by md5_alt):
//!   md5_alt (16) | canon_index_idx (u32)
//!
//! canon_index (n_canon × 30 B, sorted by md5_canon):
//!   md5_canon (16) | canon_str_off (u32) | canon_str_len (u16)
//!   | alt_list_off (u32) | alt_list_count (u32)
//!
//! alt_list (n_alt × 6 B):
//!   alt_str_off (u32) | alt_str_len (u16)
//!
//! strings: packed UTF-8 bytes.
//! ```
//!
//! `md5_alt` and `md5_canon` are MD5 of the *binary* `Identifier` form
//! (the same MD5 used by `.gri` index keys, see
//! [`crate::util::md5hash_str`]).
//!
//! [grm]: https://github.com/spice-labs-inc/goatrodeo/pull/266

use crate::util::{MD5Hash, md5hash_str};
use anyhow::{Result, bail};
use memmap2::Mmap;
use std::cmp::Ordering;
use std::sync::Arc;

/// Magic for an Aliased `.gra` sidecar. Matches goatrodeo's
/// [`GraphManager.Consts.AliasMapFileMagicNumber`].
pub const ALIAS_MAP_FILE_MAGIC: u32 = 0x0a11a5ed;

/// Layout version this reader supports.
pub const ALIAS_MAP_FILE_VERSION: u8 = 2;

/// Size of each `alt_index` entry.
const ALT_INDEX_ENTRY_BYTES: usize = 20;

/// Size of each `canon_index` entry.
const CANON_INDEX_ENTRY_BYTES: usize = 30;

/// Size of each `alt_list` entry.
const ALT_LIST_ENTRY_BYTES: usize = 6;

/// A memory-mapped `.gra` v2 alias sidecar. All lookups go through the
/// mmap'd file directly — no in-memory map, no deserialisation cost at
/// open time.
#[derive(Debug, Clone)]
pub struct AliasSidecar {
    file: Arc<Mmap>,
    n_alt: u32,
    n_canon: u32,
    alt_index_off: u32,
    canon_index_off: u32,
    alt_list_off: u32,
    strings_off: u32,
}

#[inline]
fn read_u32(slice: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(slice[offset..offset + 4].try_into().unwrap())
}

#[inline]
fn read_u16(slice: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(slice[offset..offset + 2].try_into().unwrap())
}

impl AliasSidecar {
    /// Open and validate a `.gra` v2 sidecar via mmap.
    pub fn from_mmap(file: Arc<Mmap>) -> Result<Self> {
        let bytes = &file[..];
        if bytes.len() < 32 {
            bail!("alias sidecar truncated: only {} bytes", bytes.len());
        }
        let magic = read_u32(bytes, 0);
        if magic != ALIAS_MAP_FILE_MAGIC {
            bail!(
                "Unexpected alias-sidecar magic {:08x}, expected {:08x}",
                magic,
                ALIAS_MAP_FILE_MAGIC
            );
        }
        let version = bytes[4];
        if version != ALIAS_MAP_FILE_VERSION {
            bail!(
                "Unsupported .gra version {}: this reader supports v{}",
                version,
                ALIAS_MAP_FILE_VERSION
            );
        }
        let n_alt = read_u32(bytes, 8);
        let n_canon = read_u32(bytes, 12);
        let alt_index_off = read_u32(bytes, 16);
        let canon_index_off = read_u32(bytes, 20);
        let alt_list_off = read_u32(bytes, 24);
        let strings_off = read_u32(bytes, 28);

        // Sanity-check section ranges fit within the file.
        let total = bytes.len() as u64;
        let alt_idx_end =
            alt_index_off as u64 + n_alt as u64 * ALT_INDEX_ENTRY_BYTES as u64;
        let canon_idx_end = canon_index_off as u64
            + n_canon as u64 * CANON_INDEX_ENTRY_BYTES as u64;
        let alt_list_end =
            alt_list_off as u64 + n_alt as u64 * ALT_LIST_ENTRY_BYTES as u64;
        if alt_idx_end > total
            || canon_idx_end > total
            || alt_list_end > total
            || strings_off as u64 > total
        {
            bail!(
                "alias sidecar section offsets exceed file size {} (alt_idx_end={}, canon_idx_end={}, alt_list_end={}, strings_off={})",
                total,
                alt_idx_end,
                canon_idx_end,
                alt_list_end,
                strings_off
            );
        }

        Ok(AliasSidecar {
            file,
            n_alt,
            n_canon,
            alt_index_off,
            canon_index_off,
            alt_list_off,
            strings_off,
        })
    }

    /// Number of (alternate, canonical) pairs in the sidecar.
    pub fn alt_count(&self) -> u32 {
        self.n_alt
    }

    /// Number of distinct canonicals in the sidecar.
    pub fn canon_count(&self) -> u32 {
        self.n_canon
    }

    /// Returns `true` if the sidecar is empty (no aliases).
    pub fn is_empty(&self) -> bool {
        self.n_alt == 0
    }

    /// Read a `(offset, length)` slice from the strings pool.
    fn read_string(&self, str_off: u32, str_len: u16) -> &str {
        let start = self.strings_off as usize + str_off as usize;
        let end = start + str_len as usize;
        // Strings were written from `String.getBytes("UTF-8")` on the
        // writer side; treat as UTF-8 here. Bail to empty on bad bytes
        // rather than panic — corrupt sidecar shouldn't crash the
        // server.
        std::str::from_utf8(&self.file[start..end]).unwrap_or("")
    }

    /// Binary-search `canon_index` for the entry with the given md5.
    /// Returns the entry offset (within the mmap) if found.
    fn find_canon_entry(&self, md5: &MD5Hash) -> Option<usize> {
        let mut lo: u32 = 0;
        let mut hi: u32 = self.n_canon;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_off =
                self.canon_index_off as usize + mid as usize * CANON_INDEX_ENTRY_BYTES;
            let entry_md5 = &self.file[entry_off..entry_off + 16];
            match entry_md5.cmp(&md5[..]) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Some(entry_off),
            }
        }
        None
    }

    /// Binary-search `alt_index` for the entry with the given md5.
    /// Returns the entry offset (within the mmap) if found.
    fn find_alt_entry(&self, md5: &MD5Hash) -> Option<usize> {
        let mut lo: u32 = 0;
        let mut hi: u32 = self.n_alt;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_off =
                self.alt_index_off as usize + mid as usize * ALT_INDEX_ENTRY_BYTES;
            let entry_md5 = &self.file[entry_off..entry_off + 16];
            match entry_md5.cmp(&md5[..]) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => return Some(entry_off),
            }
        }
        None
    }

    /// Read the canonical at `canon_index_idx` and return its string.
    fn canonical_at(&self, idx: u32) -> Option<&str> {
        if idx >= self.n_canon {
            return None;
        }
        let entry_off =
            self.canon_index_off as usize + idx as usize * CANON_INDEX_ENTRY_BYTES;
        let canon_off = read_u32(&self.file, entry_off + 16);
        let canon_len = read_u16(&self.file, entry_off + 20);
        Some(self.read_string(canon_off, canon_len))
    }

    /// Look up the canonical identifier for an alternate.
    ///
    /// Returns `None` if the alternate isn't in the sidecar. v1
    /// equivalent: `aliasInverseMap.get(alternate)`.
    pub fn lookup_canonical(&self, alternate: &str) -> Option<String> {
        let md5 = md5hash_str(alternate);
        let entry_off = self.find_alt_entry(&md5)?;
        let canon_idx = read_u32(&self.file, entry_off + 16);
        self.canonical_at(canon_idx).map(|s| s.to_string())
    }

    /// List the alternate identifiers registered for a canonical.
    ///
    /// Empty `Vec` if `canonical` isn't a key in the sidecar. v1
    /// equivalent: `aliasMap.get(canonical).getOrElse(TreeSet.empty)`.
    pub fn alternates_of(&self, canonical: &str) -> Vec<String> {
        let md5 = md5hash_str(canonical);
        let Some(entry_off) = self.find_canon_entry(&md5) else {
            return Vec::new();
        };
        let alt_list_start = read_u32(&self.file, entry_off + 22);
        let alt_list_count = read_u32(&self.file, entry_off + 26);
        let mut out = Vec::with_capacity(alt_list_count as usize);
        for i in 0..alt_list_count {
            let list_entry_off = self.alt_list_off as usize
                + (alt_list_start + i) as usize * ALT_LIST_ENTRY_BYTES;
            let alt_off = read_u32(&self.file, list_entry_off);
            let alt_len = read_u16(&self.file, list_entry_off + 4);
            out.push(self.read_string(alt_off, alt_len).to_string());
        }
        out
    }

    /// Read the i-th canonical entry: its identifier string and the
    /// list of alternates registered for it. `i < canon_count()`.
    ///
    /// Linear `canon_index` scan helper, used by tests and any tool
    /// that wants to enumerate the cluster's alias relationships.
    pub fn canonical_entry(&self, i: u32) -> Option<(String, Vec<String>)> {
        let canon = self.canonical_at(i)?.to_string();
        let alts = self.alternates_of(&canon);
        Some((canon, alts))
    }
}
