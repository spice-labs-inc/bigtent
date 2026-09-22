//! Shared test helpers for format-level integration tests.
//!
//! Requirement: phase 2, deliverable 7 — "Declare two test helpers: a raw
//! version 4 cluster assembler (bytes, not the writer) for golden-bytes
//! tests, and note that the version 3 assembler arrives in phase 3."
//!
//! The assembler below writes raw bytes directly — it never uses
//! `ClusterWriter` — so format tests can construct clusters that the
//! BigTent writer would never produce (wrong versions, wrong magic,
//! alternative declared algorithms) and prove reader behavior at the byte
//! level.

use bigtent::item::Item;
use bigtent::rodeo::cluster::{ClusterFileEnvelope, ClusterFileMagicNumber};
use bigtent::rodeo::data::{DataFileEnvelope, DataFileMagicNumber};
use bigtent::rodeo::index::{IndexEnvelope, IndexFileMagicNumber};
use bigtent::util::{
    KeyAlg, KeyHash, blake3hash_str, byte_slice_to_u63, md5hash_str, sha256_for_slice,
};
use std::collections::BTreeMap;
use std::path::Path;

/// The version 4 index-key algorithm constant (ADR 0002): the first 16
/// bytes (128 bits) of the BLAKE3 digest over the identifier's UTF-8
/// bytes, followed by the two big-endian u64 fields of the index entry.
pub const V4_ENCODING: &str = "BLAKE3[0..16]/Long/Long";
/// The version 3 index-key algorithm constant (as carried in `.gri` files).
pub const V3_ENCODING: &str = "MD5/Long/Long";

/// Specification for a raw, hand-assembled cluster.
#[derive(Clone)]
pub struct RawClusterSpec {
    /// `.grc` cluster envelope version
    pub cluster_version: u32,
    /// `.grd` data envelope version
    pub data_envelope_version: u32,
    /// `.gri` index envelope version (unchanged from the v3 writer: 1)
    pub index_envelope_version: u32,
    /// `.grc` algorithm declaration (None = absent, as in v3 files)
    pub cluster_encoding: Option<String>,
    /// `.gri` encoding string
    pub index_encoding: String,
    /// The key algorithm used to derive the 16-byte index keys
    pub key_alg: KeyAlg,
    /// Inner data envelope magic (defaults to the real one)
    pub data_magic: u32,
    /// Inner index envelope magic (defaults to the real one)
    pub index_magic: u32,
    /// Cluster envelope magic (defaults to the real one)
    pub cluster_magic: u32,
    /// The items (in the shape the test wants to store)
    pub items: Vec<Item>,
}

impl Default for RawClusterSpec {
    fn default() -> Self {
        RawClusterSpec {
            cluster_version: 4,
            data_envelope_version: 2,
            index_envelope_version: 1,
            cluster_encoding: Some(V4_ENCODING.to_string()),
            index_encoding: V4_ENCODING.to_string(),
            key_alg: KeyAlg::Blake3Truncated128,
            data_magic: DataFileMagicNumber,
            index_magic: IndexFileMagicNumber,
            cluster_magic: ClusterFileMagicNumber,
            items: vec![],
        }
    }
}

/// Compute the index key for an identifier under the spec's algorithm.
pub fn spec_key(spec: &RawClusterSpec, identifier: &str) -> KeyHash {
    match spec.key_alg {
        KeyAlg::Md5 => md5hash_str(identifier),
        KeyAlg::Blake3Truncated128 => blake3hash_str(identifier),
    }
}

/// Assemble a raw cluster under `dir` (must exist) and return the `.grc`
/// path. File names are content-derived exactly as production naming:
/// first 8 bytes of SHA256, high bit masked, 16 lowercase hex digits,
/// with a deterministic prefix on the `.grc` (the loader parses the name
/// positionally from the end).
pub fn assemble_raw_cluster(dir: &Path, spec: &RawClusterSpec) -> anyhow::Result<std::path::PathBuf> {
    use std::io::Write;

    // ---- .grd: magic, envelope, then [u32 len][item cbor] per item ----
    let mut grd: Vec<u8> = vec![];
    grd.write_all(&DataFileMagicNumber.to_be_bytes())?;
    let data_env = DataFileEnvelope {
        version: spec.data_envelope_version,
        magic: spec.data_magic,
        previous: 0,
        depends_on: Default::default(),
        built_from_merge: false,
        info: BTreeMap::new(),
    };
    let env_bytes = serde_cbor::to_vec(&data_env)?;
    grd.write_all(&(env_bytes.len() as u16).to_be_bytes())?;
    grd.write_all(&env_bytes)?;

    let mut entries: Vec<(KeyHash, u64, usize)> = vec![];
    for item in &spec.items {
        let cbor = serde_cbor::to_vec(item)?;
        let offset = grd.len();
        grd.write_all(&(cbor.len() as u32).to_be_bytes())?;
        grd.write_all(&cbor)?;
        entries.push((spec_key(spec, &item.identifier), 0, offset));
    }

    let grd_sha = byte_slice_to_u63(&sha256_for_slice(&grd))?;
    let grd_path = dir.join(format!("{:016x}.grd", grd_sha));
    std::fs::write(&grd_path, &grd)?;

    // ---- .gri: magic, envelope (encoding), then sorted 32-byte entries ----
    entries.sort();
    let mut gri: Vec<u8> = vec![];
    gri.write_all(&IndexFileMagicNumber.to_be_bytes())?;
    let index_env = IndexEnvelope {
        version: spec.index_envelope_version,
        magic: spec.index_magic,
        size: entries.len() as u32,
        // mangled data file hash reference
        data_files: [grd_sha].into_iter().collect(),
        encoding: spec.index_encoding.clone(),
        info: BTreeMap::new(),
    };
    let index_env_bytes = serde_cbor::to_vec(&index_env)?;
    gri.write_all(&(index_env_bytes.len() as u16).to_be_bytes())?;
    gri.write_all(&index_env_bytes)?;
    for (key, _, offset) in &entries {
        gri.write_all(key)?;
        gri.write_all(&grd_sha.to_be_bytes())?;
        gri.write_all(&(*offset as u64).to_be_bytes())?;
    }
    let gri_sha = byte_slice_to_u63(&sha256_for_slice(&gri))?;
    let gri_path = dir.join(format!("{:016x}.gri", gri_sha));
    std::fs::write(&gri_path, &gri)?;

    // ---- .grc: magic, envelope listing both files + declaration ----
    let mut grc: Vec<u8> = vec![];
    grc.write_all(&ClusterFileMagicNumber.to_be_bytes())?;
    let cluster_env = ClusterFileEnvelope {
        version: spec.cluster_version,
        magic: spec.cluster_magic,
        encoding: spec.cluster_encoding.clone(),
        data_files: vec![grd_sha],
        index_files: vec![gri_sha],
        info: BTreeMap::new(),
    };
    let cluster_env_bytes = serde_cbor::to_vec(&cluster_env)?;
    grc.write_all(&(cluster_env_bytes.len() as u16).to_be_bytes())?;
    grc.write_all(&cluster_env_bytes)?;
    let grc_sha = byte_slice_to_u63(&sha256_for_slice(&grc))?;
    let grc_path = dir.join(format!("0001_{:016x}.grc", grc_sha));
    std::fs::write(&grc_path, &grc)?;

    Ok(grc_path)
}
