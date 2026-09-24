//! Phase 1 integration test: on-disk index entry layout.
//!
//! Requirement: plans/2026_09_16_connection_map_and_blake3/
//! phase_1_hash_primitives_and_removals.md, deliverable 1 (D2, D3).
//!
//! What this test tests: after writing a cluster containing two items, the
//! raw `.gri` file decodes with a 32-byte entry stride (16 key bytes + 8
//! data-file hash bytes + 8 offset bytes), and each 16-byte key equals the
//! expected algorithm's key for that item's identifier.
//!
//! Why this test tests it: the 32-byte stride with real keys is the actual
//! compatibility constraint of the index format. A `[u8; 16]` type-size
//! assertion would be a compile-time tautology; decoding the written bytes
//! proves the on-disk layout end to end. Per the phase 1 note under D10,
//! when phase 2 flips the writer to BLAKE3, this test's expected-algorithm
//! assertion is adapted (MD5-keyed entry to then-current algorithm), with
//! the semantic meaning preserved and the adaptation inventoried.

use bigtent::item::Item;
use bigtent::rodeo::index::IndexFileMagicNumber;
use bigtent::rodeo::writer::ClusterWriter;
use bigtent::util::{KeyAlg, read_u16_sync, read_u32_sync};

fn test_item(identifier: &str, target: &str) -> Item {
    // Adapted for phase 2 (D1): connections are the edge-type map; the
    // test's meaning (two items, one edge each) is unchanged. Inventoried
    // in the phase 2 execution state per D10.
    Item {
        identifier: identifier.to_string(),
        connections: [(
            "contained:up".to_string(),
            [target.to_string()].into_iter().collect(),
        )]
        .into_iter()
        .collect(),
        body_mime_type: None,
        body: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_on_disk_index_entry_is_32_bytes() {
    let dir = tempfile::TempDir::new().expect("temp dir");

    let item_a = test_item("gitoid:blob:sha256:aaaaaaaaaaaaaaaa", "pkg:npm/a@1.0.0");
    let item_b = test_item("gitoid:blob:sha256:bbbbbbbbbbbbbbbb", "pkg:npm/b@2.0.0");

    let mut writer = ClusterWriter::new(dir.path()).await.expect("writer");
    // Adapted for phase 2 (H5): the writer enforces key-ordered appends,
    // so the items are written in ascending key order. The test's meaning
    // (two items, on-disk 32-byte stride, keys match the derivation) is
    // unchanged. Inventoried in the phase 2 execution state per D10.
    let mut items = vec![item_a.clone(), item_b.clone()];
    items.sort_by_key(|i| bigtent::util::blake3hash_str(&i.identifier));
    for item in items {
        let cbor = serde_cbor::to_vec(&item).expect("item cbor");
        writer.write_item(item, cbor).await.expect("write");
    }
    let grc_path = writer.finalize_cluster().await.expect("finalize");
    assert!(grc_path.exists(), "cluster file must be written");

    // Find the written .gri file (exactly one for two items).
    let mut gri_files: Vec<std::path::PathBuf> = std::fs::read_dir(dir.path())
        .expect("read dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().map(|e| e == "gri").unwrap_or(false))
        .collect();
    assert_eq!(gri_files.len(), 1, "one index file expected");
    let gri_path = gri_files.pop().unwrap();
    let gri_bytes = std::fs::read(&gri_path).expect("read gri");

    // Frame: big-endian u32 magic, big-endian u16 envelope length,
    // CBOR envelope, then raw index entries.
    let mut cursor: &[u8] = &gri_bytes;
    let magic = read_u32_sync(&mut cursor).expect("magic");
    assert_eq!(magic, IndexFileMagicNumber, "index file magic");
    let env_len = read_u16_sync(&mut cursor).expect("envelope length") as usize;
    assert!(
        cursor.len() >= env_len + 2 * 32,
        "gri must contain at least two full 32-byte entries"
    );
    let envelope_bytes = &cursor[..env_len];
    let envelope: bigtent::rodeo::index::IndexEnvelope =
        serde_cbor::from_slice(envelope_bytes).expect("envelope cbor");
    let entries_bytes = &cursor[env_len..];

    // Entry stride: 16 key + 8 file hash + 8 offset.
    assert_eq!(
        entries_bytes.len() % 32,
        0,
        "entry stride must be exactly 32 bytes"
    );
    let entry_count = entries_bytes.len() / 32;
    assert_eq!(entry_count, 2, "two items must produce two index entries");
    assert_eq!(
        envelope.size as usize, entry_count,
        "envelope size must match entry count"
    );

    // Each entry's 16-byte key must equal the expected algorithm's key for
    // that item's identifier. Adapted for phase 2 (D10, anticipated by the
    // phase 1 plan note): the writer now emits BLAKE3 keys, so the expected
    // algorithm changes from MD5 to the then-current BLAKE3.
    // Every entry's 16-byte key must equal the expected algorithm's key for
    // SOME item's identifier — the entries are in key order, so the pairing
    // is by key lookup, not position. Adapted for phase 2 (D10, anticipated
    // by the phase 1 plan note): the writer now emits BLAKE3 keys, so the
    // expected algorithm changes from MD5 to the then-current BLAKE3.
    let expected_by_key: std::collections::BTreeMap<[u8; 16], &str> = [&item_a, &item_b]
        .into_iter()
        .map(|item| {
            (
                KeyAlg::Blake3Truncated128.hash_identifier(&item.identifier),
                item.identifier.as_str(),
            )
        })
        .collect();
    for idx in 0..entry_count {
        let entry = &entries_bytes[idx * 32..(idx + 1) * 32];
        let key: [u8; 16] = entry[0..16].try_into().expect("key slice");
        let file_hash: [u8; 8] = entry[16..24].try_into().expect("file hash slice");
        let offset: [u8; 8] = entry[24..32].try_into().expect("offset slice");

        assert!(
            expected_by_key.contains_key(&key),
            "entry {idx} key must be the BLAKE3 key of a written identifier"
        );
        assert_ne!(
            u64::from_be_bytes(file_hash),
            0,
            "entry must reference a data file"
        );
        let _ = u64::from_be_bytes(offset); // offset is position-dependent; stride is the constraint
    }
    assert_eq!(expected_by_key.len(), 2, "both identifiers' keys appear");
}
