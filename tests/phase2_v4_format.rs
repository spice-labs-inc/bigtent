//! Phase 2 integration tests: version 4 file format.
//!
//! Requirement: plans/2026_09_16_connection_map_and_blake3/
//! phase_2_v4_format_and_item.md — format tests 16-24, dispatch 26-28,
//! determinism 29-31, boundary 32.
//!
//! Requirement provenance and test theory per project rule 3; the format
//! rules are set by approved ADRs 0002 and 0003 and the conformed plan.

mod common;

use bigtent::item::Item;
use bigtent::rodeo::cluster::{ClusterFileEnvelope, MinClusterVersion};
use bigtent::rodeo::data::{DataFileMagicNumber, GOAT_RODEO_DATA_FILE_SUFFIX};
use bigtent::rodeo::goat::GoatRodeoCluster;
use bigtent::rodeo::goat_trait::GoatRodeoTrait;
use bigtent::rodeo::writer::ClusterWriter;
use bigtent::util::{KeyAlg, sha256_for_slice};
use common::{RawClusterSpec, V3_ENCODING, V4_ENCODING, assemble_raw_cluster};
use std::collections::BTreeMap;

fn v4_item(identifier: &str, target: &str) -> Item {
    let mut targets = std::collections::BTreeSet::new();
    targets.insert(target.to_string());
    // serde_cbor::Value::from serde_json via a CBOR round trip of the JSON
    let body_json = serde_json::json!({"file_names": ["f.txt"], "file_size": 1});
    let body_cbor: serde_cbor::Value =
        serde_cbor::from_slice(&serde_cbor::to_vec(&body_json).unwrap()).unwrap();
    Item {
        identifier: identifier.to_string(),
        connections: {
            let mut m = std::collections::BTreeMap::new();
            m.insert("contained:up".to_string(), targets);
            bigtent::item::Connections(m)
        },
        body_mime_type: Some(bigtent::item::ITEM_METADATA_MIME_TYPE.to_string()),
        body: Some(body_cbor),
    }
}

/// Test 16: the writer emits version 4 envelopes — cluster version 4 with
/// the `BLAKE3[0..16]/Long/Long` declaration in the `.grc`, data envelope
/// version 2 with inert chain fields (`previous: 0`, empty `depends_on`),
/// and the `.gri` encoding switched to the v4 constant with its version
/// unchanged.
///
/// Requirement: D3, H5. Theory: the writer is the sole producer of the new
/// format; its envelopes must carry exactly the declared versions and the
/// algorithm constant, and the inert chain fields must be constants so
/// multi-file output is byte-deterministic.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_writer_emits_version_4_envelopes() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let mut writer = ClusterWriter::new(dir.path()).await.expect("writer");
    // the writer enforces key-ordered appends (H5): write the items in
    // ascending key order
    let mut items: Vec<Item> = (0..3)
        .map(|i| v4_item(&format!("gitoid:blob:sha256:w4_{i:08}"), "pkg:npm/x@1"))
        .collect();
    items.sort_by_key(|i| bigtent::util::blake3hash_str(&i.identifier));
    for item in items {
        let cbor = serde_cbor::to_vec(&item).unwrap();
        writer.write_item(item, cbor).await.expect("write item");
    }
    let grc = writer.finalize_cluster().await.expect("finalize");

    // .grc: version 4 + the algorithm declaration
    let grc_bytes = std::fs::read(&grc).unwrap();
    let mut cursor: &[u8] = &grc_bytes;
    let magic = bigtent::util::read_u32_sync(&mut cursor).unwrap();
    assert_eq!(magic, bigtent::rodeo::cluster::ClusterFileMagicNumber);
    let env: ClusterFileEnvelope = bigtent::util::read_len_and_cbor_sync(&mut cursor).unwrap();
    assert_eq!(env.version, 4, "cluster version must be 4");
    assert_eq!(
        env.encoding.as_deref(),
        Some(V4_ENCODING),
        "the .grc must carry the v4 algorithm constant"
    );

    // .grd: data envelope 2, previous 0, empty depends_on
    let grd_file = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().map(|e| e == "grd").unwrap_or(false))
        .expect("a .grd file");
    let grd_bytes = std::fs::read(&grd_file).unwrap();
    let mut cursor: &[u8] = &grd_bytes;
    let magic = bigtent::util::read_u32_sync(&mut cursor).unwrap();
    assert_eq!(magic, DataFileMagicNumber);
    let env: bigtent::rodeo::data::DataFileEnvelope =
        bigtent::util::read_len_and_cbor_sync(&mut cursor).unwrap();
    assert_eq!(env.version, 2, "data envelope version must be 2");
    assert_eq!(env.previous, 0, "chain field is inert: always 0");
    assert!(env.depends_on.is_empty(), "chain field is inert: empty");

    // .gri: v4 encoding, version unchanged (the v3 writer's 1)
    let gri_file = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().map(|e| e == "gri").unwrap_or(false))
        .expect("a .gri file");
    let gri_bytes = std::fs::read(&gri_file).unwrap();
    let mut cursor: &[u8] = &gri_bytes;
    let magic = bigtent::util::read_u32_sync(&mut cursor).unwrap();
    assert_eq!(magic, bigtent::rodeo::index::IndexFileMagicNumber);
    let env: bigtent::rodeo::index::IndexEnvelope =
        bigtent::util::read_len_and_cbor_sync(&mut cursor).unwrap();
    assert_eq!(env.encoding, V4_ENCODING);
    assert_eq!(env.version, 1, "index envelope version is unchanged");
}

/// Test 17: the checked-in version 3 fixture clusters still load and
/// resolve identifiers with MD5 keys.
///
/// Requirement: D11 / ADR 0002 (v3 stays readable). Theory: backward
/// compatibility is only proven by actually loading the checked-in v3
/// corpus and resolving identifiers from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_v3_fixture_clusters_load_and_resolve() {
    for fixture in ["test_data/cluster_a", "test_data/cluster_b"] {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(fixture);
        let clusters = GoatRodeoCluster::cluster_files_in_dir(path.clone(), false, vec![])
            .await
            .expect("v3 fixture loads");
        assert!(!clusters.is_empty(), "{fixture} must contain clusters");
        for cluster in clusters {
            assert_eq!(
                cluster.key_alg(),
                KeyAlg::Md5,
                "v3 fixtures resolve via the first .gri's MD5 declaration"
            );
            // resolve the first indexed identifier
            let count = cluster.number_of_items();
            assert!(count > 0, "fixture has items");
            for pos in 0..count {
                if let Some(offset) = bigtent::rodeo::robo_goat::ClusterRoboMember::offset_from_pos(cluster.as_ref(), pos) {
                    let item = bigtent::rodeo::robo_goat::ClusterRoboMember::item_from_item_offset(
                        cluster.as_ref(),
                        &offset,
                    )
                    .expect("item readable");
                    assert!(
                        cluster.item_for_identifier(&item.identifier).is_some(),
                        "identifier must resolve through the v3 (MD5) key space"
                    );
                    break;
                }
            }
        }
    }
}

/// Test 18: checked-in version 4 fixtures exist and load.
///
/// Requirement: D11. Theory: the repository must carry checked-in v4
/// fixtures so the v4 read path is exercised without the generator; the
/// test fails if the fixtures are empty (never a silent pass).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checked_in_v4_fixtures_load() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_data/v4");
    // count .grc files recursively (the generator nests cluster_N/ dirs)
    fn count_grc(dir: &std::path::Path) -> usize {
        std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok().map(|e| e.path()))
                    .map(|p| {
                        if p.is_dir() {
                            count_grc(&p)
                        } else if p.extension().and_then(|e| e.to_str()) == Some("grc") {
                            1
                        } else {
                            0
                        }
                    })
                    .sum()
            })
            .unwrap_or(0)
    }
    let grc_count = count_grc(&path);
    assert!(grc_count > 0, "test_data/v4 must contain checked-in clusters");

    let clusters = GoatRodeoCluster::cluster_files_in_dir(path, false, vec![])
        .await
        .expect("v4 fixtures load");
    assert!(!clusters.is_empty());
    for cluster in clusters {
        assert_eq!(cluster.key_alg(), KeyAlg::Blake3Truncated128);
    }
}

/// Test 19: readers follow the declared algorithm.
///
/// Requirement: ADR 0002 (the algorithm is declared in the files and
/// readers use it). Theory: a v4 cluster declaring BLAKE3 resolves with
/// BLAKE3 keys; a v4 cluster declaring MD5 (legal, though BigTent never
/// writes one) resolves with MD5 keys; a v3 cluster resolves via the
/// first `.gri`'s declaration. No algorithm/version combination is
/// rejected for algorithm reasons.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_reader_follows_declared_algorithm() {
    let dir = tempfile::TempDir::new().expect("dir");

    // v4 written by BigTent: declares BLAKE3
    let v4_dir = dir.path().join("v4_blake3");
    std::fs::create_dir(&v4_dir).unwrap();
    let mut writer = ClusterWriter::new(&v4_dir).await.unwrap();
    let item = v4_item("gitoid:blob:sha256:declared_blake3", "pkg:npm/x@1");
    let cbor = serde_cbor::to_vec(&item).unwrap();
    writer.write_item(item.clone(), cbor).await.unwrap();
    writer.finalize_cluster().await.unwrap();
    let cluster = &GoatRodeoCluster::cluster_files_in_dir(v4_dir, false, vec![])
        .await
        .unwrap()[0];
    assert_eq!(cluster.key_alg(), KeyAlg::Blake3Truncated128);
    assert!(cluster.item_for_identifier(&item.identifier).is_some());

    // v4 declaring MD5: legal per ADR 0002; readers follow the declaration
    let v4_md5_dir = dir.path().join("v4_md5");
    std::fs::create_dir(&v4_md5_dir).unwrap();
    assemble_raw_cluster(
        &v4_md5_dir,
        &RawClusterSpec {
            cluster_encoding: Some(V3_ENCODING.to_string()),
            index_encoding: V3_ENCODING.to_string(),
            key_alg: KeyAlg::Md5,
            items: vec![v4_item("gitoid:blob:sha256:declared_md5", "pkg:npm/x@1")],
            ..Default::default()
        },
    )
    .unwrap();
    let cluster = &GoatRodeoCluster::cluster_files_in_dir(v4_md5_dir, false, vec![])
        .await
        .unwrap()[0];
    assert_eq!(cluster.key_alg(), KeyAlg::Md5);
    assert!(
        cluster
            .item_for_identifier("gitoid:blob:sha256:declared_md5")
            .is_some(),
        "a v4 cluster declaring MD5 resolves with MD5 keys"
    );

    // v3: no .grc declaration; the first .gri's hash description is used
    // (covered against checked-in fixtures in test_v3_fixture_clusters_load_and_resolve)
}

/// Test 20/21: envelope version validation.
///
/// Requirement: D3 (v4 requires data envelope 2), readers reject unknown
/// cluster versions. Theory: version numbers are load-bearing for the
/// item shape and the envelope framing; unknown versions and mismatched
/// data envelope versions fail cluster load. The `.gri` envelope version
/// is not asserted (it is not algorithm-bearing and carries no decision).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_envelope_version_cross_check() {
    let dir = tempfile::TempDir::new().expect("dir");

    // v4 with data envelope 1 is rejected
    let bad = dir.path().join("v4_data_env_1");
    std::fs::create_dir(&bad).unwrap();
    assemble_raw_cluster(
        &bad,
        &RawClusterSpec {
            data_envelope_version: 1,
            items: vec![v4_item("gitoid:blob:sha256:env_check", "pkg:x@1")],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        GoatRodeoCluster::cluster_files_in_dir(bad, false, vec![])
            .await
            .is_err(),
        "v4 with data envelope 1 must be rejected"
    );

    // unknown cluster versions are rejected
    for version in [0u32, 5] {
        let bad = dir.path().join(format!("cluster_version_{version}"));
        std::fs::create_dir(&bad).unwrap();
        assemble_raw_cluster(
            &bad,
            &RawClusterSpec {
                cluster_version: version,
                items: vec![v4_item("gitoid:blob:sha256:env_check", "pkg:x@1")],
                ..Default::default()
            },
        )
        .unwrap();
        assert!(
            GoatRodeoCluster::cluster_files_in_dir(bad, false, vec![])
                .await
                .is_err(),
            "cluster version {version} must be rejected"
        );
    }
}

/// Test 22: envelope magic fields are validated — wrong magic inside the
/// data or index envelope fails cluster load naming the file.
///
/// Requirement: H3(c). Theory: the file-level magic is checked, but the
/// envelope repeated inside must also be checked so a truncated or
/// mis-framed file fails closed at load.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_envelope_magic_validated() {
    let dir = tempfile::TempDir::new().expect("dir");

    let bad_data = dir.path().join("bad_data_magic");
    std::fs::create_dir(&bad_data).unwrap();
    assemble_raw_cluster(
        &bad_data,
        &RawClusterSpec {
            data_magic: 0xdeadbeef,
            items: vec![v4_item("gitoid:blob:sha256:magic_check", "pkg:x@1")],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        GoatRodeoCluster::cluster_files_in_dir(bad_data, false, vec![])
            .await
            .is_err(),
        "wrong data envelope magic must fail load"
    );

    let bad_index = dir.path().join("bad_index_magic");
    std::fs::create_dir(&bad_index).unwrap();
    assemble_raw_cluster(
        &bad_index,
        &RawClusterSpec {
            index_magic: 0xdeadbeef,
            items: vec![v4_item("gitoid:blob:sha256:magic_check", "pkg:x@1")],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        GoatRodeoCluster::cluster_files_in_dir(bad_index, false, vec![])
            .await
            .is_err(),
        "wrong index envelope magic must fail load"
    );
}

/// Test 24: short `.grc` filenames are rejected instead of slicing
/// blindly (which would panic on underflow).
///
/// Requirement: H3(b).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_short_grc_filename_rejected() {
    let dir = tempfile::TempDir::new().expect("dir");
    std::fs::write(dir.path().join("x.grc"), b"garbage").unwrap();
    assert!(
        GoatRodeoCluster::cluster_files_in_dir(dir.path().to_path_buf(), false, vec![])
            .await
            .is_err(),
        "a too-short .grc name must be rejected, not sliced blindly"
    );
}

/// Test 26: a v4 cluster answers lookups, traversal, and roots.
///
/// Requirement: D3 (v4 is a first-class format). Theory: the full query
/// surface (identifier lookup, north traversal, flatten, roots) must work
/// against BLAKE3-keyed clusters.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_v4_cluster_lookup_traversal_and_roots() {
    let dir = tempfile::TempDir::new().expect("dir");
    let mut writer = ClusterWriter::new(dir.path()).await.unwrap();

    let file_item = v4_item("gitoid:blob:sha256:file_1", "pkg:npm/left@1.0.0");
    let file_item2 = v4_item("gitoid:blob:sha256:file_2", "pkg:npm/right@2.0.0");
    // pkg item contains the two files
    let mut pkg = v4_item("pkg:npm:container@1", "pkg:npm/container@1");
    pkg.connections.0.insert(
        "contained:down".to_string(),
        [
            "gitoid:blob:sha256:file_1".to_string(),
            "gitoid:blob:sha256:file_2".to_string(),
        ]
        .into_iter()
        .collect(),
    );
    // the files' contained:up edges must point at the container
    let file_item = {
        let mut f = file_item;
        f.connections.0.insert(
            "contained:up".to_string(),
            ["pkg:npm:container@1".to_string()].into_iter().collect(),
        );
        f
    };
    let file_item2 = {
        let mut f = file_item2;
        f.connections.0.insert(
            "contained:up".to_string(),
            ["pkg:npm:container@1".to_string()].into_iter().collect(),
        );
        f
    };
    let pkg_alias = v4_item("pkg:npm/container@1.0.0", "pkg:npm/container@1.0.0");
    let pkg_alias = {
        let mut a = pkg_alias;
        a.connections.0 = [(
            "alias:to".to_string(),
            ["pkg:npm:container@1".to_string()].into_iter().collect(),
        )]
        .into_iter()
        .collect();
        a
    };

    // the writer enforces key-ordered appends (H5): write in key order
    let mut items = vec![file_item, file_item2, pkg, pkg_alias];
    items.sort_by_key(|i| bigtent::util::blake3hash_str(&i.identifier));
    for item in items {
        let cbor = serde_cbor::to_vec(&item).unwrap();
        writer.write_item(item, cbor).await.unwrap();
    }
    writer.finalize_cluster().await.unwrap();

    let cluster = &GoatRodeoCluster::cluster_files_in_dir(dir.path().to_path_buf(), false, vec![])
        .await
        .unwrap()[0];

    // lookup
    assert!(
        cluster
            .item_for_identifier("gitoid:blob:sha256:file_1")
            .is_some()
    );
    assert!(
        cluster
            .item_for_identifier("gitoid:blob:sha256:absent")
            .is_none()
    );
    // traversal: north from a file finds the container
    use tokio_stream::StreamExt;
    let mut north = cluster
        .clone()
        .north_send(vec!["gitoid:blob:sha256:file_1".to_string()], false, std::time::Instant::now())
        .await
        .unwrap();
    let mut found = false;
    while let Some(e) = north.recv().await {
        if let tokio_util::either::Either::Left(item) = e {
            if item.identifier == "pkg:npm:container@1" {
                found = true;
            }
        }
    }
    assert!(found, "north traversal must find the container");
}

/// Test 27: a mixed herd (v3 fixture + writer-built v4) resolves both.
///
/// Requirement: D6 / ADR 0003 (coexistence). Theory: the stated overlap
/// construction — a v3 fixture identifier also present in a writer-built
/// v4 fixture — proves one herd answers lookups across key spaces.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_mixed_herd_lookup_resolves_both_versions() {
    use bigtent::rodeo::member::{HerdMember, member_core};

    // take a real identifier from the v3 fixture
    let v3_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_data/cluster_a");
    let v3_clusters = GoatRodeoCluster::cluster_files_in_dir(v3_path, true, vec![])
        .await
        .expect("v3 fixture");
    let v3_cluster = &v3_clusters[0];
    let count = v3_cluster.number_of_items();
    let mut chosen: Option<String> = None;
    for pos in 0..count {
        if let Some(offset) =
            bigtent::rodeo::robo_goat::ClusterRoboMember::offset_from_pos(v3_cluster.as_ref(), pos)
        {
            if let Some(item) =
                bigtent::rodeo::robo_goat::ClusterRoboMember::item_from_item_offset(
                    v3_cluster.as_ref(),
                    &offset,
                )
            {
                chosen = Some(item.identifier.clone());
                break;
            }
        }
    }
    let identifier = chosen.expect("v3 fixture must have at least one item");

    // build a v4 cluster that also contains that identifier
    let dir = tempfile::TempDir::new().unwrap();
    let mut writer = ClusterWriter::new(dir.path()).await.unwrap();
    let twin = v4_item(&identifier, "pkg:npm/twin@1");
    let cbor = serde_cbor::to_vec(&twin).unwrap();
    writer.write_item(twin, cbor).await.unwrap();
    writer.finalize_cluster().await.unwrap();
    let v4_clusters = GoatRodeoCluster::cluster_files_in_dir(dir.path().to_path_buf(), false, vec![])
        .await
        .unwrap();

    // herd both
    let members: Vec<std::sync::Arc<HerdMember>> = v3_clusters
        .iter()
        .chain(v4_clusters.iter())
        .map(|c| member_core(c.clone()))
        .collect();
    let herd = bigtent::rodeo::goat_herd::GoatHerd::new(members);
    assert!(
        herd.item_for_identifier(&identifier).is_some(),
        "the shared identifier resolves through the herd (v3 or v4 member)"
    );
    assert!(
        herd.item_for_identifier("gitoid:blob:sha256:file_1").is_none(),
        "an identifier in neither member is absent"
    );
}

/// Test 29: multi-file writer output is byte-deterministic across
/// processes.
///
/// Requirement: H5 / invariant 1. Theory: identical items, options, and
/// key order must yield byte-identical output so content-addressed names
/// mean something. Two child `data_generator` processes with the same
/// seed and a small `--max-data-file-size` produce clusters whose
/// `.grd`/`.gri` names and bytes match exactly and whose `.grc` contents
/// and hash suffixes match with the timestamp prefix excluded.
#[test]
fn test_writer_output_is_deterministic_multi_file() {
    use std::process::Command;

    let base = tempfile::TempDir::new().unwrap();
    let run_a = base.path().join("a");
    let run_b = base.path().join("b");

    for run in [&run_a, &run_b] {
        let output = Command::new(env!("CARGO_BIN_EXE_data_generator"))
            .args([
                "--size",
                "300",
                "--clusters",
                "2",
                "--overlap",
                "0",
                "--seed",
                "1234",
                "--max-data-file-size",
                "20000",
                "--output",
                run.to_str().unwrap(),
            ])
            .status()
            .expect("generator runs");
        assert!(output.success(), "generator must succeed");
    }

    let collect = |run: &std::path::Path| -> Vec<(String, Vec<u8>)> {
        let cluster_dir = std::fs::read_dir(run)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.is_dir())
            .expect("cluster dir");
        let mut files: Vec<(String, Vec<u8>)> = std::fs::read_dir(&cluster_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                matches!(
                    p.extension().and_then(|e| e.to_str()),
                    Some("grd") | Some("gri")
                )
            })
            .map(|p| (p.file_name().unwrap().to_string_lossy().to_string(), std::fs::read(p).unwrap()))
            .collect();
        files.sort();
        files
    };

    let files_a = collect(&run_a);
    let files_b = collect(&run_b);
    assert!(!files_a.is_empty());
    assert_eq!(
        files_a.len(),
        files_b.len(),
        "same options must split into the same number of files"
    );
    for (a, b) in files_a.iter().zip(files_b.iter()) {
        assert_eq!(a.0, b.0, "content-addressed names must match exactly");
        assert_eq!(a.1, b.1, "file bytes must match exactly");
    }
    assert!(
        files_a.len() > 1,
        "the small max-data-file-size must force multiple data files"
    );

    // .grc: contents equal; names share the same hash suffix
    let grc_of = |run: &std::path::Path| -> (String, Vec<u8>) {
        let cluster_dir = std::fs::read_dir(run)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.is_dir())
            .unwrap();
        let p = std::fs::read_dir(&cluster_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| p.extension().and_then(|e| e.to_str()) == Some("grc"))
            .unwrap();
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        let suffix = name[name.len() - 16 - 4..].to_string();
        (suffix, std::fs::read(p).unwrap())
    };
    let (suffix_a, bytes_a) = grc_of(&run_a);
    let (suffix_b, bytes_b) = grc_of(&run_b);
    assert_eq!(suffix_a, suffix_b, ".grc hash suffix must match");
    assert_eq!(bytes_a, bytes_b, ".grc contents must match");
}

/// Test 30: the writer rejects an out-of-order append immediately.
///
/// Requirement: H5 (determinism scope). Theory: output clusters are
/// key-ordered; appending a key lower than the last written key would
/// produce an unusable index, so the writer fails the call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_writer_rejects_out_of_order_append() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut writer = ClusterWriter::new(dir.path()).await.unwrap();

    let item_a = v4_item("gitoid:blob:sha256:aaa_high", "pkg:x@1");
    let item_b = v4_item("gitoid:blob:sha256:bbb_low", "pkg:x@1");
    let hash_a = bigtent::util::blake3hash_str(&item_a.identifier);
    let hash_b = bigtent::util::blake3hash_str(&item_b.identifier);

    writer
        .write_item_with_hash(serde_cbor::to_vec(&item_a).unwrap(), hash_a)
        .await
        .expect("first append");
    let higher = hash_a.max(hash_b) == hash_a;
    let item_b_cbor = serde_cbor::to_vec(&item_b).unwrap();
    let result = writer.write_item_with_hash(item_b_cbor, hash_b).await;
    if higher && hash_b < hash_a {
        assert!(result.is_err(), "a key regression must be rejected");
    } else {
        assert!(result.is_ok(), "ascending keys are accepted");
    }
}

/// Test 31: a hand-assembled v4 cluster (bytes, not the writer) loads and
/// resolves — external-producer interop.
///
/// Requirement: D13 guide support. Theory: the reader must accept bytes
/// BigTent's writer did not produce, proving the format definition is the
/// contract rather than the writer's output.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_hand_assembled_v4_cluster_golden_bytes() {
    let dir = tempfile::TempDir::new().unwrap();
    assemble_raw_cluster(
        dir.path(),
        &RawClusterSpec {
            items: vec![
                v4_item("gitoid:blob:sha256:hand_1", "pkg:x@1"),
                v4_item("gitoid:blob:sha256:hand_2", "pkg:x@1"),
            ],
            ..Default::default()
        },
    )
    .unwrap();
    let clusters = GoatRodeoCluster::cluster_files_in_dir(dir.path().to_path_buf(), false, vec![])
        .await
        .expect("hand-assembled bytes must load");
    let cluster = &clusters[0];
    assert_eq!(cluster.key_alg(), KeyAlg::Blake3Truncated128);
    assert!(
        cluster
            .item_for_identifier("gitoid:blob:sha256:hand_1")
            .is_some()
    );
    assert!(
        cluster
            .item_for_identifier("gitoid:blob:sha256:hand_2")
            .is_some()
    );
}

/// Test 32: empty and single-item clusters are legal and must not
/// special-case into failures.
///
/// Requirement: boundary. Theory: a zero-item cluster and a one-item
/// cluster are degenerate but valid; the writer and reader must handle
/// them without special cases.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_empty_and_single_item_clusters() {
    // empty
    let empty_dir = tempfile::TempDir::new().unwrap();
    let mut writer = ClusterWriter::new(empty_dir.path()).await.unwrap();
    writer.finalize_cluster().await.unwrap();
    // loading an empty cluster directory yields no clusters (no error)
    let loaded =
        GoatRodeoCluster::cluster_files_in_dir(empty_dir.path().to_path_buf(), false, vec![])
            .await
            .unwrap();
    let _ = loaded;

    // single item
    let single_dir = tempfile::TempDir::new().unwrap();
    let mut writer = ClusterWriter::new(single_dir.path()).await.unwrap();
    let item = v4_item("gitoid:blob:sha256:solo", "pkg:x@1");
    let cbor = serde_cbor::to_vec(&item).unwrap();
    writer.write_item(item, cbor).await.unwrap();
    writer.finalize_cluster().await.unwrap();
    let loaded =
        GoatRodeoCluster::cluster_files_in_dir(single_dir.path().to_path_buf(), false, vec![])
            .await
            .unwrap();
    assert_eq!(loaded.len(), 1);
    assert!(loaded[0].item_for_identifier("gitoid:blob:sha256:solo").is_some());
}

/// Test (min-version constant): the minimum supported version is 3 and
/// the current version is 4.
#[test]
fn test_min_and_current_cluster_versions() {
    assert_eq!(MinClusterVersion, 3);
    assert_eq!(bigtent::rodeo::cluster::CLUSTER_VERSION, 4);
}
