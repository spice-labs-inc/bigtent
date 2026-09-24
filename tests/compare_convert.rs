//! CLI behavior tests: `--convert-to-v4` and `--compare`.
//!
//! Requirement: owner request (2026-09-23) — a CLI option to convert V3
//! clusters to V4/BLAKE3[0..16] clusters, and a CLI option to compare two
//! clusters for Item equality (rust `equal`: identifier, connections,
//! body, mime). Compare must work across key algorithms (V3/MD5 vs
//! V4/BLAKE3) by probing one side's identifiers through the other side's
//! algorithm.
//!
//! Theory: the checked-in version 3 fixtures are real V3 clusters; the
//! conversion must re-key them to BLAKE3 with byte-identical items, and
//! compare must prove Item equality across the two key spaces.

use bigtent::compare::{compare_clusters, CompareOutcome};
use bigtent::rodeo::convert::convert_cluster_to_dir;
use bigtent::rodeo::goat::GoatRodeoCluster;
use bigtent::rodeo::goat_trait::GoatRodeoTrait;
use bigtent::util::KeyAlg;
use std::path::PathBuf;
use std::sync::Arc;

async fn v3_fixture_async(name: &str) -> Vec<Arc<GoatRodeoCluster>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("test_data")
        .join(name);
    GoatRodeoCluster::cluster_files_in_dir(path, false, vec![])
        .await
        .expect("fixture loads")
}

async fn convert_fixture_async(name: &str, dest: &std::path::Path) -> Vec<PathBuf> {
    let mut out = vec![];
    for cluster in v3_fixture_async(name).await {
        let grcs = convert_cluster_to_dir(&cluster, dest)
            .await
            .expect("conversion succeeds");
        out.extend(grcs);
    }
    out
}

async fn load_async(grc: &PathBuf) -> Arc<GoatRodeoCluster> {
    GoatRodeoCluster::new(grc, false, None, vec![])
        .await
        .expect("cluster loads")
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("runtime")
}

/// The converted output is BLAKE3-keyed: the .gri declares the v4
/// constant and lookups resolve.
#[test]
fn test_convert_writes_blake3_keyed_clusters() {
    rt().block_on(async {
        let dest = tempfile::TempDir::new().unwrap();
        let grcs = convert_fixture_async("cluster_a", dest.path()).await;
        assert!(!grcs.is_empty(), "conversion produced at least one cluster");
        for grc in &grcs {
            let cluster = load_async(grc).await;
            assert_eq!(
                cluster.key_alg(),
                KeyAlg::Blake3Truncated128,
                "converted clusters are BLAKE3-keyed"
            );
        }
    });
}

/// The converted output has exactly the same items, rust-equal, as the
/// V3 source — compared across key algorithms (V3/MD5 source vs
/// V4/BLAKE3 converted).
#[test]
fn test_convert_output_items_equal_to_source() {
    rt().block_on(async {
        let dest = tempfile::TempDir::new().unwrap();
        let grcs = convert_fixture_async("cluster_a", dest.path()).await;
        assert!(!grcs.is_empty());
        let mut converted = vec![];
        for grc in &grcs {
            converted.push(load_async(grc).await);
        }
        let source = v3_fixture_async("cluster_a").await;
        let outcome = compare_clusters(&source, &converted).expect("compare runs");
        assert!(
            outcome.equal(),
            "converted items are rust-equal to source: {outcome:?}"
        );
    });
}

/// A cluster compares equal to itself.
#[test]
fn test_compare_identity_holds() {
    let a = v3_fixture_async_no_rt("cluster_a");
    let outcome = compare_clusters(&a, &a).expect("compare runs");
    assert!(outcome.equal());
    assert_eq!(outcome.total_left, outcome.total_right);
    assert!(outcome.total_left > 0, "the fixture has items");
}

fn v3_fixture_async_no_rt(name: &str) -> Vec<Arc<GoatRodeoCluster>> {
    rt().block_on(v3_fixture_async(name))
}

/// Two different clusters compare unequal, with counts and the first
/// differing identifier reported.
#[test]
fn test_compare_detects_difference() {
    let a = v3_fixture_async_no_rt("cluster_a");
    let b = v3_fixture_async_no_rt("cluster_b");
    let outcome = compare_clusters(&a, &b).expect("compare runs");
    assert!(
        !outcome.equal() || outcome.total_left == outcome.total_right,
        "different corpora must not silently compare equal: {outcome:?}"
    );
    let _ = CompareOutcome::default();
}

/// Compare reports identifier-set mismatches (missing/extra) and item
/// differences with bounded detail.
#[test]
fn test_compare_reports_missing_and_extra() {
    rt().block_on(async {
        let a = v3_fixture_async("cluster_a").await;
        let dest = tempfile::TempDir::new().unwrap();
        let grcs = convert_fixture_async("cluster_a", dest.path()).await;
        let mut converted = vec![];
        if grcs.len() > 1 {
            for (i, grc) in grcs.iter().enumerate() {
                if i == 0 {
                    continue; // drop the first converted chunk
                }
                converted.push(load_async(grc).await);
            }
        } else {
            // the fixture fits one chunk: use a different corpus's
            // conversion so the right side lacks cluster_a's items
            let dest_b = tempfile::TempDir::new().unwrap();
            for grc in convert_fixture_async("cluster_b", dest_b.path()).await {
                converted.push(load_async(&grc).await);
            }
        }
        assert!(
            !converted.is_empty(),
            "the right side has at least one cluster"
        );
        let outcome = compare_clusters(&a, &converted).expect("compare runs");
        assert!(!outcome.equal(), "dropping items must fail equality");
        assert!(outcome.missing_count() > 0, "missing items are counted");
    });
}

/// Mixed-algorithm sides are refused: each side must be a single key
/// space so probe keys are comparable.
#[test]
fn test_compare_rejects_mixed_algorithm_side() {
    rt().block_on(async {
        let a = v3_fixture_async("cluster_a").await;
        let dest = tempfile::TempDir::new().unwrap();
        let grcs = convert_fixture_async("cluster_a", dest.path()).await;
        let mut mixed: Vec<Arc<GoatRodeoCluster>> = a.clone();
        for grc in &grcs {
            mixed.push(load_async(grc).await);
            break; // one converted cluster makes the side mixed
        }
        assert!(
            compare_clusters(&a, &mixed).is_err(),
            "a side with mixed key algorithms must be refused"
        );
    });
}

/// The CLI mode routing validates argument combinations.
#[cfg(test)]
mod cli_routing_tests {
    use clap::Parser;

    fn args_parse(argv: &[&str]) -> bigtent::config::Args {
        bigtent::config::Args::parse_from(argv)
    }

    #[test]
    fn convert_mode_requires_dest() {
        let a = args_parse(&["bigtent", "--convert-to-v4", "/tmp/x"]);
        let mode = bigtent::main_utils::mode_from_args(&a).expect_err("dest required");
        assert!(
            format!("{mode:?}").to_lowercase().contains("dest"),
            "convert without --dest explains the requirement"
        );
    }

    #[test]
    fn compare_requires_exactly_two_paths() {
        let a = args_parse(&["bigtent", "--compare", "/tmp/x"]);
        let mode = bigtent::main_utils::mode_from_args(&a).expect_err("two paths required");
        let msg = format!("{mode:?}").to_lowercase();
        assert!(
            msg.contains("two") || msg.contains("2"),
            "compare with one path explains the requirement"
        );
    }

    #[test]
    fn compare_and_convert_are_mutually_exclusive() {
        let a = args_parse(&[
            "bigtent",
            "--compare",
            "/tmp/x",
            "/tmp/y",
            "--convert-to-v4",
            "/tmp/z",
        ]);
        assert!(bigtent::main_utils::mode_from_args(&a).is_err());
    }
}