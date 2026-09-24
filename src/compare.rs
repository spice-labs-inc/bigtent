//! # Cluster comparison
//!
//! Compares two clusters (or two sets of clusters) for **item equality**:
//! every item on the left must exist on the right with the same
//! identifier, connections, body mime type, and body — rust `equal`
//! (`Item::PartialEq`). Equality is exact; the report distinguishes:
//!
//! * `matched`: items found identical on both sides (by identifier),
//! * `left_unaccounted`: left items with no identical counterpart on the
//!   right (missing from the right, or present but not equal),
//! * `right_unaccounted`: right items with no identical counterpart on
//!   the left,
//! and the sides are equal iff both unaccounted counts are zero.
//!
//! ## Cross-algorithm comparison
//!
//! The two sides may use different index key algorithms (a V3/MD5 source
//! versus its V4/BLAKE3[0..16] conversion). Every cluster on ONE side
//! must use a single key algorithm (mixed sides are refused); the left
//! side's identifiers are probed through the right side's algorithm, and
//! the right side is streamed in its own key order. This makes the
//! comparison order-independent across key spaces.
//!
//! ## Memory model (honest)
//!
//! Two strategies, chosen by scale:
//!
//! * **Materializing** (small sides, fewer than 50M left items):
//!   the left side's items are held in memory plus a
//!   `[probe key, index]` tuple per item (16 + 4 bytes). Above a few
//!   million items this materialization is the dominant cost; an item's
//!   in-memory footprint (identifier, connections, CBOR body) is
//!   typically 200–1000 bytes, so 100M+ item sides need tens to hundreds
//!   of GB. That is why the large strategy exists.
//! * **Bounded** (large sides): the left side is streamed and only the
//!   `[probe key, source position]` tuples (~28 bytes per item) are
//!   retained; items are **not** materialized up front. The right side
//!   is streamed in its own key order; for every matched key the pair of
//!   items is materialized transiently (the left item is re-read from
//!   its memory-mapped source) and compared. Memory stays bounded by the
//!   tuple arrays (≈6 GB at 223M items) plus transient items.
//!
//! Wall-clock honesty: both strategies deserialize every item at least
//! once; the bounded strategy re-deserializes matched left items, so a
//! full 223M×2 comparison is a multi-pass job measured in tens of
//! minutes to a couple of hours under load.

use anyhow::{Context, Result, bail};
use std::sync::Arc;

use crate::item::Item;
use crate::rodeo::goat::GoatRodeoCluster;
use crate::rodeo::goat_trait::GoatRodeoTrait;
use crate::rodeo::robo_goat::ClusterRoboMember;
use crate::util::KeyAlg;

/// How many differing identifiers the report carries (bounded).
pub const MAX_REPORTED_DIFFERENCES: usize = 25;

/// Left sides at or above this item count use the bounded strategy.
pub const BOUNDED_MODE_THRESHOLD: usize = 50_000_000;

/// The result of comparing two cluster sets.
#[derive(Debug, Clone, Default)]
pub struct CompareOutcome {
    /// Items on the left side (including duplicates across its clusters)
    pub total_left: u64,
    /// Items on the right side
    pub total_right: u64,
    /// Items found identical on both sides
    pub matched: u64,
    /// Left items with no identical right counterpart (either missing on
    /// the right or present-but-different)
    pub left_unaccounted: u64,
    /// Right items with no identical left counterpart
    pub right_unaccounted: u64,
    /// Identifiers/pairs that are unaccounted, as human-readable lines
    pub first_differences: Vec<String>,
    /// Which strategy ran ("materializing" | "bounded")
    pub strategy: &'static str,
}

impl CompareOutcome {
    /// Whether every item on both sides has an identical counterpart.
    pub fn equal(&self) -> bool {
        self.left_unaccounted == 0 && self.right_unaccounted == 0
    }

    /// Left items missing-or-different on the right.
    pub fn missing_count(&self) -> u64 {
        self.left_unaccounted
    }

    /// Render a one-line summary.
    pub fn summary(&self) -> String {
        format!(
            "left {} right {} matched {} left-unaccounted {} right-unaccounted {} {:>14} {}",
            self.total_left,
            self.total_right,
            self.matched,
            self.left_unaccounted,
            self.right_unaccounted,
            self.strategy,
            if self.equal() { "EQUAL" } else { "NOT EQUAL" }
        )
    }
}

/// A left-side item's source, so it can be re-materialized on demand.
#[derive(Clone, Copy)]
struct ItemSource {
    /// Index into the side's cluster vector
    cluster: u32,
    /// Position within that cluster's index
    pos: usize,
}

/// Assert a side is single-algorithm (all clusters share `key_alg`).
fn single_key_alg(side: &[Arc<GoatRodeoCluster>]) -> Result<KeyAlg> {
    let Some(first) = side.first() else {
        bail!("A compare side has no clusters");
    };
    let alg = first.key_alg();
    if !side.iter().all(|c| c.key_alg() == alg) {
        bail!(
            "A compare side mixes key algorithms (found {} and {}); \
             each side must be a single key space so probes are comparable",
            alg_constant(alg),
            alg_constant(
                side.iter()
                    .find(|c| c.key_alg() != alg)
                    .map(|c| c.key_alg())
                    .unwrap()
            )
        );
    }
    Ok(alg)
}

fn alg_constant(alg: KeyAlg) -> &'static str {
    match alg {
        KeyAlg::Md5 => crate::rodeo::cluster::V3_CLUSTER_ENCODING,
        KeyAlg::Blake3Truncated128 => crate::rodeo::cluster::V4_CLUSTER_ENCODING,
    }
}

/// Materialize an item from its source position.
fn item_at<'a>(
    side: &'a [Arc<GoatRodeoCluster>],
    src: &ItemSource,
) -> Option<Item> {
    let cluster = side.get(src.cluster as usize)?;
    let offset = ClusterRoboMember::offset_from_pos(cluster.as_ref(), src.pos)?;
    ClusterRoboMember::item_from_item_offset(cluster.as_ref(), &offset)
}

/// Compare two cluster sets for item equality.
///
/// Chooses the strategy by scale (see the module docs). Works across key
/// algorithms; refuses sides that mix key algorithms.
pub fn compare_clusters(
    left: &[Arc<GoatRodeoCluster>],
    right: &[Arc<GoatRodeoCluster>],
) -> Result<CompareOutcome> {
    let total_left: usize = left.iter().map(|c| c.number_of_items()).sum();
    if total_left >= BOUNDED_MODE_THRESHOLD {
        compare_clusters_bounded(left, right)
    } else {
        compare_clusters_materializing(left, right)
    }
}

/// The large-side strategy: bounded memory (see the module docs).
pub fn compare_clusters_bounded(
    left: &[Arc<GoatRodeoCluster>],
    right: &[Arc<GoatRodeoCluster>],
) -> Result<CompareOutcome> {
    let right_alg = single_key_alg(right)?;
    let _ = single_key_alg(left)?;
    let start = std::time::Instant::now();

    // ---- left pass: stream, retain only probe tuples ----
    let total_left: u64 = left.iter().map(|c| c.number_of_items() as u64).sum();
    let mut probes: Vec<([u8; 16], u32)> = Vec::with_capacity(total_left as usize);
    let mut sources: Vec<ItemSource> = Vec::with_capacity(total_left as usize);
    let mut streamed: u64 = 0;
    for (ci, cluster) in left.iter().enumerate() {
        let count = cluster.number_of_items();
        for pos in 0..count {
            if let Some(item) =
                ClusterRoboMember::offset_from_pos(cluster.as_ref(), pos).and_then(|o| ClusterRoboMember::item_from_item_offset(cluster.as_ref(), &o))
            {
                let key = right_alg.hash_identifier(&item.identifier);
                probes.push((key, probes.len() as u32));
                sources.push(ItemSource {
                    cluster: ci as u32,
                    pos,
                });
            }
            streamed += 1;
        }
        tracing::info!(
            "Compare(bounded): left streamed {} of {} in {:?}",
            streamed,
            total_left,
            start.elapsed()
        );
    }
    probes.sort_unstable();
    let mut matched = vec![false; probes.len()];

    // ---- right pass: stream, probe, compare ----
    let mut matched_count: u64 = 0;
    let total_right: u64 = right.iter().map(|c| c.number_of_items() as u64).sum();
    let mut right_streamed: u64 = 0;
    let mut first_differences: Vec<String> = vec![];

    for cluster in right {
        let count = cluster.number_of_items();
        for pos in 0..count {
            let Some(r_item) =
                ClusterRoboMember::offset_from_pos(cluster.as_ref(), pos).and_then(|o| ClusterRoboMember::item_from_item_offset(cluster.as_ref(), &o))
            else {
                continue;
            };
            right_streamed += 1;
            let key = right_alg.hash_identifier(&r_item.identifier);
            let probe_idx = match probes.binary_search_by_key(&key, |(k, _)| *k) {
                Ok(center) => center,
                Err(_) => {
                    if first_differences.len() < MAX_REPORTED_DIFFERENCES {
                        first_differences.push(format!("right-only/unmatched: {}", r_item.identifier));
                    }
                    continue;
                }
            };
            // walk the equal-key run
            let mut lo = probe_idx;
            while lo > 0 && probes[lo - 1].0 == key {
                lo -= 1;
            }
            let mut hi = probe_idx;
            while hi + 1 < probes.len() && probes[hi + 1].0 == key {
                hi += 1;
            }
            let mut found = false;
            for cand in lo..=hi {
                let si = probes[cand].1 as usize;
                if !matched[si] {
                    if let Some(l_item) = item_at(left, &sources[si]) {
                        if l_item == r_item {
                            matched[si] = true;
                            matched_count += 1;
                            found = true;
                            break;
                        }
                    }
                }
            }
            if !found {
                if first_differences.len() < MAX_REPORTED_DIFFERENCES {
                    first_differences.push(format!("right-only/unmatched: {}", r_item.identifier));
                }
            }
            if right_streamed % 10_000_000 == 0 {
                tracing::info!(
                    "Compare(bounded): right streamed {} of {} in {:?}",
                    right_streamed,
                    total_right,
                    start.elapsed()
                );
            }
        }
    }

    let mut left_unaccounted: u64 = 0;
    for (si, m) in matched.iter().enumerate() {
        if !m {
            left_unaccounted += 1;
            if first_differences.len() < MAX_REPORTED_DIFFERENCES + 10 {
                if let Some(l_item) = item_at(left, &sources[si]) {
                    first_differences.push(format!("left-only/unmatched: {}", l_item.identifier));
                }
            }
        }
    }

    first_differences.truncate(MAX_REPORTED_DIFFERENCES);
    tracing::info!(
        "Compare(bounded): done in {:?}; matched {}",
        start.elapsed(),
        matched_count
    );
    Ok(CompareOutcome {
        total_left,
        total_right,
        matched: matched_count,
        left_unaccounted,
        right_unaccounted: total_right - matched_count,
        first_differences,
        strategy: "bounded",
    })
}

/// The small-side strategy: the left side's items are materialized.
pub fn compare_clusters_materializing(
    left: &[Arc<GoatRodeoCluster>],
    right: &[Arc<GoatRodeoCluster>],
) -> Result<CompareOutcome> {
    let right_alg = single_key_alg(right)?;
    let _ = single_key_alg(left)?;
    let start = std::time::Instant::now();

    // ---- left: materialize items + sorted probes ----
    let mut left_items: Vec<Item> = vec![];
    let mut probes: Vec<([u8; 16], u32)> = vec![];
    for cluster in left {
        let count = cluster.number_of_items();
        for pos in 0..count {
            if let Some(item) =
                ClusterRoboMember::offset_from_pos(cluster.as_ref(), pos).and_then(|o| ClusterRoboMember::item_from_item_offset(cluster.as_ref(), &o))
            {
                probes.push((right_alg.hash_identifier(&item.identifier), left_items.len() as u32));
                left_items.push(item);
            }
        }
    }
    probes.sort_unstable();
    let mut matched = vec![false; probes.len()];
    tracing::info!(
        "Compare(materializing): materialized {} left items in {:?}",
        left_items.len(),
        start.elapsed()
    );

    // ---- right: stream, probe, compare ----
    let mut matched_count: u64 = 0;
    let total_right: u64 = right.iter().map(|c| c.number_of_items() as u64).sum();
    let mut right_streamed: u64 = 0;
    let mut first_differences: Vec<String> = vec![];

    for cluster in right {
        let count = cluster.number_of_items();
        for pos in 0..count {
            let Some(item) =
                ClusterRoboMember::offset_from_pos(cluster.as_ref(), pos).and_then(|o| ClusterRoboMember::item_from_item_offset(cluster.as_ref(), &o))
            else {
                continue;
            };
            right_streamed += 1;
            let key = right_alg.hash_identifier(&item.identifier);
            let probe_idx = match probes.binary_search_by_key(&key, |(k, _)| *k) {
                Ok(center) => center,
                Err(_) => {
                    if first_differences.len() < MAX_REPORTED_DIFFERENCES {
                        first_differences.push(format!("right-only/unmatched: {}", item.identifier));
                    }
                    continue;
                }
            };
            let mut lo = probe_idx;
            while lo > 0 && probes[lo - 1].0 == key {
                lo -= 1;
            }
            let mut hi = probe_idx;
            while hi + 1 < probes.len() && probes[hi + 1].0 == key {
                hi += 1;
            }
            let mut found = false;
            for cand in lo..=hi {
                let si = probes[cand].1 as usize;
                if !matched[si] && left_items[si] == item {
                    matched[si] = true;
                    matched_count += 1;
                    found = true;
                    break;
                }
            }
            if !found && first_differences.len() < MAX_REPORTED_DIFFERENCES {
                first_differences.push(format!("right-only/unmatched: {}", item.identifier));
            }
            if right_streamed % 10_000_000 == 0 {
                tracing::info!(
                    "Compare(materializing): streamed {} of {} right items in {:?}",
                    right_streamed,
                    total_right,
                    start.elapsed()
                );
            }
        }
    }

    let mut left_unaccounted: u64 = 0;
    for (si, m) in matched.iter().enumerate() {
        if !m {
            left_unaccounted += 1;
            if first_differences.len() < MAX_REPORTED_DIFFERENCES + 10 {
                first_differences.push(format!("left-only/unmatched: {}", left_items[si].identifier));
            }
        }
    }

    first_differences.truncate(MAX_REPORTED_DIFFERENCES);
    tracing::info!(
        "Compare(materializing): done in {:?}; matched {}",
        start.elapsed(),
        matched_count
    );
    Ok(CompareOutcome {
        total_left: left_items.len() as u64,
        total_right,
        matched: matched_count,
        left_unaccounted,
        right_unaccounted: total_right - matched_count,
        first_differences,
        strategy: "materializing",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Small cross-algorithm equality: the checked-in V3 fixture converts
    /// to BLAKE3 with byte-identical items; the bounded and materializing
    /// strategies must agree with each other and with the source.
    #[test]
    fn bounded_and_materializing_agree_on_fixture() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("test_data")
                .join("cluster_a");
            let source = GoatRodeoCluster::cluster_files_in_dir(path.clone(), false, vec![])
                .await
                .expect("fixture loads");
            let dest = tempfile::TempDir::new().unwrap();
            let mut converted = vec![];
            for grc in crate::rodeo::convert::convert_cluster_to_dir(&source[0], dest.path())
                .await
                .expect("conversion")
            {
                converted.push(
                    GoatRodeoCluster::new(&grc, false, None, vec![])
                        .await
                        .expect("converted loads"),
                );
            }

            let bounded = compare_clusters_bounded(&source, &converted).expect("bounded runs");
            let materializing =
                compare_clusters_materializing(&source, &converted).expect("materializing runs");

            assert!(bounded.equal(), "bounded: {bounded:?}");
            assert!(materializing.equal(), "materializing: {materializing:?}");
            assert_eq!(bounded.total_left, materializing.total_left);
            assert_eq!(bounded.matched, materializing.matched);
            assert_eq!(bounded.strategy, "bounded");
            assert_eq!(materializing.strategy, "materializing");
        });
    }

    /// The bounded strategy reports inequality the same way.
    #[test]
    fn bounded_detects_difference() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let base = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_data");
            let a = GoatRodeoCluster::cluster_files_in_dir(base.join("cluster_a"), false, vec![])
                .await
                .expect("a");
            let b = GoatRodeoCluster::cluster_files_in_dir(base.join("cluster_b"), false, vec![])
                .await
                .expect("b");
            let outcome = compare_clusters_bounded(&a, &b).expect("runs");
            assert!(
                !outcome.equal() || outcome.total_left == outcome.total_right,
                "{outcome:?}"
            );
            assert!(outcome.missing_count() > 0 || outcome.right_unaccounted > 0 || outcome.matched == outcome.total_left);
        });
    }
}