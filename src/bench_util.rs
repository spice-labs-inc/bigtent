//! # Benchmark Utilities
//!
//! This module provides utilities for generating synthetic test data and
//! collecting performance metrics during benchmark runs.

use crate::item::{CONTAINED_BY, CONTAINS, Item};
use crate::rodeo::goat_trait::GoatRodeoTrait;
use crate::rodeo::robo_goat::{ClusterRoboMember, RoboticGoat};
use crate::rodeo::writer::ClusterWriter;
use anyhow::Result;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use serde_cbor::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

fn seed_to_array(seed: u64) -> [u8; 32] {
    let mut arr = [0u8; 32];
    arr[0..8].copy_from_slice(&seed.to_le_bytes());
    arr
}

/// Synthetic item generator for creating test data
pub struct SyntheticItemGenerator {
    overlap_strategy: OverlapStrategy,
    seed: u64,
}

/// Strategy for generating overlapping items between clusters
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverlapStrategy {
    /// Items can appear in any cluster randomly
    Random,
    /// Items appear in clusters in a deterministic pattern
    Pattern,
}

impl SyntheticItemGenerator {
    pub fn new(overlap_strategy: OverlapStrategy, seed: u64) -> Self {
        Self {
            overlap_strategy,
            seed,
        }
    }

    /// Generate base items with realistic metadata
    pub fn generate_base_items(&self, count: usize) -> Vec<Item> {
        let mut rng = StdRng::from_seed(seed_to_array(self.seed));
        let mut items = Vec::with_capacity(count);

        for i in 0..count {
            let item = self.generate_item(i, &mut rng);
            items.push(item);
        }

        items
    }

    /// Generate items for a cluster with specified overlap percentage
    pub fn generate_overlap_cluster(
        &self,
        base_items: &[Item],
        cluster_num: usize,
        overlap_pct: f64,
    ) -> Vec<Item> {
        let count = base_items.len();
        let overlap_count = (count as f64 * overlap_pct / 100.0) as usize;
        let unique_count = count - overlap_count;
        let mut rng = StdRng::from_seed(seed_to_array(self.seed + cluster_num as u64));

        let mut items = Vec::with_capacity(count);

        // Select overlapping items deterministically
        for i in 0..overlap_count {
            let item_idx = if self.overlap_strategy == OverlapStrategy::Random {
                rng.random_range(0..count)
            } else {
                (i * 3 + cluster_num) % count
            };
            items.push(base_items[item_idx].clone());
        }

        // Generate unique items for this cluster
        let base_offset = count * (cluster_num + 1);
        for i in 0..unique_count {
            let item = self.generate_item(base_offset + i, &mut rng);
            items.push(item);
        }

        items
    }

    fn generate_item(&self, idx: usize, rng: &mut StdRng) -> Item {
        let gitoid = format!("gitoid:blob:sha256:{:016x}", idx);
        let file_size = rng.random_range(100..10000);

        let mut connections = BTreeSet::new();

        // Add some random connections
        let num_connections = rng.random_range(0..5);
        for _ in 0..num_connections {
            if rng.random_bool(0.7) {
                let target_idx = if idx > 0 { rng.random_range(0..idx) } else { 0 };
                let target = format!("gitoid:blob:sha256:{:016x}", target_idx);
                if rng.random_bool(0.5) {
                    connections.insert((CONTAINS.to_string(), target));
                } else {
                    connections.insert((CONTAINED_BY.to_string(), target));
                }
            }
        }

        // Add a PURL alias for ~20% of items
        if rng.random_bool(0.2) {
            let purl = if rng.random_bool(0.5) {
                let minor = rng.random_range(1..10);
                let patch = rng.random_range(0..10);
                format!("pkg:npm/package{}@{}.{}", idx % 1000, minor, patch)
            } else {
                let major = rng.random_range(1..10);
                let minor = rng.random_range(0..10);
                format!("pkg:maven/org.example{}@{}.{}", idx % 100, major, minor)
            };
            connections.insert(("alias:from".to_string(), purl));
        }

        let body = Some(Value::Map(BTreeMap::from_iter(vec![
            (
                serde_cbor::Value::Text("file_names".to_string()),
                serde_cbor::Value::Array(vec![serde_cbor::Value::Text(format!("file_{}.rs", idx))]),
            ),
            (
                serde_cbor::Value::Text("file_size".to_string()),
                serde_cbor::Value::Integer(file_size as i128),
            ),
            (
                serde_cbor::Value::Text("language".to_string()),
                serde_cbor::Value::Text(
                    (if rng.random_bool(0.5) {
                        "rust"
                    } else {
                        "javascript"
                    })
                    .to_string(),
                ),
            ),
        ])));

        Item {
            identifier: gitoid,
            connections,
            body_mime_type: Some("application/vnd.cc.goatrodeo".to_string()),
            body,
        }
    }
}

/// Write a RoboticGoat cluster to disk as a persistent cluster
pub async fn write_cluster_to_disk(
    cluster: Arc<RoboticGoat>,
    output_dir: &PathBuf,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(output_dir).await?;

    let mut cluster_writer = ClusterWriter::new(output_dir).await?;

    // Write all items
    let num_items = cluster.number_of_items();
    for pos in 0..num_items {
        if let Some(offset) = cluster.offset_from_pos(pos) {
            if let Some(item) = cluster.item_from_item_offset(&offset) {
                let cbor_bytes = serde_cbor::to_vec(&item)?;
                cluster_writer.write_item(item, cbor_bytes).await?;
            }
        }
    }

    let cluster_file = cluster_writer.finalize_cluster().await?;

    // Write history
    let history_file = cluster_file
        .canonicalize()?
        .parent()
        .expect("Should have cluster parent dir")
        .join("history.jsonl");

    let history = cluster.read_history()?;
    serde_jsonlines::write_json_lines(&history_file, &history)?;

    Ok(cluster_file)
}

/// Generate synthetic cluster and write to disk
pub async fn generate_synthetic_cluster(
    name: &str,
    items: Vec<Item>,
    output_dir: PathBuf,
) -> Result<PathBuf> {
    let cluster = RoboticGoat::new(name, items, serde_json::json!({}));
    write_cluster_to_disk(cluster, &output_dir).await
}
