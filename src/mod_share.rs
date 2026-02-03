//! # Shared Types for Cluster Operations
//!
//! This module provides shared types used across multiple modules for
//! cluster traversal and merge operations.
//!
//! ## Key Types
//!
//! - [`ClusterPos`] - Tracks position within a cluster during iteration
//! - [`next_hash_of_item_to_merge`] - Finds the next item(s) to merge across clusters
//!
//! ## Usage in Merging
//!
//! During a merge operation, multiple clusters are iterated in parallel.
//! `ClusterPos` tracks the current position in each cluster, and
//! `next_hash_of_item_to_merge` finds all items with the lowest hash
//! across all clusters (items that need to be merged together).

use std::sync::Arc;
use crate::{
  rodeo::{goat::GoatRodeoCluster, index::EitherItemOffset},
  util::MD5Hash,
};

pub struct ClusterPos {
  pub cluster: Arc<GoatRodeoCluster>,
  pub pos: usize,
  pub len: usize,
}

impl ClusterPos {
  pub async fn this_item(&self) -> Option<EitherItemOffset> {
    if self.pos >= self.len {
      None
    } else {
      match self.cluster.offset_from_pos(self.pos).await {
        Ok(v) => Some(v),
        _ => None,
      }
    }
  }

  pub fn next(&mut self) {
    self.pos += 1
  }
}

pub async fn next_hash_of_item_to_merge(
  index_holder: &mut Vec<ClusterPos>,
) -> Option<Vec<(EitherItemOffset, Arc<GoatRodeoCluster>)>> {
  let mut lowest: Option<MD5Hash> = None;
  let mut low_clusters = vec![];

  for holder in index_holder {
    let this_item = holder.this_item().await;
    match (&lowest, this_item) {
      (None, Some(either)) => {
        // found the first
        lowest = Some(*either.hash());
        low_clusters.push((either, holder));
      }
      // it's the lowe
      (Some(low), Some(either)) if low == either.hash() => {
        low_clusters.push((either, holder));
      }
      (Some(low), Some(either)) if low > either.hash() => {
        lowest = Some(*either.hash());
        low_clusters.clear();
        low_clusters.push((either,holder));
      }
      _ => {}
    }
  }

  match lowest {
    None => None,
    Some(_) => {
      let mut clusters = vec![];
      for (offset, holder) in low_clusters {
        clusters.push((offset, holder.cluster.clone()));
        holder.next();
      }
      Some(clusters)
    }
  }
}
