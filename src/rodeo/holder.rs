//! # ClusterHolder - Runtime Cluster Management
//!
//! This module provides the [`ClusterHolder`] type which manages the runtime
//! lifecycle of a cluster, including support for hot-reloading via `ArcSwap`.
//!
//! ## Hot Reload Support
//!
//! `ClusterHolder` uses [`arc_swap::ArcSwap`] to enable atomic cluster replacement
//! without service interruption. When a SIGHUP signal is received, the cluster
//! can be reloaded from disk and swapped in atomically.
//!
//! ## Thread Safety
//!
//! All operations on `ClusterHolder` are thread-safe:
//! - `get_cluster()` - Returns an `Arc` to the current cluster
//! - `update_cluster()` - Atomically swaps in a new cluster
//!
//! ## Usage
//!
//! ```rust,no_run
//! use bigtent::rodeo::goat_herd::GoatHerd;
//! use bigtent::rodeo::holder::ClusterHolder;
//! use arc_swap::ArcSwap;
//! use std::sync::Arc;
//!
//! // Given a GoatHerd (created from loaded clusters):
//! fn use_holder(holder: Arc<ClusterHolder<GoatHerd>>) {
//!     // Get current cluster for queries
//!     let current: Arc<GoatHerd> = holder.get_cluster();
//!
//!     // Hot-reload: atomically swap in a new cluster
//!     // holder.update_cluster(Arc::new(new_herd));
//! }
//! ```

use anyhow::Result;
use arc_swap::ArcSwap;
use std::{
    path::PathBuf,
    sync::{Arc, atomic::{AtomicU64, Ordering}},
    time::Instant,
};

use crate::config::Args;

use super::goat_trait::GoatRodeoTrait;

/// Health information snapshot returned by `/health`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct HealthInfo {
    pub node_count: u64,
    pub cluster_count: u64,
    pub last_reload_at: Option<String>,
    pub uptime_seconds: u64,
}

pub struct ClusterHolder<GRT: GoatRodeoTrait> {
    cluster: ArcSwap<GRT>,
    args: Arc<Args>,
    start_time: Instant,
    last_reload_epoch_ms: AtomicU64,
    cluster_count: AtomicU64,
}

impl<GRT: GoatRodeoTrait> std::fmt::Debug for ClusterHolder<GRT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterHolder")
            .field("args", &self.args)
            .field("cluster_count", &self.cluster_count.load(Ordering::Relaxed))
            .finish()
    }
}

impl<GRT: GoatRodeoTrait> ClusterHolder<GRT> {
    /// update the cluster
    pub fn update_cluster(&self, new_cluster: Arc<GRT>) {
        self.cluster.store(new_cluster);
    }

    /// Get the current GoatRodeoCluster from the server
    pub fn get_cluster(&self) -> Arc<GRT> {
        self.cluster.load().clone()
    }

    pub fn the_args(&self) -> Arc<Args> {
        self.args.clone()
    }

    /// get the path to the `purls.txt` file
    pub fn get_purl(&self) -> Result<PathBuf> {
        self.cluster.load().get_purl()
    }

    /// Record that a reload has just occurred, updating health metadata.
    pub fn record_reload(&self, cluster_count: u64) {
        use std::time::{SystemTime, UNIX_EPOCH};
        let epoch_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_reload_epoch_ms.store(epoch_ms, Ordering::Relaxed);
        self.cluster_count.store(cluster_count, Ordering::Relaxed);
    }

    /// Return a snapshot of health information.
    pub fn health_info(&self) -> HealthInfo {
        let node_count = self.cluster.load().node_count();
        let cluster_count = self.cluster_count.load(Ordering::Relaxed);
        let epoch_ms = self.last_reload_epoch_ms.load(Ordering::Relaxed);
        let last_reload_at = if epoch_ms == 0 {
            None
        } else {
            // Convert epoch millis to RFC 3339
            let secs = (epoch_ms / 1000) as i64;
            let nanos = ((epoch_ms % 1000) * 1_000_000) as u32;
            chrono::DateTime::from_timestamp(secs, nanos)
                .map(|dt| dt.to_rfc3339())
        };
        let uptime_seconds = self.start_time.elapsed().as_secs();

        HealthInfo {
            node_count,
            cluster_count,
            last_reload_at,
            uptime_seconds,
        }
    }

    /// Create a new ClusterHolder based on an existing GoatRodeo instance with the number of threads
    /// and an optional set of args. This is meant to be used to create an Index without
    /// a well defined set of Args
    pub async fn new_from_cluster(
        cluster: ArcSwap<GRT>,
        args_opt: Option<Arc<Args>>,
    ) -> Result<Arc<ClusterHolder<GRT>>> {
        let args = args_opt.unwrap_or_default();

        let ret = Arc::new(ClusterHolder {
            cluster,
            args,
            start_time: Instant::now(),
            last_reload_epoch_ms: AtomicU64::new(0),
            cluster_count: AtomicU64::new(0),
        });

        Ok(ret)
    }
}
