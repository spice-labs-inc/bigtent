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
use std::{path::PathBuf, sync::Arc};

use crate::config::Args;

use super::goat_trait::GoatRodeoTrait;

#[derive(Debug)]
pub struct ClusterHolder<GRT: GoatRodeoTrait> {
    cluster: ArcSwap<GRT>,
    args: Arc<Args>,
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

    /// Create a new ClusterHolder based on an existing GoatRodeo instance with the number of threads
    /// and an optional set of args. This is meant to be used to create an Index without
    /// a well defined set of Args
    pub async fn new_from_cluster(
        cluster: ArcSwap<GRT>,
        args_opt: Option<Arc<Args>>,
    ) -> Result<Arc<ClusterHolder<GRT>>> {
        let args = args_opt.unwrap_or_default();

        let ret = Arc::new(ClusterHolder {
            cluster: cluster,
            args: args,
        });

        Ok(ret)
    }
}
