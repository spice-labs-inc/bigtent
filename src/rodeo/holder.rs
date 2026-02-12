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
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, HistogramVec, Opts, Registry,
    TextEncoder,
};
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
    pub version: String,
    pub git_sha: String,
    pub status: String,
}

/// Prometheus metric handles for the BigTent server.
#[derive(Clone)]
pub struct MetricsHandles {
    pub registry: Registry,
    pub http_requests_total: CounterVec,
    pub http_request_duration_seconds: HistogramVec,
    pub node_count: Gauge,
    pub cluster_count: Gauge,
    pub uptime_seconds: Gauge,
    pub reload_total: Counter,
    pub reload_last_success_timestamp: Gauge,
}

impl MetricsHandles {
    fn new() -> Result<Self> {
        let registry = Registry::new_custom(None, None)?;

        // Register process metrics
        let process_collector = prometheus::process_collector::ProcessCollector::for_self();
        registry.register(Box::new(process_collector))?;

        let http_requests_total = CounterVec::new(
            Opts::new("bigtent_http_requests_total", "Total number of HTTP requests"),
            &["method", "path", "status"],
        )?;
        registry.register(Box::new(http_requests_total.clone()))?;

        let http_request_duration_seconds = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "bigtent_http_request_duration_seconds",
                "HTTP request duration in seconds",
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &["method", "path"],
        )?;
        registry.register(Box::new(http_request_duration_seconds.clone()))?;

        let node_count = Gauge::new("bigtent_node_count", "Number of nodes loaded")?;
        registry.register(Box::new(node_count.clone()))?;

        let cluster_count = Gauge::new("bigtent_cluster_count", "Number of clusters loaded")?;
        registry.register(Box::new(cluster_count.clone()))?;

        let uptime_seconds = Gauge::new("bigtent_uptime_seconds", "Server uptime in seconds")?;
        registry.register(Box::new(uptime_seconds.clone()))?;

        let reload_total = Counter::new("bigtent_reload_total", "Total number of successful reloads")?;
        registry.register(Box::new(reload_total.clone()))?;

        let reload_last_success_timestamp = Gauge::new(
            "bigtent_reload_last_success_timestamp",
            "Unix timestamp of last successful reload",
        )?;
        registry.register(Box::new(reload_last_success_timestamp.clone()))?;

        Ok(MetricsHandles {
            registry,
            http_requests_total,
            http_request_duration_seconds,
            node_count,
            cluster_count,
            uptime_seconds,
            reload_total,
            reload_last_success_timestamp,
        })
    }

    /// Encode all metrics as Prometheus text format.
    pub fn encode(&self) -> Result<String> {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }
}

pub struct ClusterHolder<GRT: GoatRodeoTrait> {
    cluster: ArcSwap<GRT>,
    args: Arc<Args>,
    start_time: Instant,
    last_reload_epoch_ms: AtomicU64,
    cluster_count: AtomicU64,
    metrics: MetricsHandles,
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

    /// Get a reference to the metrics handles.
    pub fn metrics(&self) -> &MetricsHandles {
        &self.metrics
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

        // Update Prometheus metrics
        self.metrics.reload_total.inc();
        self.metrics.reload_last_success_timestamp.set((epoch_ms / 1000) as f64);
        self.metrics.cluster_count.set(cluster_count as f64);
        self.metrics.node_count.set(self.cluster.load().node_count() as f64);
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
        let status = if node_count > 0 { "ok" } else { "degraded" }.to_string();

        // Update Prometheus gauges
        self.metrics.uptime_seconds.set(uptime_seconds as f64);
        self.metrics.node_count.set(node_count as f64);
        self.metrics.cluster_count.set(cluster_count as f64);

        HealthInfo {
            node_count,
            cluster_count,
            last_reload_at,
            uptime_seconds,
            version: env!("CARGO_PKG_VERSION").to_string(),
            git_sha: option_env!("VERGEN_GIT_SHA").unwrap_or("unknown").to_string(),
            status,
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
        let metrics = MetricsHandles::new()?;

        let ret = Arc::new(ClusterHolder {
            cluster,
            args,
            start_time: Instant::now(),
            last_reload_epoch_ms: AtomicU64::new(0),
            cluster_count: AtomicU64::new(0),
            metrics,
        });

        Ok(ret)
    }
}
