//! # Fresh Merge - Cluster Merging Algorithm
//!
//! This module implements the "fresh merge" algorithm for combining multiple
//! BigTent clusters into a single new cluster without preserving merge history.
//!
//! ## Algorithm Overview
//!
//! The merge process works as follows:
//!
//! 1. **Initialization**: Load all source clusters and create position trackers
//! 2. **Coordinator Thread**: Finds items with matching hashes across clusters
//! 3. **Worker Threads**: Fetch and merge items in parallel (20 workers)
//! 4. **Main Thread**: Writes merged items to the output cluster
//!
//! ## Threading Model
//!
//! ```text
//! ┌─────────────┐     ┌──────────────┐     ┌─────────────┐
//! │ Coordinator │────>│ Worker Pool  │────>│ Main Thread │
//! │   Thread    │     │ (   threads) │     │  (Writer)   │
//! └─────────────┘     └──────────────┘     └─────────────┘
//!       │                    │                    │
//!       │ finds items        │ fetches &          │ writes to
//!       │ to merge           │ merges items       │ output cluster
//!       ▼                    ▼                    ▼
//!   index_holder        flume channels      ClusterWriter
//! ```
//!
//! ## Memory Management
//!
//! The `merge_buffer_limit` parameter controls backpressure:
//! - Limits items queued for writing
//! - Prevents memory exhaustion on large merges
//! - Default: 10,000 items
//!
//! ## Output Files
//!
//! The merge produces:
//! - `.grc` cluster file with metadata
//! - `.gri` index files (GitOID → offset mapping)
//! - `.grd` data files (CBOR-encoded Items)
//! - `purls.txt` - Package URL listing
//! - `cluster_info.jsonl` - Cluster metadata in JSON Lines format

use rustc_hash::FxHashMap;
use serde_cbor::value::{from_value, to_value};
use serde_json::json;
use serde_jsonlines::write_json_lines;
use std::{
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet},
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::{self},
    time::{Duration, Instant},
};
use tracing::error;
#[cfg(not(test))]
use tracing::info; // Use log crate when building application

#[cfg(test)]
use std::println as info;

use crate::{
    item::{ITEM_METADATA_MIME_TYPE, Item, ItemMetaData},
    rodeo::{
        goat_trait::GoatRodeoTrait,
        index::{HasHash, ItemOffset},
        member::HerdMember,
        robo_goat::ClusterRoboMember,
        writer::ClusterWriter,
    },
    util::{MD5Hash, NiceDurationDisplay, iso8601_now, md5hash_str},
};
use anyhow::{Context, Result, bail};
use thousands::Separable;

/// Merge multiple clusters into a single new cluster.
///
/// This is the main entry point for the "fresh merge" operation, which combines
/// items from multiple source clusters into a single output cluster.
///
/// ## Algorithm Overview
///
/// 1. **Initialization Phase**
///    - Create output directory
///    - Load position trackers for all source clusters
///    - Initialize the cluster writer
///
/// 2. **Coordinator Thread**
///    - Iterates through all clusters in sorted order (by MD5 hash)
///    - Finds items with matching hashes across clusters (need merging)
///    - Sends work to worker threads via channel
///    - Implements backpressure via `merge_buffer_limit`
///
/// 3. **Worker Threads (threads)**
///    - Receive item offsets from coordinator
///    - Fetch actual Items from source clusters
///    - Merge items with same identifier
///    - Extract PURLs for the purls.txt file
///    - Send merged items to main thread
///
/// 4. **Main Thread (Writer)**
///    - Receives merged items from workers
///    - Writes to output cluster via ClusterWriter
///    - Maintains ordering via position-based sorting
///
/// ## Parameters
///
/// - `clusters`: Source clusters to merge
/// - `merge_buffer_limit`: Max items in processing queue (backpressure control)
/// - `dest_directory`: Output directory for the merged cluster
/// - `block_list`: Identifiers that should be excluded from the merged output
/// - `is_live`: Atomic flag to signal early termination
/// - `merge_buffer_size_gb`: Max size of each in-memory `.grd` data buffer in GB
///
/// ## Output Files
///
/// - `cluster_<timestamp>.grc` - Cluster metadata
/// - `index_<hash>.gri` - Index file(s)
/// - `data_<hash>.grd` - Data file(s)
/// - `purls.txt` - All Package URLs found
/// - `cluster_info.jsonl` - Merge metadata in JSON Lines format
///
/// ## Errors
///
/// Returns an error if `clusters` contains fewer than 2 members.
/// Callers must ensure at least 2 members are provided.
pub async fn merge_fresh<PB: Into<PathBuf>>(
    clusters: Vec<Arc<HerdMember>>,
    merge_buffer_limit: usize,
    dest_directory: PB,
    block_list: Arc<HashSet<String>>,
    is_live: Arc<AtomicBool>,
    merge_buffer_size_gb: usize,
) -> Result<()> {
    let start = Instant::now();
    let dest: PathBuf = dest_directory.into();

    if !is_live.load(Ordering::Relaxed) {
        bail!("Stopped running merge based on is_live");
    }

    // === PHASE 1: Initialization ===
    fs::create_dir_all(dest.clone()).with_context(|| format!("Failed reading {:?}", dest))?;
    let mut seen_purls = BTreeSet::new();

    info!(
        "Created target dir at {:?}",
        Instant::now().duration_since(start)
    );

    // Statistics tracking
    let mut loop_cnt = 0usize; // Total items processed
    let mut merge_cnt = 0usize; // Items that required merging (appeared in multiple clusters)
    let mut max_merge_len = 0usize; // Total items across all clusters (upper bound)

    // Position trackers for each source cluster
    let mut cluster_positions = vec![];

    for (idx, cluster) in clusters.iter().enumerate() {
        let index_len = cluster.number_of_items();

        cluster_positions.push(ClusterPos {
            cluster: cluster.clone(),
            pos: 0,
            cache: None,
            len: index_len,
        });

        info!(
            "Loaded cluster {} of {}, from {}",
            idx + 1,
            clusters.len(),
            cluster.name()
        );

        max_merge_len += index_len;
    }
    let cluster_pos_len = cluster_positions.len();
    let mut index_holder = IndexHolder::new(cluster_positions);

    info!(
        "Read {} indicies at {:?}",
        cluster_pos_len,
        Instant::now().duration_since(start)
    );

    let merge_buffer_size_bytes = merge_buffer_size_gb
        .checked_mul(1024 * 1024 * 1024)
        .with_context(|| {
            format!(
                "--merge-buffer-size {} GB is too large for this platform",
                merge_buffer_size_gb
            )
        })?;
    let mut cluster_writer = ClusterWriter::new_with_max_size(&dest, merge_buffer_size_bytes)
        .await
        .with_context(|| format!("Failed creating ClusterWriter for {:?}", dest))?;
    let merge_start = Instant::now();

    // === PHASE 2: Set up threading infrastructure ===
    //
    // Channel for coordinator -> workers: sends (position, items_to_merge)
    // Bounded to prevent unbounded memory growth
    let (offset_tx, offset_rx) = flume::bounded(1_000);

    // Atomic counter for backpressure control
    // Tracks how many items are currently being processed
    let holding_pen_gate = Arc::new(AtomicUsize::new(0));
    let mut threads = vec![];

    /// Check if we're under the buffer limit (can accept more work)
    fn check_limit(gate: &AtomicUsize, limit: usize) -> bool {
        gate.load(Ordering::Acquire) < limit
    }

    // === COORDINATOR THREAD ===
    // Finds items to merge by iterating through sorted indices
    let hpg = Arc::clone(&holding_pen_gate);
    let coorindator_handle = {
        let is_live = is_live.clone();
        thread::spawn(move || {
            let mut pos = 0usize; // Global position counter for ordering
            loop {
                // stop processing when not live
                if !is_live.load(Ordering::Relaxed) {
                    break;
                }

                // Only send more work if we're under the buffer limit
                if check_limit(&hpg, merge_buffer_limit) {
                    // Find all clusters that have the item with the lowest hash
                    // Returns None when all clusters are exhausted
                    match index_holder.next() {
                        Some(items_to_merge) => {
                            // Send work to worker threads
                            if is_live.load(Ordering::Relaxed) {
                                match offset_tx.send((pos, items_to_merge)) {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!(
                                            "Failed to send message in coorindator thread {e:?}"
                                        );
                                        break;
                                    }
                                };
                                pos += 1;
                            } else {
                                break;
                            }
                        }
                        // All clusters exhausted - we're done!
                        None => break,
                    }
                }
                // Backpressure: wait for workers to catch up
                else {
                    while !check_limit(&hpg, merge_buffer_limit) {
                        thread::sleep(Duration::from_millis(20));
                    }
                }
            }
        })
    };

    threads.push(coorindator_handle);

    // === WORKER THREADS ===
    // 50 threads fetch items from clusters and merge them
    // Channel for workers -> main thread: sends merged items
    let (merged_tx, merged_rx) = flume::bounded(1_000);
    for thread_num in 0..50 {
        let rx = offset_rx.clone();
        let tx = merged_tx.clone();
        let is_live = is_live.clone();
        let block_list = Arc::clone(&block_list);

        let processor_handle = thread::spawn(move || {
            // let mut cnt = 0usize;
            while let Ok((position, items_to_merge)) = rx.recv() {
                if !is_live.load(Ordering::Relaxed) {
                    break;
                }
                let start_merge = Instant::now();
                let mut to_merge = vec![];
                let mut purls = HashSet::new();
                let mut id: String = "".to_string();

                for (offset, cluster) in &items_to_merge {
                    let merge_final = match cluster.item_from_item_offset(offset) {
                        Some(v) => v,
                        None => {
                            break;
                        }
                    };

                    if block_list.contains(&merge_final.identifier) {
                        continue;
                    }

                    merge_final
                        .connections
                        .iter()
                        .filter(|v| v.1.starts_with("pkg:"))
                        .for_each(|v| {
                            purls.insert(v.1.to_string());
                        });
                    if id.len() == 0 {
                        id = merge_final.identifier.clone();
                    }
                    to_merge.push(merge_final);
                }

                if to_merge.is_empty() {
                    continue;
                }

                let merged_count = to_merge.len().saturating_sub(1);

                let mut top = {
                    let cmp = Some(ITEM_METADATA_MIME_TYPE.to_string());
                    let all_same = to_merge
                        .iter()
                        .all(|item| item.body_mime_type == cmp && item.body.is_some());

                    // if there's only 1 item, do no merging
                    if to_merge.len() == 1 {
                        to_merge.pop().unwrap()
                    } else if all_same {
                        let mut file_size = 0;
                        let mut connections = BTreeSet::new();
                        let mut bodies = Vec::with_capacity(to_merge.len());
                        for mut item in to_merge {
                            connections.append(&mut item.connections);

                            let body: ItemMetaData = from_value(item.body.unwrap()).unwrap();
                            file_size = body.file_size;
                            bodies.push(body);
                        }

                        let mut extra = BTreeMap::new();
                        let mut file_names = BTreeSet::new();
                        let mut mime_type = BTreeSet::new();

                        for mut body in bodies {
                            mime_type.append(&mut body.mime_type);
                            for (k, mut v) in body.extra {
                                match extra.get_mut(&k) {
                                    None => {
                                        extra.insert(k, v);
                                    }
                                    Some(vv) => {
                                        vv.append(&mut v);
                                    }
                                };
                            }

                            file_names.append(&mut body.file_names);
                        }

                        Item {
                            identifier: id,
                            connections,
                            body_mime_type: cmp.clone(),
                            body: Some(
                                to_value(ItemMetaData {
                                    extra,
                                    file_names,
                                    file_size,
                                    mime_type,
                                })
                                .unwrap(),
                            ),
                        }
                    } else {
                        let mut top = to_merge.pop().unwrap();

                        for i in to_merge {
                            top = top.merge(i);
                        }

                        top
                    }
                };

                top.connections
                    .retain(|(_, target)| !block_list.contains(target));

                let hash = md5hash_str(&top.identifier);
                let cbor_bytes = match serde_cbor::to_vec(&top) {
                    Ok(v) => v,
                    Err(e) => {
                        error!("Failed to CBOR serialize Item {top:?} error {e:?}");
                        continue;
                    }
                };

                let delta = Instant::now().duration_since(start_merge);
                if delta > Duration::from_secs(5) {
                    info!(
                        "Merge of {} with {} connections took {:?}",
                        top.identifier,
                        top.connections.len(),
                        delta
                    );
                }

                // only send if things are still alive
                if is_live.load(Ordering::Relaxed) {
                    match tx.send(ItemOrPurl::Item {
                        pos: position,
                        cbor_bytes,
                        merged: merged_count,
                        purls,
                        hash,
                    }) {
                        Ok(_) => {} // keep going
                        Err(e) => {
                            error!("Failed to send message in merge worker {thread_num} {e:?}");
                            break;
                        }
                    }
                } else {
                    break;
                };
            }
        });
        threads.push(processor_handle);
    }

    drop(merged_tx);
    drop(offset_rx); // make sure no more copies exist

    //// Main thread section
    ////

    let mut next_expected = 0usize;
    let mut last_log_loop_cnt = 0usize;
    let mut holding_pen = FxHashMap::default();

    while let Ok(item_or_purl) = merged_rx.recv_async().await {
        // stop processing when not live
        if !is_live.load(Ordering::Relaxed) {
            bail!("Merge Loop ending because no longer live");
        }
        match item_or_purl {
            ItemOrPurl::Item {
                pos: position,
                merged,
                cbor_bytes,
                purls,
                hash,
            } => {
                for p in purls {
                    if !seen_purls.contains(&p) {
                        //purl_file.write(format!("{}\n", p).as_bytes())?;
                        seen_purls.insert(p);
                    }
                }
                merge_cnt += merged;

                if position == next_expected {
                    // Insert new position into holding pen and increment atomic gate
                    holding_pen.insert(position, (cbor_bytes, hash));

                    while let Some((cbor_bytes, hash)) = holding_pen.remove(&next_expected) {
                        loop_cnt += 1;

                        // Decrement atomic gate for every item removed
                        cluster_writer
                            .write_item_with_hash(cbor_bytes, hash)
                            .await
                            .with_context(|| format!("Failed writing {:?}", dest))?;

                        // Log, but only once per 2M output items
                        if should_log_progress(loop_cnt, last_log_loop_cnt) {
                            last_log_loop_cnt = loop_cnt;
                            let diff = merge_start.elapsed();
                            let items_per_second = (loop_cnt as f64) / diff.as_secs_f64();
                            let (estimated_total_out, remaining_seconds, ratio) = compute_merge_eta(
                                loop_cnt,
                                merge_cnt,
                                max_merge_len,
                                items_per_second,
                            );

                            let nd: NiceDurationDisplay = remaining_seconds.into();
                            let td: NiceDurationDisplay = merge_start.elapsed().into();
                            info!(
                                "Merge cnt {}m of {}m merge cnt {}m ratio {:.2}:1 at {} estimated end {} written pURLs {} holding pen cnt {}",
                                (loop_cnt / 1_000_000).separate_with_commas(),
                                (estimated_total_out / 1_000_000).separate_with_commas(),
                                (merge_cnt / 1_000_000).separate_with_commas(),
                                ratio,
                                td,
                                nd,
                                seen_purls.len().separate_with_commas(),
                                holding_pen.len()
                            );
                        }

                        next_expected += 1;
                    }
                } else {
                    // Insert an unexpected position into the holding pen
                    holding_pen.insert(position, (cbor_bytes, hash));
                }

                // Store the current length of the holding pen
                holding_pen_gate.store(holding_pen.len(), Ordering::Release);
            }
        }
    }

    if !holding_pen.is_empty() {
        bail!(
            "Finished receiving, but holding pen is not empty {:?}",
            holding_pen
        );
    }

    {
        info!("Writing pURLs");
        let dest = dest.clone();
        tokio::task::spawn_blocking(move || {
            fn do_thing(dest: PathBuf, seen_purls: BTreeSet<String>) -> Result<()> {
                let mut purl_file = BufWriter::new(
                    File::create({
                        let mut dest = dest.clone();

                        dest.push("purls.txt");
                        dest
                    })
                    .with_context(|| format!("Failed creating {:?}", dest))?,
                );
                for purl in &seen_purls {
                    purl_file
                        .write_fmt(format_args!("{}\n", purl))
                        .with_context(|| format!("Failed writing {:?}", dest))?;
                }
                purl_file
                    .flush()
                    .with_context(|| format!("Failed flushing to {:?}", dest))?;
                Ok(())
            }

            do_thing(dest, seen_purls)
        })
        .await
        .context("Writing pURLs")??;
        info!("Wrote pURLs");
    }

    let cluster_file = cluster_writer
        .finalize_cluster()
        .await
        .with_context(|| format!("Failed finalizing cluster in {:?}", dest))?;

    let mut cluster_names = vec![];
    let mut history = vec![];
    for cluster in &clusters {
        cluster_names.push(cluster.name());
        let mut cluster_history = cluster.read_history()?;
        history.append(&mut cluster_history);
    }
    let iso_time = iso8601_now();
    let cluster_name = cluster_file
        .file_name()
        .and_then(|v| v.to_str())
        .unwrap_or("unknown");
    let last_json = json!({"date": iso_time,
			   "big_tent_commit": env!("VERGEN_GIT_SHA"),
			   "cluster_name": cluster_name,  "operation": "merge_clusters",
			   "merged_clusters": cluster_names});

    history.push(last_json);

    let history_file = cluster_file.with_file_name("history.jsonl");

    write_json_lines(&history_file, &history)
        .with_context(|| format!("Failed writing json to {:?}", history_file))?;

    info!(
        "Finished {} loops at {:?}",
        loop_cnt.separate_with_commas(),
        start.elapsed()
    );
    Ok(())
}

enum ItemOrPurl {
    // Purl(Vec<String>),
    Item {
        pos: usize,
        cbor_bytes: Vec<u8>,
        merged: usize,
        purls: HashSet<String>,
        hash: MD5Hash,
    },
}

struct ClusterPos {
    pub cluster: Arc<HerdMember>,
    pub pos: usize,
    pub len: usize,
    pub cache: Option<ItemOffset>,
}

impl ClusterPos {
    pub fn this_item(&mut self) -> Option<ItemOffset> {
        if self.pos >= self.len {
            None
        } else {
            match &self.cache {
                Some(v) => Some(*v),
                None => match self.cluster.offset_from_pos(self.pos) {
                    Some(v) => {
                        self.cache = Some(v);
                        Some(v)
                    }
                    _ => None,
                },
            }
        }
    }

    pub fn next(&mut self) {
        self.cache = None;
        self.pos += 1
    }
}

/// Entry in the coordinator min-heap. Ordered by hash ascending.
/// `BinaryHeap` is a max-heap, so we reverse the comparison.
#[derive(Clone, Eq, PartialEq)]
struct HeapEntry {
    hash: MD5Hash,
    cluster_idx: usize,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .hash
            .cmp(&self.hash)
            .then_with(|| other.cluster_idx.cmp(&self.cluster_idx))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Coordinator state that pops the next lowest hash across all clusters using a heap.
struct IndexHolder {
    clusters: Vec<ClusterPos>,
    heap: BinaryHeap<HeapEntry>,
}

impl IndexHolder {
    fn new(mut clusters: Vec<ClusterPos>) -> Self {
        let mut heap = BinaryHeap::new();
        for (idx, cluster) in clusters.iter_mut().enumerate() {
            if let Some(item) = cluster.this_item() {
                heap.push(HeapEntry {
                    hash: *item.hash(),
                    cluster_idx: idx,
                });
            }
        }
        Self { clusters, heap }
    }

    fn next(&mut self) -> Option<Vec<(ItemOffset, Arc<HerdMember>)>> {
        let first = self.heap.pop()?;
        let mut entries = vec![first];

        // Collect all clusters whose current head matches the lowest hash.
        while let Some(top) = self.heap.peek() {
            if top.hash == entries[0].hash {
                entries.push(self.heap.pop().unwrap());
            } else {
                break;
            }
        }

        let mut result = Vec::with_capacity(entries.len());
        for entry in entries {
            let cluster = &mut self.clusters[entry.cluster_idx];
            let offset = cluster.this_item()?;
            result.push((offset, cluster.cluster.clone()));
            cluster.next();
            if let Some(next_item) = cluster.this_item() {
                self.heap.push(HeapEntry {
                    hash: *next_item.hash(),
                    cluster_idx: entry.cluster_idx,
                });
            }
        }

        Some(result)
    }
}

/// Compute ETA and observed merge ratio from current progress.
/// Returns `(estimated_total_outputs, remaining_seconds, merge_ratio)`.
fn compute_merge_eta(
    loop_cnt: usize,
    merge_cnt: usize,
    max_merge_len: usize,
    items_per_second: f64,
) -> (usize, f64, f64) {
    let inputs_processed = loop_cnt.saturating_add(merge_cnt);
    let output_fraction = if inputs_processed > 0 {
        loop_cnt as f64 / inputs_processed as f64
    } else {
        0.0
    };
    let estimated_total_out = (max_merge_len as f64 * output_fraction) as usize;
    let remaining_outputs = estimated_total_out.saturating_sub(loop_cnt);
    let remaining_seconds = if items_per_second > 0.0 {
        remaining_outputs as f64 / items_per_second
    } else {
        0.0
    };
    let ratio = if loop_cnt > 0 {
        inputs_processed as f64 / loop_cnt as f64
    } else {
        0.0
    };
    (estimated_total_out, remaining_seconds, ratio)
}

/// Determine whether a progress log should be emitted at this loop count.
fn should_log_progress(loop_cnt: usize, last_log_loop_cnt: usize) -> bool {
    loop_cnt > 0 && loop_cnt.saturating_sub(last_log_loop_cnt) >= 2_000_000
}

#[cfg(test)]
fn linear_scan_next_hash(
    index_holder: &mut Vec<ClusterPos>,
) -> Option<Vec<(ItemOffset, Arc<HerdMember>)>> {
    let mut lowest: Option<MD5Hash> = None;
    let mut low_clusters = vec![];

    for holder in index_holder {
        let this_item = holder.this_item();
        match (&lowest, this_item) {
            (None, Some(either)) => {
                lowest = Some(*either.hash());
                low_clusters.push((either, holder));
            }
            (Some(low), Some(either)) if low == either.hash() => {
                low_clusters.push((either, holder));
            }
            (Some(low), Some(either)) if low > either.hash() => {
                lowest = Some(*either.hash());
                low_clusters.clear();
                low_clusters.push((either, holder));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        item::{CONTAINS, Item},
        rodeo::{
            goat::GoatRodeoCluster,
            member::{member_core, member_synth},
            robo_goat::RoboticGoat,
        },
    };
    use std::collections::BTreeSet;

    fn make_item(identifier: &str) -> Item {
        Item {
            identifier: identifier.to_string(),
            connections: BTreeSet::new(),
            body_mime_type: None,
            body: None,
        }
    }

    fn make_robo_cluster(name: &str, identifiers: &[&str]) -> Arc<HerdMember> {
        let items: Vec<Item> = identifiers.iter().map(|id| make_item(id)).collect();
        member_synth(RoboticGoat::new(name, items, serde_json::Value::Null))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_index_holder_exhausts_clusters() {
        let clusters = [
            make_robo_cluster("a", &["a", "b", "c"]),
            make_robo_cluster("b", &["d", "e", "f"]),
            make_robo_cluster("c", &["g", "h", "i"]),
        ];
        let cluster_positions: Vec<ClusterPos> = clusters
            .iter()
            .map(|cluster| ClusterPos {
                cluster: cluster.clone(),
                pos: 0,
                cache: None,
                len: cluster.number_of_items(),
            })
            .collect();
        let mut holder = IndexHolder::new(cluster_positions);
        let mut total = 0usize;
        while let Some(items) = holder.next() {
            total += items.len();
        }
        assert_eq!(total, 9, "Expected 9 items across all clusters");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_index_holder_heap_same_as_linear_scan() {
        let paths = [
            "test_data/cluster_a/2025_04_19_17_10_26_012a73d9c40dc9c0.grc",
            "test_data/cluster_b/2025_04_19_17_10_40_09ebe9a7137ee100.grc",
            "test_data/cluster_c/2025_07_24_14_43_36_68a489f4fd40c5e2.grc",
            "test_data/cluster_d/2025_07_24_14_44_14_2b39577cd0a58701.grc",
        ];
        let mut loaded = vec![];
        for path in paths {
            let cluster = GoatRodeoCluster::new(&PathBuf::from(path), false, None, vec![])
                .await
                .expect("Should load cluster");
            loaded.push(member_core(cluster));
        }

        let make_positions = |clusters: &[Arc<HerdMember>]| {
            clusters
                .iter()
                .map(|cluster| ClusterPos {
                    cluster: cluster.clone(),
                    pos: 0,
                    cache: None,
                    len: cluster.number_of_items(),
                })
                .collect::<Vec<_>>()
        };

        let heap_positions = {
            let positions = make_positions(&loaded);
            let mut holder = IndexHolder::new(positions);
            let mut seq = vec![];
            while let Some(items) = holder.next() {
                for (offset, _) in items {
                    seq.push(offset);
                }
            }
            seq
        };

        let scan_positions = {
            let mut positions = make_positions(&loaded);
            let mut seq = vec![];
            while let Some(items) = linear_scan_next_hash(&mut positions) {
                for (offset, _) in items {
                    seq.push(offset);
                }
            }
            seq
        };

        assert_eq!(heap_positions, scan_positions);
    }

    #[test]
    fn test_merge_eta_uses_observed_ratio() {
        // 100 inputs processed, 25 outputs -> 4:1 ratio, estimated total 400 * 0.25 = 100
        let (estimated, remaining, ratio) = compute_merge_eta(25, 75, 400, 10.0);
        assert_eq!(estimated, 100);
        assert!(
            (ratio - 4.0).abs() < 0.001,
            "ratio should be 4.0, got {}",
            ratio
        );
        assert!(
            (remaining - 7.5).abs() < 0.001,
            "remaining should be 7.5, got {}",
            remaining
        );
    }

    #[test]
    fn test_merge_eta_handles_no_progress() {
        let (estimated, remaining, ratio) = compute_merge_eta(0, 0, 1000, 0.0);
        assert_eq!(estimated, 0);
        assert_eq!(remaining, 0.0);
        assert_eq!(ratio, 0.0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_block_list_filters_items_and_edges() {
        use tempfile::tempdir;

        let blocked =
            "gitoid:blob:sha256:0000000000000000000000000000000000000000000000000000000000000001";
        let keep_a =
            "gitoid:blob:sha256:0000000000000000000000000000000000000000000000000000000000000002";
        let keep_b =
            "gitoid:blob:sha256:0000000000000000000000000000000000000000000000000000000000000003";

        fn make_connected_item(identifier: &str, targets: &[&str]) -> Item {
            Item {
                identifier: identifier.to_string(),
                connections: targets
                    .iter()
                    .map(|t| (CONTAINS.to_string(), t.to_string()))
                    .collect(),
                body_mime_type: None,
                body: None,
            }
        }

        let cluster_a = member_synth(RoboticGoat::new(
            "a",
            vec![
                make_connected_item(blocked, &[]),
                make_connected_item(keep_a, &[blocked]),
            ],
            serde_json::Value::Null,
        ));
        let cluster_b = member_synth(RoboticGoat::new(
            "b",
            vec![
                make_connected_item(blocked, &[]),
                make_connected_item(keep_b, &[]),
            ],
            serde_json::Value::Null,
        ));

        let temp_dir = tempdir().unwrap();
        let dest = temp_dir.path().to_path_buf();
        let block_list = {
            let mut s = HashSet::new();
            s.insert(blocked.to_string());
            Arc::new(s)
        };

        merge_fresh(
            vec![cluster_a, cluster_b],
            1_000,
            &dest,
            block_list,
            Arc::new(AtomicBool::new(true)),
            1,
        )
        .await
        .expect("merge should succeed");

        let mut clusters = GoatRodeoCluster::cluster_files_in_dir(dest, false, vec![])
            .await
            .expect("should read output cluster");
        let cluster = clusters.pop().expect("output cluster should exist");

        assert!(
            cluster.item_for_identifier(blocked).is_none(),
            "blocked item should not appear in output"
        );
        assert!(
            cluster.item_for_identifier(keep_b).is_some(),
            "keep_b should appear in output"
        );

        let kept_a = cluster
            .item_for_identifier(keep_a)
            .expect("keep_a should appear in output");
        assert!(
            !kept_a
                .connections
                .iter()
                .any(|(_, target)| target == blocked),
            "edges pointing to blocked items should be removed"
        );
    }

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_compute_merge_eta_never_panics(
            loop_cnt in 0..1_000_000usize,
            merge_cnt in 0..1_000_000usize,
            max_merge_len in 0..1_000_000usize,
            items_per_second in 0.0f64..1_000_000.0f64,
        ) {
            let (estimated, remaining, ratio) =
                compute_merge_eta(loop_cnt, merge_cnt, max_merge_len, items_per_second);
            prop_assert!(estimated <= max_merge_len || max_merge_len == 0);
            prop_assert!(remaining.is_finite());
            prop_assert!(ratio >= 0.0);
        }

        #[test]
        fn prop_should_log_progress_interval(
            last in 0..10_000_000usize,
            delta in 0..10_000_000usize,
        ) {
            let loop_cnt = last + delta;
            let should_log = should_log_progress(loop_cnt, last);
            prop_assert_eq!(should_log, delta >= 2_000_000 && loop_cnt > 0);
        }

        #[test]
        fn prop_index_holder_matches_reference(
            clusters in proptest::collection::vec(
                proptest::collection::vec("[a-z]{1,10}", 0..50usize),
                1..10usize,
            ),
        ) {
            // Ensure identifiers are unique within a cluster, but may collide across clusters.
            let mut herd_members = vec![];
            let mut all_identifiers: Vec<String> = vec![];
            for (idx, cluster_ids) in clusters.iter().enumerate() {
                let unique: Vec<String> = cluster_ids
                    .iter()
                    .enumerate()
                    .map(|(i, id)| format!("{}_{}_{}", idx, i, id))
                    .collect();
                all_identifiers.extend(unique.clone());
                herd_members.push(make_robo_cluster(&format!("cluster_{}", idx), &unique.iter().map(|s| s.as_str()).collect::<Vec<_>>()));
            }

            let positions: Vec<ClusterPos> = herd_members
                .iter()
                .map(|cluster| ClusterPos {
                    cluster: cluster.clone(),
                    pos: 0,
                    cache: None,
                    len: cluster.number_of_items(),
                })
                .collect();
            let mut holder = IndexHolder::new(positions);
            let mut heap_seq = vec![];
            while let Some(items) = holder.next() {
                heap_seq.push(items.len());
            }

            // Reference: flatten, sort by hash, group by hash.
            all_identifiers.sort_by_key(|id| md5hash_str(id));
            let mut reference = vec![];
            let mut current_group = 0usize;
            let mut prev: Option<MD5Hash> = None;
            for id in all_identifiers {
                let hash = md5hash_str(&id);
                if prev.as_ref() != Some(&hash) {
                    if current_group > 0 {
                        reference.push(current_group);
                    }
                    current_group = 1;
                    prev = Some(hash);
                } else {
                    current_group += 1;
                }
            }
            if current_group > 0 {
                reference.push(current_group);
            }

            prop_assert_eq!(heap_seq, reference);
        }
    }
}
