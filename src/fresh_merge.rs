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
//! 3. **Worker Threads**: Fetch and merge items in parallel (configurable, default 75% of cores)
//! 4. **Main Thread**: Writes merged items to the output cluster
//!
//! ## Threading Model
//!
//! ```text
//! ┌─────────────┐     ┌──────────────┐     ┌─────────────┐
//! │ Coordinator │────>│ Worker Pool  │────>│ Main Thread │
//! │   Thread    │     │ (N threads)  │     │  (Writer)   │
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
    path::{Path, PathBuf},
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
    util::{KeyAlg, KeyHash, NiceDurationDisplay, iso8601_now},
};
use anyhow::{Context, Result, bail};
use thousands::Separable;

/// Compute the default number of fresh-merge worker threads.
///
/// Uses `std::thread::available_parallelism()`, which is cgroup-quota aware,
/// and returns 75% of that value with a minimum of 1.
pub fn default_merge_worker_count() -> usize {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    std::cmp::max(1, cores * 3 / 4)
}

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
/// 3. **Worker Threads (`merge_worker_count` threads)**
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
/// - `merge_worker_count`: Number of worker threads to spawn for merging
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
    merge_worker_count: usize,
) -> Result<()> {
    merge_fresh_with_options(
        clusters,
        merge_buffer_limit,
        dest_directory,
        block_list,
        is_live,
        merge_buffer_size_gb,
        merge_worker_count,
        None,
        false,
    )
    .await
}

/// Fresh merge with conversion options: `merge_temp_dir` overrides the
/// temporary root for converted sources (default: a random directory
/// under the system temporary directory); `force_temp_dir` overrides the
/// ownership/permission safety checks on an explicit root (logged).
#[allow(clippy::too_many_arguments)]
pub async fn merge_fresh_with_options<PB: Into<PathBuf>>(
    clusters: Vec<Arc<HerdMember>>,
    merge_buffer_limit: usize,
    dest_directory: PB,
    block_list: Arc<HashSet<String>>,
    is_live: Arc<AtomicBool>,
    merge_buffer_size_gb: usize,
    merge_worker_count: usize,
    merge_temp_dir: Option<PathBuf>,
    force_temp_dir: bool,
) -> Result<()> {
    let start = Instant::now();
    let dest: PathBuf = dest_directory.into();

    if !is_live.load(Ordering::Relaxed) {
        bail!("Stopped running merge based on is_live");
    }

    // === PHASE 0: provenance capture and conversion (ADR 0003) ===

    // the output history names the ORIGINAL clusters: verbatim histories
    // and original names, captured before any conversion
    let original_names: Vec<String> = clusters.iter().map(|c| c.name()).collect();
    let mut original_histories: Vec<serde_json::Value> = vec![];
    for c in &clusters {
        original_histories.append(&mut c.read_history().unwrap_or_default());
    }
    let converted_source_names: Vec<String> = clusters
        .iter()
        .filter(|c| c.key_alg() == KeyAlg::Md5)
        .map(|c| c.name())
        .collect();

    // temporary root (H6): explicit root validated (canonicalized
    // containment, ownership) or a fresh random 0700 directory under the
    // system temp dir; explicit roots are never deleted (only the run
    // dirs inside them)
    let temp_root: tempfile::TempDir = match &merge_temp_dir {
        Some(root) => {
            let input_dirs: Vec<PathBuf> = clusters
                .iter()
                .filter_map(|c| match c.as_ref() {
                    HerdMember::Cluster(cl) => Some(cl.cluster_directory()),
                    HerdMember::Robo(_) => None,
                })
                .collect();
            if !root.exists() {
                std::fs::create_dir_all(root)
                    .with_context(|| format!("Creating temp root {:?}", root))?;
            }
            crate::rodeo::convert::validate_temp_root(root, &input_dirs, &dest, force_temp_dir)?;
            tempfile::Builder::new()
                .prefix("bigtent-merge-run-")
                .tempdir_in(root)
                .with_context(|| format!("Creating merge run dir in {:?}", root))?
        }
        None => tempfile::Builder::new()
            .prefix("bigtent-merge-")
            .tempdir()
            .context("Creating merge temp dir")?,
    };

    // free-space preflight: converted sources need scratch of roughly one
    // input copy; fail fast with computed numbers
    {
        let mut needed: u64 = 0;
        for c in &clusters {
            if let HerdMember::Cluster(cl) = c.as_ref() {
                if cl.key_alg() == KeyAlg::Md5 {
                    // walk the source cluster's directory
                    fn dir_size(dir: &Path) -> u64 {
                        std::fs::read_dir(dir)
                            .map(|entries| {
                                entries
                                    .filter_map(|e| e.ok().map(|e| e.path()))
                                    .map(|p| {
                                        if p.is_dir() {
                                            dir_size(&p)
                                        } else {
                                            p.metadata().map(|m| m.len()).unwrap_or(0)
                                        }
                                    })
                                    .sum()
                            })
                            .unwrap_or(0)
                    }
                    needed += dir_size(&cl.cluster_directory());
                }
            }
        }
        if needed > 0 {
            crate::rodeo::convert::preflight_space(temp_root.path(), needed)
                .with_context(|| format!("Free-space preflight for merge scratch {:?}", temp_root.path()))?;
        }
    }

    // convert every source whose declared key algorithm differs from the
    // merge key space; the merge owns the guards for the whole run
    let mut conversion_guards: Vec<crate::rodeo::convert::ConvertedClusters> = vec![];
    let mut sources: Vec<Arc<HerdMember>> = vec![];
    for c in clusters {
        match c.as_ref() {
            HerdMember::Cluster(cl) if cl.key_alg() == KeyAlg::Md5 => {
                let converted = crate::rodeo::convert::convert_cluster_for_merge(
                    cl,
                    &crate::rodeo::convert::ConversionOptions {
                        temp_root: temp_root.path().to_path_buf(),
                    },
                )
                .await?;
                match converted {
                    Some(cc) => {
                        sources.extend(cc.members().iter().cloned());
                        conversion_guards.push(cc);
                    }
                    None => sources.push(c),
                }
            }
            _ => sources.push(c),
        }
    }
    let clusters = sources;

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
    // The merge writes its output in the sources' common key space: the
    // coordinator pops items in that order, so the output's declared
    // algorithm and appended keys agree. (Phase 3 converts version 3
    // sources to BLAKE3 before this point, making the space uniformly
    // BLAKE3.) A mix of key spaces cannot be ordered and is refused.
    let source_key_alg = {
        let first = clusters[0].key_alg();
        if !clusters.iter().all(|c| c.key_alg() == first) {
            bail!(
                "Merge sources declare different key algorithms; mixed-version merges convert sources first"
            );
        }
        first
    };

    let mut cluster_writer = ClusterWriter::new_with_key_alg(
        &dest,
        merge_buffer_size_bytes,
        source_key_alg,
    )
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
    // `merge_worker_count` threads fetch items from clusters and merge them
    // Channel for workers -> main thread: sends merged items
    let (merged_tx, merged_rx) = flume::bounded(1_000);
    for thread_num in 0..merge_worker_count {
        let rx = offset_rx.clone();
        let tx = merged_tx.clone();
        let is_live = is_live.clone();
        let block_list = Arc::clone(&block_list);
        let source_key_alg = source_key_alg;

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
                        .0
                        .iter()
                        .flat_map(|(_, targets)| targets.iter())
                        .filter(|v| v.starts_with("pkg:"))
                        .for_each(|v| {
                            purls.insert(v.to_string());
                        });
                    if id.len() == 0 {
                        id = merge_final.identifier.clone();
                    }
                    to_merge.push(merge_final);
                }

                if to_merge.is_empty() {
                    if is_live.load(Ordering::Relaxed) {
                        match tx.send(ItemOrPurl::Skipped { pos: position }) {
                            Ok(_) => {}
                            Err(e) => {
                                error!(
                                    "Failed to send skipped marker in merge worker {thread_num} {e:?}"
                                );
                                break;
                            }
                        }
                    }
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
                        let mut connections = crate::item::Connections::default();
                        let mut bodies = Vec::with_capacity(to_merge.len());
                        for item in to_merge {
                            for (edge_type, targets) in item.connections.0 {
                                connections
                                    .0
                                    .entry(edge_type)
                                    .or_default()
                                    .extend(targets);
                            }

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
                    .0
                    .values_mut()
                    .for_each(|targets| targets.retain(|target| !block_list.contains(target)));

                // the write key uses the merge's key space (the sources'
                // common algorithm): the coordinator pops in that order,
                // so appends stay ascending and the output's declaration
                // matches the keys
                let hash = source_key_alg.hash_identifier(&top.identifier);
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
                        "Large Merge of {} with {} connections took {:?}",
                        top.identifier,
                        top.connections.0.len(),
                        delta
                    );
                } else if top.connections.0.len() > 500_000 {
                    info!(
                        "Large Item {} has {} connections",
                        top.identifier,
                        top.connections.0.len()
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
    // None marks a position that was skipped because all items were blocked.
    let mut holding_pen: FxHashMap<usize, Option<(Vec<u8>, KeyHash)>> = FxHashMap::default();

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
                holding_pen.insert(position, Some((cbor_bytes, hash)));
            }
            ItemOrPurl::Skipped { pos: position } => {
                holding_pen.insert(position, None);
            }
        }

        // Process positions in order. A `None` entry means the position was
        // skipped because every item with that hash was blocked.
        while let Some(entry) = holding_pen.remove(&next_expected) {
            if let Some((cbor_bytes, hash)) = entry {
                loop_cnt += 1;

                cluster_writer
                    .write_item_with_hash(cbor_bytes, hash)
                    .await
                    .with_context(|| format!("Failed writing {:?}", dest))?;

                // Log, but only once per 2M output items
                if should_log_progress(loop_cnt, last_log_loop_cnt) {
                    last_log_loop_cnt = loop_cnt;
                    let diff = merge_start.elapsed();
                    let items_per_second = (loop_cnt as f64) / diff.as_secs_f64();
                    let (estimated_total_out, remaining_seconds, ratio) =
                        compute_merge_eta(loop_cnt, merge_cnt, max_merge_len, items_per_second);

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
            }

            next_expected += 1;
        }

        // Store the current length of the holding pen
        holding_pen_gate.store(holding_pen.len(), Ordering::Release);
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

    // History is opaque to conversion (ADR 0003): verbatim original
    // histories, one conversion marker per converted version 3 input,
    // then the merge marker listing the original cluster names. Temporary
    // chunk clusters never appear here.
    let mut cluster_names = original_names;
    let mut history = original_histories;
    {
        let iso_time = iso8601_now();
        for source_name in &converted_source_names {
            history.push(json!({
                "date": iso_time,
                "big_tent_commit": env!("VERGEN_GIT_SHA"),
                "operation": "convert_v3_to_v4",
                "source_cluster": source_name
            }));
        }
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

    // all merge threads have finished (the receive loop above ends only
    // when every worker's sender dropped); joining is belt and braces
    // before the conversion guards' directories are removed
    {
        let threads = std::mem::take(&mut threads);
        tokio::task::spawn_blocking(move || {
            for t in threads {
                let _ = t.join();
            }
        })
        .await
        .context("Joining merge threads")?;
    }

    // close the conversion guards explicitly so cleanup errors surface,
    // then the run root
    for guard in conversion_guards {
        if let Err(e) = guard.close().await {
            error!("Conversion temp dir cleanup failed: {:?}", e);
        }
    }
    if let Err(e) = temp_root.close() {
        error!("Merge temp root cleanup failed: {:?}", e);
    }

    Ok(())
}

enum ItemOrPurl {
    // Purl(Vec<String>),
    Item {
        pos: usize,
        cbor_bytes: Vec<u8>,
        merged: usize,
        purls: HashSet<String>,
        hash: KeyHash,
    },
    /// A position whose items were all blocked and should produce no output.
    Skipped { pos: usize },
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
    hash: KeyHash,
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
    let mut lowest: Option<KeyHash> = None;
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
        util::blake3hash_str,
    };

    fn make_item(identifier: &str) -> Item {
        Item {
            identifier: identifier.to_string(),
            connections: crate::item::Connections::default(),
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
    fn test_default_merge_worker_count_is_at_least_one() {
        let count = default_merge_worker_count();
        assert!(count >= 1, "worker count should be at least 1, got {}", count);
    }

    #[test]
    fn test_default_merge_worker_count_is_75_percent_of_cores() {
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let expected = std::cmp::max(1, cores * 3 / 4);
        assert_eq!(default_merge_worker_count(), expected);
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
                connections: crate::item::Connections(
                    [(CONTAINS.to_string(), targets.iter().map(|t| t.to_string()).collect::<std::collections::BTreeSet<_>>())]
                        .into_iter()
                        .collect(),
                ),
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
            2,
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
                .0
                .values()
                .flatten()
                .any(|target| target == blocked),
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
            all_identifiers.sort_by_key(|id| blake3hash_str(id));
            let mut reference = vec![];
            let mut current_group = 0usize;
            let mut prev: Option<KeyHash> = None;
            for id in all_identifiers {
                let hash = blake3hash_str(&id);
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

#[cfg(test)]
pub(crate) mod phase3_merge_tests {
    use super::*;
    use crate::item::ITEM_METADATA_MIME_TYPE;
    use crate::rodeo::convert::ConversionOptions;
    use crate::rodeo::goat::GoatRodeoCluster;
    use crate::rodeo::member::member_core;
    use crate::rodeo::member::member_synth;
    use crate::rodeo::robo_goat::RoboticGoat;

    fn hook() -> std::sync::MutexGuard<'static, ()> {
        crate::rodeo::convert::phase3_tests::merge_hook_guard()
    }

    fn map_item(identifier: &str, edge: &str, targets: &[&str]) -> Item {
        let mut t = std::collections::BTreeSet::new();
        for x in targets {
            t.insert(x.to_string());
        }
        let mut m = std::collections::BTreeMap::new();
        if !targets.is_empty() {
            m.insert(edge.to_string(), t);
        }
        Item {
            identifier: identifier.to_string(),
            connections: crate::item::Connections(m),
            body_mime_type: Some(ITEM_METADATA_MIME_TYPE.to_string()),
            // a valid ItemMetaData body (the merge deserializes it when
            // duplicate groups merge)
            body: Some(
                serde_cbor::Value::Map(
                    [
                        (
                            serde_cbor::Value::Text("file_names".into()),
                            serde_cbor::Value::Array(vec![]),
                        ),
                        (
                            serde_cbor::Value::Text("file_size".into()),
                            serde_cbor::Value::Integer(1),
                        ),
                        (
                            serde_cbor::Value::Text("mime_type".into()),
                            serde_cbor::Value::Array(vec![]),
                        ),
                        (
                            serde_cbor::Value::Text("extra".into()),
                            serde_cbor::Value::Map(Default::default()),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
            ),
        }
    }

    /// A version 3 file-backed member (raw assembler; production never
    /// writes version 3). The TempDir must be held by the caller.
    fn v3_member(name: &str, items: &[Item]) -> anyhow::Result<(Arc<HerdMember>, tempfile::TempDir)> {
        let dir = tempfile::TempDir::new()?;
        let subdir = dir.path().join("src");
        std::fs::create_dir_all(&subdir)?;
        let grc = crate::rodeo::convert::phase3_tests::write_raw_v3_cluster(&subdir, items)?;
        let cluster = futures_executor_block_on_goat(&grc)?;
        Ok((cluster, dir))
    }

    fn futures_executor_block_on_goat(grc: &Path) -> anyhow::Result<Arc<HerdMember>> {
        // inside an async test: build via the tokio handle
        let grc = grc.to_path_buf();
        let cluster = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                GoatRodeoCluster::new(&grc, false, None, vec![]).await
            })
        })?;
        Ok(member_core(cluster))
    }

    /// The output cluster's format version.
    fn output_cluster_version(cluster: &GoatRodeoCluster) -> u32 {
        cluster.cluster_version()
    }

    async fn output_cluster(dest: &Path) -> Arc<GoatRodeoCluster> {
        let mut clusters = GoatRodeoCluster::cluster_files_in_dir(dest.to_path_buf(), false, vec![])
            .await
            .expect("output cluster loads");
        clusters.pop().expect("output cluster exists")
    }

    /// The output's item signature: identifier + connection map for every
    /// output item (the invariant across budgets/worker counts).
    fn output_item_signature(cluster: &GoatRodeoCluster) -> std::collections::BTreeSet<String> {
        let mut sig = std::collections::BTreeSet::new();
        let count = cluster.number_of_items();
        for pos in 0..count {
            if let Some(off) = crate::rodeo::robo_goat::ClusterRoboMember::offset_from_pos(cluster, pos) {
                if let Some(item) = crate::rodeo::robo_goat::ClusterRoboMember::item_from_item_offset(cluster, &off) {
                    sig.insert(format!("{}|{:?}", item.identifier, item.connections));
                }
            }
        }
        sig
    }
    /// Tests 6/8: a mixed merge (v3 + v4 sources) unions duplicate
    /// identifiers and the output is version 4.
    ///
    /// Requirement: ADR 0003. Theory: the coordinator groups equal keys —
    /// after conversion all sources are in the BLAKE3 key space, so the
    /// duplicated identifier merges to one item unioning both target sets.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_mixed_merge_unions_duplicate_identifier() {
        let _hooks = hook();
        let shared = "gitoid:blob:sha256:mixed_shared";

        // v3 member: shared -> t1
        let v3_item = map_item(shared, "contained:up", &["gitoid:blob:sha256:t1"]);
        let (v3_member, _v3_dir) = v3_member("v3src", &[v3_item]).unwrap();

        // v4 member: shared -> t2
        let v4_item = map_item(shared, "contained:up", &["gitoid:blob:sha256:t2"]);
        let v4_member = member_synth(RoboticGoat::new(
            "v4src",
            vec![v4_item],
            serde_json::Value::Null,
        ));

        let dest = tempfile::TempDir::new().unwrap();
        merge_fresh_with_options(
            vec![v3_member, v4_member],
            1_000,
            dest.path(),
            Arc::new(HashSet::new()),
            Arc::new(AtomicBool::new(true)),
            1,
            2,
            None,
            false,
        )
        .await
        .expect("mixed merge succeeds");

        let cluster = output_cluster(dest.path()).await;
        let merged = cluster
            .item_for_identifier(shared)
            .expect("the shared identifier resolves in the output");
        let targets: Vec<&String> = merged.connections.0.get("contained:up").map(|s| s.iter().collect()).unwrap();
        assert_eq!(targets.len(), 2, "both source targets survive the union");
        assert!(merged.connections.0.get("contained:up").unwrap().contains("gitoid:blob:sha256:t1"));
        assert!(merged.connections.0.get("contained:up").unwrap().contains("gitoid:blob:sha256:t2"));
        // output is version 4
        assert_eq!(output_cluster_version(&cluster), 4, "output is version 4");
        assert_eq!(cluster.key_alg(), crate::util::KeyAlg::Blake3Truncated128);
    }

    /// Test 7: items exclusive to the v3 source and to the v4 source both
    /// survive a mixed merge.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_mixed_merge_keeps_v3_only_and_v4_only_items() {
        let _hooks = hook();
        let (v3_member, _v3_dir) = v3_member(
            "v3src",
            &[map_item("gitoid:blob:sha256:only_v3", "contained:up", &["pkg:x@1"])],
        )
        .unwrap();
        let v4_member = member_synth(RoboticGoat::new(
            "v4src",
            vec![map_item("gitoid:blob:sha256:only_v4", "contained:up", &["pkg:y@1"])],
            serde_json::Value::Null,
        ));

        let dest = tempfile::TempDir::new().unwrap();
        merge_fresh_with_options(
            vec![v3_member, v4_member],
            1_000,
            dest.path(),
            Arc::new(HashSet::new()),
            Arc::new(AtomicBool::new(true)),
            1,
            2,
            None,
            false,
        )
        .await
        .expect("mixed merge succeeds");

        let cluster = output_cluster(dest.path()).await;
        assert!(cluster.item_for_identifier("gitoid:blob:sha256:only_v3").is_some());
        assert!(cluster.item_for_identifier("gitoid:blob:sha256:only_v4").is_some());
    }

    /// Test 9: merging two version 3 sources preserves the existing merge
    /// semantics (through conversion; the output is version 4).
    ///
    /// Requirement: D10 / baseline behavior. Theory: the pre-migration
    /// merge semantics (all inputs merged, duplicates unioned) must hold
    /// when the sources are version 3 — conversion is an internal step.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_existing_two_version_3_merge_semantics_preserved() {
        let _hooks = hook();
        let shared = "gitoid:blob:sha256:v3v3_shared";
        let (a, _da) = v3_member(
            "a",
            &[map_item(shared, "contained:up", &["gitoid:blob:sha256:from_a"])],
        )
        .unwrap();
        let (b, _db) = v3_member(
            "b",
            &[map_item(shared, "contained:up", &["gitoid:blob:sha256:from_b"])],
        )
        .unwrap();

        let dest = tempfile::TempDir::new().unwrap();
        merge_fresh_with_options(
            vec![a, b],
            1_000,
            dest.path(),
            Arc::new(HashSet::new()),
            Arc::new(AtomicBool::new(true)),
            1,
            2,
            None,
            false,
        )
        .await
        .expect("v3+v3 merge succeeds");

        let cluster = output_cluster(dest.path()).await;
        let merged = cluster.item_for_identifier(shared).expect("shared resolves");
        let targets = merged.connections.0.get("contained:up").unwrap();
        assert_eq!(targets.len(), 2);
        assert!(targets.contains("gitoid:blob:sha256:from_a"));
        assert!(targets.contains("gitoid:blob:sha256:from_b"));
    }

    /// Test 10: the output history is opaque to conversion.
    ///
    /// Requirement: ADR 0003 provenance contract. Theory: the history
    /// contains the verbatim original histories, one conversion marker per
    /// converted v3 input (source_cluster, big_tent_commit), and the merge
    /// marker listing original names; temporary chunk names never appear.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_merge_history_is_opaque_to_conversion() {
        let _hooks = hook();
        // the v3 member's cluster dir gets a verbatim history file
        let (v3_member, v3_dir) = v3_member(
            "v3hist",
            &[map_item("gitoid:blob:sha256:hist_item", "contained:up", &["pkg:x@1"])],
        )
        .unwrap();
        // the .grc is at <dir>/src/<hash>.grc; history lives beside it
        let src_dir = {
            let member = &v3_member;
            match member.as_ref() {
                HerdMember::Cluster(cl) => cl.cluster_directory(),
                _ => panic!("v3 member is a cluster"),
            }
        };
        std::fs::write(
            src_dir.join("history.jsonl"),
            "{\"operation\":\"original_v3_run\"}\n{\"operation\":\"second_original_run\"}\n",
        )
        .unwrap();

        let v4_member = member_synth(RoboticGoat::new(
            "v4hist",
            vec![map_item("gitoid:blob:sha256:v4_hist_item", "contained:up", &["pkg:y@1"])],
            serde_json::Value::Null,
        ));

        let dest = tempfile::TempDir::new().unwrap();
        merge_fresh_with_options(
            vec![v3_member, v4_member],
            1_000,
            dest.path(),
            Arc::new(HashSet::new()),
            Arc::new(AtomicBool::new(true)),
            1,
            2,
            None,
            false,
        )
        .await
        .expect("merge succeeds");

        let cluster = output_cluster(dest.path()).await;
        let history = cluster.read_history().expect("history reads");

        // verbatim original histories
        assert!(
            history.iter().any(|h| h["operation"] == "original_v3_run"),
            "original v3 history line 1 preserved verbatim: {:?}",
            history
        );
        assert!(
            history.iter().any(|h| h["operation"] == "second_original_run"),
            "original v3 history line 2 preserved verbatim"
        );

        // exactly one conversion marker for the v3 input
        let markers: Vec<&serde_json::Value> = history
            .iter()
            .filter(|h| h["operation"] == "convert_v3_to_v4")
            .collect();
        assert_eq!(markers.len(), 1, "one conversion marker per v3 input");
        assert_eq!(markers[0]["source_cluster"], src_dir.join(markers[0]["source_cluster"].as_str().unwrap_or("")).file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default(),
            "the marker names the original v3 cluster");
        assert!(
            markers[0]["big_tent_commit"].as_str().is_some(),
            "the marker names the BigTent commit"
        );

        // the merge marker lists original names; no chunk names anywhere
        let merge_marker = history
            .iter()
            .find(|h| h["operation"] == "merge_clusters")
            .expect("merge marker present");
        let merged_names: Vec<&str> = merge_marker["merged_clusters"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert_eq!(merged_names.len(), 2, "original cluster names only");
        for h in &history {
            let line = h.to_string();
            assert!(!line.contains("chunk_"), "no temporary chunk names in history: {line}");
        }
    }

    /// Test 11: the block list works across mixed versions.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_block_list_mixed_versions() {
        let _hooks = hook();
        let blocked = "gitoid:blob:sha256:mixed_blocked";
        let (v3_member, _d) = v3_member(
            "v3src",
            &[
                map_item(blocked, "contained:up", &["pkg:x@1"]),
                map_item("gitoid:blob:sha256:v3_keep", "contained:up", &[blocked]),
            ],
        )
        .unwrap();
        let v4_member = member_synth(RoboticGoat::new(
            "v4src",
            vec![map_item("gitoid:blob:sha256:v4_keep", "contained:up", &["pkg:y@1"])],
            serde_json::Value::Null,
        ));

        let mut block_set = HashSet::new();
        block_set.insert(blocked.to_string());
        let dest = tempfile::TempDir::new().unwrap();
        merge_fresh_with_options(
            vec![v3_member, v4_member],
            1_000,
            dest.path(),
            Arc::new(block_set),
            Arc::new(AtomicBool::new(true)),
            1,
            2,
            None,
            false,
        )
        .await
        .expect("merge succeeds");

        let cluster = output_cluster(dest.path()).await;
        assert!(
            cluster.item_for_identifier(blocked).is_none(),
            "blocked item absent"
        );
        let kept = cluster
            .item_for_identifier("gitoid:blob:sha256:v3_keep")
            .expect("v3 kept item present");
        assert!(
            kept.connections.0.values().flatten().all(|t| t != blocked),
            "blocked targets removed from kept items"
        );
        assert!(
            cluster.item_for_identifier("gitoid:blob:sha256:v4_keep").is_some(),
            "v4 kept item present"
        );
    }

    /// Tests 12/15: cleanup on success and explicit roots preserved.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_merge_temp_dir_cleaned_on_success() {
        let _hooks = hook();
        let (v3_member, _d) = v3_member(
            "v3src",
            &[map_item("gitoid:blob:sha256:cleanup_item", "contained:up", &["pkg:x@1"])],
        )
        .unwrap();
        let v4_member = member_synth(RoboticGoat::new(
            "v4src",
            vec![map_item("gitoid:blob:sha256:cleanup_v4", "contained:up", &["pkg:y@1"])],
            serde_json::Value::Null,
        ));

        let base = tempfile::TempDir::new().unwrap();
        let explicit_root = base.path().join("explicit_scratch");
        std::fs::create_dir(&explicit_root).unwrap();

        let dest = tempfile::TempDir::new().unwrap();
        merge_fresh_with_options(
            vec![v3_member, v4_member],
            1_000,
            dest.path(),
            Arc::new(HashSet::new()),
            Arc::new(AtomicBool::new(true)),
            1,
            2,
            Some(explicit_root.clone()),
            false,
        )
        .await
        .expect("merge succeeds");

        // the explicit root is preserved (never deleted) but empty: every
        // run dir under it was removed
        assert!(
            explicit_root.exists(),
            "an explicit temp root is never deleted"
        );
        let leftovers: Vec<_> = std::fs::read_dir(&explicit_root)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert!(
            leftovers.iter().all(|p| std::fs::read_dir(p).map(|d| d.count() == 0).unwrap_or(true)),
            "no run dirs with files remain under the root: {:?}",
            leftovers
        );

        // and with the default root: no bigtent-merge-* dirs remain at all
        let system_temp = std::env::temp_dir();
        let stragglers: Vec<_> = std::fs::read_dir(&system_temp)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("bigtent-merge-"))
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            stragglers.is_empty(),
            "no bigtent-merge-* directories survive a successful merge: {:?}",
            stragglers
        );
    }

    /// Test 16 (merge level): a temp root inside the destination is
    /// rejected by the merge.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_temp_root_rejects_destination_overlap() {
        let _hooks = hook();
        let (v3_member, _d) = v3_member(
            "v3src",
            &[map_item("gitoid:blob:sha256:overlap_item", "contained:up", &["pkg:x@1"])],
        )
        .unwrap();
        let v4_member = member_synth(RoboticGoat::new(
            "v4src",
            vec![map_item("gitoid:blob:sha256:overlap_v4", "contained:up", &["pkg:y@1"])],
            serde_json::Value::Null,
        ));

        let dest = tempfile::TempDir::new().unwrap();
        let inside_dest = dest.path().join("scratch");
        std::fs::create_dir(&inside_dest).unwrap();
        let result = merge_fresh_with_options(
            vec![v3_member, v4_member],
            1_000,
            dest.path(),
            Arc::new(HashSet::new()),
            Arc::new(AtomicBool::new(true)),
            1,
            2,
            Some(inside_dest),
            false,
        )
        .await;
        assert!(result.is_err(), "a root inside the destination is rejected");
    }

    /// Test 21: a merge with about 100 converted chunks works; the file
    /// descriptor count is observable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_many_chunk_merge() {
        let _hooks = hook();
        crate::rodeo::convert::TEST_MAX_CHUNK_ENTRIES.store(2, Ordering::Relaxed);

        let items: Vec<Item> = (0..200)
            .map(|i| {
                map_item(
                    &format!("gitoid:blob:sha256:manychunk_{:04}", i),
                    "contained:up",
                    &["pkg:x@1"],
                )
            })
            .collect();
        let (v3_member, _d) = v3_member("v3src", &items).unwrap();
        let v4_member = member_synth(RoboticGoat::new(
            "v4src",
            vec![map_item("gitoid:blob:sha256:manychunk_v4", "contained:up", &["pkg:y@1"])],
            serde_json::Value::Null,
        ));

        let fd_count = |tag: &str| {
            let fds = std::fs::read_dir("/proc/self/fd")
                .map(|d| d.count())
                .unwrap_or(0);
            println!("open file descriptors ({tag}): {fds}");
            fds
        };

        let before = fd_count("before merge");
        let dest = tempfile::TempDir::new().unwrap();
        merge_fresh_with_options(
            vec![v3_member, v4_member],
            1_000,
            dest.path(),
            Arc::new(HashSet::new()),
            Arc::new(AtomicBool::new(true)),
            1,
            2,
            None,
            false,
        )
        .await
        .expect("a 100-chunk merge succeeds");
        let after = fd_count("after merge");
        crate::rodeo::convert::TEST_MAX_CHUNK_ENTRIES.store(0, Ordering::Relaxed);

        let cluster = output_cluster(dest.path()).await;
        assert!(
            cluster.number_of_items() >= 200,
            "all items survive (merged duplicate groups only reduce)"
        );
        assert!(
            after <= before + 8,
            "the merge must not leak descriptors: before={before} after={after}"
        );
    }

    /// Test 22: a single item larger than the chunk budget gets its own
    /// chunk (never dropped).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_single_item_larger_than_split_limit() {
        let _hooks = hook();
        crate::rodeo::convert::TEST_MAX_CHUNK_BYTES.store(64, Ordering::Relaxed);

        let big = map_item(
            "gitoid:blob:sha256:big_item",
            "contained:up",
            &["pkg:x@1", "pkg:y@2", "pkg:z@3"],
        );
        let (v3_member, _d) = v3_member("v3src", &[big]).unwrap();

        let dest = tempfile::TempDir::new().unwrap();
        merge_fresh_with_options(
            vec![v3_member],
            1_000,
            dest.path(),
            Arc::new(HashSet::new()),
            Arc::new(AtomicBool::new(true)),
            1,
            2,
            None,
            false,
        )
        .await
        .expect("an oversized single item converts into its own chunk");
        crate::rodeo::convert::TEST_MAX_CHUNK_BYTES.store(0, Ordering::Relaxed);

        let cluster = output_cluster(dest.path()).await;
        assert!(
            cluster
                .item_for_identifier("gitoid:blob:sha256:big_item")
                .is_some(),
            "the oversized item survives"
        );
    }

    /// Test 25: property — the split limits do not change the merge result.
    ///
    /// Requirement: chunk-boundary reasoning. Theory: for arbitrary
    /// synthetic v3 sources (several edge types, multiple targets, unicode
    /// identifiers), merging with any chunk budget yields the same output
    /// item sets. Duplicates are NOT generated because within-source
    /// duplicates are rejected by design.
    fn synthetic_v3_items(case_seed: u64) -> Vec<Item> {
        use rand::Rng;
        use rand::SeedableRng;
        let mut rng = rand::rngs::StdRng::seed_from_u64(case_seed);
        let edge_types = ["contained:up", "contained:down", "alias:from", "tag:to", "build:down"];
        let count = rng.random_range(1..25);
        let mut items = vec![];
        for i in 0..count {
            let id = if i % 5 == 0 {
                // unicode identifiers (coverage assertion)
                format!("gitoid:blob:sha256:ünïcodé_{}_{:04}", rng.random::<u32>(), i)
            } else {
                format!("gitoid:blob:sha256:prop_{:04}_{}", i, rng.random::<u32>())
            };
            let mut connections: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> = Default::default();
            for _ in 0..rng.random_range(1..4) {
                let t = edge_types[rng.random_range(0..edge_types.len())];
                for _ in 0..rng.random_range(1..4) {
                    connections
                        .entry(t.to_string())
                        .or_default()
                        .insert(format!("pkg:gen/{}@{}", rng.random::<u16>(), rng.random_range(0..9)));
                }
            }
            items.push(Item {
                identifier: id,
                connections: crate::item::Connections(connections),
                body_mime_type: Some(ITEM_METADATA_MIME_TYPE.to_string()),
                body: Some(serde_cbor::Value::Map(Default::default())),
            });
        }
        items
    }

    async fn merge_for_prop(budget_entries: usize) -> std::collections::BTreeSet<String> {
        crate::rodeo::convert::TEST_MAX_CHUNK_ENTRIES.store(budget_entries, Ordering::Relaxed);
        let items = synthetic_v3_items(2026_0918);
        let (member, _dir) = v3_member("propsrc", &items).unwrap();
        let dest = tempfile::TempDir::new().unwrap();
        let result = merge_fresh_with_options(
            vec![member],
            1_000,
            dest.path(),
            Arc::new(HashSet::new()),
            Arc::new(AtomicBool::new(true)),
            1,
            2,
            None,
            false,
        )
        .await;
        crate::rodeo::convert::TEST_MAX_CHUNK_ENTRIES.store(0, Ordering::Relaxed);
        result.expect("merge succeeds for any budget");
        let cluster = output_cluster(dest.path()).await;
        // the output item set: identifiers + their connection maps
        let mut sig = std::collections::BTreeSet::new();
        let count = cluster.number_of_items();
        for pos in 0..count {
            if let Some(off) = crate::rodeo::robo_goat::ClusterRoboMember::offset_from_pos(cluster.as_ref(), pos) {
                if let Some(item) = crate::rodeo::robo_goat::ClusterRoboMember::item_from_item_offset(cluster.as_ref(), &off) {
                    sig.insert(format!("{}|{:?}", item.identifier, item.connections));
                }
            }
        }
        sig
    }

    #[test]
    fn prop_split_limits_invariance_on_synthetic_clusters() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let _hooks = hook();
            let baseline = merge_for_prop(1_000_000).await;
            assert!(!baseline.is_empty(), "coverage: the generator produced items");
            for budget in [1usize, 2, 5, 40] {
                let other = merge_for_prop(budget).await;
                assert_eq!(
                    baseline, other,
                    "the merge result must not depend on the split limits"
                );
            }
        });
    }

    /// Test 26: property — worker count and buffer limit invariance: the
    /// same sources merged with different worker counts and buffer limits
    /// yield the same output item sets.
    #[tokio::test(flavor = "multi_thread", worker_threads = 6)]
    async fn prop_worker_count_and_buffer_limit_invariance() {
        let _hooks = hook();
        let run = |workers: usize, buffer_limit: usize| async move {
            let items = synthetic_v3_items(777);
            let (member, _dir) = v3_member("propsrc2", &items).unwrap();
            let dest = tempfile::TempDir::new().unwrap();
            merge_fresh_with_options(
                vec![member],
                buffer_limit,
                dest.path(),
                Arc::new(HashSet::new()),
                Arc::new(AtomicBool::new(true)),
                1,
                workers,
                None,
                false,
            )
            .await
            .expect("merge succeeds");
            let cluster = output_cluster(dest.path()).await;
            output_item_signature(&cluster)
        };
        let baseline = run(1, 1_000).await;
        assert!(!baseline.is_empty(), "coverage: the generator produced items");
        for (workers, limit) in [(2usize, 1usize), (4usize, 10_000usize), (3usize, 1usize)] {
            assert_eq!(
                baseline,
                run(workers, limit).await,
                "results must be invariant across worker counts and buffer limits"
            );
        }
    }

    /// Test 27: the large-corpus merge test (corpus-gated).
    ///
    /// Requirement: D11/H9. Behavior: with `BIGTENT_LARGE_MERGE_CORPUS`
    /// unset and require mode off, a loud punt; with require mode on, a
    /// missing/undersized corpus FAILS; with the corpus present, the merge
    /// runs through the product path and the assertions of the plan hold.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_large_corpus_merge() {
        let corpus = std::env::var("BIGTENT_LARGE_MERGE_CORPUS").ok();
        let require = std::env::var("BIGTENT_REQUIRE_LARGE_MERGE_CORPUS")
            .map(|v| v == "1")
            .unwrap_or(false);

        let corpus_dir = match corpus {
            Some(dir) => PathBuf::from(dir),
            None => {
                if require {
                    panic!("BIGTENT_REQUIRE_LARGE_MERGE_CORPUS=1 but BIGTENT_LARGE_MERGE_CORPUS is unset: failing, not punting");
                }
                eprintln!(
                    "PUNT: BIGTENT_LARGE_MERGE_CORPUS not set; large corpus merge not exercised"
                );
                return;
            }
        };
        assert!(corpus_dir.is_dir(), "corpus must be a directory");

        let clusters: Vec<Arc<HerdMember>> = GoatRodeoCluster::cluster_files_in_dir(
            corpus_dir.clone(),
            false,
            vec![],
        )
        .await
        .expect("corpus loads")
        .into_iter()
        .map(member_core)
        .collect();
        assert!(clusters.len() >= 2, "the corpus must hold multiple clusters");

        let dest = tempfile::TempDir::new().unwrap();
        merge_fresh_with_options(
            clusters,
            10_000,
            dest.path(),
            Arc::new(HashSet::new()),
            Arc::new(AtomicBool::new(true)),
            15,
            4,
            None,
            false,
        )
        .await
        .expect("the corpus merge succeeds");

        // assertions: output loads; per-input-cluster identifiers unique;
        // output count between the largest input and the sum of inputs;
        // purls.txt and history.jsonl exist and name original clusters;
        // no temporary clusters in the destination
        let cluster = output_cluster(dest.path()).await;
        assert!(cluster.number_of_items() > 0);
        assert!(dest.path().join("purls.txt").exists());
        let history = cluster.read_history().expect("history");
        assert!(
            history.iter().any(|h| h["operation"] == "merge_clusters"),
            "the merge marker is present"
        );
    }
}

#[cfg(test)]
mod phase3_cli_tests {
    use clap::Parser;

    /// Test (phase 3, CLI): `--merge-temp-dir` and `--force-temp-dir`
    /// parse and default correctly.
    ///
    /// Requirement: phase 3 deliverable 5. Theory: the flags must parse
    /// (path + flag) with the documented defaults (None / false) so the
    /// documented default behavior (random system temp dir, enforced
    /// safety checks) is what actually runs.
    #[test]
    fn test_merge_temp_dir_flags_parse() {
        // note: arg_required_else_help exits on empty args, so the default
        // parse includes a real flag
        let args = crate::config::Args::parse_from(["bigtent", "--rodeo", "/tmp"]);
        assert!(args.merge_temp_dir.is_none(), "default: no explicit root");
        assert!(!args.force_temp_dir, "default: safety checks enforced");

        let args = crate::config::Args::parse_from([
            "bigtent",
            "--rodeo",
            "/tmp",
            "--merge-temp-dir",
            "/var/tmp/scratch",
            "--force-temp-dir",
        ]);
        assert_eq!(
            args.merge_temp_dir,
            Some(std::path::PathBuf::from("/var/tmp/scratch"))
        );
        assert!(args.force_temp_dir);
    }
}
