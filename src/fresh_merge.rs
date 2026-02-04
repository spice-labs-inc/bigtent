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
//! │   Thread    │     │ (20 threads) │     │  (Writer)   │
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

#[cfg(not(test))]
use log::info;
use serde_json::json;
use serde_jsonlines::write_json_lines;
use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread::{self},
    time::{Duration, Instant},
}; // Use log crate when building application

#[cfg(test)]
use std::println as info;

use crate::{
    item::Item,
    rodeo::{
        goat_trait::GoatRodeoTrait,
        index::{HasHash, ItemOffset},
        member::HerdMember,
        robo_goat::ClusterRoboMember,
        writer::ClusterWriter,
    },
    util::{MD5Hash, NiceDurationDisplay, iso8601_now},
};
use anyhow::{Result, bail};
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
/// 3. **Worker Threads (20 threads)**
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
///
/// ## Output Files
///
/// - `cluster_<timestamp>.grc` - Cluster metadata
/// - `index_<hash>.gri` - Index file(s)
/// - `data_<hash>.grd` - Data file(s)
/// - `purls.txt` - All Package URLs found
/// - `cluster_info.jsonl` - Merge metadata in JSON Lines format
pub async fn merge_fresh<PB: Into<PathBuf>>(
    clusters: Vec<Arc<HerdMember>>,
    merge_buffer_limit: usize,
    dest_directory: PB,
) -> Result<()> {
    let start = Instant::now();
    let dest: PathBuf = dest_directory.into();

    // === PHASE 1: Initialization ===
    fs::create_dir_all(dest.clone())?;
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
    let mut index_holder = vec![];

    for (idx, cluster) in clusters.iter().enumerate() {
        let index_len = cluster.number_of_items();

        index_holder.push(ClusterPos {
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

    info!(
        "Read indicies at {:?}",
        Instant::now().duration_since(start)
    );

    let mut cluster_writer = ClusterWriter::new(&dest).await?;
    let merge_start = Instant::now();

    // === PHASE 2: Set up threading infrastructure ===
    //
    // Channel for coordinator -> workers: sends (position, items_to_merge)
    // Bounded to 200,000 to prevent unbounded memory growth
    let (offset_tx, offset_rx) = flume::bounded(200_000);

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
    let coorindator_handle = thread::spawn(move || {
        let mut pos = 0usize; // Global position counter for ordering
        loop {
            // Only send more work if we're under the buffer limit
            if check_limit(&hpg, merge_buffer_limit) {
                // Find all clusters that have the item with the lowest hash
                // Returns None when all clusters are exhausted
                match next_hash_of_item_to_merge(&mut index_holder) {
                    Some(items_to_merge) => {
                        // Send work to worker threads
                        offset_tx
                            .send((pos, items_to_merge))
                            .expect("Should be able to send the message");
                        pos += 1;
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
    });

    threads.push(coorindator_handle);

    // === WORKER THREADS ===
    // 20 threads fetch items from clusters and merge them
    // Channel for workers -> main thread: sends merged items
    let (merged_tx, merged_rx) = flume::bounded(200_000);
    for thread_num in 0..20 {
        let rx = offset_rx.clone();
        let tx = merged_tx.clone();

        let processor_handle = thread::spawn(move || {
            let mut cnt = 0usize;
            while let Ok((position, items_to_merge)) = rx.recv() {
                let mut to_merge = vec![];
                let mut purls = vec![];
                for (offset, cluster) in &items_to_merge {
                    let merge_final = cluster
                        .item_from_item_offset(offset)
                        .expect("Should get the item");
                    merge_final
                        .connections
                        .iter()
                        .filter(|v| v.1.starts_with("pkg:"))
                        .for_each(|v| purls.push(v.1.to_string()));

                    to_merge.push(merge_final);
                }

                let mut top = match to_merge.pop() {
                    Some(v) => v,
                    None => continue,
                };

                for i in to_merge {
                    top = top.merge(i);
                }

                let cbor_bytes =
                    serde_cbor::to_vec(&top).expect("Should be able to encode an item");

                tx.send(ItemOrPurl::Item {
                    pos: position,
                    item: top,
                    cbor_bytes,
                    merged: items_to_merge.len() - 1,
                    purls,
                })
                .expect("Should send message");
                cnt += 1;
                if false && cnt % 500_000 == 0 {
                    info!(
                        "Thread {} at cnt {}",
                        thread_num,
                        cnt.separate_with_commas()
                    );
                }
            }
        });
        threads.push(processor_handle);
    }

    drop(merged_tx);
    drop(offset_rx); // make sure no more copies exist

    //// Main thread section
    ////

    let mut next_expected = 0usize;
    let mut holding_pen = HashMap::new();

    while let Ok(item_or_purl) = merged_rx.recv_async().await {
        match item_or_purl {
            ItemOrPurl::Item {
                pos: position,
                item,
                merged,
                cbor_bytes,
                purls,
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
                    holding_pen.insert(position, (item, cbor_bytes));

                    while let Some((item, cbor_bytes)) = holding_pen.remove(&next_expected) {
                        loop_cnt += 1;

                        // Decrement atomic gate for every item removed
                        cluster_writer.write_item(item, cbor_bytes).await?;

                        // Log, but only occasionally
                        if loop_cnt % 2_500_000 == 0 {
                            let diff = merge_start.elapsed();
                            let items_per_second = (loop_cnt as f64) / diff.as_secs_f64();
                            let remaining = (((max_merge_len - merge_cnt) - loop_cnt) as f64)
                                / items_per_second;

                            let nd: NiceDurationDisplay = remaining.into();
                            let td: NiceDurationDisplay = merge_start.elapsed().into();
                            info!(
                                "Merge cnt {}m of {}m merge cnt {}m at {} estimated end {} written pURLs {} holding pen cnt {}",
                                (loop_cnt / 1_000_000).separate_with_commas(),
                                ((max_merge_len - merge_cnt) / 1_000_000).separate_with_commas(),
                                (merge_cnt / 1_000_000).separate_with_commas(),
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
                    holding_pen.insert(position, (item, cbor_bytes));
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

    info!("Writing pURLs");
    tokio::task::spawn_blocking(move || {
        fn do_thing(dest: PathBuf, seen_purls: BTreeSet<String>) -> Result<()> {
            let mut purl_file = BufWriter::new(File::create({
                let mut dest = dest.clone();

                dest.push("purls.txt");
                dest
            })?);
            for purl in &seen_purls {
                purl_file.write_fmt(format_args!("{}\n", purl))?;
            }
            purl_file.flush()?;
            Ok(())
        }

        do_thing(dest, seen_purls)
    })
    .await??;
    info!("Wrote pURLs");

    let cluster_file = cluster_writer.finalize_cluster().await?;

    let mut cluster_names = vec![];
    let mut history = vec![];
    for cluster in &clusters {
        cluster_names.push(cluster.name());
        let mut cluster_history = cluster.read_history()?;
        history.append(&mut cluster_history);
    }
    let iso_time = iso8601_now();
    let cluster_name = format!(
        "{}",
        cluster_file
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("unknown")
    );
    let last_json = json!({"date": iso_time,
			   "big_tent_commit": env!("VERGEN_GIT_SHA"),
			   "cluster_name": cluster_name,  "operation": "merge_clusters",
			   "merged_clusters": cluster_names});

    history.push(last_json);

    let history_file = cluster_file
        .canonicalize()?
        .parent()
        .expect("Should have cluster parent dir")
        .join("history.jsonl");

    write_json_lines(&history_file, &history)?;

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
        item: Item,
        cbor_bytes: Vec<u8>,
        merged: usize,
        purls: Vec<String>,
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
                Some(v) => Some(v.clone()),
                None => match self.cluster.offset_from_pos(self.pos) {
                    Some(v) => {
                        self.cache = Some(v.clone());
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

fn next_hash_of_item_to_merge(
    index_holder: &mut Vec<ClusterPos>,
) -> Option<Vec<(ItemOffset, Arc<HerdMember>)>> {
    let mut lowest: Option<MD5Hash> = None;
    let mut low_clusters = vec![];

    for holder in index_holder {
        let this_item = holder.this_item();
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
