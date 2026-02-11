//! # GoatRodeoTrait - Core Graph Database Abstraction
//!
//! This module defines the primary trait for interacting with BigTent's graph database.
//! The trait abstracts over different storage backends, allowing the same operations
//! to work with single clusters, multi-cluster herds, or synthetic in-memory clusters.
//!
//! ## Trait Overview
//!
//! [`GoatRodeoTrait`] provides:
//! - **Item Retrieval**: `item_for_identifier()` - fetch items by GitOID
//! - **Alias Resolution**: `antialias_for()` - resolve alias chains to canonical items
//! - **Graph Traversal**: `north_send()` - find containers/builders, `stream_flattened_items()` - find contents
//! - **Root Discovery**: `roots()` - find top-level items (expensive O(n) operation)
//!
//! ## Implementations
//!
//! - [`super::goat::GoatRodeoCluster`] - File-backed single cluster
//! - [`super::goat_herd::GoatHerd`] - Aggregates multiple clusters
//! - [`super::robo_goat::RoboticGoat`] - In-memory synthetic cluster
//!
//! ## Graph Traversal Terminology
//!
//! - **North**: Upward traversal - find what contains/builds this item
//! - **South/Flatten**: Downward traversal - find what this item contains
//! - **Anti-alias**: Follow `alias:to` edges to find canonical identifier

use std::{
    collections::HashSet,
    future::Future,
    path::PathBuf,
    sync::{Arc, atomic::AtomicU32},
    time::Instant,
};

use anyhow::{Result, bail};
use log::info;
use scopeguard::defer;
use thousands::Separable;
use tokio::sync::mpsc::Receiver;
use tokio_util::either::Either;

use crate::{
    item::{EdgeType, Item},
    util::MD5Hash,
};

/// Removing synthetic clusters, but still needed to aggregate
/// multiple small clusters means having a trait that can
/// be implemented for a single cluster or for something holding
/// a group of clusters
pub trait GoatRodeoTrait: Send + Sync {
    /// get the number of nodes
    fn node_count(&self) -> u64;
    /// get the purls.txt file
    fn get_purl(&self) -> Result<PathBuf>;
    /// get the number of items this cluster is managing
    fn number_of_items(&self) -> usize;

    /// Get the history for the cluster
    fn read_history(&self) -> Result<Vec<serde_json::Value>>;

    /// given a `Vec` of identifiers, find all the items that contain those
    /// items, etc. until there's no contents left.
    fn north_send(
        self: Arc<Self>,
        gitoids: Vec<String>,
        purls_only: bool,
        start: Instant,
    ) -> impl Future<Output = Result<Receiver<Either<Item, String>>>> + Send;

    /// Find all the Identifiers for all the items that either have no `contained:up` and
    /// `builds:up` or has a `tag:from`. These are the "root" `Item`s. This is
    /// and *INSANELY EXPENSIVE* operations... It's O(n) where n is the number
    /// of Items in the cluster
    fn roots(self: Arc<Self>) -> impl Future<Output = Receiver<Item>> + Send;

    /// given an identifier, return the associated item
    fn item_for_identifier(&self, data: &str) -> Option<Item>;

    /// given an MD5 hash, find the item for the hash
    fn item_for_hash(&self, hash: MD5Hash) -> Option<Item>;

    /// given an identifier, traverse the graph to find the anti-aliased Item
    fn antialias_for(self: Arc<Self>, data: &str) -> Option<Item>;

    /// create a stream for the flattened items. If any of the `gitoids` cannot
    /// be found, an `Err` is put in the stream and the flattening is completed.
    ///
    /// If `source` is true, the flattening includes "built from" and the returned
    /// items are the items that the code was built from
    fn stream_flattened_items(
        self: Arc<Self>,
        gitoids: Vec<String>,
        source: bool,
    ) -> impl Future<Output = Result<Receiver<Either<Item, String>>>> + Send;

    /// is the identifier known to the system
    fn has_identifier(&self, identifier: &str) -> bool;

    /// is the collection empty. Always false for GoatRodeoCluster, true if GoatHerd has
    /// no clusters
    fn is_empty(&self) -> bool;
}

pub async fn impl_stream_flattened_items<GRT: GoatRodeoTrait + 'static>(
    the_self: Arc<GRT>,
    gitoids: Vec<String>,
    source: bool,
) -> Result<Receiver<Either<Item, String>>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Either<Item, String>>(256);
    let mut to_find = HashSet::new();
    let mut not_found = vec![];

    for s in gitoids {
        match the_self.has_identifier(&s) {
            true => {
                to_find.insert(s);
            }
            false => {
                not_found.push(s);
            }
        }
    }

    if !not_found.is_empty() {
        bail!("The following items are not found {:?}", not_found);
    }

    // populate the channel
    tokio::spawn(async move {
        let mut processed = HashSet::new();
        while !to_find.is_empty() {
            let mut new_to_find = HashSet::new();
            for identifier in to_find.clone() {
                match the_self.item_for_identifier(&identifier) {
                    Some(item) => {
                        // deal with anti-aliasing
                        item.connections
                            .iter()
                            .filter(|a| a.0.is_alias_to())
                            .for_each(|s| {
                                if !to_find.contains(&s.1) && !processed.contains(&s.1) {
                                    new_to_find.insert(s.1.clone());
                                }
                            });

                        // process
                        for s in item
                            .connections
                            .iter()
                            .filter(|a| a.0.is_contains_down() || (source && a.0.is_built_from()))
                        {
                            if !to_find.contains(&s.1) && !processed.contains(&s.1) {
                                new_to_find.insert(s.1.clone());

                                // if we are looking at source only, only include build sources
                                if !source || s.0.is_built_from() {
                                    let _ = tx.send(Either::Right(s.1.clone())).await;
                                }
                            }
                        }
                    }
                    _ => {}
                }
                processed.insert(identifier);
            }

            to_find = new_to_find;
        }
    });

    Ok(rx)
}

/// Resolve an identifier to its canonical (non-alias) item.
///
/// Aliases allow multiple identifiers to point to the same logical item.
/// This function follows `alias:to` edges until it reaches an item that
/// is not itself an alias.
///
/// ## Algorithm
///
/// 1. Look up the initial identifier
/// 2. If the item has an `alias:to` edge, follow it
/// 3. Repeat until finding an item without `alias:to`
/// 4. Return the canonical item
///
/// ## Example
///
/// ```text
/// pkg:npm/lodash@4.17.21 --[alias:to]--> gitoid:blob:sha256:abc123
/// ```
///
/// Looking up the PURL returns the GitOID item.
///
/// ## Returns
///
/// - `Some(Item)` - The canonical item
/// - `None` - If the identifier doesn't exist or alias chain is broken
pub fn impl_antialias_for<GRT: GoatRodeoTrait + 'static>(
    the_self: Arc<GRT>,
    data: &str,
) -> Option<Item> {
    // Start with the item for the given identifier
    let mut ret = match the_self.item_for_identifier(data) {
        Some(i) => i,
        _ => return None,
    };

    // Follow alias:to edges until we find a non-alias item
    while ret.is_alias() {
        match ret.connections.iter().find(|x| x.0.is_alias_to()) {
            Some(v) => {
                // Follow the alias to the target
                ret = match the_self.item_for_identifier(&v.1) {
                    Some(v) => v,
                    _ => return None, // Broken alias chain
                };
            }
            None => {
                return None; // Inconsistent: is_alias() true but no alias:to edge
            }
        }
    }

    Some(ret)
}
/// Traverse the graph "north" (upward) to find containers and builders.
///
/// Starting from the given GitOIDs, this function follows edges upward through
/// the graph to find all items that contain or build the starting items.
///
/// ## Algorithm
///
/// Uses breadth-first search with deduplication:
///
/// 1. Start with initial GitOIDs in `to_find` set
/// 2. For each unvisited item:
///    - Mark as visited (`found` set)
///    - Get the item and find its containers (`contained_by()`)
///    - Add containers to `to_find` for next iteration
///    - Stream the item (or its PURLs) to the response channel
/// 3. Repeat until no new items to visit
///
/// ## Parameters
///
/// - `gitoids`: Starting points for the traversal
/// - `purls_only`: If true, only stream Package URLs, not full Items
/// - `start`: Timestamp for logging elapsed time
///
/// ## Returns
///
/// A channel receiver that streams either:
/// - `Either::Left(Item)` - Full item (when `purls_only=false`)
/// - `Either::Right(String)` - Package URL (when `purls_only=true`)
pub async fn impl_north_send<GRT: GoatRodeoTrait + 'static>(
    the_self: Arc<GRT>,
    gitoids: Vec<String>,
    purls_only: bool,
    start: Instant,
) -> Result<Receiver<Either<Item, String>>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Either<Item, String>>(256);

    // Spawn async task to populate the channel
    tokio::spawn(async move {
        let mut found = HashSet::new(); // Items we've already visited
        let mut to_find = HashSet::new(); // Items we need to visit
        let mut found_purls = HashSet::<String>::new(); // Dedupe PURLs
        to_find.extend(gitoids);

        let cnt = AtomicU32::new(0);

        defer! {
          info!("North: Sent {} items in {:?}", cnt.load(std::sync::atomic::Ordering::Relaxed).separate_with_commas(),
          start.elapsed());
        }

        // Set difference: a - b (items in a but not in b)
        fn less(a: &HashSet<String>, b: &HashSet<String>) -> HashSet<String> {
            let mut ret = a.clone();
            for i in b.iter() {
                ret.remove(i);
            }
            ret
        }

        // BFS loop: continue until no new items to visit
        loop {
            // Get items we haven't visited yet
            let to_search = less(&to_find, &found);

            if to_search.len() == 0 {
                break; // All reachable items visited
            }

            for this_oid in to_search {
                found.insert(this_oid.clone()); // Mark as visited

                match the_self.item_for_identifier(&this_oid) {
                    Some(item) => {
                        // Find containers/builders of this item (upward edges)
                        let and_then = item.contained_by();

                        if purls_only {
                            // Only stream unique PURLs
                            for purl in item.find_purls() {
                                if !found_purls.contains(&purl) {
                                    let _ = tx.send(Either::Right(purl.clone())).await;
                                    found_purls.insert(purl);
                                }
                            }
                        } else {
                            // Stream the full item
                            let _ = tx.send(Either::Left(item)).await;
                        }

                        // Add containers to next iteration's work
                        to_find = to_find.union(&and_then).map(|s| s.clone()).collect();
                    }
                    _ => {} // Item not found, skip
                }
                cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    });
    Ok(rx)
}
