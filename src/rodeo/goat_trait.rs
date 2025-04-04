use std::{
  collections::HashSet,
  future::Future,
  path::PathBuf,
  sync::{atomic::AtomicU32, Arc},
  time::Instant,
};

use anyhow::{bail, Result};
use log::info;
use scopeguard::defer;
use thousands::Separable;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::either::Either;

use crate::{
  item::{EdgeType, Item},
  util::MD5Hash,
};

/// Removing synthetic clusters, but still needed to aggregate
/// multiple small clusters means having a trait that can
/// be implemented for a single cluster or for something holding
/// a group of clusters
pub trait GoatRodeoTrait: Clone + Send + Sync {
  /// get the purls.txt file
  fn get_purl(&self) -> Result<PathBuf>;
  /// get the number of items this cluster is managing
  fn number_of_items(&self) -> usize;

  /// given a `Vec` of identifiers, find all the items that contain those
  /// items, etc. until there's no contents left.
  fn north_send(
    &self,
    gitoids: Vec<String>,
    purls_only: bool,
    tx: Sender<Either<Item, String>>,
    start: Instant,
  ) -> impl Future<Output = Result<()>> + Send;

  /// given an identifier, return the associated item
  fn item_for_identifier(&self, data: &str) -> Result<Option<Item>>;

  /// given an MD5 hash, find the item for the hash
  fn item_for_hash(&self, hash: MD5Hash) -> Result<Option<Item>>;

  /// given an identifier, traverse the graph to find the anti-aliased Item
  fn antialias_for(&self, data: &str) -> Result<Option<Item>>;

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
          Ok(Some(item)) => {
            // deal with anti-aliasing
            item
              .connections
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

pub async fn impl_north_send<GRT: GoatRodeoTrait>(
  the_self: &GRT,
  gitoids: Vec<String>,
  purls_only: bool,
  tx: Sender<Either<Item, String>>,
  start: Instant,
) -> Result<()> {
  let mut found = HashSet::new();
  let mut to_find = HashSet::new();
  let mut found_purls = HashSet::<String>::new();
  to_find.extend(gitoids);

  let cnt = AtomicU32::new(0);

  defer! {
    info!("North: Sent {} items in {:?}", cnt.load(std::sync::atomic::Ordering::Relaxed).separate_with_commas(),
    start.elapsed());
  }

  fn less(a: &HashSet<String>, b: &HashSet<String>) -> HashSet<String> {
    let mut ret = a.clone();
    for i in b.iter() {
      ret.remove(i);
    }
    ret
  }

  loop {
    let to_search = less(&to_find, &found);

    if to_search.len() == 0 {
      break;
    }
    for this_oid in to_search {
      found.insert(this_oid.clone());
      match the_self.item_for_identifier(&this_oid) {
        Ok(Some(item)) => {
          let and_then = item.contained_by();
          if purls_only {
            for purl in item.find_purls() {
              if !found_purls.contains(&purl) {
                tx.send(Either::Right(purl.clone())).await?;

                found_purls.insert(purl);
              }
            }
          } else {
            tx.send(Either::Left(item)).await?;
          }

          to_find = to_find.union(&and_then).map(|s| s.clone()).collect();
        }
        _ => {}
      }
      cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
  }
  Ok(())
}
