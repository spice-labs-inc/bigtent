use anyhow::Result;
use std::{
  fs::File,
  io::{Read, Write},
  path::PathBuf,
  sync::Arc,
};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::either::Either;
use uuid::Uuid;

use crate::item::Item;

use super::{
  goat::GoatRodeoCluster,
  goat_trait::{GoatRodeoTrait, impl_north_send, impl_stream_flattened_items},
};

/// a collection of goat rodeo instances
#[derive(Debug, Clone)]
pub struct GoatHerd {
  herd: Vec<Arc<GoatRodeoCluster>>,
  uuid: String,
}

impl GoatHerd {
  /// create a new herd
  pub fn new(herd: Vec<Arc<GoatRodeoCluster>>) -> GoatHerd {
    GoatHerd {
      herd,
      uuid: Uuid::new_v4().to_string(),
    }
  }

  /// get the herd
  pub fn get_herd(&self) -> &Vec<Arc<GoatRodeoCluster>> {
    &self.herd
  }
}

impl GoatRodeoTrait for GoatHerd {
    /// Get the history for the cluster
    fn read_history(&self) -> Result<Vec<serde_json::Value>> {
      let mut ret = vec![];
      for cluster in &self.herd {
        let mut history = cluster.read_history()?;
        ret.append(&mut history);
      }
      Ok(ret)
    }

  fn get_purl(&self) -> Result<std::path::PathBuf> {
    let filename = format!("{}.txt", self.uuid);
    let ret = PathBuf::from(filename).canonicalize()?;
    if ret.exists() && ret.is_file() {
      return Ok(ret);
    }

    let mut dest = File::create(&ret)?;
    for grc in &self.herd {
      let mut from = File::open(grc.get_purl()?)?;
      let mut buf = vec![];
      from.read_to_end(&mut buf)?;
      dest.write_all(&buf)?
    }

    dest.flush()?;
    Ok(ret)
  }

  fn number_of_items(&self) -> usize {
    let mut sum = 0;
    for grc in &self.herd {
      sum += grc.number_of_items();
    }
    sum
  }

  async fn north_send(
    &self,
    gitoids: Vec<String>,
    purls_only: bool,
    tx: Sender<Either<Item, String>>,
    start: std::time::Instant,
  ) -> Result<()> {
    impl_north_send(self, gitoids, purls_only, tx, start).await
  }

  fn item_for_identifier(&self, data: &str) -> Result<Option<Item>> {
    let mut items = vec![];
    for grc in &self.herd {
      match grc.item_for_identifier(data)? {
        None => {}
        Some(item) => {
          items.push(item);
        }
      }
    }

    Ok(Item::merge_items(items))
  }

  fn item_for_hash(&self, hash: crate::util::MD5Hash) -> Result<Option<Item>> {
    let mut items = vec![];
    for grc in &self.herd {
      match grc.item_for_hash(hash)? {
        None => {}
        Some(item) => {
          items.push(item);
        }
      }
    }

    Ok(Item::merge_items(items))
  }

  fn antialias_for(&self, data: &str) -> Result<Option<Item>> {
    let mut items = vec![];
    for grc in &self.herd {
      match grc.antialias_for(data)? {
        None => {}
        Some(item) => {
          items.push(item);
        }
      }
    }

    Ok(Item::merge_items(items))
  }

  async fn stream_flattened_items(
    self: Arc<Self>,
    gitoids: Vec<String>,
    source: bool,
  ) -> Result<Receiver<Either<Item, String>>> {
    impl_stream_flattened_items(self, gitoids, source).await
  }

  fn has_identifier(&self, identifier: &str) -> bool {
    for grc in &self.herd {
      if grc.has_identifier(identifier) {
        return true;
      }
    }
    false
  }

  fn is_empty(&self) -> bool {
    self.herd.is_empty()
  }
}
