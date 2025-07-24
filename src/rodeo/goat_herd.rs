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
    let filename = format!("/tmp/{}.txt", self.uuid);
    let ret = PathBuf::from(&filename);
    if ret.exists() && ret.is_file() {
      return Ok(ret);
    }

    let mut dest = File::create(&ret)?;
    for grc in &self.herd {
      let the_purl = grc.get_purl()?;
      let mut from = File::open(the_purl)?;
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

  fn node_count(&self) -> u64 {
    let mut ret = 0;
    for goat in &self.herd {
      ret += goat.node_count();
    }
    ret
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_purls_and_merge() {
  use crate::item::EdgeType;
  let path = PathBuf::from("test_data/cluster_a/2025_04_19_17_10_26_012a73d9c40dc9c0.grc");
  let cluster_a = GoatRodeoCluster::new(&path.parent().unwrap().to_path_buf(), &path, false)
    .await
    .expect("Should get first cluster");
  let path2 = PathBuf::from("test_data/cluster_b/2025_04_19_17_10_40_09ebe9a7137ee100.grc");
  let cluster_b = GoatRodeoCluster::new(&path2.parent().unwrap().to_path_buf(), &path2, false)
    .await
    .expect("Should load cluster b");
  let path_c = PathBuf::from("test_data/cluster_c/2025_07_24_14_43_36_68a489f4fd40c5e2.grc");
  let cluster_c = GoatRodeoCluster::new(&path_c.parent().unwrap().to_path_buf(), &path_c, false)
    .await
    .expect("Should get cluster c");
  let path_d = PathBuf::from("test_data/cluster_d/2025_07_24_14_44_14_2b39577cd0a58701.grc");
  let cluster_d = GoatRodeoCluster::new(&path_d.parent().unwrap().to_path_buf(), &path_d, false)
    .await
    .expect("Should get cluster d");
  let herd = GoatHerd::new(vec![
    cluster_a.clone(),
    cluster_b.clone(),
    cluster_c.clone(),
    cluster_d.clone(),
  ]);
  let purls = herd.get_purl().expect("Should get purls");
  let all_purls = std::fs::read_to_string(&purls).expect("Should read string");

  assert!(
    all_purls.len() > 200,
    "Should have some characters in the purls"
  );

  let dest_dir =
    <PathBuf as std::str::FromStr>::from_str("merge_out").expect("Should create a directory");
  let _ = std::fs::remove_dir_all(&dest_dir);

  let herd2 = herd.clone();

  crate::fresh_merge::merge_fresh(herd.herd, &dest_dir)
    .await
    .expect("Should do a merge");

  let mut clusters = GoatRodeoCluster::cluster_files_in_dir(dest_dir.clone(), false)
    .await
    .expect("Should get cluster files");

  let cluster = clusters.pop().expect("Should have found a cluster");

  let history = cluster.read_history().expect("Should read history");

  assert!(
    history.len() >= 3,
    "Expecting some history, got {:?}",
    history
  );

  let tags = herd2
    .item_for_identifier("tags")
    .expect("Should get tags")
    .expect("Should get tags from option");
  let tagged: Vec<String> = tags
    .connections
    .iter()
    .filter(|conn| conn.0.is_tag_to())
    .map(|conn| conn.1.clone())
    .collect();

  assert_eq!(tagged.len(), 2, "Expecting 2 tags, got {:?}", tagged);
}
