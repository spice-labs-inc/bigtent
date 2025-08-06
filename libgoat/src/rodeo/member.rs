use crate::{item::Item, util::MD5Hash};

use super::{
  goat::GoatRodeoCluster,
  goat_trait::GoatRodeoTrait,
  index::ItemOffset,
  robo_goat::{ClusterRoboMember, RoboticGoat},
};
use anyhow::Result;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::mpsc::Receiver;
use tokio_util::either::Either;

#[derive(Debug, Clone)]
pub enum HerdMember {
  Robo(Arc<RoboticGoat>),
  Cluster(Arc<GoatRodeoCluster>),
}

pub fn member_core(it: Arc<GoatRodeoCluster>) -> Arc<HerdMember> {
  Arc::new(HerdMember::Cluster(it))
}
pub fn member_synth(it: Arc<RoboticGoat>) -> Arc<HerdMember> {
  Arc::new(HerdMember::Robo(it))
}

impl ClusterRoboMember for HerdMember {
  fn name(&self) -> String {
    match self {
      HerdMember::Robo(goat_synth) => goat_synth.name(),
      HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.name(),
    }
  }

  fn offset_from_pos(&self, pos: usize) -> Result<ItemOffset> {
    match self {
      HerdMember::Robo(goat_synth) => goat_synth.offset_from_pos(pos),
      HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.offset_from_pos(pos),
    }
  }

  fn item_from_item_offset(&self, offset: &ItemOffset) -> Result<Item> {
    match self {
      HerdMember::Robo(goat_synth) => goat_synth.item_from_item_offset(offset),
      HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.item_from_item_offset(offset),
    }
  }
}

impl GoatRodeoTrait for HerdMember {
  fn node_count(&self) -> u64 {
    match self {
      HerdMember::Robo(goat_synth) => goat_synth.node_count(),
      HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.node_count(),
    }
  }

  fn get_purl(&self) -> Result<PathBuf> {
    match self {
      HerdMember::Robo(goat_synth) => goat_synth.get_purl(),
      HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.get_purl(),
    }
  }

  fn number_of_items(&self) -> usize {
    match self {
      HerdMember::Robo(goat_synth) => goat_synth.number_of_items(),
      HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.number_of_items(),
    }
  }

  fn read_history(&self) -> Result<Vec<serde_json::Value>> {
    match self {
      HerdMember::Robo(goat_synth) => goat_synth.read_history(),
      HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.read_history(),
    }
  }

  async fn north_send(
    self: Arc<Self>,
    gitoids: Vec<String>,
    purls_only: bool,
    start: std::time::Instant,
  ) -> Result<Receiver<Either<Item, String>>> {
    match &*self {
      HerdMember::Robo(goat_synth) => {
        goat_synth
          .clone()
          .north_send(gitoids, purls_only, start)
          .await
      }
      HerdMember::Cluster(goat_rodeo_cluster) => {
        goat_rodeo_cluster
          .clone()
          .north_send(gitoids, purls_only, start)
          .await
      }
    }
  }

  async fn roots(self: Arc<HerdMember>) -> Receiver<Item> {
    match &*self {
      HerdMember::Robo(goat_synth) => goat_synth.clone().roots().await,
      HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.clone().roots().await,
    }
  }

  fn item_for_identifier(&self, data: &str) -> Result<Option<Item>> {
    match self {
      HerdMember::Robo(goat_synth) => goat_synth.item_for_identifier(data),
      HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.item_for_identifier(data),
    }
  }

  fn item_for_hash(&self, hash: MD5Hash) -> Result<Option<Item>> {
    match self {
      HerdMember::Robo(goat_synth) => goat_synth.item_for_hash(hash),
      HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.item_for_hash(hash),
    }
  }

  fn antialias_for(self: Arc<Self>, data: &str) -> Result<Option<Item>> {
    match &*self {
      HerdMember::Robo(goat_synth) => goat_synth.clone().antialias_for(data),
      HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.clone().antialias_for(data),
    }
  }

  async fn stream_flattened_items(
    self: Arc<Self>,
    gitoids: Vec<String>,
    source: bool,
  ) -> Result<Receiver<Either<Item, String>>> {
    match &*self {
      HerdMember::Robo(goat_synth) => {
        goat_synth
          .clone()
          .stream_flattened_items(gitoids, source)
          .await
      }
      HerdMember::Cluster(goat_rodeo_cluster) => {
        goat_rodeo_cluster
          .clone()
          .stream_flattened_items(gitoids, source)
          .await
      }
    }
  }

  fn has_identifier(&self, identifier: &str) -> bool {
    match self {
      HerdMember::Robo(goat_synth) => goat_synth.has_identifier(identifier),
      HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.has_identifier(identifier),
    }
  }

  fn is_empty(&self) -> bool {
    match self {
      HerdMember::Robo(goat_synth) => goat_synth.is_empty(),
      HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.is_empty(),
    }
  }
}
