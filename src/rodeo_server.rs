use anyhow::Result;
use arc_swap::ArcSwap;
#[cfg(not(test))]
use log::info;
use std::{collections::HashSet, sync::Arc};
use crate::structs::EdgeType;

#[cfg(test)]
use std::println as info;
use toml::{map::Map, Table};
// use num_traits::ops::bytes::ToBytes;
use crate::{
  config::Args,
  index_file::{IndexLoc, ItemOffset},
  rodeo::GoatRodeoCluster,
  structs::Item,
};

pub type MD5Hash = [u8; 16];

#[derive(Debug)]
pub struct RodeoServer {
  // threads: Mutex<Vec<JoinHandle<()>>>,
  cluster: Arc<ArcSwap<GoatRodeoCluster>>,
  args: Args,
  config_table: ArcSwap<Table>,
}

impl RodeoServer {
  pub async fn find(&self, hash: MD5Hash) -> Result<Option<ItemOffset>> {
    self.cluster.load().find(hash).await
  }

  /// get the cluster arc swap so that the cluster can be substituted
  pub fn get_cluster_arcswap(&self) -> Arc<ArcSwap<GoatRodeoCluster>> {
    self.cluster.clone()
  }

  /// Get the current GoatRodeoCluster from the server
  pub fn get_cluster(&self) -> Arc<GoatRodeoCluster> {
    let the_cluster: Arc<GoatRodeoCluster> = (&(*self.get_cluster_arcswap().load())).clone();
    the_cluster
  }

  pub async fn entry_for(&self, file_hash: u64, offset: u64) -> Result<Item> {
    self.cluster.load().entry_for(file_hash, offset).await
  }

  pub async fn data_for_entry_offset(&self, index_loc: &IndexLoc) -> Result<Item> {
    self.cluster.load().data_for_entry_offset(index_loc).await
  }

  pub async fn data_for_hash(&self, hash: MD5Hash) -> Result<Item> {
    self.cluster.load().data_for_hash(hash).await
  }

  pub async fn data_for_key(&self, data: &str) -> Result<Item> {
    self.cluster.load().data_for_key(data).await
  }

  pub async fn antialias_for(&self, data: &str) -> Result<Item> {
    self.cluster.load().antialias_for(data).await
  }

  pub fn contained_by(data: &Item) -> HashSet<String> {
    let mut ret = HashSet::new();
    for edge in data.connections.iter() {
      if edge.0.is_contained_by_up() || edge.0.is_to_right() {
        ret.insert(edge.1.clone());
      }
    }

    ret
  }



  pub fn get_config_table(&self) -> Arc<Map<String, toml::Value>> {
    self.config_table.load().clone()
  }

  pub async fn rebuild(&self) -> Result<()> {
    let config_table = self.args.read_conf_file().await?;
    let new_cluster =
      GoatRodeoCluster::cluster_from_config(self.args.conf_file()?, &config_table).await?;
    self.cluster.store(Arc::new(new_cluster));
    Ok(())
  }

  pub fn the_args(&self) -> Args {
    self.args.clone()
  }

  /// Create a new Index based on an existing GoatRodeo instance with the number of threads
  /// and an optional set of args. This is meant to be used to create an Index without
  /// a well defined set of Args
  pub async fn new_from_cluster(
    cluster: Arc<ArcSwap<GoatRodeoCluster>>,
    args_opt: Option<Args>,
  ) -> Result<Arc<RodeoServer>> {
    // do this in a block so the index is released at the end of the block
    if false {
      // dunno if this is a good idea...
      info!("Loading full index...");
      cluster.load().get_index().await?;
      info!("Loaded index")
    }

    let args = args_opt.unwrap_or_default();
    let config_table = args.read_conf_file().await.unwrap_or_default();

    let ret = Arc::new(RodeoServer {
      config_table: ArcSwap::new(Arc::new(config_table)),
      cluster: cluster,
      args: args,
    });

    Ok(ret)
  }

  pub async fn new(args: Args) -> Result<Arc<RodeoServer>> {
    let config_table = args.read_conf_file().await?;

    let cluster = Arc::new(ArcSwap::new(Arc::new(
      GoatRodeoCluster::cluster_from_config(args.conf_file()?, &config_table).await?,
    )));

    RodeoServer::new_from_cluster(cluster, Some(args)).await
  }
}
