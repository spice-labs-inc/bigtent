use anyhow::Result;
use arc_swap::ArcSwap;
#[cfg(not(test))]
use log::info;
use std::sync::Arc;

use crate::{config::Args, rodeo::GoatRodeoCluster};
#[cfg(test)]
use std::println as info;
use toml::{Table, map::Map};

#[derive(Debug, Clone)]
pub struct ClusterHolder {
  cluster: Arc<ArcSwap<GoatRodeoCluster>>,
  args: Args,
  config_table: Arc<ArcSwap<Table>>,
}

impl ClusterHolder {
  /// update the cluster
  pub fn update_cluster(&self, new_cluster: Arc<GoatRodeoCluster>) {
    self.cluster.store(new_cluster);
  }

  /// Get the current GoatRodeoCluster from the server
  pub fn get_cluster(&self) -> Arc<GoatRodeoCluster> {
    self.cluster.load().clone()
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
  ) -> Result<Arc<ClusterHolder>> {
    // do this in a block so the index is released at the end of the block
    if false {
      // dunno if this is a good idea...
      info!("Loading full index...");
      cluster.load().get_index().await?;
      info!("Loaded index")
    }

    let args = args_opt.unwrap_or_default();
    let config_table = args.read_conf_file().await.unwrap_or_default();

    let ret = Arc::new(ClusterHolder {
      config_table: Arc::new(ArcSwap::new(Arc::new(config_table))),
      cluster: cluster,
      args: args,
    });

    Ok(ret)
  }

  pub async fn new(args: Args) -> Result<Arc<ClusterHolder>> {
    let config_table = args.read_conf_file().await?;

    let cluster = Arc::new(ArcSwap::new(Arc::new(
      GoatRodeoCluster::cluster_from_config(args.conf_file()?, &config_table).await?,
    )));

    ClusterHolder::new_from_cluster(cluster, Some(args)).await
  }
}
