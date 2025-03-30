use anyhow::Result;
use arc_swap::ArcSwap;
use std::sync::Arc;

use crate::{config::Args, rodeo::goat::GoatRodeoCluster};

#[derive(Debug, Clone)]
pub struct ClusterHolder {
  cluster: Arc<ArcSwap<GoatRodeoCluster>>,
  args: Args,
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

  pub fn the_args(&self) -> Args {
    self.args.clone()
  }

  /// Create a new ClusterHolder based on an existing GoatRodeo instance with the number of threads
  /// and an optional set of args. This is meant to be used to create an Index without
  /// a well defined set of Args
  pub async fn new_from_cluster(
    cluster: Arc<ArcSwap<GoatRodeoCluster>>,
    args_opt: Option<Args>,
  ) -> Result<Arc<ClusterHolder>> {
    let args = args_opt.unwrap_or_default();

    let ret = Arc::new(ClusterHolder {
      cluster: cluster,
      args: args,
    });

    Ok(ret)
  }
}
