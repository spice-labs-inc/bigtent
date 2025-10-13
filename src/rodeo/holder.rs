use anyhow::Result;
use arc_swap::ArcSwap;
use std::{path::PathBuf, sync::Arc};

use crate::config::Args;

use super::goat_trait::GoatRodeoTrait;

#[derive(Debug)]
pub struct ClusterHolder<GRT: GoatRodeoTrait> {
  cluster: Arc<ArcSwap<GRT>>,
  args: Args,
}

impl<GRT: GoatRodeoTrait> ClusterHolder<GRT> {
  /// update the cluster
  pub fn update_cluster(&self, new_cluster: Arc<GRT>) {
    self.cluster.store(new_cluster);
  }

  /// Get the current GoatRodeoCluster from the server
  pub fn get_cluster(&self) -> Arc<GRT> {
    self.cluster.load().clone()
  }

  pub fn the_args(&self) -> Args {
    self.args.clone()
  }

  /// get the path to the `purls.txt` file
  pub fn get_purl(&self) -> Result<PathBuf> {
    self.cluster.load().get_purl()
  }

  /// Create a new ClusterHolder based on an existing GoatRodeo instance with the number of threads
  /// and an optional set of args. This is meant to be used to create an Index without
  /// a well defined set of Args
  pub async fn new_from_cluster(
    cluster: Arc<ArcSwap<GRT>>,
    args_opt: Option<Args>,
  ) -> Result<Arc<ClusterHolder<GRT>>> {
    let args = args_opt.unwrap_or_default();

    let ret = Arc::new(ClusterHolder {
      cluster: cluster,
      args: args,
    });

    Ok(ret)
  }
}
