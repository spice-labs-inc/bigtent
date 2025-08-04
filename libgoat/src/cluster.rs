use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterFileEnvelope {
  pub version: u32,
  pub magic: u32,
  pub data_files: Vec<u64>,
  pub index_files: Vec<u64>,
  pub info: BTreeMap<String, String>,
}

#[allow(non_upper_case_globals)]
pub const ClusterFileMagicNumber: u32 = 0xba4a4a; // Banana
#[allow(non_upper_case_globals)]
pub const MinClusterVersion: u32 = 3;
pub const CLUSTER_VERSION: u32 = MinClusterVersion;

impl ClusterFileEnvelope {
  pub fn validate(&self) -> Result<()> {
    if self.magic != ClusterFileMagicNumber {
      bail!("Loaded a cluster with an invalid magic number: {:?}", self);
    }

    if self.version != MinClusterVersion {
      bail!(
        "Loaded a Cluster with version {} but this code only supports version {} Clusters",
        self.version,
        MinClusterVersion
      );
    }

    Ok(())
  }
}

impl std::fmt::Display for ClusterFileEnvelope {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "ClusterFileEnvelope {{v: {}, data_files: {:?}, index_files: {:?}, info: {:?}}}",
      self.version,
      self
        .data_files
        .iter()
        .map(|h| format!("{:016x}", h))
        .collect::<Vec<String>>(),
      self
        .index_files
        .iter()
        .map(|h| format!("{:016x}", h))
        .collect::<Vec<String>>(),
      self.info,
    )
  }
}
