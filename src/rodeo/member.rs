//! # HerdMember - Unified Cluster Abstraction
//!
//! This module provides [`HerdMember`], an enum that unifies different cluster
//! implementations behind a common interface, enabling polymorphic cluster handling.
//!
//! ## Variants
//!
//! - [`HerdMember::Cluster`] - Wraps a file-backed [`GoatRodeoCluster`]
//! - [`HerdMember::Robo`] - Wraps an in-memory [`RoboticGoat`]
//!
//! ## Purpose
//!
//! `HerdMember` allows [`super::goat_herd::GoatHerd`] to manage heterogeneous
//! collections of clusters. For example, a herd might contain:
//! - Multiple file-backed clusters from different directories
//! - Synthetic clusters generated at runtime
//!
//! ## Trait Implementations
//!
//! `HerdMember` implements both:
//! - [`GoatRodeoTrait`] - Full query interface
//! - [`ClusterRoboMember`] - Low-level item access
//!
//! Both delegate to the underlying variant.
//!
//! ## Helper Functions
//!
//! - [`member_core`] - Wrap a `GoatRodeoCluster` in a `HerdMember`
//! - [`member_synth`] - Wrap a `RoboticGoat` in a `HerdMember`

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

impl HerdMember {
    pub fn get_blob(&self) -> Option<String> {
        match self {
            HerdMember::Robo(_robotic_goat) => None,
            HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.get_blob(),
        }
    }

    /// if there's a SHA256 associate with the member, return it
    pub fn get_sha(&self) -> Vec<[u8; 32]> {
        match self {
            HerdMember::Robo(_robotic_goat) => vec![],
            HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.get_sha(),
        }
    }

    pub fn get_directory(&self) -> Option<PathBuf> {
        match self {
            HerdMember::Robo(_robotic_goat) => None,
            HerdMember::Cluster(goat_rodeo_cluster) => Some(goat_rodeo_cluster.get_directory()),
        }
    }
}

impl ClusterRoboMember for HerdMember {
    fn name(&self) -> String {
        match self {
            HerdMember::Robo(goat_synth) => goat_synth.name(),
            HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.name(),
        }
    }

    fn offset_from_pos(&self, pos: usize) -> Option<ItemOffset> {
        match self {
            HerdMember::Robo(goat_synth) => goat_synth.offset_from_pos(pos),
            HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.offset_from_pos(pos),
        }
    }

    fn item_from_item_offset(&self, offset: &ItemOffset) -> Option<Item> {
        match self {
            HerdMember::Robo(goat_synth) => goat_synth.item_from_item_offset(offset),
            HerdMember::Cluster(goat_rodeo_cluster) => {
                goat_rodeo_cluster.item_from_item_offset(offset)
            }
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

    fn item_for_identifier(&self, data: &str) -> Option<Item> {
        match self {
            HerdMember::Robo(goat_synth) => goat_synth.item_for_identifier(data),
            HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.item_for_identifier(data),
        }
    }

    fn item_for_hash(&self, hash: MD5Hash) -> Option<Item> {
        match self {
            HerdMember::Robo(goat_synth) => goat_synth.item_for_hash(hash),
            HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.item_for_hash(hash),
        }
    }

    fn antialias_for(self: Arc<Self>, data: &str) -> Option<Item> {
        match &*self {
            HerdMember::Robo(goat_synth) => goat_synth.clone().antialias_for(data),
            HerdMember::Cluster(goat_rodeo_cluster) => {
                goat_rodeo_cluster.clone().antialias_for(data)
            }
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
            HerdMember::Cluster(goat_rodeo_cluster) => {
                goat_rodeo_cluster.has_identifier(identifier)
            }
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            HerdMember::Robo(goat_synth) => goat_synth.is_empty(),
            HerdMember::Cluster(goat_rodeo_cluster) => goat_rodeo_cluster.is_empty(),
        }
    }
}
