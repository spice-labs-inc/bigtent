//! High Wire: the query system's walk grammar and engine.
//!
//! Prototype under active design discussion; see the phase plans under
//! `plans/2026_09_23_highwire/`.

use std::sync::Arc;

pub mod compiler;
pub mod error;
pub mod grammar;
mod grammar_impl_serde;
pub mod walk;

/// THE WALK'S ENTRY RULE: choose the cheapest lookup that can actually serve
/// the walk.
///
/// * A [`GoatHerd`](crate::rodeo::goat_herd::GoatHerd) with exactly one member
///   that is a V4 cluster → the cluster itself: the walk reads its raw bytes
///   through the Sansho byte source, and no `Item` is ever materialized.
/// * A `GoatRodeoCluster` that is V3 → wrapped in a one-member herd: the herd
///   path materializes `Item`s through the Rust decoder, which understands
///   V3's storage (indefinite-length CBOR) that the byte source cannot parse.
/// * A `GoatRodeoCluster` that is V4 → passed straight through: no
///   materialization, no wrapping.
/// * Anything else — multi-member herds, synthetic members, herds holding V3
///   members — → the herd, unchanged: there is nothing cheaper to collapse to.
pub(crate) fn walk_lookup(lookup: crate::highwire::walk::HerdOrCluster) -> crate::highwire::walk::HerdOrCluster {
    use crate::highwire::walk::HerdOrCluster;
    use crate::rodeo::goat_herd::GoatHerd;
    use crate::rodeo::member::{member_core, HerdMember};

    match lookup {
        HerdOrCluster::Cluster(cluster) => {
            if cluster.cluster_version() == 4 {
                HerdOrCluster::Cluster(cluster)
            } else {
                HerdOrCluster::Herd(Arc::new(GoatHerd::new(vec![member_core(cluster)])))
            }
        }
        HerdOrCluster::Herd(herd) => {
            let members = herd.get_herd();
            if let [single] = members.as_slice()
                && let HerdMember::Cluster(cluster) = single.as_ref()
                && cluster.cluster_version() == 4
            {
                return HerdOrCluster::Cluster(Arc::clone(cluster));
            }
            HerdOrCluster::Herd(herd)
        }
    }
}
