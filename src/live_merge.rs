#[cfg(not(test))]
use log::info;
use std::{collections::HashMap, time::Instant}; // Use log crate when building application

use crate::{
  cluster_writer::ClusterWriter,
  index_file::{IndexLoc, ItemOffset},
  mod_share::{update_top, ClusterPos},
  rodeo::GoatRodeoCluster,
  structs::{Item, ItemMergeResult, MetaData},
  util::path_plus_timed,
};
use anyhow::{bail, Result};
use im::OrdMap;
#[cfg(test)]
use std::println as info;
use thousands::Separable;

pub fn perform_merge<MDT>(clusters: Vec<GoatRodeoCluster<MDT>>) -> Result<GoatRodeoCluster<MDT>>
where
  for<'de2> MDT: MetaData<'de2>,
{
  let start = Instant::now();
  if clusters.is_empty() {
    bail!("`live_merge` requires at least one cluster");
  }

  if clusters.len() == 1 {
    return Ok(clusters[0].clone());
  }

  let mut index_holder = vec![];

  let mut max_size = 0usize;

  for cluster in &clusters {
    let index = cluster.get_index()?;
    let index_len = index.len();
    max_size += index_len;
    index_holder.push(ClusterPos {
      cluster: index.clone(),
      pos: 0,
      len: index.len(),
      thing: 0,
    });
  }

  info!(
    "Loaded indexes at {:?}",
    Instant::now().duration_since(start)
  );

  // build submap
  let mut submap = HashMap::new();
  for b in &clusters {
    submap.insert(b.cluster_file_hash, b.clone());
  }

  let mut new_index = Vec::with_capacity(max_size * 90usize / 100usize);
  let mut loop_cnt = 0usize;

  let mut top = OrdMap::new();

  let ihl = index_holder.len();
  update_top(&mut top, &mut index_holder, 0..ihl);

  while !index_holder.iter().all(|v| v.pos >= v.len) || !top.is_empty() {
    let (next, opt_min) = top.without_min();
    top = opt_min;
    match next {
      Some(mut items) if items.len() > 0 => {
        // update the list
        update_top(&mut top, &mut index_holder, items.iter().map(|v| v.which));

        // we've only got one... so just put it in the new index
        if items.len() == 1 {
          new_index.push(items.pop().unwrap().item_offset);
        } else {
          let computed_loc = items
            .iter()
            .flat_map(|item| item.item_offset.loc.as_vec())
            .collect();
          let new_item_offset = ItemOffset {
            hash: items[0].item_offset.hash,
            loc: IndexLoc::Chain(computed_loc),
          };
          new_index.push(new_item_offset);
        }
      }
      _ => {
        break;
      }
    }

    loop_cnt += 1;
    if loop_cnt % 1_000_000 == 0 {
      info!(
        "Loop {} of {} at {:?}",
        loop_cnt.separate_with_commas(),
        max_size.separate_with_commas(),
        Instant::now().duration_since(start)
      );
    }
  }

  let mut data_files = HashMap::new();
  let mut index_files = HashMap::new();
  for b in clusters.iter() {
    data_files.extend(b.data_files.clone());
    index_files.extend(b.index_files.clone());
  }

  let b0 = &clusters[0];
  info!("Merge took {:?}", Instant::now().duration_since(start));
  b0.create_synthetic_with(new_index, data_files, index_files, submap)
}

#[cfg(feature = "longtest")]
#[test]
fn test_live_merge() {
  use std::{path::PathBuf, time::Instant};
  let start = Instant::now();

  let test_paths: Vec<String> = vec![
    "../../tmp/oc_dest/result_aa",
    "../../tmp/oc_dest/result_ab",
    "../../tmp/oc_dest/result_ac",
    // "../../tmp/oc_dest/result_ad",
    // "../../tmp/oc_dest/result_ae",
    // "../../tmp/oc_dest/result_af",
    // "../../tmp/oc_dest/result_ag",
    // "../../tmp/oc_dest/result_ah",
    // "../../tmp/oc_dest/result_ai",
    // "../../tmp/oc_dest/result_aj",
    // "../../tmp/oc_dest/result_ak",
    // "../../tmp/oc_dest/result_al",
    // "../../tmp/oc_dest/result_am",
    // "../../tmp/oc_dest/result_an",
    // "../../tmp/oc_dest/result_ao",
    // "../../tmp/oc_dest/result_ap",
  ]
  .into_iter()
  .filter(|p| {
    let pb: PathBuf = p.into();
    pb.exists() && pb.is_dir()
  })
  .map(|v| v.to_string())
  .collect();

  if test_paths.len() < 2 {
    return;
  }

  let test_clusters: Vec<GoatRodeoCluster> = test_paths
    .iter()
    .flat_map(|v| GoatRodeoCluster::cluster_files_in_dir(v.into()).unwrap())
    .collect();

  info!(
    "Got test clusters {:?}",
    Instant::now().duration_since(start)
  );

  let cluster_copy = test_clusters.clone();

  let dest_cluster = perform_merge(test_clusters).expect("The cluster should merge");

  assert!(dest_cluster.synthetic, "Should be synthetic");
  info!("Merged {:?}", Instant::now().duration_since(start));

  do_test(cluster_copy.clone(), &dest_cluster, "Synth", start);

  let persisted_cluster = persist_synthetic(dest_cluster)
    .expect("We should be able to turn the synthetic cluster into a normal cluster");

  do_test(cluster_copy, &persisted_cluster, "Pers", start);

  fn do_test(
    cluster_copy: Vec<GoatRodeoCluster>,
    persisted_cluster: &GoatRodeoCluster,
    name: &str,
    start: Instant,
  ) {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut loop_cnt = 0usize;

    for test_cluster in cluster_copy {
      let index = test_cluster.get_index().unwrap();
      for item in index.iter() {
        if rng.gen_range(0..1000) == 42 {
          let old = test_cluster.data_for_hash(item.hash).unwrap().1;
          let new = persisted_cluster.data_for_hash(item.hash).unwrap().1;
          assert_eq!(
            old.identifier, new.identifier,
            "Expecting the same identifiers"
          );

          for i in old.connections.iter() {
            assert!(
              new.connections.contains(i),
              "Expecting {} to contain {:?}",
              new.identifier,
              i
            );
          }

          match (&new.metadata, &old.metadata) {
            (None, Some(omd)) => assert!(
              false,
              "for {} new did not contain metadata, old did {:?}",
              new.identifier, omd
            ),
            (Some(nmd), Some(omd)) => {
              for file_name in omd.file_names.iter() {
                assert!(
                  nmd.file_names.contains(file_name),
                  "Expected new to contain filenames id {} old filenames {:?} new filenames {:?}",
                  new.identifier,
                  omd.file_names,
                  nmd.file_names
                );
              }
            }
            _ => {}
          }
        }
        loop_cnt += 1;
        if loop_cnt % 500_000 == 0 {
          info!(
            "{} Testing Loop {} at {:?} cluster {:?}",
            name,
            loop_cnt.separate_with_commas(),
            std::time::Instant::now().duration_since(start),
            test_cluster.cluster_path,
          );
        }
      }
    }
  }
}

pub fn persist_synthetic<MDT>(cluster: GoatRodeoCluster<MDT>) -> Result<GoatRodeoCluster<MDT>>
where
  for<'de2> MDT: MetaData<'de2>,
{
  // if it's not synthetic, just return it
  if !cluster.synthetic {
    return Ok(cluster);
  }

  let enclosed = cluster.all_sub_clusters();
  let root_path = GoatRodeoCluster::common_parent_dir(enclosed.as_slice())?;
  let target_dir = path_plus_timed(&root_path, "synthetic_cluster");
  std::fs::create_dir_all(&target_dir)?;
  let mut loop_cnt = 0usize;
  let mut merge_cnt = 0usize;
  let start = Instant::now();

  let mut writer = ClusterWriter::new(&target_dir)?;

  for idx in cluster.get_index()?.iter() {
    match idx.loc {
      IndexLoc::Loc { offset, file_hash } => writer.add_index(idx.hash, file_hash, offset)?,
      IndexLoc::Chain(_) => {
        let found = cluster.vec_for_entry_offset(&idx.loc)?;
        if found.len() == 0 {
          // weird, but do nothing... don't add the index
        } else if found.len() == 1 {
          // if we only find one, then just write the index for that item
          writer.add_index(idx.hash, found[0].reference.0, found[0].reference.1)?;
        } else {
          let the_ref = found[0].reference;
          match Item::merge_vecs(found) {
            ItemMergeResult::Same => {
              // just add the index
              writer.add_index(idx.hash, the_ref.0, the_ref.1)?
            }
            ItemMergeResult::ContainsAll(loc_ref) => {
              // just add the index
              writer.add_index(idx.hash, loc_ref.0, loc_ref.1)?
            }
            ItemMergeResult::New(item) => {
              // need a real new item
              merge_cnt += 1;
              writer.write_item(item)?
            }
          }
        }
      }
    }

    loop_cnt += 1;
    if loop_cnt % 1_000_000 == 0 {
      info!(
        "Persist synthetic loop {} merged {} at {:?}",
        loop_cnt.separate_with_commas(),
        merge_cnt.separate_with_commas(),
        Instant::now().duration_since(start)
      );
    }
  }

  let new_cluster_path = writer.finalize_cluster()?;
  GoatRodeoCluster::new(&root_path, &new_cluster_path)
}
