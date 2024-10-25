use flume::Receiver;
use im::OrdMap;

use log::error;
#[cfg(not(test))]
use log::info;
use std::{
  fs::{self, File},
  io::{BufWriter, Write},
  path::PathBuf,
  time::Instant,
}; // Use log crate when building application

#[cfg(test)]
use std::println as info;

use crate::{
  cluster_writer::ClusterWriter,
  mod_share::{update_top, ClusterPos},
  rodeo::GoatRodeoCluster,
  rodeo_server::MD5Hash,
  structs::{EdgeType, Item, MetaData},
  util::NiceDurationDisplay,
};
use anyhow::Result;
use thousands::Separable;

pub fn merge_fresh<PB: Into<PathBuf>, MDT>(
  clusters: Vec<GoatRodeoCluster<MDT>>,
  use_threads: bool,
  dest_directory: PB,
) -> Result<()>
where
  for<'de2> MDT: MetaData<'de2> + 'static,
{
  let start = Instant::now();
  let dest: PathBuf = dest_directory.into();

  fs::create_dir_all(dest.clone())?;

  let mut purl_file = BufWriter::new(File::create({
    let mut dest = dest.clone();

    dest.push("purls.txt");
    dest
  })?);

  info!(
    "Created target dir at {:?}",
    Instant::now().duration_since(start)
  );

  let mut loop_cnt = 0usize;
  let mut merge_cnt = 0usize;
  let mut max_merge_len = 0usize;

  let mut index_holder = vec![];

  for cluster in &clusters {
    let index = cluster.get_index()?;
    let index_len = index.len();

    let (tx, rx) = flume::bounded(32);

    if use_threads {
      let index_shadow = index.clone();
      let cluster_shadow = cluster.clone();

      std::thread::spawn(move || {
        let mut loop_cnt = 0usize;
        let start = Instant::now();
        let index_len = index_shadow.len();
        for io in index_shadow.iter() {
          let item = cluster_shadow
            .data_for_entry_offset(&io.loc)
            .expect("Expected to load data");

          match tx.send((item, loop_cnt)) {
            Ok(_) => {}
            Err(e) => {
              error!(
                "On {:016x} failed to send {} of {} error {}",
                cluster_shadow.cluster_file_hash,
                loop_cnt.separate_with_commas(),
                index_len.separate_with_commas(),
                e
              );
              panic!(
                "On {:016x} failed to send {} of {} error {}",
                cluster_shadow.cluster_file_hash,
                loop_cnt.separate_with_commas(),
                index_len.separate_with_commas(),
                e
              );
            }
          };
          loop_cnt += 1;
          if false && loop_cnt % 2_500_000 == 0 {
            info!(
              "Cluster fetcher {:016x} loop {} of {} at {:?}",
              cluster_shadow.cluster_file_hash,
              loop_cnt.separate_with_commas(),
              index_len.separate_with_commas(),
              start.elapsed()
            );
          }
        }
      });
    }

    index_holder.push(ClusterPos {
      cluster: index.clone(),
      pos: 0,
      len: index.len(),
      thing: if use_threads { Some(rx) } else { None },
    });

    max_merge_len += index_len;
  }

  info!(
    "Read indicies at {:?}",
    Instant::now().duration_since(start)
  );

  let mut cluster_writer = ClusterWriter::new(&dest)?;
  let merge_start = Instant::now();

  let mut top = OrdMap::new();

  let ihl = index_holder.len();
  update_top(&mut top, &mut index_holder, 0..ihl);

  while !index_holder.iter().all(|v| v.pos >= v.len) || !top.is_empty() {
    let (next, opt_min) = top.without_min();
    top = opt_min;

    fn get_item_and_pos<MDT>(
      hash: MD5Hash,
      which: usize,
      index_holder: &mut Vec<ClusterPos<Option<Receiver<(Item<MDT>, usize)>>>>,
      clusters: &Vec<GoatRodeoCluster<MDT>>,
    ) -> Result<(Item<MDT>, usize)>
    where
      for<'de2> MDT: MetaData<'de2>,
    {
      match &index_holder[which].thing {
        Some(rx) => rx.recv().map_err(|e| e.into()),
        _ => match clusters[which].data_for_hash(hash) {
          Ok(i) => Ok((i, 0)),
          Err(e) => Err(e),
        },
      }
    }

    match next {
      Some(items) if items.len() > 0 => {
        // update the list
        update_top(&mut top, &mut index_holder, items.iter().map(|v| v.which));

        let mut to_merge = vec![];

        for merge_base in items {
          let (mut merge_final, _the_pos) = get_item_and_pos(
            merge_base.item_offset.hash,
            merge_base.which,
            &mut index_holder,
            &clusters,
          )?;

          merge_final.remove_references();
          match merge_final
            .connections
            .iter()
            .filter(|v| v.1.starts_with("pkg:"))
            .collect::<Vec<&(EdgeType, String)>>()
          {
            v if !v.is_empty() => {
              for (_, purl) in v {
                purl_file.write(format!("{}\n", purl).as_bytes())?;
              }
            }
            _ => {}
          }
          to_merge.push(merge_final);
        }

        let mut top = to_merge.pop().unwrap();
        merge_cnt += to_merge.len();
        top.remove_references();
        for mut i in to_merge {
          i.remove_references();
          top = top.merge(i);
        }
        let cur_pos = cluster_writer.cur_pos() as u64;
        top.reference.1 = cur_pos;

        cluster_writer.write_item(top)?;

        loop_cnt += 1;

        if loop_cnt % 2_500_000 == 0 {
          let diff = merge_start.elapsed();
          let items_per_second = (loop_cnt as f64) / diff.as_secs_f64();
          let remaining = (((max_merge_len - merge_cnt) - loop_cnt) as f64) / items_per_second;

          let nd: NiceDurationDisplay = remaining.into();
          let td: NiceDurationDisplay = merge_start.elapsed().into();
          info!(
            "{} cnt {}m of {}m merge cnt {} position {}M at {} estimated end {}",
            if use_threads { "T-Merge" } else { "Merge" },
            (loop_cnt / 1_000_000).separate_with_commas(),
            ((max_merge_len - merge_cnt) / 1_000_000).separate_with_commas(),
            merge_cnt.separate_with_commas(),
            (cur_pos >> 20).separate_with_commas(),
            td,
            nd
          );
        }
      }
      _ => {
        break;
      }
    }
  }

  cluster_writer.finalize_cluster()?;
  info!(
    "Finished {} loops at {:?}",
    loop_cnt.separate_with_commas(),
    start.elapsed()
  );
  Ok(())
}

#[cfg(feature = "longtest")]
#[test]
fn test_merge() {
  use rand::Rng;
  let mut rng = rand::thread_rng();

  let target_path = "/tmp/bt_merge_test";
  // delete the dest dir
  let _ = fs::remove_dir_all(target_path);

  info!("Removed directory");

  let test_paths: Vec<String> = vec![
    "../../tmp/oc_dest/result_aa",
    "../../tmp/oc_dest/result_ab",
    // "../../tmp/oc_dest/result_ac",
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

  info!("Got test clusters");

  for should_thread in vec![true, false] {
    let cluster_copy = test_clusters.clone();

    merge_fresh(test_clusters.clone(), should_thread, &target_path)
      .expect("Should merge correctly");

    // a block so the file gets closed
    {
      let mut purl_path = PathBuf::from(target_path);
      purl_path.push("purls.txt");
      let mut purl_file = BufReader::new(File::open(purl_path).expect("Should have purls"));
      let mut read = String::new();
      purl_file.read_line(&mut read).expect("Must read a line");
      assert!(read.len() > 4, "Expected to read a pURL");
    }

    let dest_cluster = GoatRodeoCluster::cluster_files_in_dir(target_path.into())
      .expect("Got a valid cluster in test directory")
      .pop()
      .expect("And it should have at least 1 element");

    let mut loop_cnt = 0usize;
    let start = Instant::now();
    for test_cluster in cluster_copy {
      let index = test_cluster.get_index().unwrap();
      for item in index.iter() {
        if rng.gen_range(0..1000) == 42 {
          let old = test_cluster.data_for_hash(item.hash).unwrap().1;
          let new = dest_cluster.data_for_hash(item.hash).unwrap().1;
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
            "MTesting Loop {} at {:?} cluster {:?} threaded {}",
            loop_cnt,
            Instant::now().duration_since(start),
            test_cluster.cluster_path,
            should_thread
          );
        }
      }
    }
  }
}
