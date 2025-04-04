#[cfg(not(test))]
use log::info;
use std::{
  collections::{BTreeSet, HashMap},
  fs::{self, File},
  io::{BufWriter, Write},
  path::PathBuf,
  sync::Arc,
  thread,
  time::Instant,
}; // Use log crate when building application

#[cfg(test)]
use std::println as info;

use crate::{
  item::Item,
  rodeo::{
    goat::GoatRodeoCluster,
    goat_trait::GoatRodeoTrait,
    index::{HasHash, ItemOffset},
    writer::ClusterWriter,
  },
  util::{MD5Hash, NiceDurationDisplay},
};
use anyhow::Result;
use thousands::Separable;

pub async fn merge_fresh<PB: Into<PathBuf>>(
  clusters: Vec<Arc<GoatRodeoCluster>>,
  dest_directory: PB,
) -> Result<()> {
  let start = Instant::now();
  let dest: PathBuf = dest_directory.into();

  fs::create_dir_all(dest.clone())?;
  let mut seen_purls = BTreeSet::new();

  info!(
    "Created target dir at {:?}",
    Instant::now().duration_since(start)
  );

  let mut loop_cnt = 0usize;
  let mut merge_cnt = 0usize;
  let mut max_merge_len = 0usize;

  let mut index_holder = vec![];

  for (idx, cluster) in clusters.iter().enumerate() {
    let index_len = cluster.number_of_items();

    index_holder.push(ClusterPos {
      cluster: cluster.clone(),
      pos: 0,
      cache: None,
      len: index_len,
    });

    info!(
      "Loaded cluster {} of {}, from {}",
      idx + 1,
      clusters.len(),
      cluster.name()
    );

    max_merge_len += index_len;
  }

  info!(
    "Read indicies at {:?}",
    Instant::now().duration_since(start)
  );

  let mut cluster_writer = ClusterWriter::new(&dest).await?;
  let merge_start = Instant::now();

  let (offset_tx, offset_rx) = flume::bounded(200_000);

  let mut threads = vec![];

  let coorindator_handle = thread::spawn(move || {
    let mut pos = 0usize;
    while let Some(items_to_merge) = next_hash_of_item_to_merge(&mut index_holder) {
      offset_tx
        .send((pos, items_to_merge))
        .expect("Should be able to send the message");
      pos += 1;
    }
  });

  threads.push(coorindator_handle);

  let (merged_tx, merged_rx) = flume::bounded(200_000);

  for thread_num in 0..20 {
    let rx = offset_rx.clone();
    let tx = merged_tx.clone();
    let processor_handle = thread::spawn(move || {
      let mut cnt = 0usize;
      while let Ok((position, items_to_merge)) = rx.recv() {
        let mut to_merge = vec![];
        let mut purls = vec![];
        for (offset, cluster) in &items_to_merge {
          let merge_final = cluster
            .item_from_item_offset(offset)
            .expect("Should get the item");
          merge_final
            .connections
            .iter()
            .filter(|v| v.1.starts_with("pkg:"))
            .for_each(|v| purls.push(v.1.to_string()));

          to_merge.push(merge_final);
        }

        let mut top = to_merge.pop().unwrap();

        for i in to_merge {
          top = top.merge(i);
        }

        let cbor_bytes = serde_cbor::to_vec(&top).expect("Should be able to encode an item");

        tx.send(ItemOrPurl::Item {
          pos: position,
          item: top,
          cbor_bytes,
          merged: items_to_merge.len() - 1,
          purls,
        })
        .expect("Should send message");
        cnt += 1;
        if false && cnt % 500_000 == 0 {
          info!(
            "Thread {} at cnt {}",
            thread_num,
            cnt.separate_with_commas()
          );
        }
      }
    });
    threads.push(processor_handle);
  }

  drop(merged_tx);
  drop(offset_rx); // make sure no more copies exist

  let mut next_expected = 0usize;
  let mut holding_pen = HashMap::new();

  while let Ok(item_or_purl) = merged_rx.recv_async().await {
    match item_or_purl {
      ItemOrPurl::Item {
        pos: position,
        item,
        merged,
        cbor_bytes,
        purls,
      } => {
        for p in purls {
          if !seen_purls.contains(&p) {
            //purl_file.write(format!("{}\n", p).as_bytes())?;
            seen_purls.insert(p);
          }
        }
        merge_cnt += merged;
        if position == next_expected {
          holding_pen.insert(position, (item, cbor_bytes));
          while let Some((item, cbor_bytes)) = holding_pen.remove(&next_expected) {
            cluster_writer.write_item(item, cbor_bytes).await?;

            loop_cnt += 1;

            if loop_cnt % 2_500_000 == 0 {
              let diff = merge_start.elapsed();
              let items_per_second = (loop_cnt as f64) / diff.as_secs_f64();
              let remaining = (((max_merge_len - merge_cnt) - loop_cnt) as f64) / items_per_second;

              let nd: NiceDurationDisplay = remaining.into();
              let td: NiceDurationDisplay = merge_start.elapsed().into();
              info!(
                "Merge cnt {}m of {}m merge cnt {}m at {} estimated end {} written pURLs {} holding pen cnt {}",
                (loop_cnt / 1_000_000).separate_with_commas(),
                ((max_merge_len - merge_cnt) / 1_000_000).separate_with_commas(),
                (merge_cnt / 1_000_000).separate_with_commas(),
                td,
                nd,
                seen_purls.len().separate_with_commas()
                ,holding_pen.len()
              );
            }

            next_expected += 1;
          }
        } else {
          holding_pen.insert(position, (item, cbor_bytes));
        }
      }
    }
  }

  if !holding_pen.is_empty() {
    panic!(
      "Finished receiving, but holding pen is not empty {:?}",
      holding_pen
    );
  }

  info!("Writing pURLs");
  tokio::task::spawn_blocking(move || {
    fn do_thing(dest: PathBuf, seen_purls: BTreeSet<String>) -> Result<()> {
      let mut purl_file = BufWriter::new(File::create({
        let mut dest = dest.clone();

        dest.push("purls.txt");
        dest
      })?);
      for purl in &seen_purls {
        purl_file.write_fmt(format_args!("{}\n", purl))?;
      }
      purl_file.flush()?;
      Ok(())
    }

    do_thing(dest, seen_purls)
  })
  .await??;
  info!("Wrote pURLs");

  cluster_writer.finalize_cluster().await?;
  info!(
    "Finished {} loops at {:?}",
    loop_cnt.separate_with_commas(),
    start.elapsed()
  );
  Ok(())
}

enum ItemOrPurl {
  // Purl(Vec<String>),
  Item {
    pos: usize,
    item: Item,
    cbor_bytes: Vec<u8>,
    merged: usize,
    purls: Vec<String>,
  },
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

struct ClusterPos {
  pub cluster: Arc<GoatRodeoCluster>,
  pub pos: usize,
  pub len: usize,
  pub cache: Option<ItemOffset>,
}

impl ClusterPos {
  pub fn this_item(&mut self) -> Option<ItemOffset> {
    if self.pos >= self.len {
      None
    } else {
      match &self.cache {
        Some(v) => Some(v.clone()),
        None => match self.cluster.offset_from_pos(self.pos) {
          Ok(v) => {
            self.cache = Some(v.clone());
            Some(v)
          }
          _ => None,
        },
      }
    }
  }

  pub fn next(&mut self) {
    self.cache = None;
    self.pos += 1
  }
}

fn next_hash_of_item_to_merge(
  index_holder: &mut Vec<ClusterPos>,
) -> Option<Vec<(ItemOffset, Arc<GoatRodeoCluster>)>> {
  let mut lowest: Option<MD5Hash> = None;
  let mut low_clusters = vec![];

  for holder in index_holder {
    let this_item = holder.this_item();
    match (&lowest, this_item) {
      (None, Some(either)) => {
        // found the first
        lowest = Some(*either.hash());
        low_clusters.push((either, holder));
      }
      // it's the lowe
      (Some(low), Some(either)) if low == either.hash() => {
        low_clusters.push((either, holder));
      }
      (Some(low), Some(either)) if low > either.hash() => {
        lowest = Some(*either.hash());
        low_clusters.clear();
        low_clusters.push((either, holder));
      }
      _ => {}
    }
  }

  match lowest {
    None => None,
    Some(_) => {
      let mut clusters = vec![];
      for (offset, holder) in low_clusters {
        clusters.push((offset, holder.cluster.clone()));
        holder.next();
      }
      Some(clusters)
    }
  }
}
