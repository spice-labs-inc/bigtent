use std::{collections::HashMap, time::Instant};

use crate::{
    index::{IndexLoc, ItemOffset},
    rodeo::GoatRodeoBundle,
};
use anyhow::{bail, Result};
use thousands::Separable;

pub fn live_merge(bundles: Vec<GoatRodeoBundle>) -> Result<GoatRodeoBundle> {
    let start = Instant::now();
    if bundles.is_empty() {
        bail!("`live_merge` requires at least one bundle");
    }

    if bundles.len() == 1 {
        return Ok(bundles[0].clone());
    }

    let mut index_holder = vec![];

    let mut max_size = 0usize;

    for bundle in &bundles {
        let index = bundle.get_index()?;
        let index_len = index.len();
        max_size += index_len;
        index_holder.push((index.clone(), 0usize, index_len));
    }

    println!(
        "Loaded indexes at {:?}",
        Instant::now().duration_since(start)
    );

    let mut new_index: Vec<ItemOffset> = Vec::with_capacity(max_size);
    let mut loop_cnt = 0usize;

    loop {
        loop_cnt += 1;
        if loop_cnt % 1_000_000 == 0 {
            println!(
                "Loop {} of {} at {:?}",
                loop_cnt.separate_with_commas(),
                max_size.separate_with_commas(),
                Instant::now().duration_since(start)
            );
        }

        let mut top = vec![];
        let mut vec_pos = 0;
        {
            for (idx, pos, len) in index_holder.iter() {
                if *pos < *len {
                    top.push((idx[*pos].clone(), vec_pos));
                }
                vec_pos += 1;
            }
        }

        // we've run out of elements
        if top.is_empty() {
            break;
        }

        top.sort_by_key(|i| i.0.hash);
        let len = top.len();
        let mut pos = 1;
        let merge_base = &top[0];
        let mut to_merge = vec![];
        while pos < len && top[pos].0.hash == merge_base.0.hash {
            to_merge.push(&top[pos]);
            pos += 1;
        }

        index_holder[merge_base.1].1 += 1;

        if to_merge.is_empty() {
            new_index.push(merge_base.0.clone());
        } else {
            let mut computed_loc = merge_base.0.loc.as_vec();
            computed_loc.append(
                &mut to_merge
                    .iter()
                    .flat_map(|il| {
                        index_holder[il.1].1 += 1;
                        il.0.loc.as_vec()
                    })
                    .collect(),
            );
            let new_item_offset = ItemOffset {
                hash: merge_base.0.hash,
                loc: IndexLoc::Chain(computed_loc),
            };
            new_index.push(new_item_offset);
        }
    }

    let mut data_files = HashMap::new();
    let mut index_files = HashMap::new();
    for b in bundles.iter() {
        data_files.extend(b.data_files.clone());
        index_files.extend(b.index_files.clone());
    }

    let b0 = &bundles[0];
    println!("Merge took {:?}", Instant::now().duration_since(start));
    Ok(b0.clone_with(new_index, data_files, index_files))
}

#[test]
fn test_live_merge() {
    use std::time::Instant;
    let start = Instant::now();

    let test_paths: Vec<String> = vec![
        "../../tmp/oc_dest/result_aa".into(),
        "../../tmp/oc_dest/result_ab".into(),
        "../../tmp/oc_dest/result_ac".into(),
        "../../tmp/oc_dest/result_ad".into(),
    ];

    let test_bundles: Vec<GoatRodeoBundle> = test_paths
        .iter()
        .flat_map(|v| GoatRodeoBundle::bundle_files_in_dir(v.into()).unwrap())
        .collect();

    println!(
        "Got test bundles {:?}",
        Instant::now().duration_since(start)
    );

    let bundle_copy = test_bundles.clone();

    let dest_bundle = live_merge(test_bundles).expect("The bundle should merge");

    println!("Merged {:?}", Instant::now().duration_since(start));

    let mut loop_cnt = 0usize;

    for test_bundle in bundle_copy {
        let index = test_bundle.get_index().unwrap();
        for item in index.iter() {
            let old = test_bundle.data_for_hash(item.hash).unwrap().1;
            let new = dest_bundle.data_for_hash(item.hash).unwrap().1;
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
                        assert!(nmd.file_names.contains(file_name), "Expected new to contain filenames id {} old filenames {:?} new filenames {:?}",
              new.identifier, omd.file_names, nmd.file_names);
                    }
                }
                _ => {}
            }

            loop_cnt += 1;
            if loop_cnt % 50_000 == 0 {
                println!(
                    "Testing Loop {} at {:?} bundle {:?}",
                    loop_cnt.separate_with_commas(),
                    std::time::Instant::now().duration_since(start),
                    test_bundle.envelope_path,
                );
            }
        }
    }
}
