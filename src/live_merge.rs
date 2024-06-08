use std::{collections::HashMap, fs, time::Instant};

use crate::{
    bundle_writer::BundleWriter,
    index_file::{IndexLoc, ItemOffset},
    rodeo::GoatRodeoBundle,
    structs::{Item, ItemMergeResult},
    util::path_plus_timed,
};
use anyhow::{bail, Result};
use thousands::Separable;

pub fn perform_merge(bundles: Vec<GoatRodeoBundle>) -> Result<GoatRodeoBundle> {
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

    // build submap
    let mut submap = HashMap::new();
    for b in &bundles {
        submap.insert(b.bundle_file_hash, b.clone());
    }

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
    b0.create_synthetic_with(new_index, data_files, index_files, submap)
}

//#[cfg(feature = "longtest")]
#[test]
fn test_live_merge() {
    use std::{path::PathBuf, time::Instant};
    let start = Instant::now();

    let test_paths: Vec<String> = vec![
        "../../tmp/oc_dest/result_aa",
        "../../tmp/oc_dest/result_ab",
        "../../tmp/oc_dest/result_ac",
        "../../tmp/oc_dest/result_ad",
        "../../tmp/oc_dest/result_ae",
        "../../tmp/oc_dest/result_af",
        "../../tmp/oc_dest/result_ag",
        "../../tmp/oc_dest/result_ah",
        "../../tmp/oc_dest/result_ai",
        "../../tmp/oc_dest/result_aj",
        "../../tmp/oc_dest/result_ak",
        "../../tmp/oc_dest/result_al",
        "../../tmp/oc_dest/result_am",
        "../../tmp/oc_dest/result_an",
        "../../tmp/oc_dest/result_ao",
        "../../tmp/oc_dest/result_ap",
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

    let test_bundles: Vec<GoatRodeoBundle> = test_paths
        .iter()
        .flat_map(|v| GoatRodeoBundle::bundle_files_in_dir(v.into()).unwrap())
        .collect();

    println!(
        "Got test bundles {:?}",
        Instant::now().duration_since(start)
    );

    let bundle_copy = test_bundles.clone();

    let dest_bundle = perform_merge(test_bundles).expect("The bundle should merge");

    assert!(dest_bundle.synthetic, "Should be synthetic");
    println!("Merged {:?}", Instant::now().duration_since(start));

    do_test(bundle_copy.clone(), &dest_bundle, "Synth", start);

    let persisted_bundle = persist_synthetic(dest_bundle)
        .expect("We should be able to turn the synthetic bundle into a normal bundle");

    do_test(bundle_copy, &persisted_bundle, "Pers", start);

    fn do_test(
        bundle_copy: Vec<GoatRodeoBundle>,
        persisted_bundle: &GoatRodeoBundle,
        name: &str,
        start: Instant,
    ) {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut loop_cnt = 0usize;

        for test_bundle in bundle_copy {
            let index = test_bundle.get_index().unwrap();
            for item in index.iter() {
                if rng.gen_range(0..1000) == 42 {
                    let old = test_bundle.data_for_hash(item.hash).unwrap().1;
                    let new = persisted_bundle.data_for_hash(item.hash).unwrap().1;
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
                                assert!(nmd.file_names.contains(file_name), 
                                  "Expected new to contain filenames id {} old filenames {:?} new filenames {:?}",
                                  new.identifier, omd.file_names, nmd.file_names);
                            }
                        }
                        _ => {}
                    }
                }
                loop_cnt += 1;
                if loop_cnt % 500_000 == 0 {
                    println!(
                        "{} Testing Loop {} at {:?} bundle {:?}",name,
                        loop_cnt.separate_with_commas(),
                        std::time::Instant::now().duration_since(start),
                        test_bundle.bundle_path,
                    );
                }
            }
        }
    }
}

pub fn persist_synthetic(bundle: GoatRodeoBundle) -> Result<GoatRodeoBundle> {
    // if it's not synthetic, just return it
    if !bundle.synthetic {
        return Ok(bundle);
    }

    let enclosed = bundle.all_sub_bundles();
    let root_path = GoatRodeoBundle::common_parent_dir(enclosed.as_slice())?;
    let target_dir = path_plus_timed(&root_path, "synthetic_bundle");
    fs::create_dir_all(&target_dir)?;
    let mut loop_cnt = 0usize;
    let mut merge_cnt = 0usize;
    let start = Instant::now();

    let mut writer = BundleWriter::new(&target_dir)?;

    for idx in bundle.get_index()?.iter() {
        match idx.loc {
            IndexLoc::Loc { offset, file_hash } => writer.add_index(idx.hash, file_hash, offset)?,
            IndexLoc::Chain(_) => {
                let found = bundle.vec_for_entry_offset(&idx.loc)?;
                if found.len() == 0 {
                    // weird, but do nothing... don't add the index
                } else if found.len() == 1 {
                    // if we only find one, then just write the index for that item
                    writer.add_index(idx.hash, found[0].1.reference.0, found[0].1.reference.1)?;
                } else {
                    let the_ref = found[0].1.reference;
                    match Item::merge_vecs(found.into_iter().map(|v| v.1).collect()) {
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
            println!(
                "Persist synthetic loop {} merged {} at {:?}",
                loop_cnt.separate_with_commas(),
                merge_cnt.separate_with_commas(),
                Instant::now().duration_since(start)
            );
        }
    }

    let new_bundle_path = writer.finalize_bundle()?;
    GoatRodeoBundle::new(&root_path, &new_bundle_path)
}
