use std::{
    fs::{self},
    path::PathBuf,
    time::Instant,
};

use crate::{bundle_writer::BundleWriter, rodeo::GoatRodeoBundle};
use anyhow::Result;
use thousands::Separable;

pub fn merge_no_history<PB: Into<PathBuf>>(
    bundles: Vec<GoatRodeoBundle>,
    dest_directory: PB,
) -> Result<()> {
    let start = Instant::now();
    let dest: PathBuf = dest_directory.into();

    fs::create_dir_all(dest.clone())?;

    println!(
        "Created target dir at {:?}",
        Instant::now().duration_since(start)
    );

    let mut item_cnt = 0usize;
    let mut merge_cnt = 0usize;

    let mut index_holder = vec![];

    for bundle in &bundles {
        let index = bundle.get_index()?;
        let index_len = index.len();
        index_holder.push((index, 0usize, index_len));
    }

    println!(
        "Read indicies at {:?}",
        Instant::now().duration_since(start)
    );

    let mut loop_cnt = 0usize;

    let mut bundle_writer = BundleWriter::new(&dest)?;

    loop {
        loop_cnt += 1;

        let mut top = vec![];
        let mut vec_pos = 0;
        for (idx, pos, len) in &mut index_holder {
            if *pos < *len {
                top.push((idx[*pos].clone(), vec_pos));
            }
            vec_pos += 1;
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

        let mut merge_to = bundles[merge_base.1]
            .data_for_entry_offset(&merge_base.0.loc)?
            .1;
        merge_to.remove_references();
        index_holder[merge_base.1].1 += 1;

        let mut merge_from = vec![];
        for m in to_merge {
            let mut tmp_item = bundles[m.1].data_for_entry_offset(&m.0.loc)?.1;
            tmp_item.remove_references();
            merge_from.push(tmp_item);

            index_holder[m.1].1 += 1;
        }

        let mut merge_final = merge_to.clone();
        merge_cnt += merge_from.len();

        for m2 in merge_from {
            merge_final = merge_final.merge(m2);
        }
        let cur_pos = bundle_writer.cur_pos() as u64;
        merge_final.reference.1 = cur_pos;

        bundle_writer.write_item(merge_final)?;

        item_cnt += 1;

        if item_cnt % 250_000 == 0 {
            println!(
                "Item cnt {} merge cnt {} position {} at {:?}",
                item_cnt.separate_with_commas(),
                merge_cnt.separate_with_commas(),
                cur_pos.separate_with_commas(),
                Instant::now().duration_since(start)
            );
        }
    }

    bundle_writer.finalize_bundle()?;
    println!(
        "Finished {} loops at {:?}",
        loop_cnt.separate_with_commas(),
        Instant::now().duration_since(start)
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

    println!("Removed directory");

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
    let test_bundles: Vec<GoatRodeoBundle> = test_paths
        .iter()
        .flat_map(|v| GoatRodeoBundle::bundle_files_in_dir(v.into()).unwrap())
        .collect();

    println!("Got test bundles");

    let bundle_copy = test_bundles.clone();

    merge_no_history(test_bundles, &target_path).expect("Should merge correctly");

    let dest_bundle = GoatRodeoBundle::bundle_files_in_dir(target_path.into())
        .expect("Got a valid bundle in test directory")
        .pop()
        .expect("And it should have at least 1 element");

    let mut loop_cnt = 0usize;
    let start = Instant::now();
    for test_bundle in bundle_copy {
        let index = test_bundle.get_index().unwrap();
        for item in index.iter() {
            if rng.gen_range(0..1000) == 42 {
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
            }

            loop_cnt += 1;
            if loop_cnt % 500_000 == 0 {
                println!(
                    "MTesting Loop {} at {:?} bundle {:?}",
                    loop_cnt,
                    Instant::now().duration_since(start),
                    test_bundle.bundle_path,
                );
            }
        }
    }
}
