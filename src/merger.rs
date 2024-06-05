use std::{
    collections::HashMap,
    fs::{self, File},
    io::Write,
    path::PathBuf,
    time::Instant,
};

use crate::{
    envelopes::{ItemEnvelope, MD5, MULTIFILE_NOOP},
    index::MD5Hash,
    rodeo::{BundleFileEnvelope, DataFileEnvelope, GoatRodeoBundle, IndexEnvelope},
    util::{
        byte_slice_to_u63, md5hash_str, millis_now, sha256_for_slice, write_envelope, write_int,
        write_long, write_short, write_short_signed,
    },
};
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
    let mut written: Vec<(u64, u64)> = vec![];
    let mut loop_cnt = 0usize;

    loop {
        loop_cnt += 1;
        println!(
            "Entering loop {} at {:?}",
            loop_cnt,
            Instant::now().duration_since(start)
        );
        let mut no_more_elements = false;
        let mut dest_data: Vec<u8> = vec![];
        let mut index_pos: Vec<(MD5Hash, u64)> = vec![];
        {
            let data_writer = &mut dest_data;

            write_int(data_writer, GoatRodeoBundle::DataFileMagicNumber)?;

            let data_envelope = DataFileEnvelope {
                version: 1,
                magic: GoatRodeoBundle::DataFileMagicNumber,
                the_type: "Goat Rodeo Data".into(),
                previous: written.last().unwrap_or(&(0u64, 0u64)).0,
                depends_on: vec![],
                timestamp: millis_now(),
                built_from_merge: false,
                info: HashMap::new(),
            };

            write_envelope(data_writer, &data_envelope)?;

            let mut previous_position = 0usize;

            loop {
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
                    no_more_elements = true;
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

                let cur_pos = data_writer.len();

                let mut merge_final = merge_to.clone();
                merge_cnt += merge_from.len();

                for m2 in merge_from {
                    merge_final = merge_final.merge(m2);
                }

                merge_final.reference.1 = cur_pos as u64;

                let item_bytes = serde_cbor::to_vec(&merge_final)?;

                let the_hash = md5hash_str(&merge_final.identifier);
                let item_envelope = ItemEnvelope {
                    key_md5: MD5 { hash: the_hash },
                    position: cur_pos as u64,
                    timestamp: millis_now(),
                    previous_version: MULTIFILE_NOOP,
                    backpointer: previous_position as u64,
                    data_len: item_bytes.len() as u32,
                    data_format: crate::envelopes::PayloadFormat::CBOR,
                    data_type: crate::envelopes::PayloadType::ENTRY,
                    compression: crate::envelopes::PayloadCompression::NONE,
                    merged_with_previous: false,
                };
                let item_env_bytes = serde_cbor::to_vec(&item_envelope)?;
                write_short(data_writer, item_env_bytes.len() as u16)?;
                write_int(data_writer, item_bytes.len() as u32)?;

                data_writer.write(&item_env_bytes)?;
                data_writer.write(&item_bytes)?;
                index_pos.push((the_hash, cur_pos as u64));

                previous_position = cur_pos;

                item_cnt += 1;

                if item_cnt % 250_000 == 0 {
                    println!(
                        "Loop {} Item cnt {} merge cnt {} position {} at {:?}",
                        loop_cnt,
                        item_cnt.separate_with_commas(),
                        merge_cnt.separate_with_commas(),
                        cur_pos.separate_with_commas(),
                        Instant::now().duration_since(start)
                    );
                }
                // write at each 16gb -- avoid wrap-around
                if cur_pos > 16_000_000_000usize {
                    break;
                }
            }

            println!(
                "Loop {} Writing a blob at {:?}",
                loop_cnt,
                Instant::now().duration_since(start)
            );

            // write tail data

            write_short_signed(data_writer, -1)?; // a marker that says end of file

            // write final back-pointer (to the last entry record)
            write_long(data_writer, previous_position as u64)?;
        }
        // compute SHA256 of data

        println!(
            "computing grd sha {:?}",
            Instant::now().duration_since(start)
        );
        let data_reader: &[u8] = &dest_data;
        let grd_sha = byte_slice_to_u63(&sha256_for_slice(data_reader))?;
        println!(
            "computed grd sha {:?}",
            Instant::now().duration_since(start)
        );
        // write the .grd file
        {
            let grd_file_path = dest.join(format!("{:016x}.grd", grd_sha));

            let mut grd_file = File::create(grd_file_path)?;
            grd_file.write_all(&dest_data)?;
            grd_file.flush()?;
        }

        println!(
            "loop {} wrote grd {:?}",
            loop_cnt,
            Instant::now().duration_since(start)
        );

        // free up 16gb
        dest_data.clear();

        // build up index

        let mut index_file = vec![];
        {
            let index_writer = &mut index_file;
            write_int(index_writer, GoatRodeoBundle::IndexFileMagicNumber)?;
            let index_env = IndexEnvelope {
                version: 1,
                magic: GoatRodeoBundle::IndexFileMagicNumber,
                the_type: "Goat Rodeo Index".into(),
                size: index_pos.len() as u32,
                data_files: vec![grd_sha],
                encoding: "MD5/Long/Long".into(),
                timestamp: millis_now(),
                info: HashMap::new(),
            };
            write_envelope(index_writer, &index_env)?;
            for (hash, pos) in &index_pos {
                index_writer.write(hash)?;
                write_long(index_writer, grd_sha)?;
                write_long(index_writer, *pos)?;
            }
        }

        // compute sha256 of index
        let index_reader: &[u8] = &index_file;
        let gri_sha = byte_slice_to_u63(&sha256_for_slice(index_reader))?;

        // write the .gri file
        {
            let gri_file_path = dest.join(format!("{:016x}.gri", gri_sha));
            let mut gri_file = File::create(gri_file_path)?;
            gri_file.write_all(&index_file)?;
            gri_file.flush()?;
        }

        // push sha256 of data and index
        written.push((grd_sha, gri_sha));

        println!(
            "loop {} wrote index {:?}",
            loop_cnt,
            Instant::now().duration_since(start)
        );
        if no_more_elements {
            break;
        }
    }

    let mut bundle_file = vec![];
    {
        let bundle_writer = &mut bundle_file;
        write_int(bundle_writer, GoatRodeoBundle::BundleFileMagicNumber)?;
        let bundle_env = BundleFileEnvelope {
            version: 1,
            magic: GoatRodeoBundle::BundleFileMagicNumber,

            timestamp: millis_now(),
            info: HashMap::new(),
            the_type: "Goat Rodeo Bundle".into(),
            data_files: written.iter().map(|v| v.0).collect(),
            index_files: written.iter().map(|v| v.1).collect(),
        };
        write_envelope(bundle_writer, &bundle_env)?;
    }

    // compute sha256 of index
    let bundle_reader: &[u8] = &bundle_file;
    let grb_sha = byte_slice_to_u63(&sha256_for_slice(bundle_reader))?;

    // write the .grb file
    {
        use chrono::prelude::*;

        let now: DateTime<Utc> = Utc::now();

        let grb_file_path = dest.join(format!(
            "{:04}_{:02}_{:02}_{:02}_{:02}_{:02}_{:016x}.grb",
            now.year(),
            now.month(),
            now.day(),
            now.hour(),
            now.minute(),
            now.second(),
            grb_sha
        ));
        let mut grb_file = File::create(grb_file_path)?;
        grb_file.write_all(&bundle_file)?;
        grb_file.flush()?;
    }
    println!(
        "Finished {} loops at {:?}",
        loop_cnt,
        Instant::now().duration_since(start)
    );
    Ok(())
}

#[cfg(feature = "longtest")]
#[test]
fn test_merge() {
    let target_path = "/tmp/bt_merge_test";
    // delete the dest dir
    let _ = fs::remove_dir_all(target_path);

    println!("Removed directory");

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
                    loop_cnt,
                    Instant::now().duration_since(start),
                    test_bundle.envelope_path,
                );
            }
        }
    }
}
