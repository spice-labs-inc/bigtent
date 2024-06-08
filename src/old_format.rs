#![allow(dead_code)]

#[cfg(feature = "oldstuff")]
use anyhow::Result;
#[cfg(feature = "oldstuff")]
use std::{
    fs::File,
    io::{BufRead, BufReader},
};
#[cfg(feature = "oldstuff")]
use crate::{rodeo_server::MD5Hash, util::md5hash_str};

#[cfg(feature = "oldstuff")]
fn read_old_file(path: &str) -> Result<Vec<(MD5Hash, String, String)>> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut ret = vec![];
    loop {
        let mut line = String::new();
        let len = reader.read_line(&mut line)?;
        if len == 0 {
            break;
        }

        match (line.find(","), line.find("||,||")) {
            (Some(key), Some(offset)) => {
                let json = line[offset + 5..].to_string();
                let pk = line[key + 1..offset].to_string();
                let hash = md5hash_str(&pk);
                let hash_bytes = hash;
                ret.push((hash_bytes, pk, json));
            }
            _ => {}
        }
    }
    Ok(ret)
}

#[cfg(feature = "oldstuff")]
#[test]
fn test_old_vs_new() {
    use crate::{rodeo_server::MD5Hash, rodeo::GoatRodeoBundle, util::find_item};
    use serde_json::Value;
    use std::time::Instant;

    if false {
        let start = Instant::now();
        let dir_path = "/home/dpp/tmp/result_aa/".into();
        let env_path =
            "/home/dpp/tmp/result_aa/2024_05_31_17_17_35_7cc0059908ab1ed6.grb".into();
        println!("About to load bundle");
        let grb = GoatRodeoBundle::new(&dir_path, &env_path).unwrap();
        println!(
            "Loaded bundle at {:?}",
            Instant::now().duration_since(start)
        );
        let mut good = true;

        let new_index = grb.get_index().unwrap();
        println!(
            "Got new index at {:?}",
            Instant::now().duration_since(start)
        );
        let index = read_old_file("/data/dpp_stuff/to_analyze/done/aa.txt".into()).unwrap();
        println!(
            "Got old index at {:?}",
            Instant::now().duration_since(start)
        );

        let mut cnt = 0;
        let mut fail_cnt = 0;
        for e in &index {
            let hash: MD5Hash = e.0;
            match find_item(hash, &new_index) {
                Some(_) => {
                    // ignore... found
                }
                None => {
                    //let line = index.line_for_hash(e.hash).unwrap();
                    let info: Value = serde_json::from_str(&e.2).unwrap();
                    let id = info
                        .as_object()
                        .unwrap()
                        .get("identifier")
                        .unwrap()
                        .as_str()
                        .unwrap();
                    let filename = info
                        .as_object()
                        .unwrap()
                        .get("metadata")
                        .unwrap()
                        .as_object()
                        .unwrap()
                        .get("filename")
                        .unwrap()
                        .as_array()
                        .unwrap()[0]
                        .as_str()
                        .unwrap();
                    let contained_by = info
                        .as_object()
                        .unwrap()
                        .get("containedBy")
                        .unwrap()
                        .as_array()
                        .unwrap()
                        .into_iter()
                        .map(|v| v.as_str().unwrap().to_string());

                    println!(
                        "failed for {} with id {} contained by {:?}",
                        filename, id, contained_by
                    );
                    for s in contained_by {
                        let cont_hash = md5hash_str(&s).to_be_bytes();
                        match find_item(cont_hash, &new_index) {
                            Some(_) => {}
                            None => {
                                let containing: Vec<String> = index
                                    .iter()
                                    .filter(|v| v.1 == s)
                                    .map(|v| v.2.clone())
                                    .collect();
                                println!("Failed for {} and {}. filename {} and couldn't find containing hash {}... contained by {:?}", e.1, cnt, filename, s, containing);
                            }
                        }
                    }

                    fail_cnt += 1;
                    good = false;
                }
            }
            cnt += 1;
            if cnt % 100_000 == 0 {
                println!("Loop {} failed {}", cnt, fail_cnt);
            }
        }
        assert!(good, "Failed!");
    }
}
