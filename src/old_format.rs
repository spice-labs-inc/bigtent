#![allow(dead_code)]

#[cfg(feature = "oldstuff")]
use crate::{rodeo_server::MD5Hash, util::md5hash_str};
#[cfg(feature = "oldstuff")]
use anyhow::Result;
#[cfg(feature = "oldstuff")]
use std::{
  fs::File,
  io::{BufRead, BufReader},
};

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
