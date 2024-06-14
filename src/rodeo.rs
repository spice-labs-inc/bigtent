use anyhow::{anyhow, bail, Result};
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::BufReader;
use std::ops::Deref;
use std::path::Path;
use std::{
  collections::HashMap,
  fs::{read_dir, File},
  path::PathBuf,
  sync::{Arc, Mutex},
};
use thousands::Separable;
use toml::Table; // Use log crate when building application

use crate::data_file::DataFile;
use crate::envelopes::ItemEnvelope;
use crate::index_file::{IndexFile, IndexLoc, ItemOffset};
use crate::live_merge::perform_merge;
use crate::rodeo_server::MD5Hash;
use crate::structs::{EdgeType, Item};
use crate::util::{
  byte_slice_to_u63, find_common_root_dir, find_item, hex_to_u64, is_child_dir, md5hash_str,
  read_len_and_cbor, read_u32, sha256_for_reader, sha256_for_slice,
};
#[cfg(not(test))]
use log::info;

#[cfg(test)]
use std::println as info;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BundleFileEnvelope {
  pub version: u32,
  pub magic: u32,
  pub the_type: String,
  pub data_files: Vec<u64>,
  pub index_files: Vec<u64>,
  pub timestamp: i64,
  pub info: HashMap<String, String>,
}

impl std::fmt::Display for BundleFileEnvelope {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "BundleFileEnvelope {{v: {}, data_files: {:?}, index_files: {:?}, info: {:?}}}",
      self.version,
      self
        .data_files
        .iter()
        .map(|h| format!("{:016x}", h))
        .collect::<Vec<String>>(),
      self
        .index_files
        .iter()
        .map(|h| format!("{:016x}", h))
        .collect::<Vec<String>>(),
      self.info,
    )
  }
}

#[derive(Debug, Clone)]
pub struct GoatRodeoBundle {
  pub envelope: BundleFileEnvelope,
  pub bundle_file_hash: u64,
  pub path: PathBuf,
  pub bundle_path: PathBuf,
  pub data_files: HashMap<u64, DataFile>,
  pub index_files: HashMap<u64, IndexFile>,
  pub sub_bundles: HashMap<u64, GoatRodeoBundle>,
  pub synthetic: bool,
  index: Arc<ArcSwap<Option<Arc<Vec<ItemOffset>>>>>,
  building_index: Arc<Mutex<bool>>,
}

impl GoatRodeoBundle {
  #[allow(non_upper_case_globals)]
  pub const DataFileMagicNumber: u32 = 0x00be1100; // Bell

  #[allow(non_upper_case_globals)]
  pub const IndexFileMagicNumber: u32 = 0x54154170; // Shishitō

  #[allow(non_upper_case_globals)]
  pub const BundleFileMagicNumber: u32 = 0xba4a4a; // Banana

  // Reset the index. Should be done with extreme care
  pub fn reset_index(&self) -> () {
    self.index.store(Arc::new(None));
  }

  pub fn create_synthetic_with(
    &self,
    index: Vec<ItemOffset>,
    data_files: HashMap<u64, DataFile>,
    index_files: HashMap<u64, IndexFile>,
    sub_bundles: HashMap<u64, GoatRodeoBundle>,
  ) -> Result<GoatRodeoBundle> {
    if sub_bundles.len() == 0 {
      bail!("A synthetic bundle must have at least one underlying real bundle")
    }

    let my_hash = {
      let mut keys = sub_bundles
        .keys()
        .into_iter()
        .map(|v| *v)
        .collect::<Vec<u64>>();

      keys.sort();
      let mut merkle = vec![];
      for k in keys {
        merkle.append(&mut k.to_be_bytes().into_iter().collect());
      }
      byte_slice_to_u63(&sha256_for_slice(&mut merkle)).unwrap()
    };

    Ok(GoatRodeoBundle {
      envelope: self.envelope.clone(),
      path: self.path.clone(),
      bundle_path: self.bundle_path.clone(),
      data_files: data_files,
      index_files: index_files,
      index: Arc::new(ArcSwap::new(Arc::new(Some(Arc::new(index))))),
      building_index: Arc::new(Mutex::new(false)),
      bundle_file_hash: my_hash,
      sub_bundles,
      synthetic: true,
    })
  }

  pub fn new(root_dir: &PathBuf, bundle_path: &PathBuf) -> Result<GoatRodeoBundle> {
    if !is_child_dir(root_dir, bundle_path)? {
      bail!(
        "The bundle path {:?} must be in the root dir {:?}",
        bundle_path,
        root_dir
      );
    }
    let file_path = bundle_path.clone();
    let file_name = match file_path.file_name().map(|e| e.to_str()).flatten() {
      Some(n) => n.to_string(),
      _ => bail!("Unable to get filename for {:?}", file_path),
    };

    let hash: u64 = match hex_to_u64(&file_name[(file_name.len() - 20)..(file_name.len() - 4)]) {
      Some(v) => v,
      _ => bail!("Unable to get hex value for filename {}", file_name),
    };

    // open and get info from the data file
    let mut bundle_file = File::open(file_path.clone())?;

    let sha_u64 = byte_slice_to_u63(&sha256_for_reader(&mut bundle_file)?)?;

    if sha_u64 != hash {
      bail!(
        "Bundle {:?}: expected the sha256 of '{:016x}.grb' to match the hash, but got hash {:016x}",
        file_path,
        hash,
        sha_u64
      );
    }

    let mut bundle_file = BufReader::new(File::open(file_path.clone())?);
    let dfp: &mut BufReader<File> = &mut bundle_file;
    let magic = read_u32(dfp)?;
    if magic != GoatRodeoBundle::BundleFileMagicNumber {
      bail!(
        "Unexpected magic number {:x}, expecting {:x} for data file {:?}",
        magic,
        GoatRodeoBundle::BundleFileMagicNumber,
        file_path
      );
    }

    let env: BundleFileEnvelope = read_len_and_cbor(dfp)?;

    if env.magic != GoatRodeoBundle::BundleFileMagicNumber {
      bail!("Loaded a bundle with an invalid magic number: {:?}", env);
    }

    info!("Loaded bundle {:?} {}", bundle_path.file_name(), env);

    let mut index_files = HashMap::new();
    let mut indexed_hashes: HashSet<u64> = HashSet::new();

    for index_file in &env.index_files {
      let the_file = IndexFile::new(&root_dir, *index_file)?;

      for data_hash in &the_file.envelope.data_files {
        indexed_hashes.insert(*data_hash);
      }

      info!("Loaded index {:016x}.gri", index_file);

      index_files.insert(*index_file, the_file);
    }

    let mut data_files = HashMap::new();
    for data_file in &env.data_files {
      let the_file = DataFile::new(&root_dir, *data_file)?;
      data_files.insert(*data_file, the_file);
    }

    for data_key in data_files.keys() {
      if !indexed_hashes.contains(data_key) {
        bail!(
          "Data file '{:016x}.grd' found, but it is not referenced by any index files",
          data_key
        )
      }

      indexed_hashes.remove(data_key);
    }

    if indexed_hashes.len() != 0 {
      bail!("Indexes looking for files {}, but they were not found", {
        let mut files = vec![];
        for v in indexed_hashes {
          files.push(format!("{:016x}.grd", v));
        }
        files.join(",")
      });
    }

    Ok(GoatRodeoBundle {
      envelope: env,
      path: root_dir.clone(),
      bundle_path: bundle_path.clone(),
      data_files: data_files,
      index_files: index_files,
      index: Arc::new(ArcSwap::new(Arc::new(None))),
      building_index: Arc::new(Mutex::new(false)),
      bundle_file_hash: sha_u64,
      sub_bundles: HashMap::new(),
      synthetic: false,
    })
  }

  pub fn common_parent_dir(bundles: &[GoatRodeoBundle]) -> Result<PathBuf> {
    let paths = bundles.into_iter().map(|b| b.bundle_path.clone()).collect();
    find_common_root_dir(paths)
  }

  pub fn bundle_from_config(file_name: PathBuf, conf: &Table) -> Result<GoatRodeoBundle> {
    let root_dir_key = conf.get("root_dir");
    let root_dir: PathBuf = match root_dir_key {
      Some(toml::Value::String(s)) => {
        let rd = PathBuf::from(shellexpand::tilde(s).to_string());
        if !rd.exists() || !rd.is_dir() {
          bail!("The supplied root directory {:?} is not a directory", rd);
        };

        rd
      }
      _ => bail!("Must supply a `root_dir` entry in the configuration file"),
    };

    let bundle_path_key = conf.get("bundle_path");
    let envelope_paths: Vec<PathBuf> = match bundle_path_key {
      Some(toml::Value::Array(av)) => av
        .iter()
        .flat_map(|v| match v {
          toml::Value::String(index) => Some(PathBuf::from(shellexpand::tilde(index).to_string())),
          _ => None,
        })
        .collect(),
      Some(toml::Value::String(index)) => {
        vec![PathBuf::from(shellexpand::tilde(index).to_string())]
      }
      _ => bail!(format!(
        "Could not find 'bundle_path' key in configuration file {:?}",
        file_name
      )),
    };

    let mut envelope_dirs = vec![];
    for p in envelope_paths {
      envelope_dirs.push(p.clone());
    }

    let mut bundles = vec![];

    for path in envelope_dirs {
      bundles.push(GoatRodeoBundle::new(&root_dir, &path)?);
    }

    if bundles.len() == 0 {
      // this is weird... we should have gotten at least one item
      bail!("No bundles were specified in {}", bundle_path_key.unwrap());
    } else if bundles.len() == 1 {
      return Ok(bundles.pop().unwrap()); // okay because just checked length
    } else {
      // turn the bundles into one
      perform_merge(bundles)
    }
  }

  // All the non-synthetic sub-bundles
  pub fn all_sub_bundles(&self) -> Vec<GoatRodeoBundle> {
    let mut ret = vec![];

    if !self.sub_bundles.is_empty() {
      for (_, sub) in self.sub_bundles.iter() {
        ret.append(&mut sub.all_sub_bundles());
      }
    } else {
      ret.push(self.clone());
    }

    ret
  }

  pub fn get_index(&self) -> Result<Arc<Vec<ItemOffset>>> {
    let tmp = self.index.load().clone();
    match tmp.deref() {
      Some(v) => {
        return Ok(v.clone());
      }
      None => {}
    }

    let mut my_lock = self
      .building_index
      .lock()
      .map_err(|e| anyhow!("Failed to lock {:?}", e))?;
    *my_lock = false;

    // test again after acquiring the lock
    let tmp = self.index.load().clone();
    match tmp.deref() {
      Some(v) => {
        return Ok(v.clone());
      }
      None => {}
    }

    let mut ret = vec![];

    let mut vecs = vec![];

    for index_hash in self.envelope.index_files.iter().rev() {
      let index = match self.index_files.get(index_hash) {
        Some(f) => f,
        None => bail!("Couldn't find index {:016x}.gri", index_hash),
      };
      info!("Reading index {:016x}.gri", index_hash);
      let the_index = index.read_index()?;
      if the_index.len() > 0 {
        vecs.push(the_index);
      }
    }

    info!("Read all indexes");
    vecs.sort_by(|a, b| b[0].hash.cmp(&a[0].hash));
    info!("Sorted Index of indexes");
    let index_cnt = vecs.len();
    for pos in 0..vecs.len() {
      let mut the_index = match vecs.pop() {
        Some(v) => v,
        _ => bail!("Popped and failed!"),
      };

      info!("Appending {} of {} index components", pos, index_cnt);

      if ret.len() == 0 {
        ret = the_index;
      } else if the_index.len() == 0 {

        // do nothing... nothing to sort
      } else if the_index[0].hash > ret[ret.len() - 1].hash {
        // append the_index to ret
        ret.append(&mut the_index);
      } else if the_index[the_index.len() - 1].hash < ret[0].hash {
        info!("Prepending!");
        // append ret to the_index
        the_index.append(&mut ret);
        ret = the_index;
      } else {
        info!("Intermixing!!!!");
        let rl = ret.len();
        let il = the_index.len();
        let mut dest = Vec::with_capacity(rl + il);
        let mut rp = ret.into_iter();
        let mut ip = the_index.into_iter();
        let mut nr = rp.next();
        let mut ni = ip.next();

        while nr.is_some() || ni.is_some() {
          match (&nr, &ni) {
            (None, None) => {
              break;
            }
            (None, Some(_)) => {
              dest.push(ni.unwrap()); // avoids a shared reference problem
              ni = ip.next();
            }
            (Some(_), None) => {
              dest.push(nr.unwrap()); // avoids a shared reference problem
              nr = rp.next();
            }
            (Some(v1), Some(v2)) if v1.hash < v2.hash => {
              dest.push(nr.unwrap()); // unwrap okay because tested in pattern match
              nr = rp.next();
            }
            (Some(v1), Some(v2)) if v1.hash > v2.hash => {
              dest.push(ni.unwrap()); // unwrap okay because tested in pattern match
              ni = ip.next();
            }
            (Some(_), Some(_)) => todo!("Merge two entries"),
          }
        }

        ret = dest;
      }
    }

    info!(
      "Created index with {} items",
      ret.len().separate_with_commas()
    );

    let ret_arc = Arc::new(ret);
    self.index.store(Arc::new(Some(ret_arc.clone())));
    // keep the lock alive
    *my_lock = false;

    Ok(ret_arc)
  }

  pub fn data_file(&self, hash: u64) -> Result<Arc<Mutex<BufReader<File>>>> {
    match self.data_files.get(&hash) {
      Some(df) => Ok(df.file.clone()),
      None => bail!("Data file '{:016x}.grd' not found", hash),
    }
  }

  pub fn find_file(path: &PathBuf, hash: u64, suffix: &str) -> Result<File> {
    fn find(name: &str, dir: &Path) -> Result<Option<PathBuf>> {
      for entry in dir.read_dir()?.into_iter() {
        let entry = entry?;

        if match entry.file_name().into_string() {
          Ok(s) => s,
          _ => bail!("Couldn't convert {:?} to a string", entry),
        } == name
        {
          return Ok(Some(entry.path()));
        }
        let path = entry.path();
        if path.is_dir() {
          match find(name, &path)? {
            Some(s) => return Ok(Some(s)),
            _ => {}
          }
        }
      }
      Ok(None)
    }
    let file_name = format!("{:016x}.{}", hash, suffix);
    match find(&file_name, path)? {
      Some(f) => Ok(File::open(f)?),
      _ => bail!("Couldn't find {} in {:?}", file_name, path),
    }
  }

  pub fn bundle_files_in_dir(path: PathBuf) -> Result<Vec<GoatRodeoBundle>> {
    let the_dir = read_dir(path.clone())?;
    let mut ret = vec![];
    for f in the_dir {
      let file = f?;
      let file_extn = file
        .path()
        .extension()
        .map(|e| e.to_str())
        .flatten()
        .map(|s| s.to_owned());

      if file.file_type()?.is_file() && file_extn == Some("grb".into()) {
        let walker = GoatRodeoBundle::new(&path, &file.path())?;
        ret.push(walker)
      }
    }

    Ok(ret)
  }

  pub fn find(&self, hash: MD5Hash) -> Result<Option<ItemOffset>> {
    let index = self.get_index()?;
    Ok(find_item(hash, &index))
  }

  pub fn entry_for(&self, file_hash: u64, offset: u64) -> Result<(ItemEnvelope, Item)> {
    let data_files = &self.data_files;
    let data_file = data_files.get(&file_hash);
    match data_file {
      Some(df) => {
        let (env, mut item) = df.read_envelope_and_item_at(offset)?;
        match item.reference.0 {
          0 => item.reference.0 = file_hash,
          v if v != file_hash => {
            bail!(
              "Got item {} that should have had a file_hash of {:016x}, but had {:016x}",
              item.identifier,
              file_hash,
              item.reference.0,
            )
          }
          _ => {}
        }

        if item.reference.1 != offset {
          bail!(
            "Expecting item {} to have offset {}, but reported offset {}",
            item.identifier,
            offset,
            item.reference.1
          )
        }

        Ok((env, item))
      }
      None => bail!("Couldn't find file for hash {:x}", file_hash),
    }
  }

  pub fn antialias_for(&self, data: &str) -> Result<Item> {
    let mut ret = self.data_for_key(data)?;
    while ret.is_alias() {
      match ret.connections.iter().find(|x| x.1 == EdgeType::AliasTo) {
        Some(v) => {
          ret = self.data_for_key(&v.0)?;
        }
        None => {
          bail!("Unexpected situation");
        }
      }
    }

    Ok(ret)
  }

  pub fn data_for_entry_offset(&self, index_loc: &IndexLoc) -> Result<(ItemEnvelope, Item)> {
    match index_loc {
      loc @ IndexLoc::Loc { file_hash, .. } => Ok(self.entry_for(*file_hash, loc.offset())?),
      IndexLoc::Chain(offsets) => {
        let mut ret = vec![];
        for offset in offsets {
          let some = self.data_for_entry_offset(&offset)?;
          ret.push(some);
        }
        if ret.len() == 0 {
          bail!("Got a location chain with zero entries!!");
        } else if ret.len() == 1 {
          Ok(ret.pop().unwrap()) // we know this is okay because we just tested length
        } else {
          let (env, base) = ret.pop().unwrap(); // we know this is okay because ret has more than 1 element
          let mut to_merge = vec![];
          for (_, item) in ret {
            to_merge.push(item);
          }
          let mut final_base = base.clone();

          for m2 in to_merge {
            final_base = final_base.merge(m2);
          }
          Ok((env, final_base))
        }
      }
    }
  }

  pub fn vec_for_entry_offset(&self, index_loc: &IndexLoc) -> Result<Vec<(ItemEnvelope, Item)>> {
    match index_loc {
      loc @ IndexLoc::Loc { file_hash, .. } => Ok(vec![self.entry_for(*file_hash, loc.offset())?]),
      IndexLoc::Chain(offsets) => {
        let mut ret = vec![];
        for offset in offsets {
          let mut some = self.vec_for_entry_offset(&offset)?;
          ret.append(&mut some);
        }
        Ok(ret)
      }
    }
  }

  pub fn data_for_hash(&self, hash: MD5Hash) -> Result<(ItemEnvelope, Item)> {
    let entry_offset = match self.find(hash)? {
      Some(eo) => eo,
      _ => bail!(format!("Could not find entry for hash {:x?}", hash)),
    };

    self.data_for_entry_offset(&entry_offset.loc)
  }

  pub fn data_for_key(&self, data: &str) -> Result<Item> {
    let md5_hash = md5hash_str(data);
    self.data_for_hash(md5_hash).map(|v| v.1)
  }
}

#[test]
fn test_antialias() {
  use std::{path::PathBuf, time::Instant};
  let start = Instant::now();
  let goat_rodeo_test_data: PathBuf = "../goatrodeo/res_for_big_tent/".into();
  // punt test is the files don't exist
  if !goat_rodeo_test_data.is_dir() {
    return;
  }

  let mut files =
    match GoatRodeoBundle::bundle_files_in_dir("../goatrodeo/res_for_big_tent/".into()) {
      Ok(v) => v,
      Err(e) => {
        assert!(false, "Failure to read files {:?}", e);
        return;
      }
    };
  println!("Read bundle at {:?}", start.elapsed());

  assert!(files.len() > 0, "Need a bundle");
  let bundle = files.pop().expect("Expect a bundle");
  let index = bundle.get_index().expect("To be able to get the index");

  let mut aliases = vec![];
  for v in index.iter() {
    let (_, item) = bundle.data_for_hash(v.hash).expect("Should get an item");
    if item.is_alias() {
      aliases.push(item);
    }
  }

  println!("Got aliases at {:?}", start.elapsed());

  for ai in aliases.iter().take(20) {
    println!("Antialias for {}", ai.identifier);
    let new_item = bundle
      .antialias_for(&ai.identifier)
      .expect("Expect to get the antialiased thing");
    assert!(!new_item.is_alias(), "Expecting a non-aliased thing");
    assert!(
      new_item
        .connections
        .iter()
        .filter(|x| x.0 == ai.identifier)
        .count()
        > 0,
      "Expecting a match for {}",
      ai.identifier
    );
  }

  println!("Tested aliases at {:?}", start.elapsed());
}

#[test]
fn test_generated_bundle() {
  use std::time::Instant;

  let test_paths: Vec<String> = vec![
    "../../tmp/oc_dest/result_aa".into(),
    "../../tmp/oc_dest/result_ab".into(),
    "../goatrodeo/res_for_big_tent/".into(),
  ];

  for test_path in &test_paths {
    info!("\n\n==============\n\nTesting path {}", test_path);
    let goat_rodeo_test_data: PathBuf = test_path.into();
    // punt test is the files don't exist
    if !goat_rodeo_test_data.is_dir() {
      break;
    }

    let start = Instant::now();
    let files = match GoatRodeoBundle::bundle_files_in_dir(test_path.into()) {
      Ok(v) => v,
      Err(e) => {
        assert!(false, "Failure to read files {:?}", e);
        return;
      }
    };

    info!(
      "Finding files took {:?}",
      Instant::now().duration_since(start)
    );
    assert!(files.len() > 0, "We should find some files");
    let mut pass_num = 0;
    for bundle in files {
      pass_num += 1;
      let complete_index = bundle.get_index().unwrap();

      info!(
        "Loaded index for pass {} at {:?}",
        pass_num,
        Instant::now().duration_since(start)
      );

      assert!(
        complete_index.len() > 200,
        "Expecting a large index, got {} entries",
        complete_index.len()
      );
      info!("Index size {}", complete_index.len());

      for i in complete_index.deref() {
        bundle.find(i.hash).unwrap().unwrap();
      }
    }
  }
}

#[test]
fn test_files_in_dir() {
  use std::time::{Duration, Instant};

  let goat_rodeo_test_data: PathBuf = "../goatrodeo/res_for_big_tent/".into();
  // punt test is the files don't exist
  if !goat_rodeo_test_data.is_dir() {
    return;
  }

  let files = match GoatRodeoBundle::bundle_files_in_dir("../goatrodeo/res_for_big_tent/".into()) {
    Ok(v) => v,
    Err(e) => {
      assert!(false, "Failure to read files {:?}", e);
      return;
    }
  };

  assert!(files.len() > 0, "We should find some files");
  let mut total_index_size = 0;
  for bundle in &files {
    assert!(bundle.data_files.len() > 0);
    assert!(bundle.index_files.len() > 0);

    let start = Instant::now();
    let complete_index = bundle.get_index().unwrap();

    total_index_size += complete_index.len();

    assert!(complete_index.len() > 100_000, "the index should be large");
    let time = Instant::now().duration_since(start);
    let start = Instant::now();
    bundle.get_index().unwrap(); // should be from cache
    let time2 = Instant::now().duration_since(start);
    assert!(
      time > Duration::from_millis(60),
      "Building the initial index should take more than 60ms, but took {:?}",
      time
    );
    assert!(
      time2 < Duration::from_millis(3),
      "Pulling from the cache should be less than 3 millis, but took {:?}",
      time2
    );
    info!("Initial index build {:?} and subsequent {:?}", time, time2);
  }

  assert!(
    total_index_size > 100_000,
    "must read at least 100,000 items, but only got {}",
    total_index_size
  );
}
