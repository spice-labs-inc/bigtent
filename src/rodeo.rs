use anyhow::{ bail, Context, Result};
use arc_swap::ArcSwap;
use scopeguard::defer;
use serde::{Deserialize, Serialize};
use std::{
  collections::{BTreeMap, HashMap, HashSet},
  ops::Deref,
  path::{Path, PathBuf},
  sync::{atomic::AtomicU32, Arc},
  time::Instant,
};
use thousands::Separable;
use tokio::{fs::{read_dir, File}, io::BufReader, sync::{mpsc::Sender, Mutex}};
use toml::Table; // Use log crate when building application

use crate::{
  data_file::DataFile,
  index_file::{IndexFile, IndexLoc, ItemOffset},
  live_merge::perform_merge,
  rodeo_server::{MD5Hash, RodeoServer},
  structs::{EdgeType, Item},
  util::{
    byte_slice_to_u63, find_common_root_dir, find_item, hex_to_u64, is_child_dir, md5hash_str,
    read_len_and_cbor, read_u32, sha256_for_reader, sha256_for_slice,
  },
};
#[cfg(not(test))]
use log::info;

#[cfg(test)]
use std::println as info;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterFileEnvelope {
  pub version: u32,
  pub magic: u32,
  pub data_files: Vec<u64>,
  pub index_files: Vec<u64>,
  pub info: BTreeMap<String, String>,
}

impl ClusterFileEnvelope {
  pub fn validate(&self) -> Result<()> {
    if self.magic != ClusterFileMagicNumber {
      bail!("Loaded a cluster with an invalid magic number: {:?}", self);
    }

    if self.version != 2 {
      bail!(
        "Loaded a Cluster with version {} but this code only supports version 2 Clusters",
        self.version
      );
    }

    Ok(())
  }
}

impl std::fmt::Display for ClusterFileEnvelope {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "ClusterFileEnvelope {{v: {}, data_files: {:?}, index_files: {:?}, info: {:?}}}",
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
pub struct GoatRodeoCluster {
  pub envelope: ClusterFileEnvelope,
  pub cluster_file_hash: u64,
  pub path: PathBuf,
  pub cluster_path: PathBuf,
  pub data_files: HashMap<u64, Arc<DataFile<File>>>,
  pub index_files: HashMap<u64, IndexFile>,
  pub sub_clusters: HashMap<u64, GoatRodeoCluster>,
  pub synthetic: bool,
  index: Arc<ArcSwap<Option<Arc<Vec<ItemOffset>>>>>,
  building_index: Arc<Mutex<bool>>,
}

#[allow(non_upper_case_globals)]
pub const DataFileMagicNumber: u32 = 0x00be1100; // Bell

#[allow(non_upper_case_globals)]
pub const IndexFileMagicNumber: u32 = 0x54154170; // Shishitō

#[allow(non_upper_case_globals)]
pub const ClusterFileMagicNumber: u32 = 0xba4a4a; // Banana

impl GoatRodeoCluster {
  // Reset the index. Should be done with extreme care
  pub fn reset_index(&self) -> () {
    self.index.store(Arc::new(None));
  }

  pub fn create_synthetic_with(
    &self,
    index: Vec<ItemOffset>,
    data_files: HashMap<u64, Arc<DataFile<File>>>,
    index_files: HashMap<u64, IndexFile>,
    sub_clusters: HashMap<u64, GoatRodeoCluster>,
  ) -> Result<GoatRodeoCluster> {
    if sub_clusters.len() == 0 {
      bail!("A synthetic cluster must have at least one underlying real cluster")
    }

    let my_hash = {
      let mut keys = sub_clusters
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

    Ok(GoatRodeoCluster {
      envelope: self.envelope.clone(),
      path: self.path.clone(),
      cluster_path: self.cluster_path.clone(),
      data_files: data_files,
      index_files: index_files,
      index: Arc::new(ArcSwap::new(Arc::new(Some(Arc::new(index))))),
      building_index: Arc::new(Mutex::new(false)),
      cluster_file_hash: my_hash,
      sub_clusters,
      synthetic: true,
    })
  }

  /// Create a Goat Rodeo cluster with no contents
  pub fn blank(root_dir: &PathBuf) -> GoatRodeoCluster {
    GoatRodeoCluster {
      envelope: ClusterFileEnvelope {
        version: 1,
        magic: 1,
        data_files: vec![],
        index_files: vec![],
        info: BTreeMap::new(),
      },
      path: root_dir.clone(),
      cluster_path: root_dir.clone(),
      data_files: HashMap::new(),
      index_files: HashMap::new(),
      index: Arc::new(ArcSwap::new(Arc::new(None))),
      building_index: Arc::new(Mutex::new(false)),
      cluster_file_hash: 0,
      sub_clusters: HashMap::new(),
      synthetic: false,
    }
  }

  pub async fn new(root_dir: &PathBuf, cluster_path: &PathBuf) -> Result<GoatRodeoCluster> {
    let start = Instant::now();
    if !is_child_dir(root_dir, cluster_path)? {
      bail!(
        "The cluster path {:?} must be in the root dir {:?}",
        cluster_path,
        root_dir
      );
    }
    let file_path = cluster_path.clone();
    let file_name = match file_path.file_name().map(|e| e.to_str()).flatten() {
      Some(n) => n.to_string(),
      _ => bail!("Unable to get filename for {:?}", file_path),
    };

    let hash: u64 = match hex_to_u64(&file_name[(file_name.len() - 20)..(file_name.len() - 4)]) {
      Some(v) => v,
      _ => bail!("Unable to get hex value for filename {}", file_name),
    };

    // open and get info from the data file
    let mut cluster_file = File::open(file_path.clone())
      .await
      .with_context(|| format!("opening cluster {:?}", file_path))?;

    let sha_u64 = byte_slice_to_u63(&sha256_for_reader(&mut cluster_file).await?)?;

    if sha_u64 != hash {
      bail!(
        "Cluster {:?}: expected the sha256 of '{:016x}.grc' to match the hash, but got hash \
         {:016x}",
        file_path,
        hash,
        sha_u64
      );
    }

    let mut cluster_file = BufReader::new(File::open(file_path.clone()).await?);
    let dfp: &mut BufReader<File> = &mut cluster_file;
    let magic = read_u32(dfp).await?;
    if magic != ClusterFileMagicNumber {
      bail!(
        "Unexpected magic number {:x}, expecting {:x} for data file {:?}",
        magic,
        ClusterFileMagicNumber,
        file_path
      );
    }

    let env: ClusterFileEnvelope = read_len_and_cbor(dfp).await?;

    env.validate()?;

    info!(
      "Loaded cluster {:?} {} at {:?}",
      cluster_path,
      env,
      start.elapsed()
    );

    let mut index_files = HashMap::new();
    let mut indexed_hashes: HashSet<u64> = HashSet::new();

    for index_file in &env.index_files {
      let the_file = IndexFile::new(
        &root_dir,
        *index_file,
        false, /*TODO -- optional testing of index hash */
      ).await?;

      for data_hash in &the_file.envelope.data_files {
        indexed_hashes.insert(*data_hash);
      }

      info!("Loaded index {:016x}.gri", index_file);

      index_files.insert(*index_file, the_file);
    }

    let mut data_files = HashMap::new();
    for data_file in &env.data_files {
      let the_file = Arc::new(DataFile::new(&root_dir, *data_file).await?);
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

    info!("New cluster load time {:?}", start.elapsed());

    Ok(GoatRodeoCluster {
      envelope: env,
      path: root_dir.clone(),
      cluster_path: cluster_path.clone(),
      data_files: data_files,
      index_files: index_files,
      index: Arc::new(ArcSwap::new(Arc::new(None))),
      building_index: Arc::new(Mutex::new(false)),
      cluster_file_hash: sha_u64,
      sub_clusters: HashMap::new(),
      synthetic: false,
    })
  }

  pub fn common_parent_dir(clusters: &[GoatRodeoCluster]) -> Result<PathBuf> {
    let paths = clusters
      .into_iter()
      .map(|b| b.cluster_path.clone())
      .collect();
    find_common_root_dir(paths)
  }

  pub async fn cluster_from_config(file_name: PathBuf, conf: &Table) -> Result<GoatRodeoCluster> {
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

    let cluster_path_key = conf.get("cluster_path");
    let envelope_paths: Vec<PathBuf> = match cluster_path_key {
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
        "Could not find 'cluster_path' key in configuration file {:?}",
        file_name
      )),
    };

    let mut envelope_dirs = vec![];
    for p in envelope_paths {
      envelope_dirs.push(p.clone());
    }

    let mut clusters = vec![];

    for path in envelope_dirs {
      clusters.push(GoatRodeoCluster::new(&root_dir, &path).await?);
    }

    if clusters.len() == 0 {
      // this is weird... we should have gotten at least one item
      bail!(
        "No clusters were specified in {}",
        cluster_path_key.unwrap()
      );
    } else if clusters.len() == 1 {
      return Ok(clusters.pop().unwrap()); // okay because just checked length
    } else {
      // turn the clusters into one
      perform_merge(clusters).await
    }
  }

  // All the non-synthetic sub-clusters
  pub async fn all_sub_clusters(&self) -> Vec<GoatRodeoCluster> {
    let mut ret = vec![];

    if !self.sub_clusters.is_empty() {
      for (_, sub) in self.sub_clusters.iter() {
        let mut to_append = Box::pin(sub.all_sub_clusters()).await;
        ret.append(&mut to_append);
      }
    } else {
      ret.push(self.clone());
    }

    ret
  }

  pub async fn get_index(&self) -> Result<Arc<Vec<ItemOffset>>> {
    let tmp = self.index.load().clone();
    match tmp.deref() {
      Some(v) => {
        return Ok(v.clone());
      }
      None => {}
    }

    let start = Instant::now();

    let mut my_lock = self
      .building_index
      .lock().await;
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
      info!(
        "Reading index {:016x}.gri at {:?}",
        index_hash,
        start.elapsed()
      );
      let the_index = index.read_index().await?;
      if the_index.len() > 0 {
        vecs.push(the_index);
      }
    }

    info!("Read all indexes at {:?}", start.elapsed());
    vecs.sort_by(|a, b| b[0].hash.cmp(&a[0].hash));
    info!("Sorted Index of indexes at {:?}", start.elapsed());
    let index_cnt = vecs.len();
    for pos in 0..vecs.len() {
      let mut the_index = match vecs.pop() {
        Some(v) => v,
        _ => bail!("Popped and failed!"),
      };

      info!(
        "Appending {} of {} index components at {:?}",
        pos + 1,
        index_cnt,
        start.elapsed()
      );

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
      "Created index with {} items at {:?}",
      ret.len().separate_with_commas(),
      start.elapsed()
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

  pub async fn find_file(path: &PathBuf, hash: u64, suffix: &str) -> Result<File> {
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
      Some(f) => Ok(File::open(f).await?),
      _ => bail!("Couldn't find {} in {:?}", file_name, path),
    }
  }

  pub async fn cluster_files_in_dir(path: PathBuf) -> Result<Vec<GoatRodeoCluster>> {
    let mut the_dir = read_dir(path.clone()).await?;
    let mut ret = vec![];

    while let Some(f) = the_dir.next_entry().await? {
    //for f in the_dir.next_entry().await {
      let file = f;
      let file_extn = file
        .path()
        .extension()
        .map(|e| e.to_str())
        .flatten()
        .map(|s| s.to_owned());

      if file.file_type().await?.is_file() && file_extn == Some("grc".into()) {
        let walker = GoatRodeoCluster::new(&path, &file.path()).await?;
        ret.push(walker)
      }
    }

    Ok(ret)
  }

  pub async fn find(&self, hash: MD5Hash) -> Result<Option<ItemOffset>> {
    let index = self.get_index().await?;
    Ok(find_item(hash, &index))
  }

  pub async fn entry_for(&self, file_hash: u64, offset: u64) -> Result<Item> {
    let data_files = &self.data_files;
    let data_file = data_files.get(&file_hash);
    match data_file {
      Some(df) => {
        let mut item = df.read_item_at(offset).await?;
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

        Ok(item)
      }
      None => bail!("Couldn't find file for hash {:x}", file_hash),
    }
  }

pub async fn north_send(
    &self,
    gitoids: Vec<String>,
    purls_only: bool,
    tx: Sender<serde_json::Value>,
    start: Instant,
  ) -> Result<()> {
    let mut found = HashSet::new();
    let mut to_find = HashSet::new();
    let mut found_purls = HashSet::<String>::new();
    to_find.extend(gitoids);

    let cnt = AtomicU32::new(0);

    defer! {
      info!("North: Sent {} items in {:?}", cnt.load(std::sync::atomic::Ordering::Relaxed).separate_with_commas(),
      start.elapsed());
    }

    fn less(a: &HashSet<String>, b: &HashSet<String>) -> HashSet<String> {
      let mut ret = a.clone();
      for i in b.iter() {
        ret.remove(i);
      }
      ret
    }

    loop {
      let to_search = less(&to_find, &found);

      if to_search.len() == 0 {
        break;
      }
      for this_oid in to_search {
        found.insert(this_oid.clone());
        match self.data_for_key(&this_oid).await {
          Ok(item) => {
            let and_then = RodeoServer::contained_by(&item);
            if purls_only {
              for purl in item.find_purls() {
                if !found_purls.contains(&purl) {
                  tx.send(purl.clone().into()).await?;
                  
                  found_purls.insert(purl);
                }
              }
            } else {
              tx.send(item.into()).await?;
            }
            
            to_find = to_find.union(&and_then).map(|s| s.clone()).collect();
          }
          _ => {}
        }
        cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
      }
    }
    Ok(())
  }

  pub async fn antialias_for(&self, data: &str) -> Result<Item> {
    let mut ret = self.data_for_key(data).await?;
    while ret.is_alias() {
      match ret.connections.iter().find(|x| x.0.is_alias_to()) {
        Some(v) => {
          ret = self.data_for_key(&v.1).await?;
        }
        None => {
          bail!("Unexpected situation");
        }
      }
    }

    Ok(ret)
  }

  pub async fn data_for_entry_offset(&self, index_loc: &IndexLoc) -> Result<Item> {
    match index_loc {
      loc @ IndexLoc::Loc { file_hash, .. } => Ok(self.entry_for(*file_hash, loc.offset()).await?),
      IndexLoc::Chain(offsets) => {
        let mut ret: Vec<Item> = vec![];
        for offset in offsets {
          let some = Box::pin(self.data_for_entry_offset(&offset)).await?;
          ret.push(some);
        }
        if ret.len() == 0 {
          bail!("Got a location chain with zero entries!!");
        } else if ret.len() == 1 {
          Ok(ret.pop().unwrap()) // we know this is okay because we just tested length
        } else {
          let base = ret.pop().unwrap(); // we know this is okay because ret has more than 1 element
          let mut to_merge = vec![];
          for item in ret {
            to_merge.push(item);
          }
          let mut final_base = base.clone();

          for m2 in to_merge {
            final_base = final_base.merge(m2);
          }
          Ok(final_base)
        }
      }
    }
  }

  pub async fn vec_for_entry_offset(&self, index_loc: &IndexLoc) -> Result<Vec<Item>> {
    match index_loc {
      loc @ IndexLoc::Loc { file_hash, .. } => Ok(vec![self.entry_for(*file_hash, loc.offset()).await?]),
      IndexLoc::Chain(offsets) => {
        let mut ret = vec![];
        for offset in offsets {
         let mut some = Box::pin(self.vec_for_entry_offset(&offset)).await?;
         ret.append(&mut some);
        }
        Ok(ret)
      }
    }
  }

  pub async fn data_for_hash(&self, hash: MD5Hash) -> Result<Item> {
    let entry_offset = match self.find(hash).await? {
      Some(eo) => eo,
      _ => bail!(format!("Could not find entry for hash {:x?}", hash)),
    };

    self.data_for_entry_offset(&entry_offset.loc).await
  }

  pub  async fn data_for_key(&self, data: &str) -> Result<Item> {
    let md5_hash = md5hash_str(data);
    self.data_for_hash(md5_hash).await
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_antialias() {
  use std::{path::PathBuf, time::Instant};
  let start = Instant::now();
  let goat_rodeo_test_data: PathBuf = "../goatrodeo/res_for_big_tent/".into();
  // punt test is the files don't exist
  if !goat_rodeo_test_data.is_dir() {
    return;
  }

  let mut files =
    match GoatRodeoCluster::cluster_files_in_dir("../goatrodeo/res_for_big_tent/".into()).await {
      Ok(v) => v,
      Err(e) => {
        assert!(false, "Failure to read files {:?}", e);
        return;
      }
    };
  println!("Read cluster at {:?}", start.elapsed());

  assert!(files.len() > 0, "Need a cluster");
  let cluster = files.pop().expect("Expect a cluster");
  let index = cluster.get_index().await.expect("To be able to get the index");

  let mut aliases = vec![];
  for v in index.iter() {
    let item = cluster.data_for_hash(v.hash).await.expect("Should get an item");
    if item.is_alias() {
      aliases.push(item);
    }
  }

  println!("Got aliases at {:?}", start.elapsed());

  for ai in aliases.iter().take(20) {
    println!("Antialias for {}", ai.identifier);
    let new_item = cluster
      .antialias_for(&ai.identifier).await
      .expect("Expect to get the antialiased thing");
    assert!(!new_item.is_alias(), "Expecting a non-aliased thing");
    assert!(
      new_item
        .connections
        .iter()
        .filter(|x| x.1 == ai.identifier)
        .count()
        > 0,
      "Expecting a match for {}",
      ai.identifier
    );
  }

  println!("Tested aliases at {:?}", start.elapsed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_generated_cluster() {
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
    let files = match GoatRodeoCluster::cluster_files_in_dir(test_path.into()).await {
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
    for cluster in files {
      pass_num += 1;
      let complete_index = cluster.get_index().await.unwrap();

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
        cluster.find(i.hash).await.unwrap().unwrap();
      }
    }
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_files_in_dir() {
  use std::time::{Duration, Instant};

  let goat_rodeo_test_data: PathBuf = "../goatrodeo/res_for_big_tent/".into();
  // punt test is the files don't exist
  if !goat_rodeo_test_data.is_dir() {
    return;
  }

  let files = match GoatRodeoCluster::cluster_files_in_dir("../goatrodeo/res_for_big_tent/".into()).await
  {
    Ok(v) => v,
    Err(e) => {
      assert!(false, "Failure to read files {:?}", e);
      return;
    }
  };

  assert!(files.len() > 0, "We should find some files");
  let mut total_index_size = 0;
  for cluster in &files {
    assert!(cluster.data_files.len() > 0);
    assert!(cluster.index_files.len() > 0);

    let start = Instant::now();
    let complete_index = cluster.get_index().await.unwrap();

    total_index_size += complete_index.len();

    assert!(
      complete_index.len() > 7_000,
      "the index should be large, but has {} items",
      complete_index.len()
    );
    let time = Instant::now().duration_since(start);
    let start = Instant::now();
    cluster.get_index().await.unwrap(); // should be from cache
    let time2 = Instant::now().duration_since(start);

    assert!(
      time2 < Duration::from_millis(3),
      "Pulling from the cache should be less than 3 millis, but took {:?}",
      time2
    );
    info!("Initial index build {:?} and subsequent {:?}", time, time2);
  }

  assert!(
    total_index_size > 7_000,
    "must read at least 7,000 items, but only got {}",
    total_index_size
  );
}
