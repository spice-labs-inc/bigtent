use anyhow::{Context, Result, bail};
use arc_swap::ArcSwap;
use log::error;
use memmap2::Mmap;
use serde_jsonlines::json_lines;
use std::{
  cmp::Ordering,
  collections::{HashMap, HashSet},
  fs::File,
  ops::Deref,
  path::{Path, PathBuf},
  sync::Arc,
  time::Instant,
};
use thousands::Separable;
use tokio_util::either::Either;

use tokio::{
  fs::{File as TokioFile, read_dir},
  io::BufReader,
  sync::{Mutex, mpsc::Receiver},
};

use crate::{
  item::Item,
  util::{
    MD5Hash, byte_slice_to_u63, find_common_root_dir, hex_to_u64, md5hash_str, read_len_and_cbor,
    read_u32, sha256_for_reader,
  },
};
#[cfg(not(test))]
use log::info;

#[cfg(test)]
use std::println as info;

use super::{
  cluster::{ClusterFileEnvelope, ClusterFileMagicNumber},
  data::{DataFile, GOAT_RODEO_CLUSTER_FILE_SUFFIX},
  goat_trait::{GoatRodeoTrait, impl_antialias_for, impl_north_send, impl_stream_flattened_items},
  index::{GetOffset, IndexFile, ItemLoc, ItemOffset, find_item_offset},
  robo_goat::ClusterRoboMember,
};

#[derive(Debug, Clone, Copy)]
struct IndexOffset {
  pub index_file: u64,
  pub start: usize,
  pub len: usize,
}

impl IndexOffset {
  pub fn from_index_files(index_files: &Vec<Arc<IndexFile>>) -> Arc<Vec<IndexOffset>> {
    let mut ret = vec![];
    let mut total_offset = 0usize;

    let mut sorted_index_files = index_files.clone();
    sorted_index_files.sort_by(|a, b| {
      a.read_index_at_byte_offset(0)
        .expect("Should be able to load index")
        .hash
        .cmp(
          &b.read_index_at_byte_offset(0)
            .expect("Should be able to load index")
            .hash,
        )
    });
    for f in &sorted_index_files {
      ret.push(IndexOffset {
        index_file: f.hash_for_file_lookup,
        start: total_offset,
        len: f.data_len(),
      });
      total_offset += f.data_len();
    }
    Arc::new(ret)
  }

  pub fn find(offsets: &Vec<IndexOffset>, pos: usize) -> Option<IndexOffset> {
    match offsets.binary_search_by(|cmp| {
      if pos < cmp.start {
        Ordering::Greater
      } else if pos >= cmp.start + cmp.len {
        Ordering::Less
      } else {
        Ordering::Equal
      }
    }) {
      Ok(v) => Some(offsets[v]),
      _ => None,
    }
  }
}

#[derive(Debug, Clone)]
pub struct GoatRodeoCluster {
  envelope: ClusterFileEnvelope,
  cluster_file_hash: u64,
  cluster_path: PathBuf,
  data_files: HashMap<u64, Arc<DataFile>>,
  index_files: HashMap<u64, Arc<IndexFile>>,
  index_offset: Arc<Vec<IndexOffset>>,
  index: Arc<ArcSwap<Option<Arc<Vec<ItemOffset>>>>>,
  building_index: Arc<Mutex<bool>>,
  name: String,
  number_of_items: usize,
  load_index: bool,
}

impl GoatRodeoTrait for GoatRodeoCluster {
  /// create a stream for the flattened items. If any of the `gitoids` cannot
  /// be found, an `Err` is put in the stream and the flattening is completed.
  ///
  /// If `source` is true, the flattening includes "built from" and the returned
  /// items are the items that the code was built from
  async fn stream_flattened_items(
    self: Arc<GoatRodeoCluster>,
    gitoids: Vec<String>,
    source: bool,
  ) -> Result<Receiver<Either<Item, String>>> {
    impl_stream_flattened_items(self, gitoids, source).await
  }

  fn get_purl(&self) -> Result<PathBuf> {
    Ok(
      self
        .cluster_path
        .canonicalize()?
        .with_file_name("purls.txt"),
    )
  }

  /// get the number of items this cluster is managing
  fn number_of_items(&self) -> usize {
    self.number_of_items
  }

  async fn north_send(
    self: Arc<GoatRodeoCluster>,
    gitoids: Vec<String>,
    purls_only: bool,
    start: Instant,
  ) -> Result<Receiver<Either<Item, String>>> {
    impl_north_send(self, gitoids, purls_only, start).await
  }

  fn item_for_identifier(&self, data: &str) -> Option<Item> {
    let md5_hash = md5hash_str(data);
    self.item_for_hash(md5_hash)
  }

  fn item_for_hash(&self, hash: MD5Hash) -> Option<Item> {
    match self.hash_to_item_offset(hash) {
      Some(eo) => self.item_from_index_loc(&eo.loc),
      None => None,
    }
  }

  fn antialias_for(self: Arc<Self>, data: &str) -> Option<Item> {
    impl_antialias_for(self, data)
  }

  fn has_identifier(&self, identifier: &str) -> bool {
    match self.identifier_to_item_offset(identifier) {
      Some(_) => true,
      _ => false,
    }
  }

  fn is_empty(&self) -> bool {
    false
  }

  fn read_history(&self) -> Result<Vec<serde_json::Value>> {
    let history_path = self
      .cluster_path
      .canonicalize()?
      .with_file_name("history.jsonl");

    if !history_path.exists() || !history_path.is_file() {
      return Ok(vec![]);
    }

    let lines = json_lines::<serde_json::Value, _>(&history_path)?;
    let mut ret = vec![];
    for line in lines {
      ret.push(line?);
    }

    Ok(ret)
  }

  fn node_count(&self) -> u64 {
    let mut ret = 0;
    for io in self.index_offset.iter() {
      ret += io.len as u64;
    }
    ret
  }

  async fn roots(self: Arc<Self>) -> Receiver<Item> {
    let (tx, rx) = tokio::sync::mpsc::channel(256);

    let _ = tokio::spawn(async move {
      for offset in 0..self.number_of_items {
        if let Some(item_offset) = self.offset_from_pos(offset) {
          if let Some(item) = self.item_from_item_offset(&item_offset) {
            if item.is_root_item() {
              let _ = tx.send(item).await;
            }
          }
        }
      }
    });

    rx
  }
}
impl ClusterRoboMember for GoatRodeoCluster {
  /// get the name of the cluster
  fn name(&self) -> String {
    self.name.clone()
  }

  fn offset_from_pos(&self, pos: usize) -> Option<ItemOffset> {
    if self.load_index && self.index.load().is_some() {
      match &**self.index.load() {
        Some(index) => {
          if pos < index.len() {
            Some(index[pos])
          } else {
            error!(
              "for {:?} in offset_from_pos: Pos {} outside index len {}",
              self.cluster_path,
              pos,
              index.len()
            );
            return None;
          }
        }
        None => {
          return None;
        }
      }
    } else {
      let byte_pos = pos * 32;
      match IndexOffset::find(&self.index_offset, byte_pos) {
        Some(index_hash) => {
          match self.index_files.get(&index_hash.index_file) {
            Some(index) => {
              let relative_byte_pos = byte_pos - index_hash.start; // get the item
              match index.read_index_at_byte_offset(relative_byte_pos) {
                Ok(v) => return Some(v),
                Err(e) => {
                  error!("Unable to read at {} error {:?}", relative_byte_pos, e);
                  return None;
                }
              }
            }
            None => {
              error!(
                "for {:?} in offset_from_pos: Pos {} lead to index file {:x} which can't be found",
                self.cluster_path, pos, index_hash.index_file
              );
              return None;
            }
          }
        }
        None => {
          error!(
            "for {:?} in offset_from_pos 2: Pos {} outside index len {}",
            self.cluster_path, pos, self.number_of_items
          );
          return None;
        }
      }
    }
  }

  fn item_from_item_offset(&self, item_offset: &ItemOffset) -> Option<Item> {
    self.item_from_index_loc(&item_offset.loc)
  }
}
impl GoatRodeoCluster {
  /// get the cluster file hash
  pub fn get_cluster_file_hash(&self) -> u64 {
    self.cluster_file_hash
  }

  /// Get the data file mapping
  pub fn get_data_files<'a>(&'a self) -> &'a HashMap<u64, Arc<DataFile>> {
    &self.data_files
  }

  /// Get the index file mapping
  pub fn get_index_files<'a>(&'a self) -> &'a HashMap<u64, Arc<IndexFile>> {
    &self.index_files
  }

  pub async fn new(cluster_path: &PathBuf, pre_cache_index: bool) -> Result<Arc<GoatRodeoCluster>> {
    let load_index = pre_cache_index;
    let start = Instant::now();
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
    let mut cluster_file = TokioFile::open(file_path.clone())
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

    let mut cluster_file = BufReader::new(TokioFile::open(file_path.clone()).await?);
    let dfp: &mut BufReader<TokioFile> = &mut cluster_file;
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

    //let mut index_files = HashMap::new();
    let mut indexed_hashes: HashSet<u64> = HashSet::new();

    let parent = cluster_path
      .parent()
      .ok_or(anyhow::anyhow!("Couldn't get parent directory"))?
      .to_path_buf();

    let mut index_vec = vec![];
    let mut number_of_items = 0usize;
    for index_file in &env.index_files {
      let the_file = IndexFile::new(
        &parent,
        *index_file,
        false, /*TODO -- optional testing of index hash */
      )
      .await?;
      number_of_items += the_file.envelope.size as usize;
      for data_hash in &the_file.envelope.data_files {
        indexed_hashes.insert(*data_hash);
      }

      info!("Loaded index {:016x}.gri", index_file);

      index_vec.push(Arc::new(the_file));
    }

    let mut data_files = HashMap::new();
    for data_file in &env.data_files {
      let the_file = Arc::new(DataFile::new(&parent, *data_file).await?);
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

    let ret = Arc::new(GoatRodeoCluster {
      envelope: env,
      cluster_path: cluster_path.clone(),
      data_files,
      index_files: IndexFile::vec_of_index_files_to_hash_lookup(&index_vec),
      index: Arc::new(ArcSwap::new(Arc::new(None))),
      building_index: Arc::new(Mutex::new(false)),
      cluster_file_hash: sha_u64,
      number_of_items,
      name: format!(
        "{}",
        cluster_path
          .file_name()
          .and_then(|n| n.to_str())
          .unwrap_or("unknown_cluster_name")
      ),
      load_index,
      index_offset: IndexOffset::from_index_files(&index_vec),
    });

    ret.clone().kick_off_index_build_if_load_true().await;

    Ok(ret)
  }

  async fn kick_off_index_build_if_load_true(self: Arc<GoatRodeoCluster>) {
    if self.load_index {
      tokio::task::spawn(async move {
        let _ = self.get_md5_to_item_offset_index_if_load_index_true().await;
      });
    }
  }

  pub fn common_parent_dir(clusters: &[GoatRodeoCluster]) -> Result<PathBuf> {
    let paths = clusters
      .into_iter()
      .map(|b| b.cluster_path.clone())
      .collect();
    find_common_root_dir(paths)
  }

  /// Big Tent has an in-memory index of MD5 hash of identifier to the
  /// location (`ItemOffset`) of the indexed `Item`. This function
  /// returns the index. If the index has not been loaded from disk
  /// (loading in all the `.gri` files referenced in the `.grc` cluster definition)
  /// the items are loaded. The load process can take a while (3+ minutes for 2B
  /// index entries). If there are multiple threads, waiting on the load,
  /// only one will actually do the load
  async fn get_md5_to_item_offset_index_if_load_index_true(&self) -> Result<Arc<Vec<ItemOffset>>> {
    // get the index
    let tmp = self.index.load().clone();

    // if it's already built, just return it
    match tmp.deref() {
      Some(v) => {
        return Ok(v.clone());
      }
      None => {}
    }

    if !self.load_index {
      bail!("Index is not being created")
    }

    let start = Instant::now();

    let mut my_lock = self.building_index.lock().await;
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

      let the_index = index.read_index()?;
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

    let ret_arc: Arc<Vec<ItemOffset>> = Arc::new(ret.into());
    self.index.store(Arc::new(Some(ret_arc.clone())));
    // keep the lock alive
    *my_lock = false;

    Ok(ret_arc)
  }

  /// Data files (files that contain `Item`s) are named based on the 8 most significant
  /// bytes of their sha256 (with the most significant bit set to 0). This data structure
  /// opens each of the data files so that when an `Item` needs to be read, the
  /// `Mutex`ed file handle can be looked up in a hash table. This function
  /// Does the lookup
  pub fn find_data_file_from_sha256(&self, hash: u64) -> Result<Arc<Mmap>> {
    match self.data_files.get(&hash) {
      Some(df) => Ok(df.file.clone()),
      None => bail!("Data file '{:016x}.grd' not found", hash),
    }
  }

  /// Big Tent and Goat Rodeo store data and index files with names based on the
  /// sha256 of the file contents. This function, given a root_path, finds
  /// the file with the correct hash. Note that this is the most significant
  /// 8 bytes of the sha256 with the most significant bit set to zero. This
  /// is because the JVM's `long` is signed
  pub async fn find_data_or_index_file_from_sha256(
    root_path: &PathBuf,
    hash: u64,
    suffix: &str,
  ) -> Result<File> {
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
    match find(&file_name, root_path)? {
      Some(f) => Ok(File::open(f)?),
      _ => bail!("Couldn't find {} in {:?}", file_name, root_path),
    }
  }

  /// Given a path, find all the `.grc` files in the path
  pub async fn cluster_files_in_dir(
    path: PathBuf,
    pre_cache_index: bool,
  ) -> Result<Vec<Arc<GoatRodeoCluster>>> {
    let mut the_dir = read_dir(path.clone()).await?;
    let mut ret = vec![];

    while let Some(f) = the_dir.next_entry().await? {
      let file = f;
      let file_extn = file
        .path()
        .extension()
        .map(|e| e.to_str())
        .flatten()
        .map(|s| s.to_owned());

      let file_type = file.file_type().await?;

      if file_type.is_file() && file_extn == Some(GOAT_RODEO_CLUSTER_FILE_SUFFIX.into()) {
        let walker = GoatRodeoCluster::new(&file.path(), pre_cache_index).await?;
        ret.push(walker)
      } else if file_type.is_dir() {
        let mut more = Box::pin(GoatRodeoCluster::cluster_files_in_dir(
          file.path().clone(),
          pre_cache_index,
        ))
        .await?;
        ret.append(&mut more);
      }
    }

    Ok(ret)
  }

  /// given an identifier, convert it into a hash and then look up the hash in the
  /// index. This is a fast operation as it's just a binary search of the index which is
  /// in memory
  pub fn identifier_to_item_offset(&self, identifier: &str) -> Option<ItemOffset> {
    let hash = md5hash_str(identifier);
    self.hash_to_item_offset(hash)
  }

  /// from a hash, find the ItemOffset
  pub fn hash_to_item_offset(&self, hash: MD5Hash) -> Option<ItemOffset> {
    if self.load_index && self.index.load().is_some() {
      match &**self.index.load() {
        Some(index) => find_item_offset(hash, index),
        None => return None,
      }
    } else {
      if self.number_of_items() == 0 {
        return None;
      }
      let mut low = 0;
      let mut hi = self.number_of_items() - 1;

      while low <= hi {
        let mid = low + (hi - low) / 2;
        let entry = self.offset_from_pos(mid)?;
        match entry.hash.cmp(&hash) {
          Ordering::Less => low = mid + 1,
          Ordering::Equal => return Some(entry.clone()),
          Ordering::Greater if mid != 0 => hi = mid - 1,
	  Ordering::Greater /* mid == 0 */ => return None,
        }
      }

      None
    }
  }

  /// given a hash, find an item
  pub fn item_for_file_and_offset(&self, file_hash: u64, offset: usize) -> Option<Item> {
    let data_files = &self.data_files;
    let data_file = data_files.get(&file_hash);
    match data_file {
      Some(df) => {
        let item = df.read_item_at(offset)?;

        Some(item)
      }
      None => {
        panic!("Couldn't find file for hash {:x}", file_hash);
      }
    }
  }

  pub fn item_from_index_loc(&self, index_loc: &ItemLoc) -> Option<Item> {
    Some(self.item_for_file_and_offset(index_loc.get_file_hash(), index_loc.get_offset())?)
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_antialias() {
  use std::{path::PathBuf, time::Instant};
  let start = Instant::now();
  let goat_rodeo_test_data: PathBuf = "../goatrodeo/res_for_big_tent/".into();
  // punt test if the files don't exist
  if !goat_rodeo_test_data.is_dir() {
    return;
  }

  let mut files =
    match GoatRodeoCluster::cluster_files_in_dir("../goatrodeo/res_for_big_tent/".into(), true)
      .await
    {
      Ok(v) => v,
      Err(e) => {
        assert!(false, "Failure to read files {:?}", e);
        return;
      }
    };
  println!("Read cluster at {:?}", start.elapsed());

  assert!(files.len() > 0, "Need a cluster");
  let cluster = files.pop().expect("Expect a cluster");
  let index = cluster
    .get_md5_to_item_offset_index_if_load_index_true()
    .await
    .expect("To be able to get the index");

  let mut aliases = vec![];
  for v in index.iter() {
    let item = cluster
      .item_for_hash(*crate::rodeo::index::HasHash::hash(v))
      .expect("And it should be a Some");
    if item.is_alias() {
      aliases.push(item);
    }
  }

  println!("Got aliases at {:?}", start.elapsed());

  for ai in aliases.iter().take(20) {
    println!("Antialias for {}", ai.identifier);
    let new_item = cluster
      .clone()
      .antialias_for(&ai.identifier)
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
    // punt test if the files don't exist
    if !goat_rodeo_test_data.is_dir() {
      break;
    }

    let start = Instant::now();
    let files = match GoatRodeoCluster::cluster_files_in_dir(test_path.into(), true).await {
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
      let complete_index = cluster
        .get_md5_to_item_offset_index_if_load_index_true()
        .await
        .unwrap();

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
        cluster
          .hash_to_item_offset(*crate::rodeo::index::HasHash::hash(i))
          .unwrap();
      }
    }
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_files_in_dir() {
  use std::time::{Duration, Instant};

  let goat_rodeo_test_data: PathBuf = "../goatrodeo/res_for_big_tent/".into();
  // punt test if the files don't exist
  if !goat_rodeo_test_data.is_dir() {
    return;
  }

  let files =
    match GoatRodeoCluster::cluster_files_in_dir("../goatrodeo/res_for_big_tent/".into(), true)
      .await
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
    let complete_index = cluster
      .get_md5_to_item_offset_index_if_load_index_true()
      .await
      .unwrap();

    total_index_size += complete_index.len();

    assert!(
      complete_index.len() > 7_000,
      "the index should be large, but has {} items",
      complete_index.len()
    );
    let time = Instant::now().duration_since(start);
    let start = Instant::now();
    cluster
      .get_md5_to_item_offset_index_if_load_index_true()
      .await
      .unwrap(); // should be from cache
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

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_roots() {
  let to_test = vec![
    (
      "test_data/cluster_a/2025_04_19_17_10_26_012a73d9c40dc9c0.grc",
      1,
    ),
    (
      "test_data/cluster_b/2025_04_19_17_10_40_09ebe9a7137ee100.grc",
      1,
    ),
    (
      "test_data/cluster_c/2025_07_24_14_43_36_68a489f4fd40c5e2.grc",
      1,
    ),
    (
      "test_data/cluster_d/2025_07_24_14_44_14_2b39577cd0a58701.grc",
      5,
    ),
  ];
  for (file, cnt) in to_test {
    let path = PathBuf::from(file);
    println!("Getting cluster {}", file);
    let cluster = GoatRodeoCluster::new(&path, false)
      .await
      .expect("Should get cluster");
    println!("Goat cluster {}", file);
    let mut rx = cluster.roots().await;
    let mut info = vec![];
    while let Some(item) = rx.recv().await {
      info.push(item);
    }
    println!("Pushed {} items for {}", info.len(), file);

    assert!(
      info.len() == cnt,
      "for {} expected {}, actual len {} and filenames {:?}",
      file,
      cnt,
      info.len(),
      info
    );
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_generated_cluster_no_index() {
  use std::time::Instant;

  let test_paths: Vec<String> = vec![
    "../../tmp/oc_dest/result_aa".into(),
    "../../tmp/oc_dest/result_ab".into(),
    "../goatrodeo/res_for_big_tent/".into(),
  ];

  for test_path in &test_paths {
    info!("\n\n==============\n\nTesting path {}", test_path);
    let goat_rodeo_test_data: PathBuf = test_path.into();
    // punt test if the files don't exist
    if !goat_rodeo_test_data.is_dir() {
      break;
    }

    let start = Instant::now();
    let files = match GoatRodeoCluster::cluster_files_in_dir(test_path.into(), false).await {
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

      info!(
        "Loaded index for pass {} at {:?}",
        pass_num,
        Instant::now().duration_since(start)
      );

      assert!(
        cluster.number_of_items() > 200,
        "Expecting a large index, got {} entries",
        cluster.number_of_items()
      );
      info!("Index size {}", cluster.number_of_items());

      for i in 0..cluster.number_of_items() {
        let offset = cluster
          .offset_from_pos(i)
          .expect("Should be able to get the ItemOffset");
        cluster
          .hash_to_item_offset(*crate::rodeo::index::HasHash::hash(&offset))
          .unwrap();
      }
    }
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_files_in_dir_no_index_load() {
  use std::time::Instant;

  let goat_rodeo_test_data: PathBuf = "../goatrodeo/res_for_big_tent/".into();
  // punt test if the files don't exist
  if !goat_rodeo_test_data.is_dir() {
    return;
  }

  let files =
    match GoatRodeoCluster::cluster_files_in_dir("../goatrodeo/res_for_big_tent/".into(), false)
      .await
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
    let complete_index_len = cluster.number_of_items();
    total_index_size += complete_index_len;

    assert!(
      complete_index_len > 7_000,
      "the index should be large, but has {} items",
      complete_index_len
    );
    let time = Instant::now().duration_since(start);

    info!("Initial index build {:?} ", time);
  }

  assert!(
    total_index_size > 7_000,
    "must read at least 7,000 items, but only got {}",
    total_index_size
  );
}
