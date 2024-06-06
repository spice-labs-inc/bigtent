use anyhow::{anyhow, bail, Result};
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::ops::Deref;
use std::{
    collections::HashMap,
    fs::{read_dir, File},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::envelopes::ItemEnvelope;
use crate::index::{IndexLoc, ItemOffset, MD5Hash};
use crate::structs::Item;
use crate::util::{
    byte_slice_to_u63, find_item, hex_to_u64, md5hash_str, millis_now, read_cbor, read_len_and_cbor, read_u16, read_u32, sha256_for_reader, sha256_for_slice
};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct IndexEnvelope {
    pub version: u32,
    pub magic: u32,
    pub the_type: String,
    pub size: u32,
    pub data_files: Vec<u64>,
    pub encoding: String,
    pub timestamp: i64,
    pub info: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct IndexFile {
    pub envelope: IndexEnvelope,
    pub file: Arc<Mutex<BufReader<File>>>,
    pub data_offset: u64,
}

impl IndexFile {
    pub fn new(dir: &PathBuf, hash: u64) -> Result<IndexFile> {
        // ensure we close `file` after computing the hash
        {
            let mut file = GoatRodeoBundle::find_file(&dir, hash, "gri")?;

            let tested_hash = byte_slice_to_u63(&sha256_for_reader(&mut file)?)?;
            if tested_hash != hash {
                bail!(
                    "Index file for {:016x} does not match {:016x}",
                    hash,
                    tested_hash
                );
            }
        }

        let mut file = GoatRodeoBundle::find_file(&dir, hash, "gri")?;

        let ifp = &mut file;
        let magic = read_u32(ifp)?;
        if magic != GoatRodeoBundle::IndexFileMagicNumber {
            bail!(
                "Unexpected magic number {:x}, expecting {:x} for data file {:016x}.gri",
                magic,
                GoatRodeoBundle::IndexFileMagicNumber,
                hash
            );
        }

        let idx_env: IndexEnvelope = read_len_and_cbor(ifp)?;

        let idx_pos: u64 = file.seek(SeekFrom::Current(0))?;

        Ok(IndexFile {
            envelope: idx_env,
            file: Arc::new(Mutex::new(BufReader::new(file))),
            data_offset: idx_pos,
        })
    }

    pub fn read_index(&self) -> Result<Vec<ItemOffset>> {
        let mut ret = Vec::with_capacity(self.envelope.size as usize);
        let mut last = [0u8; 16];
        // let mut not_sorted = false;

        let mut my_file = self
            .file
            .lock()
            .map_err(|e| anyhow!("Failed to lock {:?}", e))?;
        let fp: &mut BufReader<File> = &mut my_file;
        fp.seek(SeekFrom::Start(self.data_offset))?;
        let mut buf = vec![];
        fp.read_to_end(&mut buf)?;

        let mut info: &[u8] = &buf;
        for _ in 0..self.envelope.size {
            let eo = ItemOffset::read(&mut info)?;
            if eo.hash < last {
                // not_sorted = true;
                bail!("Not sorted!!! last {:?} eo.hash {:?}", last, eo.hash);
            }
            last = eo.hash;
            ret.push(eo);
        }

        // if not_sorted {
        //     ret.sort_by(|a, b| a.hash.cmp(&b.hash))
        // }

        Ok(ret)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct DataFileEnvelope {
    pub version: u32,
    pub magic: u32,
    pub the_type: String,
    pub previous: u64,
    pub depends_on: Vec<u64>,
    pub timestamp: i64,
    pub built_from_merge: bool,
    pub info: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct DataFile {
    pub envelope: DataFileEnvelope,
    pub file: Arc<Mutex<BufReader<File>>>,
    pub data_offset: u64,
}

impl DataFile {
    pub fn new(dir: &PathBuf, hash: u64) -> Result<DataFile> {
        let mut data_file = GoatRodeoBundle::find_file(dir, hash, "grd")?;
        let dfp: &mut File = &mut data_file;
        let magic = read_u32(dfp)?;
        if magic != GoatRodeoBundle::DataFileMagicNumber {
            bail!(
                "Unexpected magic number {:x}, expecting {:x} for data file {:016x}.grd",
                magic,
                GoatRodeoBundle::DataFileMagicNumber,
                hash
            );
        }

        let env: DataFileEnvelope = read_len_and_cbor(dfp)?;

        let cur_pos: u64 = data_file.stream_position()?;
        // FIXME do additional validation of the envelope
        Ok(DataFile {
            envelope: env,
            file: Arc::new(Mutex::new(BufReader::with_capacity(4096, data_file))),
            data_offset: cur_pos,
        })
    }

    fn seek_to(file: &mut BufReader<File>, desired_pos: u64) -> Result<()> {
        let pos = file.stream_position()?;
        if pos == desired_pos {
            return Ok(());
        }

        let rel_seek = (desired_pos as i64) - (pos as i64);

        file.seek_relative(rel_seek)?;
        Ok(())
    }

    pub fn read_envelope_at(&self, pos: u64) -> Result<ItemEnvelope> {
        let mut my_file = self
            .file
            .lock()
            .map_err(|e| anyhow!("Failed to lock {:?}", e))?;
        DataFile::seek_to(&mut my_file, pos)?;
        let my_reader: &mut BufReader<File> = &mut my_file;
        let len = read_u16(my_reader)?;
        let _ = read_u32(my_reader)?;
        read_cbor(my_reader, len as usize)
    }

    pub fn read_envelope_and_item_at(&self, pos: u64) -> Result<(ItemEnvelope, Item)> {
        let mut my_file = self
            .file
            .lock()
            .map_err(|e| anyhow!("Failed to lock {:?}", e))?;
        DataFile::seek_to(&mut my_file, pos)?;
        let my_reader: &mut BufReader<File> = &mut my_file;
        let env_len = read_u16(my_reader)?;
        let item_len = read_u32(my_reader)?;
        let env = read_cbor(my_reader, env_len as usize)?;
        let item = read_cbor(&mut *my_file, item_len as usize)?;
        Ok((env, item))
    }
}

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

#[derive(Debug, Clone)]
pub struct GoatRodeoBundle {
    pub envelope: BundleFileEnvelope,
    pub bundle_file_hash: u64,
    pub path: PathBuf,
    pub envelope_path: PathBuf,
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
        sub_bundles: HashMap<u64, GoatRodeoBundle>
    ) -> GoatRodeoBundle {
        GoatRodeoBundle {
            envelope: self.envelope.clone(),
            path: self.path.clone(),
            envelope_path: self.envelope_path.clone(),
            data_files: data_files,
            index_files: index_files,
            index: Arc::new(ArcSwap::new(Arc::new(Some(Arc::new(index))))),
            building_index: Arc::new(Mutex::new(false)),
            bundle_file_hash: byte_slice_to_u63(&sha256_for_slice(&millis_now().to_be_bytes())).unwrap(),
            sub_bundles,
            synthetic: true,
        }
    }

    pub fn new(dir: &PathBuf, envelope_path: &PathBuf) -> Result<GoatRodeoBundle> {
        let file_path = envelope_path.clone();
        let file_name = match file_path.file_name().map(|e| e.to_str()).flatten() {
            Some(n) => n.to_string(),
            _ => bail!("Unable to get filename for {:?}", file_path),
        };

        let hash: u64 = match hex_to_u64(&file_name[(file_name.len() - 20)..(file_name.len() - 4)])
        {
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

        let mut index_files = HashMap::new();
        let mut indexed_hashes: HashSet<u64> = HashSet::new();

        for index_file in &env.index_files {
            let the_file = IndexFile::new(&dir, *index_file)?;

            for data_hash in &the_file.envelope.data_files {
                indexed_hashes.insert(*data_hash);
            }

            index_files.insert(*index_file, the_file);
        }

        let mut data_files = HashMap::new();
        for data_file in &env.data_files {
            let the_file = DataFile::new(&dir, *data_file)?;
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
            path: dir.clone(),
            envelope_path: envelope_path.clone(),
            data_files: data_files,
            index_files: index_files,
            index: Arc::new(ArcSwap::new(Arc::new(None))),
            building_index: Arc::new(Mutex::new(false)),
            bundle_file_hash: sha_u64,
            sub_bundles: HashMap::new(),
            synthetic: false
        })
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

        for index in self.index_files.values() {
            let mut the_index = index.read_index()?;
            if ret.len() == 0 {
                ret = the_index;
            } else if the_index.len() == 0 {

                // do nothing... nothing to sort
            } else if the_index[0].hash > ret[ret.len() - 1].hash {
                // append the_index to ret
                ret.append(&mut the_index);
            } else if the_index[the_index.len() - 1].hash < ret[0].hash {
                // append ret to the_index
                the_index.append(&mut ret);
                ret = the_index;
            } else {
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

    fn find_file(path: &PathBuf, hash: u64, suffix: &str) -> Result<File> {
        let mut path = path.clone();
        path.push(format!("{:016x}", hash));
        path.set_extension(suffix);
        Ok(File::open(path)?)
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
                        bail!("Got item {} that should have had a file_hash of {:016x}, but had {:016x}", item.identifier, file_hash, item.reference.0,)
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

    pub fn data_for_entry_offset(&self, index_loc: &IndexLoc) -> Result<(ItemEnvelope, Item)> {
        match index_loc {
            IndexLoc::Loc { offset, file_hash } => Ok(self.entry_for(*file_hash, *offset)?),
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
fn test_generated_bundle() {
    use std::time::Instant;

    let test_paths: Vec<String> = vec![
        "../../tmp/oc_dest/result_aa".into(),
        "../../tmp/oc_dest/result_ab".into(),
        "../goatrodeo/res_for_big_tent/".into(),
    ];

    for test_path in &test_paths {
        println!("\n\n==============\n\nTesting path {}", test_path);
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

        println!(
            "Finding files took {:?}",
            Instant::now().duration_since(start)
        );
        assert!(files.len() > 0, "We should find some files");
        let mut pass_num = 0;
        for bundle in files {
            pass_num += 1;
            let complete_index = bundle.get_index().unwrap();

            println!(
                "Loaded index for pass {} at {:?}",
                pass_num,
                Instant::now().duration_since(start)
            );

            assert!(
                complete_index.len() > 200,
                "Expecting a large index, got {} entries",
                complete_index.len()
            );
            println!("Index size {}", complete_index.len());

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

    let files = match GoatRodeoBundle::bundle_files_in_dir("../goatrodeo/res_for_big_tent/".into())
    {
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
        println!("Initial index build {:?} and subsequent {:?}", time, time2);
    }

    assert!(
        total_index_size > 100_000,
        "must read at least 100,000 items, but only got {}",
        total_index_size
    );
}
