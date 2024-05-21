use anyhow::{anyhow, bail, Result};
use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::{Seek, SeekFrom};
use std::ops::Deref;

use std::{
    collections::HashMap,
    fs::{read_dir, File},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::envelopes::EntryEnvelope;
use crate::index::EntryOffset;
use crate::structs::Item;
use crate::util::{
    byte_slice_to_u63, hex_to_u64, read_cbor, read_len_and_cbor, read_u16, read_u32,
    sha256_for_reader,
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
    pub file: Arc<Mutex<File>>,
    pub data_offset: u64,
}

impl IndexFile {
    pub fn new(dir: &PathBuf, hash: u64) -> Result<IndexFile> {
        let mut file = GoatRodeoBundle::find_file(&dir, hash, "gri")?;
        let tested_hash = byte_slice_to_u63(&sha256_for_reader(&mut file)?)?;
        if tested_hash != hash {
            bail!(
                "Index file for {:016x} does not match {:016x}",
                hash,
                tested_hash
            );
        }

        let mut file = GoatRodeoBundle::find_file(&dir, hash, "gri")?;

        let ifp: &mut File = &mut file;
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
            file: Arc::new(Mutex::new(file)),
            data_offset: idx_pos,
        })
    }

    pub fn read_index(&self) -> Result<Vec<EntryOffset>> {
        let mut ret = Vec::with_capacity(self.envelope.size as usize);
        let mut last = [0u8; 16];
        let mut not_sorted = false;
        let mut my_file = self
            .file
            .lock()
            .map_err(|e| anyhow!("Failed to lock {:?}", e))?;
        let fp: &mut File = &mut my_file;
        fp.seek(SeekFrom::Start(self.data_offset))?;

        for _ in 0..self.envelope.size {
            let eo = EntryOffset::read(fp)?;
            if eo.hash < last {
                not_sorted = true;
            }
            last = eo.hash;
            ret.push(eo);
        }

        if not_sorted {
            ret.sort_by(|a, b| a.hash.cmp(&b.hash))
        }

        Ok(ret)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct DataEnvelope {
    version: u32,
    magic: u32,
    the_type: String,
    previous: u64,
    depends_on: Vec<u64>,
    timestamp: i64,
    built_from_merge: bool,
    info: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct DataFile {
    pub envelope: DataEnvelope,
    pub file: Arc<Mutex<File>>,
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

        let env: DataEnvelope = read_len_and_cbor(dfp)?;

        let cur_pos: u64 = data_file.seek(SeekFrom::Current(0))?;
        // FIXME do additional validation of the envelope
        Ok(DataFile {
            envelope: env,
            file: Arc::new(Mutex::new(data_file)),
            data_offset: cur_pos,
        })
    }

    pub fn read_envelope_at(&self, pos: u64) -> Result<EntryEnvelope> {
        let mut my_file = self
            .file
            .lock()
            .map_err(|e| anyhow!("Failed to lock {:?}", e))?;
        my_file.seek(SeekFrom::Start(pos))?;
        let len = read_u16(&mut my_file.deref())?;
        let _ = read_u32(&mut my_file.deref())?;
        read_cbor(&mut my_file.deref(), len as usize)
    }

    pub fn read_envelope_and_item_at(&self, pos: u64) -> Result<(EntryEnvelope, Item)> {
        let mut my_file = self
            .file
            .lock()
            .map_err(|e| anyhow!("Failed to lock {:?}", e))?;
        my_file.seek(SeekFrom::Start(pos))?;
        let env_len = read_u16(&mut my_file.deref())?;
        let item_len = read_u32(&mut my_file.deref())?;
        let env = read_cbor(&mut my_file.deref(), env_len as usize)?;
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
    pub path: PathBuf,
    pub envelope_path: PathBuf,
    pub data_files: HashMap<u64, DataFile>,
    pub index_files: HashMap<u64, IndexFile>,
    index: Arc<ArcSwap<Option<Arc<Vec<EntryOffset>>>>>,
    building_index: Arc<Mutex<bool>>,
}

impl GoatRodeoBundle {
    #[allow(non_upper_case_globals)]
    pub const DataFileMagicNumber: u32 = 0x00be1100; // Bell

    #[allow(non_upper_case_globals)]
    pub const IndexFileMagicNumber: u32 = 0x54154170; // Shishitō

    #[allow(non_upper_case_globals)]
    pub const BundleFileMagicNumber: u32 = 0xba4a4a; // Banana

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

        let mut fs_file = File::open(file_path.clone())?;
        let tested_hash = byte_slice_to_u63(&sha256_for_reader(&mut fs_file)?)?;
        if tested_hash != hash {
            bail!(
                "Bundle file file for '{:016x}.grb' does not match actual hash {:016x}",
                hash,
                tested_hash
            );
        }

        // open and get info from the data file
        let mut bundle_file = File::open(file_path.clone())?;

        let sha_u64 = byte_slice_to_u63(&sha256_for_reader(&mut bundle_file)?)?;

        if sha_u64 != hash {
            bail!(
                "Expected the sha256 of '{:016x}.grb' to match the hash, but got hash {:016x}",
                hash,
                sha_u64
            );
        }

        bundle_file = File::open(file_path.clone())?;
        let dfp: &mut File = &mut bundle_file;
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
        })
    }

    pub fn get_index(&self) -> Result<Arc<Vec<EntryOffset>>> {
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
            } else if the_index.len() == 0 { // do nothing... nothing to sort
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
                            dest.push(ni.unwrap());
                            ni = ip.next();
                        }
                        (Some(_), None) => {
                            dest.push(nr.unwrap());
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

    pub fn data_file(&self, hash: u64) -> Result<Arc<Mutex<File>>> {
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
}

#[test]
fn test_generated_bundle() {
    use crate::index::Index;
    use std::time::Instant;

    let test_path = "../goatrodeo/frood_dir/";

    let goat_rodeo_test_data: PathBuf = test_path.into();
    // punt test is the files don't exist
    if !goat_rodeo_test_data.is_dir() {
        return;
    }

    let start = Instant::now();
    let files = match GoatRodeoBundle::bundle_files_in_dir(test_path.into()) {
        Ok(v) => v,
        Err(e) => {
            assert!(false, "Failure to read files {:?}", e);
            todo!()
        }
    };

    println!(
        "Finding files took {:?}",
        Instant::now().duration_since(start)
    );
    assert!(files.len() > 0, "We should find some files");
    let mut pass_num = 0;
    for bundle in files {
        let complete_index = bundle.get_index().unwrap();
        assert!(
            complete_index.len() > 200,
            "Expecting a large index, got {} entries",
            complete_index.len()
        );
        println!("Index size {}", complete_index.len());
        let index = Index::new(bundle, 4, None);
        let mut purls = vec![];
        for idx in complete_index.iter() {
            match index.data_for_hash(idx.hash) {
                Ok((_env, item)) => {
                    if item.identifier.starts_with("pkg:") {
                        purls.push(item.identifier.clone());
                    }
                }
                Err(e) => {
                    assert!(false, "Failed with error {}", e);
                    todo!();
                }
            }
        }

        pass_num += 1;
        println!(
            "Read index for pass {} at {:?}",
            pass_num,
            Instant::now().duration_since(start)
        );

        assert!(
            purls.len() > 20,
            "We expect a bunch of pURLs, but got {}",
            purls.len()
        );

        let start = Instant::now();
        let purl_cnt = purls.len();

        for p in purls {
            let item = index.data_for_key(&p).unwrap();
            let actual = index.data_for_key(&item.connections[0].0).unwrap();
            assert!(
                actual.alt_identifiers.contains(&p),
                "the connected item should contain the pURL"
            );
        }

        println!(
            "Fetched {} items for pass {} at {:?}",
            purl_cnt,
            pass_num,
            Instant::now().duration_since(start)
        );
    }
}

#[test]
fn test_files_in_dir() {
    use std::time::{Duration, Instant};

    let goat_rodeo_test_data: PathBuf = "../goatrodeo/frood_dir/".into();
    // punt test is the files don't exist
    if !goat_rodeo_test_data.is_dir() {
        return;
    }

    let files = match GoatRodeoBundle::bundle_files_in_dir("../goatrodeo/frood_dir/".into()) {
        Ok(v) => v,
        Err(e) => {
            assert!(false, "Failure to read files {:?}", e);
            todo!()
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
            time > Duration::from_millis(100),
            "Building the initial index should take more than 100ms, but took {:?}",
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
