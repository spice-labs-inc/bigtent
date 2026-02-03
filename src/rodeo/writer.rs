//! # ClusterWriter - Cluster File Generation
//!
//! This module handles writing new cluster files during merge operations.
//! It manages the creation of `.grc`, `.gri`, and `.grd` files with proper
//! formatting and checksums.
//!
//! ## Writing Process
//!
//! 1. **Initialize**: Create writer with destination directory
//! 2. **Write Items**: Call `write_item()` for each Item
//! 3. **Flush**: Periodically flush data and index files when size limits reached
//! 4. **Finalize**: Generate final cluster file with all references
//!
//! ## File Size Limits
//!
//! To keep files manageable, the writer enforces size limits:
//! - **Data files**: Max 15 GB (`MAX_DATA_FILE_SIZE`)
//! - **Index files**: Max 25M entries (`MAX_INDEX_CNT`)
//!
//! When limits are reached, current files are finalized and new ones started.
//!
//! ## Buffer Management
//!
//! Data is buffered in memory (`dest_data`) before being written to disk.
//! Index entries are accumulated in `index_info` and sorted before writing.
//! This enables efficient sequential writes while maintaining sorted order.
//!
//! ## Output Files
//!
//! The writer produces:
//! - `cluster_<timestamp>.grc` - Cluster metadata
//! - `index_<hash>.gri` - Index files (multiple if data is large)
//! - `data_<hash>.grd` - Data files (multiple if data is large)
//!
//! File names include SHA256 hashes for content-addressable storage.

use log::error;
#[cfg(not(test))]
use log::info;
use tokio::{fs::File, io::AsyncWriteExt, sync::Mutex};

#[cfg(test)]
use std::println as info;
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    mem::{self, swap},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};

use crate::{
    item::Item,
    rodeo::index::{IndexEnvelope, IndexFileMagicNumber},
    util::{
        MD5Hash, byte_slice_to_u63, md5hash_str, path_plus_timed, sha256_for_slice, write_envelope,
        write_int, write_long, write_short_signed, write_usize_sync,
    },
};

use super::{
    cluster::{CLUSTER_VERSION, ClusterFileEnvelope, ClusterFileMagicNumber},
    data::{DataFileEnvelope, DataFileMagicNumber},
};

struct IndexInfo {
    hash: MD5Hash,
    file_hash: u64,
    offset: usize,
}

pub struct ClusterWriter {
    dir: PathBuf,
    dest_data: Vec<u8>, // ShaWriter,
    index_info: Vec<IndexInfo>,
    previous_hash: Arc<AtomicU64>,
    previous_position: usize,
    seen_data_files: Arc<Mutex<BTreeSet<u64>>>,
    index_files: Arc<Mutex<BTreeSet<u64>>>,
    items_written: usize,
    current_write_cnt: Arc<AtomicUsize>,
}

impl ClusterWriter {
    /// Maximum size of a single data file: 15 GB
    ///
    /// When the current data file exceeds this size, it's finalized and a new one is started.
    /// This keeps individual files manageable and allows for parallel processing during reads.
    const MAX_DATA_FILE_SIZE: usize = 15 * 1024 * 1024 * 1024;

    /// Maximum number of index entries per index file: 25 million
    ///
    /// Each index entry is 32 bytes (16 MD5 + 8 file hash + 8 offset), so 25M entries
    /// equals ~800 MB per index file. This limit ensures index files remain memory-mappable.
    const MAX_INDEX_CNT: usize = 25 * 1024 * 1024;

    #[inline]
    fn make_dest_buffer() -> Vec<u8> {
        // tests can run on small RAM machines, allocate a smaller buffer for tests
        if cfg!(test) {
            Vec::with_capacity(20_000_000)
        } else if cfg!(not(test)) {
            // and a bigger buffer for runtime
            Vec::with_capacity(20_000_000_000)
        } else {
            panic!("How can this be both not test and test?!");
        }
    }

    #[inline]
    fn make_index_buffer() -> Vec<IndexInfo> {
        Vec::with_capacity(10_000)
    }

    pub async fn new<I: Into<PathBuf>>(dir: I) -> Result<ClusterWriter> {
        let dir_path: PathBuf = dir.into();
        if !dir_path.exists() {
            tokio::fs::create_dir_all(&dir_path).await?;
        }
        if !dir_path.is_dir() {
            bail!(
                "Writing Clusters requires a directory... got {:?}",
                dir_path
            );
        }

        let mut my_writer = ClusterWriter {
            dir: dir_path,
            dest_data: ClusterWriter::make_dest_buffer(),
            index_info: ClusterWriter::make_index_buffer(),
            previous_hash: Arc::new(AtomicU64::new(0)),
            previous_position: 0,
            seen_data_files: Arc::new(Mutex::new(BTreeSet::new())),
            index_files: Arc::new(Mutex::new(BTreeSet::new())),
            items_written: 0,
            current_write_cnt: Arc::new(AtomicUsize::new(0)),
        };

        my_writer.write_data_envelope_start().await?;

        Ok(my_writer)
    }

    pub fn cur_pos(&self) -> usize {
        self.dest_data.len()
    }

    pub fn previous_pos(&self) -> usize {
        self.previous_position
    }

    /// add an `Item` to the cluster. for good performance
    /// `Item`s should be written in order by MD5 hash of the `item.identifier`
    pub async fn write_item(&mut self, item: Item, cbor_bytes: Vec<u8>) -> Result<()> {
        let the_hash = md5hash_str(&item.identifier);
        let cur_pos = self.dest_data.len();

        let item_bytes = cbor_bytes; //serde_cbor::to_vec(&item)?;

        write_int(&mut self.dest_data, item_bytes.len() as u32).await?;

        (&mut self.dest_data).write_all(&item_bytes).await?;
        self.index_info.push(IndexInfo {
            hash: the_hash,
            offset: cur_pos,
            file_hash: 0,
        });

        self.previous_position = cur_pos;

        if self.index_info.len() > ClusterWriter::MAX_INDEX_CNT
            || self.dest_data.len() > ClusterWriter::MAX_DATA_FILE_SIZE
        {
            self.write_data_and_index().await?;
        }

        self.items_written += 1;

        if self.items_written % 250_000 == 0 {}

        Ok(())
    }

    pub async fn finalize_cluster(&mut self) -> Result<PathBuf> {
        if self.previous_position != 0 {
            self.write_data_and_index().await?;
            info!("Waiting for data and index file write to complete");
            while self
                .current_write_cnt
                .load(std::sync::atomic::Ordering::Relaxed)
                > 0
            {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            info!("Data and index file write complete");
        }

        let mut cluster_file = vec![];
        {
            let cluster_writer = &mut cluster_file;
            write_int(cluster_writer, ClusterFileMagicNumber).await?;
            let cluster_env = ClusterFileEnvelope {
                version: CLUSTER_VERSION,
                magic: ClusterFileMagicNumber,
                info: BTreeMap::new(),
                data_files: self
                    .seen_data_files
                    .lock()
                    .await
                    .iter()
                    .map(|v| *v)
                    .collect(),
                index_files: self.index_files.lock().await.iter().map(|v| *v).collect(),
            };
            write_envelope(cluster_writer, &cluster_env).await?;
        }

        // compute sha256 of index
        let cluster_reader: &[u8] = &cluster_file;
        let grc_sha = byte_slice_to_u63(&sha256_for_slice(cluster_reader))?;

        // write the .grc file

        let grc_file_path = path_plus_timed(&self.dir, &format!("{:016x}.grc", grc_sha));
        let mut grc_file = File::create(&grc_file_path).await?;
        grc_file.write_all(&cluster_file).await?;
        grc_file.flush().await?;

        Ok(grc_file_path)
    }

    pub fn finish_writing_and_reset(&mut self) -> Vec<u8> {
        let mut the_backing = ClusterWriter::make_dest_buffer();

        mem::swap(&mut self.dest_data, &mut the_backing);

        the_backing
    }

    pub async fn write_data_and_index(&mut self) -> Result<()> {
        // spin lock on writing... only one of these can run at once

        while self
            .current_write_cnt
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        if self.previous_position != 0 {
            let mut new_index_info = ClusterWriter::make_index_buffer();
            swap(&mut self.index_info, &mut new_index_info);

            write_short_signed(&mut self.dest_data, -1).await?; // a marker that says end of file

            // write final back-pointer (to the last entry record)
            write_usize_sync(&mut self.dest_data, self.previous_position)?;

            let data = Arc::new(self.finish_writing_and_reset());

            let self_dir = self.dir.clone();
            let self_start = Instant::now(); // self.start.clone();
            let previous_hash = self.previous_hash.clone();
            let seen_data_files = self.seen_data_files.clone();

            let index_files = self.index_files.clone();
            let counter = self.current_write_cnt.clone();
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed); // increment the write count
            tokio::task::spawn(async move {
                info!(
                    "computing grd sha {:?}",
                    Instant::now().duration_since(self_start)
                );
                async fn perform_write(
                    data: Arc<Vec<u8>>,
                    self_dir: PathBuf,
                    self_start: Instant,
                    previous_hash: Arc<AtomicU64>,
                    new_index_info: Vec<IndexInfo>,
                    seen_data_files: Arc<Mutex<BTreeSet<u64>>>,
                    index_files: Arc<Mutex<BTreeSet<u64>>>,
                ) -> Result<()> {
                    let data_ref = data.clone();
                    let sha256 =
                        tokio::task::spawn_blocking(move || sha256_for_slice(&data_ref)).await?;
                    let grd_sha =
                        byte_slice_to_u63(&sha256).context("Should be able to shorten sha")?;
                    let grd_file_path = self_dir.join(format!("{:016x}.grd", grd_sha));
                    let mut grd_file = File::create(grd_file_path).await?;
                    grd_file.write_all(&data).await?;
                    grd_file.flush().await.context("Should flush .grd file")?;

                    info!(
                        "computed grd sha and wrote at {:?}",
                        Instant::now().duration_since(self_start)
                    );

                    previous_hash.store(grd_sha, std::sync::atomic::Ordering::Relaxed);

                    let mut found_hashes = HashSet::new();
                    if grd_sha != 0 {
                        found_hashes.insert(grd_sha);
                    }
                    for v in &new_index_info {
                        if v.file_hash != 0 {
                            found_hashes.insert(v.file_hash);
                        }
                    }
                    seen_data_files.lock().await.insert(grd_sha);

                    let mut index_file = vec![];
                    {
                        let index_writer = &mut index_file;
                        write_int(index_writer, IndexFileMagicNumber).await?;
                        let index_env = IndexEnvelope {
                            version: 1,
                            magic: IndexFileMagicNumber,
                            size: new_index_info.len() as u32,
                            data_files: found_hashes.clone(),
                            encoding: "MD5/Long/Long".into(),
                            info: BTreeMap::new(),
                        };
                        write_envelope(index_writer, &index_env).await?;
                        for v in &new_index_info {
                            std::io::Write::write_all(index_writer, &v.hash)?;
                            write_long(
                index_writer,
                if v.file_hash == 0 {
                  if grd_sha == 0 {
                    bail!("Got an index with a zero marker file_hash, but no file was written?!?");
                  }
                  grd_sha
                } else {
                  v.file_hash
                },
              )
              .await?;
                            write_usize_sync(index_writer, v.offset)?;
                        }
                    }

                    info!(
                        "computing gri sha {:?}",
                        Instant::now().duration_since(self_start)
                    );

                    // compute sha256 of index
                    let index_arc = Arc::new(index_file);
                    let index_reader = index_arc.clone();
                    let gri_sha = tokio::task::spawn_blocking(move || {
                        byte_slice_to_u63(&sha256_for_slice(&index_reader))
                    })
                    .await??;
                    {
                        let mut owned_index_file = index_files.lock().await;
                        owned_index_file.insert(gri_sha);
                    }
                    // write the .gri file
                    {
                        let gri_file_path = self_dir.join(format!("{:016x}.gri", gri_sha));
                        let mut gri_file = File::create(gri_file_path).await?;
                        gri_file.write_all(&index_arc).await?;
                        gri_file.flush().await?;
                    }
                    info!(
                        "computed gri sha and wrote index file {:?}",
                        Instant::now().duration_since(self_start)
                    );
                    seen_data_files.lock().await.extend(found_hashes);
                    Ok(())
                }
                let ret = perform_write(
                    data,
                    self_dir,
                    self_start,
                    previous_hash,
                    new_index_info,
                    seen_data_files,
                    index_files,
                )
                .await;
                match &ret {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Failed to write the data envelope! {:?}", e);
                    }
                }
                counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                ret
            });
            self.previous_position = 0;

            self.write_data_envelope_start().await?;
        }

        Ok(())
    }

    pub async fn add_index(&mut self, hash: MD5Hash, file_hash: u64, offset: usize) -> Result<()> {
        self.index_info.push(IndexInfo {
            hash,
            offset,
            file_hash,
        });

        if self.index_info.len() > ClusterWriter::MAX_INDEX_CNT {
            self.write_data_and_index().await?;
            self.write_data_envelope_start().await?;
        }
        Ok(())
    }

    async fn write_data_envelope_start(&mut self) -> Result<()> {
        write_int(&mut self.dest_data, DataFileMagicNumber).await?;

        let data_envelope = DataFileEnvelope {
            version: DATA_FILE_ENVELOPE_VERSION,
            magic: DataFileMagicNumber,
            previous: self
                .previous_hash
                .load(std::sync::atomic::Ordering::Relaxed),
            depends_on: self.seen_data_files.lock().await.clone(),
            built_from_merge: false,
            info: BTreeMap::new(),
        };

        write_envelope(&mut self.dest_data, &data_envelope).await?;

        self.previous_position = 0;
        Ok(())
    }
}

pub const DATA_FILE_ENVELOPE_VERSION: u32 = 1u32;
