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

use tokio::{fs::File, io::AsyncWriteExt, sync::Mutex};
use tracing::error;
#[cfg(not(test))]
use tracing::info;

#[cfg(test)]
use std::println as info;
use std::{
    collections::{BTreeMap, BTreeSet},
    mem::{self, swap},
    path::PathBuf,
    sync::{
        Arc,
        atomic::AtomicUsize,
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};

use crate::{
    item::Item,
    rodeo::index::{IndexEnvelope, IndexFileMagicNumber},
    util::{
        KeyHash, byte_slice_to_u63, path_plus_timed, sha256_for_slice, write_envelope,
        write_int, write_long, write_short_signed, write_usize_sync,
    },
};

use super::{
    cluster::{CLUSTER_VERSION, ClusterFileEnvelope, ClusterFileMagicNumber},
    data::{DataFileEnvelope, DataFileMagicNumber},
};

struct IndexInfo {
    hash: KeyHash,
    file_hash: u64,
    offset: usize,
}

pub struct ClusterWriter {
    dir: PathBuf,
    dest_data: Vec<u8>, // ShaWriter,
    index_info: Vec<IndexInfo>,
    /// the key space this writer appends and declares (fresh writers:
    /// BLAKE3; the merge: its sources' common key space)
    key_alg: crate::util::KeyAlg,
    /// the last appended index key; appends must never regress (output
    /// clusters are key-ordered so the index is usable)
    last_key: Option<KeyHash>,
    previous_position: usize,
    seen_data_files: Arc<Mutex<BTreeSet<u64>>>,
    index_files: Arc<Mutex<BTreeSet<u64>>>,
    items_written: usize,
    current_write_cnt: Arc<AtomicUsize>,
    max_data_file_size: usize,
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

    /// The data-file size split limit (shared with the conversion's chunk
    /// budget: one set of limits).
    pub(crate) const fn max_data_file_size() -> usize {
        Self::MAX_DATA_FILE_SIZE
    }

    /// The index entry count split limit.
    pub(crate) const fn max_index_entries() -> usize {
        Self::MAX_INDEX_CNT
    }

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
        Self::new_full(
            dir,
            Self::MAX_DATA_FILE_SIZE,
            crate::util::KeyAlg::Blake3Truncated128,
            None,
        )
        .await
    }

    pub async fn new_with_max_size<I: Into<PathBuf>>(
        dir: I,
        max_data_file_size: usize,
    ) -> Result<ClusterWriter> {
        Self::new_full(
            dir,
            max_data_file_size,
            crate::util::KeyAlg::Blake3Truncated128,
            None,
        )
        .await
    }

    /// Create a writer that declares and appends keys from `key_alg`'s key
    /// space.
    ///
    /// Fresh writers use the version 4 default (BLAKE3). The merge passes
    /// its sources' common key space: the coordinator pops items in that
    /// order, so the output's declared algorithm and appended keys agree
    /// (phase 3's conversion makes that space uniformly BLAKE3).
    pub async fn new_with_key_alg<I: Into<PathBuf>>(
        dir: I,
        max_data_file_size: usize,
        key_alg: crate::util::KeyAlg,
    ) -> Result<ClusterWriter> {
        Self::new_full(dir, max_data_file_size, key_alg, None).await
    }

    /// Create a writer with an explicit destination buffer capacity.
    ///
    /// The default writer reserves a large destination buffer up front;
    /// the conversion path (phase 3) needs a bounded one instead. The
    /// default behavior is unchanged when `dest_buffer_capacity` is None.
    pub async fn new_bounded<I: Into<PathBuf>>(
        dir: I,
        max_data_file_size: usize,
        key_alg: crate::util::KeyAlg,
        dest_buffer_capacity: usize,
    ) -> Result<ClusterWriter> {
        Self::new_full(
            dir,
            max_data_file_size,
            key_alg,
            Some(dest_buffer_capacity),
        )
        .await
    }

    async fn new_full<I: Into<PathBuf>>(
        dir: I,
        max_data_file_size: usize,
        key_alg: crate::util::KeyAlg,
        dest_buffer_capacity: Option<usize>,
    ) -> Result<ClusterWriter> {
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

        let dest_data = match dest_buffer_capacity {
            Some(capacity) => Vec::with_capacity(capacity),
            None => ClusterWriter::make_dest_buffer(),
        };

        let mut my_writer = ClusterWriter {
            dir: dir_path,
            dest_data,
            index_info: ClusterWriter::make_index_buffer(),
            key_alg,
            last_key: None,
            previous_position: 0,
            seen_data_files: Arc::new(Mutex::new(BTreeSet::new())),
            index_files: Arc::new(Mutex::new(BTreeSet::new())),
            items_written: 0,
            current_write_cnt: Arc::new(AtomicUsize::new(0)),
            max_data_file_size,
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
    /// `Item`s should be written in order by hash of the `item.identifier`
    pub async fn write_item(&mut self, item: Item, cbor_bytes: Vec<u8>) -> Result<()> {
        let the_hash = self.key_alg.hash_identifier(&item.identifier);
        self.write_item_with_hash(cbor_bytes, the_hash).await
    }

    /// add CBOR-encoded item bytes using a precomputed index key.
    /// This avoids recomputing the hash in the writer hot path.
    ///
    /// Version 4 output is key-ordered: appending a key **lower** than
    /// the last appended key is rejected immediately, because it would
    /// produce an index that cannot be searched. Equal keys are accepted
    /// (duplicate identifiers merge downstream, and are not an error).
    ///
    /// The data-envelope chain fields are inert in version 4: every file
    /// carries `previous: 0` and an empty `depends_on`, and BigTent
    /// neither maintains nor consults the chain, which keeps multi-file
    /// output byte-deterministic (H5).
    pub async fn write_item_with_hash(
        &mut self,
        cbor_bytes: Vec<u8>,
        the_hash: KeyHash,
    ) -> Result<()> {
        if let Some(last) = &self.last_key {
            if the_hash < *last {
                bail!(
                    "Out-of-order append: key {:02x?} is lower than the last appended key {:02x?}",
                    the_hash,
                    last
                );
            }
        }
        self.last_key = Some(the_hash);

        let cur_pos = self.dest_data.len();

        let item_bytes = cbor_bytes; //serde_cbor::to_vec(&item)?;

        write_int(&mut self.dest_data, item_bytes.len() as u32).await?;

        self.dest_data.write_all(&item_bytes).await?;
        self.index_info.push(IndexInfo {
            hash: the_hash,
            offset: cur_pos,
            file_hash: 0,
        });

        self.previous_position = cur_pos;

        if self.index_info.len() > ClusterWriter::MAX_INDEX_CNT
            || self.dest_data.len() > self.max_data_file_size
        {
            self.write_data_and_index().await?;
        }

        self.items_written += 1;

        Ok(())
    }

    pub async fn finalize_cluster(&mut self) -> Result<PathBuf> {
        if self.previous_position != 0 {
            self.write_data_and_index().await?;
        }
        // Wait for any in-flight data/index file writes (they update the file sets the
        // .grc file will reference) before building the cluster file.
        info!("Waiting for data and index file write to complete");
        while self
            .current_write_cnt
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        info!("Data and index file write complete");

        let mut cluster_file = vec![];
        {
            let cluster_writer = &mut cluster_file;
            write_int(cluster_writer, ClusterFileMagicNumber).await?;
            let cluster_env = ClusterFileEnvelope {
                version: CLUSTER_VERSION,
                magic: ClusterFileMagicNumber,
                info: BTreeMap::new(),
                data_files: self.seen_data_files.lock().await.iter().copied().collect(),
                index_files: self.index_files.lock().await.iter().copied().collect(),
                encoding: Some(self.key_alg_encoding().to_string()),
            };
            write_envelope(cluster_writer, &cluster_env).await?;
        }

        // compute sha256 of index
        let cluster_reader: &[u8] = &cluster_file;
        let grc_sha = byte_slice_to_u63(&sha256_for_slice(cluster_reader))
            .context(format!("Reading {:?}", cluster_file))?;

        // write the .grc file
        let grc_file_path = path_plus_timed(&self.dir, &format!("{:016x}.grc", grc_sha));
        let context = format!("Failed writing {:?}", grc_file_path);
        let mut grc_file = File::create(&grc_file_path)
            .await
            .context(context.clone())?;
        grc_file
            .write_all(&cluster_file)
            .await
            .context(context.clone())?;
        grc_file.flush().await.context(context.clone())?;

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
            let seen_data_files = self.seen_data_files.clone();

            let index_files = self.index_files.clone();
            let counter = self.current_write_cnt.clone();
            counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed); // increment the write count
            let encoding = self.key_alg_encoding();
            tokio::task::spawn(async move {
                info!(
                    "computing grd sha {:?}",
                    Instant::now().duration_since(self_start)
                );
                async fn perform_write(
                    data: Arc<Vec<u8>>,
                    self_dir: PathBuf,
                    self_start: Instant,
                    encoding: &'static str,
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

                    let mut found_hashes = BTreeSet::new();
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
                            // the index envelope version is unchanged (the
                            // v3 writer emitted 1); the encoding string is
                            // what names the key algorithm (ADR 0002)
                            version: 1,
                            magic: IndexFileMagicNumber,
                            size: new_index_info.len() as u32,
                            data_files: found_hashes.clone(),
                            encoding: encoding.into(),
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
                    encoding,
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

    /// The algorithm constant this writer declares (ADR 0002).
    fn key_alg_encoding(&self) -> &'static str {
        match self.key_alg {
            crate::util::KeyAlg::Md5 => super::cluster::V3_CLUSTER_ENCODING,
            crate::util::KeyAlg::Blake3Truncated128 => super::cluster::V4_CLUSTER_ENCODING,
        }
    }

    async fn write_data_envelope_start(&mut self) -> Result<()> {
        write_int(&mut self.dest_data, DataFileMagicNumber).await?;

        // chain fields are inert in version 4 (H5): always `previous: 0`
        // and an empty `depends_on`
        let data_envelope = DataFileEnvelope {
            version: DATA_FILE_ENVELOPE_VERSION,
            magic: DataFileMagicNumber,
            previous: 0,
            depends_on: BTreeSet::new(),
            built_from_merge: false,
            info: BTreeMap::new(),
        };

        write_envelope(&mut self.dest_data, &data_envelope).await?;

        self.previous_position = 0;
        Ok(())
    }
}

/// Version 4 data file envelope version: the item shape changed (the
/// connections map), so the data envelope version is bumped from 1.
pub const DATA_FILE_ENVELOPE_VERSION: u32 = 2u32;

#[cfg(test)]
mod tests {
    use super::*;

    fn tiny_item() -> Item {
        Item {
            identifier: "gitoid:test".to_string(),
            connections: crate::item::Connections::default(),
            body_mime_type: None,
            body: None,
        }
    }

    #[tokio::test]
    async fn test_new_with_max_size_respects_limit() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut writer = ClusterWriter::new_with_max_size(dir.path(), 1)
            .await
            .expect("Should create writer");
        let cbor = serde_cbor::to_vec(&tiny_item()).unwrap();
        writer
            .write_item_with_hash(cbor, [0u8; 16])
            .await
            .expect("Should write item");
        writer
            .finalize_cluster()
            .await
            .expect("Should finalize cluster");

        let grd_files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "grd")
                    .unwrap_or(false)
            })
            .collect();
        assert!(
            !grd_files.is_empty(),
            "Should produce at least one .grd file with a tiny max size"
        );
    }

    #[tokio::test]
    async fn test_write_item_with_hash_uses_provided_hash() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut writer = ClusterWriter::new(dir.path())
            .await
            .expect("Should create writer");
        let cbor = serde_cbor::to_vec(&tiny_item()).unwrap();
        let hash = [42u8; 16];
        writer
            .write_item_with_hash(cbor, hash)
            .await
            .expect("Should write item with precomputed hash");
        assert_eq!(writer.items_written, 1);
        writer
            .finalize_cluster()
            .await
            .expect("Should finalize cluster");
    }
}
