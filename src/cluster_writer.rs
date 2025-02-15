#[cfg(not(test))]
use log::{info, trace};
use tokio::{fs::File, io::AsyncWriteExt};

use std::{
  collections::{BTreeMap, BTreeSet, HashSet},
  path::PathBuf,
  time::Instant,
};
#[cfg(test)]
use std::{println as info, println as trace};

use anyhow::{bail, Result};

use crate::{
  data_file::DataFileEnvelope,
  index_file::IndexEnvelope,
  rodeo::{ClusterFileEnvelope, ClusterFileMagicNumber, DataFileMagicNumber, IndexFileMagicNumber},
  sha_writer::ShaWriter,
  structs::Item,
  util::{
    byte_slice_to_u63, md5hash_str, path_plus_timed, sha256_for_slice, write_envelope, write_int,
    write_long, write_short_signed, MD5Hash,
  },
};

struct IndexInfo {
  hash: MD5Hash,
  file_hash: u64,
  offset: u64,
}

pub struct ClusterWriter {
  dir: PathBuf,
  dest_data: ShaWriter,
  index_info: Vec<IndexInfo>,
  previous_hash: u64,
  previous_position: u64,
  seen_data_files: BTreeSet<u64>,
  index_files: BTreeSet<u64>,
  start: Instant,
  items_written: usize,
}

impl ClusterWriter {
  // 15GB
  const MAX_DATA_FILE_SIZE: u64 = 15 * 1024 * 1024 * 1024;
  // 25M
  const MAX_INDEX_CNT: usize = 25 * 1024 * 1024;

  #[inline]
  fn make_dest_buffer() -> ShaWriter {
    ShaWriter::new(10_000_000)
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
      previous_hash: 0,
      previous_position: 0,
      seen_data_files: BTreeSet::new(),
      index_files: BTreeSet::new(),
      start: Instant::now(),
      items_written: 0,
    };

    my_writer.write_data_envelope_start().await?;

    Ok(my_writer)
  }

  pub fn cur_pos(&self) -> u64 {
    self.dest_data.pos()
  }

  pub fn previous_pos(&self) -> u64 {
    self.previous_position
  }

  pub async fn write_item(&mut self, item: Item) -> Result<()> {
    let the_hash = md5hash_str(&item.identifier);
    let cur_pos = self.dest_data.pos();

    let item_bytes = serde_cbor::to_vec(&item)?;

    write_int(&mut self.dest_data, item_bytes.len() as u32).await?;

    (&mut self.dest_data).write_all(&item_bytes).await?;
    self.index_info.push(IndexInfo {
      hash: the_hash,
      offset: cur_pos,
      file_hash: 0,
    });

    self.previous_position = cur_pos;

    if self.index_info.len() > ClusterWriter::MAX_INDEX_CNT
      || self.dest_data.pos() > ClusterWriter::MAX_DATA_FILE_SIZE
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
    }
    let mut cluster_file = vec![];
    {
      let cluster_writer = &mut cluster_file;
      write_int(cluster_writer, ClusterFileMagicNumber).await?;
      let cluster_env = ClusterFileEnvelope {
        version: 1,
        magic: ClusterFileMagicNumber,
        info: BTreeMap::new(),
        data_files: self.seen_data_files.iter().map(|v| *v).collect(),
        index_files: self.index_files.iter().map(|v| *v).collect(),
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

  pub async fn write_data_and_index(&mut self) -> Result<()> {
    let mut grd_sha = 0;
    if self.previous_position != 0 {
      write_short_signed(&mut self.dest_data, -1).await?; // a marker that says end of file

      // write final back-pointer (to the last entry record)
      write_long(&mut self.dest_data, self.previous_position).await?;

      info!(
        "computing grd sha {:?}",
        Instant::now().duration_since(self.start)
      );

      let (data, sha) = self.dest_data.finish_writing_and_reset()?;
      // let data_reader: &[u8] = &self.dest_data;
      // grd_sha = byte_slice_to_u63(&sha256_for_slice(data_reader))?;
      grd_sha = byte_slice_to_u63(&sha)?;

      // write the .grd file

      let grd_file_path = self.dir.join(format!("{:016x}.grd", grd_sha));

      let mut grd_file = File::create(grd_file_path).await?;
      grd_file.write_all(&data).await?;
      grd_file.flush().await?;

      info!(
        "computed grd sha and wrote at {:?}",
        Instant::now().duration_since(self.start)
      );
      self.previous_position = 0;
      self.previous_hash = grd_sha;
      self.seen_data_files.insert(grd_sha);
      self.write_data_envelope_start().await?;
    }

    if self.index_info.len() > 0 {
      let mut found_hashes = HashSet::new();
      if grd_sha != 0 {
        found_hashes.insert(grd_sha);
      }
      for v in &self.index_info {
        if v.file_hash != 0 {
          found_hashes.insert(v.file_hash);
        }
      }

      let mut index_file = vec![];
      {
        let index_writer = &mut index_file;
        write_int(index_writer, IndexFileMagicNumber).await?;
        let index_env = IndexEnvelope {
          version: 1,
          magic: IndexFileMagicNumber,
          size: self.index_info.len() as u32,
          data_files: found_hashes.clone(),
          encoding: "MD5/Long/Long".into(),
          info: BTreeMap::new(),
        };
        write_envelope(index_writer, &index_env).await?;
        for v in &self.index_info {
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
          write_long(index_writer, v.offset).await?;
        }
      }

      info!(
        "computing gri sha {:?}",
        Instant::now().duration_since(self.start)
      );

      // compute sha256 of index
      let index_reader: &[u8] = &index_file;
      let gri_sha = byte_slice_to_u63(&sha256_for_slice(index_reader))?;
      self.index_files.insert(gri_sha);
      // write the .gri file
      {
        let gri_file_path = self.dir.join(format!("{:016x}.gri", gri_sha));
        let mut gri_file = File::create(gri_file_path).await?;
        gri_file.write_all(&index_file).await?;
        gri_file.flush().await?;
      }
      info!(
        "computed gri sha and wrote index file {:?}",
        Instant::now().duration_since(self.start)
      );
      self.index_info = ClusterWriter::make_index_buffer();
      self.seen_data_files.extend(found_hashes);
    }
    self.dump_file_names();
    Ok(())
  }

  fn dump_file_names(&self) {
    trace!("Data");
    for d in &self.seen_data_files {
      trace!("{:016x}", d);
    }

    trace!("Index");
    for d in &self.index_files {
      trace!("{:016x}", d);
    }
  }

  pub async fn add_index(&mut self, hash: MD5Hash, file_hash: u64, offset: u64) -> Result<()> {
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
      version: 1,
      magic: DataFileMagicNumber,
      previous: self.previous_hash,
      depends_on: self.seen_data_files.clone(),
      built_from_merge: false,
      info: BTreeMap::new(),
    };

    write_envelope(&mut self.dest_data, &data_envelope).await?;

    self.previous_position = 0;
    Ok(())
  }
}
