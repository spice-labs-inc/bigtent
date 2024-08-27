#[cfg(not(test))]
use log::{info, trace}; // Use log crate when building application

use std::{
  collections::{BTreeMap, BTreeSet, HashSet},
  fs::{self, File},
  path::PathBuf,
  time::Instant,
};
#[cfg(test)]
use std::{println as info, println as trace};

use anyhow::{bail, Result};

use crate::{
  data_file::DataFileEnvelope,
  index_file::IndexEnvelope,
  rodeo::{ClusterFileEnvelope, GoatRodeoCluster},
  rodeo_server::MD5Hash,
  sha_writer::ShaWriter,
  structs::Item,
  util::{
    byte_slice_to_u63, md5hash_str, path_plus_timed, sha256_for_slice, write_envelope, write_int,
    write_long, write_short_signed,
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
  const MAX_INDEX_CNT: usize = 25 * 1024 * 1024; // 25M
  const MAX_DATA_FILE_SIZE: u64 = 15 * 1024 * 1024 * 1024; // 15GB
  #[inline]
  fn make_dest_buffer() -> ShaWriter {
    ShaWriter::new(10_000_000)
  }

  #[inline]
  fn make_index_buffer() -> Vec<IndexInfo> {
    Vec::with_capacity(10_000)
  }

  pub fn new<I: Into<PathBuf>>(dir: I) -> Result<ClusterWriter> {
    let dir_path: PathBuf = dir.into();
    if !dir_path.exists() {
      fs::create_dir_all(&dir_path)?;
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

    my_writer.write_data_envelope_start()?;

    Ok(my_writer)
  }

  pub fn cur_pos(&self) -> u64 {
    self.dest_data.pos()
  }

  pub fn previous_pos(&self) -> u64 {
    self.previous_position
  }

  pub fn write_item(&mut self, mut item: Item) -> Result<()> {
    use std::io::Write;
    let the_hash = md5hash_str(&item.identifier);
    let cur_pos = self.dest_data.pos();

    item.reference = (0, cur_pos);

    let item_bytes = serde_cbor::to_vec(&item)?;

    write_int(&mut self.dest_data, item_bytes.len() as u32)?;

    (&mut self.dest_data).write_all(&item_bytes)?;
    self.index_info.push(IndexInfo {
      hash: the_hash,
      offset: cur_pos,
      file_hash: 0,
    });

    self.previous_position = cur_pos;

    if self.index_info.len() > ClusterWriter::MAX_INDEX_CNT
      || self.dest_data.pos() > ClusterWriter::MAX_DATA_FILE_SIZE
    {
      self.write_data_and_index()?;
    }

    self.items_written += 1;

    if self.items_written % 250_000 == 0 {}

    Ok(())
  }

  pub fn finalize_cluster(&mut self) -> Result<PathBuf> {
    use std::io::Write;
    if self.previous_position != 0 {
      self.write_data_and_index()?;
    }
    let mut cluster_file = vec![];
    {
      let cluster_writer = &mut cluster_file;
      write_int(cluster_writer, GoatRodeoCluster::ClusterFileMagicNumber)?;
      let cluster_env = ClusterFileEnvelope {
        version: 1,
        magic: GoatRodeoCluster::ClusterFileMagicNumber,
        info: BTreeMap::new(),
        data_files: self.seen_data_files.iter().map(|v| *v).collect(),
        index_files: self.index_files.iter().map(|v| *v).collect(),
      };
      write_envelope(cluster_writer, &cluster_env)?;
    }

    // compute sha256 of index
    let cluster_reader: &[u8] = &cluster_file;
    let grc_sha = byte_slice_to_u63(&sha256_for_slice(cluster_reader))?;

    // write the .grc file

    let grc_file_path = path_plus_timed(&self.dir, &format!("{:016x}.grc", grc_sha));
    let mut grc_file = File::create(&grc_file_path)?;
    grc_file.write_all(&cluster_file)?;
    grc_file.flush()?;

    Ok(grc_file_path)
  }

  pub fn write_data_and_index(&mut self) -> Result<()> {
    use std::io::Write;
    let mut grd_sha = 0;
    if self.previous_position != 0 {
      write_short_signed(&mut self.dest_data, -1)?; // a marker that says end of file

      // write final back-pointer (to the last entry record)
      write_long(&mut self.dest_data, self.previous_position)?;

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

      let mut grd_file = File::create(grd_file_path)?;
      grd_file.write_all(&data)?;
      grd_file.flush()?;

      info!(
        "computed grd sha and wrote at {:?}",
        Instant::now().duration_since(self.start)
      );
      self.previous_position = 0;
      self.previous_hash = grd_sha;
      self.seen_data_files.insert(grd_sha);
      self.write_data_envelope_start()?;
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
        write_int(index_writer, GoatRodeoCluster::IndexFileMagicNumber)?;
        let index_env = IndexEnvelope {
          version: 1,
          magic: GoatRodeoCluster::IndexFileMagicNumber,
          size: self.index_info.len() as u32,
          data_files: found_hashes.clone(),
          encoding: "MD5/Long/Long".into(),
          info: BTreeMap::new(),
        };
        write_envelope(index_writer, &index_env)?;
        for v in &self.index_info {
          index_writer.write_all(&v.hash)?;
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
          )?;
          write_long(index_writer, v.offset)?;
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
        let mut gri_file = File::create(gri_file_path)?;
        gri_file.write_all(&index_file)?;
        gri_file.flush()?;
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
  pub fn add_index(&mut self, hash: MD5Hash, file_hash: u64, offset: u64) -> Result<()> {
    self.index_info.push(IndexInfo {
      hash,
      offset,
      file_hash,
    });

    if self.index_info.len() > ClusterWriter::MAX_INDEX_CNT {
      self.write_data_and_index()?;
      self.write_data_envelope_start()?;
    }
    Ok(())
  }

  fn write_data_envelope_start(&mut self) -> Result<()> {
    write_int(&mut self.dest_data, GoatRodeoCluster::DataFileMagicNumber)?;

    let data_envelope = DataFileEnvelope {
      version: 1,
      magic: GoatRodeoCluster::DataFileMagicNumber,
      previous: self.previous_hash,
      depends_on: self.seen_data_files.clone(),
      built_from_merge: false,
      info: BTreeMap::new(),
    };

    write_envelope(&mut self.dest_data, &data_envelope)?;

    self.previous_position = 0;
    Ok(())
  }
}
