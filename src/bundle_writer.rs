use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    path::PathBuf,
    time::Instant,
};

use anyhow::{bail, Result};

use crate::{
    data_file::DataFileEnvelope,
    envelopes::{ItemEnvelope, MD5},
    index_file::IndexEnvelope,
    rodeo::{BundleFileEnvelope, GoatRodeoBundle},
    rodeo_server::MD5Hash,
    sha_writer::ShaWriter,
    structs::Item,
    util::{
        byte_slice_to_u63, md5hash_str, millis_now, path_plus_timed, sha256_for_slice,
        write_envelope, write_int, write_long, write_short, write_short_signed,
    },
};

struct IndexInfo {
    hash: MD5Hash,
    file_hash: u64,
    offset: u64,
}

pub struct BundleWriter {
    dir: PathBuf,
    dest_data: ShaWriter,
    index_info: Vec<IndexInfo>,
    previous_hash: u64,
    previous_position: u64,
    seen_data_files: HashSet<u64>,
    index_files: HashSet<u64>,
    start: Instant,
    items_written: usize,
}

impl BundleWriter {
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

    pub fn new<I: Into<PathBuf>>(dir: I) -> Result<BundleWriter> {
        let dir_path: PathBuf = dir.into();
        if !dir_path.exists() {
            fs::create_dir_all(&dir_path)?;
        }
        if !dir_path.is_dir() {
            bail!("Writing Bundles requires a directory... got {:?}", dir_path);
        }

        let mut my_writer = BundleWriter {
            dir: dir_path,
            dest_data: BundleWriter::make_dest_buffer(),
            index_info: BundleWriter::make_index_buffer(),
            previous_hash: 0,
            previous_position: 0,
            seen_data_files: HashSet::new(),
            index_files: HashSet::new(),
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

        let item_envelope = ItemEnvelope {
            key_md5: MD5 { hash: the_hash },
            position: cur_pos,
            backpointer: self.previous_pos(),
            data_len: item_bytes.len() as u32,
            data_type: crate::envelopes::PayloadType::ENTRY,
        };

        let item_env_bytes = serde_cbor::to_vec(&item_envelope)?;
        write_short(&mut self.dest_data, item_env_bytes.len() as u16)?;
        write_int(&mut self.dest_data, item_bytes.len() as u32)?;

        (&mut self.dest_data).write_all(&item_env_bytes)?;
        (&mut self.dest_data).write_all(&item_bytes)?;
        self.index_info.push(IndexInfo {
            hash: item_envelope.key_md5.hash,
            offset: cur_pos,
            file_hash: 0,
        });

        self.previous_position = cur_pos;

        if self.index_info.len() > BundleWriter::MAX_INDEX_CNT
            || self.dest_data.pos() > BundleWriter::MAX_DATA_FILE_SIZE
        {
            self.write_data_and_index()?;
        }

        self.items_written += 1;

        if self.items_written % 250_000 == 0 {}

        Ok(())
    }

    pub fn finalize_bundle(&mut self) -> Result<PathBuf> {
        use std::io::Write;
        if self.previous_position != 0 {
            self.write_data_and_index()?;
        }
        let mut bundle_file = vec![];
        {
            let bundle_writer = &mut bundle_file;
            write_int(bundle_writer, GoatRodeoBundle::BundleFileMagicNumber)?;
            let bundle_env = BundleFileEnvelope {
                version: 1,
                magic: GoatRodeoBundle::BundleFileMagicNumber,

                timestamp: millis_now(),
                info: HashMap::new(),
                the_type: "Goat Rodeo Bundle".into(),
                data_files: self.seen_data_files.iter().map(|v| *v).collect(),
                index_files: self.index_files.iter().map(|v| *v).collect(),
            };
            write_envelope(bundle_writer, &bundle_env)?;
        }

        // compute sha256 of index
        let bundle_reader: &[u8] = &bundle_file;
        let grb_sha = byte_slice_to_u63(&sha256_for_slice(bundle_reader))?;

        // write the .grb file

        let grb_file_path = path_plus_timed(&self.dir, &format!("{:016x}.grb", grb_sha));
        let mut grb_file = File::create(&grb_file_path)?;
        grb_file.write_all(&bundle_file)?;
        grb_file.flush()?;

        Ok(grb_file_path)
    }

    pub fn write_data_and_index(&mut self) -> Result<()> {
        use std::io::Write;
        let mut grd_sha = 0;
        if self.previous_position != 0 {
            write_short_signed(&mut self.dest_data, -1)?; // a marker that says end of file

            // write final back-pointer (to the last entry record)
            write_long(&mut self.dest_data, self.previous_position)?;

            println!(
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

            println!(
                "computed grd sha and wrote at {:?}",
                Instant::now().duration_since(self.start)
            );
            // self.dest_data = BundleWriter::make_dest_buffer();
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
                write_int(index_writer, GoatRodeoBundle::IndexFileMagicNumber)?;
                let index_env = IndexEnvelope {
                    version: 1,
                    magic: GoatRodeoBundle::IndexFileMagicNumber,
                    the_type: "Goat Rodeo Index".into(),
                    size: self.index_info.len() as u32,
                    data_files: found_hashes.clone(),
                    encoding: "MD5/Long/Long".into(),
                    timestamp: millis_now(),
                    info: HashMap::new(),
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

            println!(
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
            println!(
                "computed gri sha and wrote index file {:?}",
                Instant::now().duration_since(self.start)
            );
            self.index_info = BundleWriter::make_index_buffer();
            self.seen_data_files.extend(found_hashes);
        }
        // self.dump_file_names();
        Ok(())
    }
    // fn dump_file_names(&self) {
    //     println!("Data");
    //     for d in &self.seen_data_files {
    //         println!("{:016x}", d);
    //     }

    //     println!("Index");
    //     for d in &self.index_files {
    //         println!("{:016x}", d);
    //     }
    // }
    pub fn add_index(&mut self, hash: MD5Hash, file_hash: u64, offset: u64) -> Result<()> {
        self.index_info.push(IndexInfo {
            hash,
            offset,
            file_hash,
        });

        if self.index_info.len() > BundleWriter::MAX_INDEX_CNT {
            self.write_data_and_index()?;
            self.write_data_envelope_start()?;
        }
        Ok(())
    }

    fn write_data_envelope_start(&mut self) -> Result<()> {
        write_int(&mut self.dest_data, GoatRodeoBundle::DataFileMagicNumber)?;

        let data_envelope = DataFileEnvelope {
            version: 1,
            magic: GoatRodeoBundle::DataFileMagicNumber,
            the_type: "Goat Rodeo Data".into(),
            previous: self.previous_hash,
            depends_on: self.seen_data_files.clone(),
            timestamp: millis_now(),
            built_from_merge: false,
            info: HashMap::new(),
        };

        write_envelope(&mut self.dest_data, &data_envelope)?;

        self.previous_position = 0;
        Ok(())
    }
}
