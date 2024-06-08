use std::{collections::{HashMap, HashSet}, fs::File, io::{BufReader, Seek}, path::PathBuf, sync::{Arc, Mutex}};
use anyhow::{bail, Result, anyhow};
use serde::{Deserialize, Serialize};
use crate::{envelopes::ItemEnvelope, rodeo::GoatRodeoBundle, structs::Item, util::{read_cbor, read_len_and_cbor, read_u16, read_u32}};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct DataFileEnvelope {
    pub version: u32,
    pub magic: u32,
    pub the_type: String,
    pub previous: u64,
    pub depends_on: HashSet<u64>,
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
