use std::{collections::HashMap, io::Write};

use crate::{
    envelopes::{ItemEnvelope, MD5, MULTIFILE_NOOP}, index::MD5Hash, rodeo::{ DataFileEnvelope, GoatRodeoBundle}, util::{md5hash_str, millis_now, write_envelope, write_int, write_short}
};
use anyhow::{bail, Result};

pub fn merge_no_history(what: Vec<GoatRodeoBundle>) -> Result<()> {
    let mut index_holder = vec![];

    for bundle in &what {
        let index = bundle.get_index()?;
        let index_len = index.len();
        index_holder.push((index, 0usize, index_len));
    }

    let mut written: Vec<(u64, u64)> = vec![];

    let mut dest_data: Vec<u8> = vec![];
    let data_writer = &mut dest_data;

    write_int(data_writer, GoatRodeoBundle::DataFileMagicNumber)?;

    let data_envelope = DataFileEnvelope {
        version: 1,
        magic: GoatRodeoBundle::DataFileMagicNumber,
        the_type: "Goat Rodeo Data".into(),
        previous: written.last().unwrap_or(&(0u64, 0u64)).0,
        depends_on: vec![],
        timestamp: millis_now(),
        built_from_merge: false,
        info: HashMap::new(),
    };

    write_envelope(data_writer, &data_envelope)?;

    let mut dest_index: Vec<u8> = vec![];
    let mut index_writer: &mut [u8] = &mut dest_index;
    let mut index_pos: Vec<(MD5Hash, u64)> = vec![];
    let mut previous_position = 0u64;

    loop {
        let mut top = vec![];
        let mut vec_pos = 0;
        for (idx, pos, len) in &mut index_holder {
            if *pos < *len {
                top.push((idx[*pos].clone(), vec_pos));
            }
            vec_pos += 1;
        }

        // we've run out of elements
        if top.is_empty() {
            break;
        }

        top.sort_by_key(|i| i.0.hash);
        let len = top.len();
        let mut pos = 1;
        let merge_base = top[0].clone();
        let mut to_merge = vec![];
        while pos < len && top[pos].0.hash == merge_base.0.hash {
            to_merge.push(top[pos].clone());
            pos += 1;
        }

        let mut merge_to = what[merge_base.1]
            .data_for_entry_offset(&merge_base.0.loc)?
            .1;
        merge_to.remove_references();
        index_holder[merge_base.1].1 += 1;

        let mut merge_from = vec![];
        for m in to_merge {
            let mut tmp_item = what[m.1].data_for_entry_offset(&m.0.loc)?.1;
            tmp_item.remove_references();
            merge_from.push(tmp_item);

            index_holder[m.1].1 += 1;
        }

        let cur_pos = data_writer.len() as u64;

        let mut merge_final = merge_to.clone();
        for m2 in merge_from {
          merge_final = merge_final.merge(m2);
        }

        merge_final.reference.1 = cur_pos;

        let item_bytes = serde_cbor::to_vec(&merge_final)?;

        
        let the_hash = md5hash_str(&merge_final.identifier);
        let item_envelope = ItemEnvelope {
            key_md5: MD5 {
                hash: the_hash,
            },
            position:  cur_pos ,
            timestamp: millis_now(),
            previous_version: MULTIFILE_NOOP,
            backpointer: previous_position,
            data_len: item_bytes.len() as u32,
            data_format: crate::envelopes::PayloadFormat::CBOR,
            data_type: crate::envelopes::PayloadType::ENTRY,
            compression: crate::envelopes::PayloadCompression::NONE,
            merged_with_previous: false,
        };
        let item_env_bytes = serde_cbor::to_vec(&item_envelope)?;
        write_short(data_writer, item_env_bytes.len() as u16)?;
        write_int(data_writer, item_bytes.len() as u32)?;
        data_writer.write(&item_env_bytes)?;
        data_writer.write(&item_bytes)?;
        index_pos.push((the_hash, cur_pos));
        if data_writer.len() > 16_000_000_000 {
          todo!("Write data file and index");
        }
       
        previous_position = cur_pos;
    }

    todo!("Finish writing the final data file, final index, and the bundle");
    bail!("FIXME");
}
