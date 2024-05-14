use anyhow::Result;
use std::io::{Read, Seek};

use crate::index::{EntryOffset, IndexLoc};

pub struct FileReader<R: Read + Seek> {
    the_file: R,
    buffer: [u8; 4096],
    inner_pos: usize,
    current_buffer_size: isize,
    file_hash: u64
}

impl<R> FileReader<R>
where
    R: Read + Seek,
{
    pub fn new(file_hash: u64,file: R) -> FileReader<R> {
        return FileReader {
            the_file: file,
            inner_pos: 0,
            current_buffer_size: -1,
            buffer: [0u8; 4096],
            file_hash
        };
    }

    pub fn ensure_buffer(&mut self) -> Result<bool> {
        if (self.inner_pos as isize) < self.current_buffer_size {
            return Ok(false);
        }

        let num_read = self.the_file.read(&mut self.buffer)?;

        if num_read == 0 {
            return Ok(true);
        }

        self.current_buffer_size = num_read as isize;
        self.inner_pos = 0;
        Ok(false)
    }

    fn read_char(&mut self) -> Result<(u8, bool)> {
        if (self.inner_pos as isize) >= self.current_buffer_size {
            let end = self.ensure_buffer()?;
            if end {
                return Ok((0u8, true));
            }
        }

        let ret = self.buffer[self.inner_pos];
        self.inner_pos += 1;
        return Ok((ret, false));
    }

    pub fn next_hash(&mut self) -> Result<(Option<EntryOffset>, bool)> {
        let end = self.ensure_buffer()?;

        if end {
            return Ok((None, true));
        }

        let mut hex = vec![];

        let file_pos = self.the_file.seek(std::io::SeekFrom::Current(0))?;

        let the_pos = file_pos - (self.current_buffer_size as u64 - self.inner_pos as u64);
        let mut found_comma = false;

        loop {
            let (next_char, the_end) = self.read_char()?;
            if the_end {
                return Ok((None, true));
            }

            if next_char == b'\n' {
                let hash = build_hash_offset(String::from_utf8(hex)?, the_pos, self.file_hash)?;
                return Ok((Some(hash), false));
            }

            if next_char == b',' {
                found_comma = true;
            }

            if !found_comma {
                hex.push(next_char);
            }
        }
    }

    pub fn build_index(&mut self) -> Result<Vec<EntryOffset>> {
        let mut ret = vec![];
        let mut cnt = 0;
        loop {
            let (eoo, end) = self.next_hash()?;
            match eoo {
                Some(eo) => {
                    ret.push(eo);
                }
                _ => {}
            };

            if end {
                break;
            }
            cnt += 1;
            if cnt % 100_000 == 0 {
              use thousands::Separable;
                println!("Loops {}", cnt.separate_with_commas());
            }
        }

        Ok(ret)
    }
}

fn build_hash_offset(hash: String, pos: u64, file: u64) -> Result<EntryOffset> {
    let real_hash = match hash.find(",") {
        Some(x) => hash[..x].to_string(),
        _ => hash,
    };

    let hash_bin = u128::from_str_radix(&real_hash, 16)?.to_be_bytes();
    Ok(EntryOffset {
        hash: hash_bin,
        loc: IndexLoc::Loc { offset: pos, file_hash: file },
    })
}
