use anyhow::{bail, Result};
use arc_swap::ArcSwap;
use flume::{Receiver, Sender};
use im::HashSet;
use pipe::PipeWriter;
use scopeguard::defer;
use serde_json::Value as SJValue;
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Seek, Write},
    sync::{atomic::AtomicU32, Arc, Mutex},
    thread::{self, JoinHandle},
    time::Instant,
};
use thousands::Separable;
use toml::{map::Map, Table, Value};

use crate::{
    config::Args,
    util::{find_entry, md5hash_str},
};

#[derive(Debug)]
pub struct Index {
    threads: Mutex<Vec<JoinHandle<()>>>,
    path_and_index: ArcSwap<(String, Vec<EntryOffset>)>,
    args: Args,
    config_table: ArcSwap<Table>,
    // oc_path: ArcSwap<String>,
    sender: Sender<IndexMsg>,
    receiver: Receiver<IndexMsg>,
}

impl Index {
    pub fn find(&self, hash: u128) -> Option<EntryOffset> {
        find_entry(hash, &self.path_and_index.load().1)
    }

    pub fn line_for_hash_from_file<R: Seek + Read>(
        &self,
        hash: u128,
        file: &mut BufReader<R>,
    ) -> Result<String> {
        let entry_offset = match self.find(hash) {
            Some(eo) => eo,
            _ => bail!(format!("Could not find entry for hash {:x}", hash)),
        };

        file.seek(std::io::SeekFrom::Start(entry_offset.loc))?;

        let mut ret = String::new();
        file.read_line(&mut ret)?;
        Ok(match ret.find("||,||") {
            Some(offset) => ret[offset + 5..].to_string(),
            _ => ret,
        })
    }

    pub fn line_for_str_from_file<R: Seek + Read>(
        &self,
        data: &str,
        file: &mut BufReader<R>,
    ) -> Result<String> {
        self.line_for_hash_from_file(md5hash_str(data), file)
    }

    pub fn line_for_hash(&self, hash: u128) -> Result<String> {
        let file = File::open(&*self.path_and_index.load().0)?;
        let mut buf = BufReader::new(file);
        self.line_for_hash_from_file(hash, &mut buf)
    }

    pub fn line_for_string(&self, data: &str) -> Result<String> {
        self.line_for_hash(md5hash_str(data))
    }

    pub fn bulk_serve(&self, data: Vec<String>, dest: PipeWriter) -> Result<()> {
        self.sender
            .send(IndexMsg::Bulk(data, dest))
            .map_err(|e| e.into())
    }

    pub fn do_north_serve(
        &self,
        data: String,
        gitoid: String,
        hash: u128,
        dest: PipeWriter,
    ) -> Result<()> {
        self.sender
            .send(IndexMsg::North {
                hash: hash,
                gitoid: gitoid,
                initial_body: data,
                tx: dest,
            })
            .map_err(|e| e.into())
    }

    pub fn shutdown(&self) -> () {
        for _ in 0..self.args.num_threads() + 10 {
            self.sender.send(IndexMsg::End).unwrap();
        }

        // destroy the old thread pool
        let mut threads = self.threads.lock().unwrap(); // self.threads.swap(Arc::new(vec![]));;

        for _ in 0..threads.len() - 1 {
            let y = threads.pop().unwrap();
            y.join().unwrap();
        }
    }

    fn contained_by(data: &str) -> Result<HashSet<String>> {
        let v: SJValue = serde_json::from_str(data)?;
        let mut ret = HashSet::new();

        if let Some(obj) = v.as_object() {
            if let Some(contained_by) = obj.get("containedBy") {
                if let Some(arr) = contained_by.as_array() {
                    for v in arr {
                        if let Some(gitoid) = v.as_str() {
                            ret.insert(gitoid.to_string());
                        }
                    }
                }
            }
        }

        Ok(ret)
    }

    fn north_send(&self, gitoid: String, initial_body: String, tx: PipeWriter) -> Result<()> {
        let start = Instant::now();
        let file = File::open(&*self.path_and_index.load().0)?;
        let mut br = BufWriter::new(tx);
        let mut buf = BufReader::new(file);
        let mut found = HashSet::new();
        let mut to_find = Index::contained_by(&initial_body)?;
        found.insert(gitoid.clone());
        br.write_fmt(format_args!("{{ \"{}\": {}\n", gitoid, initial_body))?;
        let cnt = AtomicU32::new(0);

        defer! {
          println!("Sent {} in {:?}", cnt.load(std::sync::atomic::Ordering::Relaxed).separate_with_commas(), 
          Instant::now().duration_since(start));
        }
        fn less(a: &HashSet<String>, b: &HashSet<String>) -> HashSet<String> {
            let mut ret = a.clone();
            for i in b.clone() {
                ret = ret.without(&i);
            }
            ret
        }

        loop {
            let to_search = less(&to_find, &found);

            if to_search.len() == 0 {
                break;
            }
            for this_oid in to_search {
                found = found.update(this_oid.clone());
                match self.line_for_str_from_file(&this_oid, &mut buf) {
                    Ok(item) => {
                        br.write_fmt(format_args!(",\n \"{}\": [{}]\n", this_oid, item))?;
                        let and_then = Index::contained_by(&item)?;
                        to_find = to_find.union(and_then);
                    }
                    _ => {
                        br.write_fmt(format_args!(",\n \"{}\": []\n", this_oid,))?;
                    }
                }
                cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        br.write(b"}\n")?;
        Ok(())
    }

    fn bulk_send(&self, data: Vec<String>, dest: PipeWriter) -> Result<()> {
        let file = File::open(&*self.path_and_index.load().0)?;
        let mut br = BufWriter::new(dest);
        let mut buf = BufReader::new(file);
        let mut first = true;
        br.write(b"{\n")?;
        for v in data {
            if !first {
                br.write(b",")?;
            }
            first = false;
            match self.line_for_str_from_file(&v, &mut buf) {
                Ok(json) => {
                    br.write_fmt(format_args!("\"{}\": [{}]\n", v, json))?;
                }
                Err(_) => {
                    br.write_fmt(format_args!("\"{}\": []\n", v))?;
                }
            }
        }
        br.write(b"}\n")?;

        Ok(())
    }

    fn thread_handler_loop(index: Arc<Index>) -> Result<()> {
        loop {
            match index.receiver.recv()? {
                IndexMsg::End => {
                    break;
                }
                IndexMsg::Bulk(data, dest) => {
                    let start = Instant::now();
                    let data_len = data.len();
                    match index.bulk_send(data, dest) {
                        Ok(_) => {}
                        Err(e) =>
                        // FIXME log
                        {
                            println!("Bulk failure {:?}", e);
                        }
                    }
                    println!(
                        "Bulk send of {} items took {:?}",
                        data_len,
                        Instant::now().duration_since(start)
                    );
                }
                IndexMsg::North {
                    hash: _,
                    gitoid,
                    initial_body,
                    tx,
                } => {
                    let start = Instant::now();

                    match index.north_send(gitoid.clone(), initial_body, tx) {
                        Ok(_) => {}
                        Err(e) =>
                        // FIXME log
                        {
                            println!("Bulk failure {:?}", e);
                        }
                    }
                    println!(
                        "North of {} took {:?}",
                        gitoid,
                        Instant::now().duration_since(start)
                    );
                }
            }
        }
        Ok(())
    }

    pub fn get_config_table(&self) -> Arc<Map<String, Value>> {
        self.config_table.load().clone()
    }

    fn info_from_config(
        file_name: &str,
        conf: &Table,
    ) -> Result<(String, String, Vec<EntryOffset>)> {
        let oc_path = match conf.get("oc_path") {
            Some(Value::String(index)) => shellexpand::tilde(index).to_string(),
            _ => bail!(format!(
                "Could not find 'oc_path' key in configuration file {}",
                file_name
            )),
        };

        let index_name = match conf.get("oc_index") {
            Some(Value::String(index)) => shellexpand::tilde(index).to_string(),
            _ => bail!(format!(
                "Could not find 'oc_index' key in configuration file {}",
                file_name
            )),
        };

        let oc_index = EntryOffset::build_from_index_file(&index_name)?;

        Ok((oc_path, index_name, oc_index))
    }

    pub fn rebuild(&self) -> Result<()> {
        let config_table = self.args.read_conf_file()?;
        let (oc_path, _, oc_index) =
            Index::info_from_config(&self.args.conf_file()?, &config_table)?;
        self.path_and_index.store(Arc::new((oc_path, oc_index)));
        Ok(())
    }

    pub fn the_args(&self) -> Args {
        self.args.clone()
    }

    pub fn new_arc(args: Args) -> Result<Arc<Index>> {
        let (tx, rx) = flume::unbounded();

        let config_table = args.read_conf_file()?;

        let (oc_path, _, oc_index) = Index::info_from_config(&args.conf_file()?, &config_table)?;

        let ret = Arc::new(Index {
            threads: Mutex::new(vec![]),
            config_table: ArcSwap::new(Arc::new(config_table)),
            path_and_index: ArcSwap::new(Arc::new((oc_path.clone(), oc_index))),
            args: args.clone(),
            receiver: rx.clone(),
            sender: tx.clone(),
        });

        // let mut handles = vec![];
        for _x in 0..args.num_threads() {
            let my_index = ret.clone();
            let handle = thread::spawn(move || {
                Index::thread_handler_loop(my_index).unwrap();
            });

            // put in braces to ensure the lock is retained for a very short time
            {
                // unwrap because there's no way the lock is poisoned
                let mut x = ret.threads.lock().unwrap();
                x.push(handle);
            }
        }

        Ok(ret)
    }
}
#[derive(Clone)]
enum IndexMsg {
    End,
    Bulk(Vec<String>, PipeWriter),
    North {
        #[allow(dead_code)]
        hash: u128,
        gitoid: String,
        initial_body: String,
        tx: PipeWriter,
    },
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EntryOffset {
    pub hash: u128,
    pub loc: u64,
}

impl EntryOffset {
    pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
        writer.write(&self.hash.to_be_bytes())?;
        writer.write(&self.loc.to_be_bytes())?;
        Ok(())
    }

    pub fn read<R: Read>(reader: &mut R) -> Result<EntryOffset> {
        let mut hash_bytes = u128::default().to_be_bytes();
        let mut loc_bytes = u64::default().to_be_bytes();
        let hl = reader.read(&mut hash_bytes)?;
        let ll = reader.read(&mut loc_bytes)?;
        if hl != hash_bytes.len() || ll != loc_bytes.len() {
            bail!("Failed to read enough bytes for EntryOffset")
        }
        Ok(EntryOffset {
            hash: u128::from_be_bytes(hash_bytes),
            loc: u64::from_be_bytes(loc_bytes),
        })
    }

    pub fn build_from_index_file(file_name: &str) -> Result<Vec<EntryOffset>> {
        // make sure the buffer is a multiple of 24 (the length of the u128 + u64)
        let mut reader = BufReader::with_capacity(24 * 4096, File::open(file_name)?);
        let mut stuff = vec![];

        loop {
            match EntryOffset::read(&mut reader) {
                Ok(eo) => {
                    stuff.push(eo);
                }
                Err(e) => {
                    if stuff.len() > 0 {
                        break;
                    }
                    return Err(e);
                }
            }
        }

        Ok(stuff)
    }
}
