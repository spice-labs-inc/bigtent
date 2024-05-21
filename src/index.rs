use anyhow::{bail, Result};
use arc_swap::ArcSwap;
use flume::{Receiver, Sender};
use pipe::PipeWriter;
use scopeguard::defer;
use std::{
    collections::HashSet,
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf,
    sync::{atomic::AtomicU32, Arc, Mutex},
    thread::{self, JoinHandle},
    time::Instant,
};
use thousands::Separable;
use toml::{map::Map, Table};
// use num_traits::ops::bytes::ToBytes;
use crate::{
    config::Args,
    envelopes::EntryEnvelope,
    rodeo::GoatRodeoBundle,
    structs::{EdgeType, Item},
    util::{cbor_to_json_str, find_entry, md5hash_str},
};

pub type MD5Hash = [u8; 16];

#[derive(Debug)]
pub struct Index {
    threads: Mutex<Vec<JoinHandle<()>>>,
    path_and_index: ArcSwap<GoatRodeoBundle>,

    args: Args,
    config_table: ArcSwap<Table>,
    // oc_path: ArcSwap<String>,
    sender: Sender<IndexMsg>,
    receiver: Receiver<IndexMsg>,
}

impl Index {
    pub fn find(&self, hash: MD5Hash) -> Result<Option<EntryOffset>> {
        let index = self.path_and_index.load().get_index()?;
        Ok(find_entry(hash, &index))
    }

    pub fn entry_for(&self, file_hash: u64, offset: u64) -> Result<(EntryEnvelope, Item)> {
        let data_files = &self.path_and_index.load().data_files;
        let data_file = data_files.get(&file_hash);
        match data_file {
            Some(df) => {
              let (env, mut item) = df.read_envelope_and_item_at(offset)?;
              match item.reference.0 {
                0 => {item.reference.0 = file_hash}
                v if v != file_hash => {bail!("Got item {} that should have had a file_hash of {:016x}, but had {:016x}", item.identifier, file_hash, item.reference.0,)}
                _ => {}
              }

              if item.reference.1 != offset {
                bail!("Expecting item {} to have offset {}, but reported offset {}", item.identifier, offset, item.reference.1)
              }

              Ok((env, item))
            },
            None => bail!("Couldn't find file for hash {:x}", file_hash),
        }
    }

    pub fn data_for_entry_offset(
        &self,
        index_loc: &IndexLoc,
    ) -> Result<(EntryEnvelope, Item)> {
        match index_loc {
            IndexLoc::Loc { offset, file_hash } => Ok(self.entry_for(*file_hash, *offset)?),
            IndexLoc::Chain(offsets) => {
                let mut ret = vec![];
                for offset in offsets {
                    let some = self.data_for_entry_offset(&offset)?;
                    ret.push(some);
                }
                if ret.len() == 0 {
                  bail!("Got a location chain with zero entries!!");
                } else if ret.len() == 1 {
                  Ok(ret.pop().unwrap()) // we know this is okay because we just tested length
                } else {

                  let (env, base) = ret.pop().unwrap(); // we know this is okay because ret has more than 1 element
                  let mut to_merge = vec![];
                  for (_, item) in ret {
                    to_merge.push(item);
                  }
                  let final_base = base.merge(to_merge);
                  Ok((env, final_base))
                }
            }
        }
    }

    pub fn data_for_hash(&self, hash: MD5Hash) -> Result<(EntryEnvelope, Item)> {
        let entry_offset = match self.find(hash)? {
            Some(eo) => eo,
            _ => bail!(format!("Could not find entry for hash {:x?}", hash)),
        };

        self.data_for_entry_offset(&entry_offset.loc)
    }


    pub fn data_for_key(&self, data: &str) -> Result<Item> {
        let md5_hash = md5hash_str(data);
        self.data_for_hash(md5_hash).map(|v| v.1)
    }


    pub fn bulk_serve(&self, data: Vec<String>, dest: PipeWriter) -> Result<()> {
        self.sender
            .send(IndexMsg::Bulk(data, dest))
            .map_err(|e| e.into())
    }

    pub fn do_north_serve(
        &self,
        data: Item,
        gitoid: String,
        hash: MD5Hash,
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

    fn contained_by(data: &Item) -> HashSet<String> {
        let mut ret = HashSet::new();
        for edge in data.connections.iter() {
            if edge.1 == EdgeType::ContainedBy {
                ret.insert(edge.0.clone());
            }
        }

        ret
    }

    fn north_send(&self, gitoid: String, initial_body: Item, tx: PipeWriter) -> Result<()> {
        let start = Instant::now();
        let mut br = BufWriter::new(tx);
        let mut found = HashSet::new();
        let mut to_find = Index::contained_by(&initial_body);
        found.insert(gitoid.clone());
        br.write_fmt(format_args!("{{ \"{}\": {:?}\n", gitoid, initial_body))?;
        let cnt = AtomicU32::new(0);

        defer! {
          println!("Sent {} in {:?}", cnt.load(std::sync::atomic::Ordering::Relaxed).separate_with_commas(),
          Instant::now().duration_since(start));
        }
        fn less(a: &HashSet<String>, b: &HashSet<String>) -> HashSet<String> {
            let mut ret = a.clone();
            for i in b.clone() {
                ret.remove(&i);
            }
            ret
        }

        loop {
            let to_search = less(&to_find, &found);

            if to_search.len() == 0 {
                break;
            }
            for this_oid in to_search {
                found.insert(this_oid.clone());
                match self.data_for_key(&this_oid) {
                    Ok(item) => {
                        br.write_fmt(format_args!(
                            ",\n \"{}\": [{}]\n",
                            this_oid,
                            cbor_to_json_str(&item)?
                        ))?;
                        let and_then = Index::contained_by(&item);
                        to_find = to_find.union(&and_then).map(|s| s.clone()).collect();
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
        let mut br = BufWriter::new(dest);
        let mut first = true;
        br.write(b"{\n")?;
        for v in data {
            if !first {
                br.write(b",")?;
            }
            first = false;
            match self.data_for_key(&v) {
                Ok(cbor) => {
                    br.write_fmt(format_args!("\"{}\": [{}]\n", v, cbor_to_json_str(&cbor)?))?;
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

    pub fn get_config_table(&self) -> Arc<Map<String, toml::Value>> {
        self.config_table.load().clone()
    }

    fn info_from_config(file_name: &str, conf: &Table) -> Result<GoatRodeoBundle> {
        let index_name = match conf.get("bundle_path") {
            Some(toml::Value::String(index)) => shellexpand::tilde(index).to_string(),
            _ => bail!(format!(
                "Could not find 'bundle_path' key in configuration file {}",
                file_name
            )),
        };

        let envelope_path = PathBuf::from(index_name.clone());
        let envelope_dir = match envelope_path.parent() {
            Some(path) => path.to_path_buf(),
            None => bail!(
                "Path Bundle '{}' does not have a parent directory",
                index_name
            ),
        };

        let bundle = GoatRodeoBundle::new(&envelope_dir, &envelope_path);

        bundle
    }

    pub fn rebuild(&self) -> Result<()> {
        let config_table = self.args.read_conf_file()?;
        let new_bundle = Index::info_from_config(&self.args.conf_file()?, &config_table)?;
        self.path_and_index.store(Arc::new(new_bundle));
        Ok(())
    }

    pub fn the_args(&self) -> Args {
        self.args.clone()
    }

    /// Create a new Index based on an existing GoatRodeo instance with the number of threads
    /// and an optional set of args. This is meant to be used to create an Index without
    /// a well defined set of Args
    pub fn new(bundle: GoatRodeoBundle, num_threads: u16, args_opt: Option<Args>) -> Arc<Index> {
        let (tx, rx) = flume::unbounded();

        let args = args_opt.unwrap_or_default();
        let config_table = args.read_conf_file().unwrap_or_default();

        let ret = Arc::new(Index {
            threads: Mutex::new(vec![]),
            config_table: ArcSwap::new(Arc::new(config_table)),
            path_and_index: ArcSwap::new(Arc::new(bundle)),
            args: args,
            receiver: rx.clone(),
            sender: tx.clone(),
        });

        // let mut handles = vec![];
        for _x in 0..num_threads {
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

        ret
    }

    pub fn new_arc(args: Args) -> Result<Arc<Index>> {
        let (tx, rx) = flume::unbounded();

        let config_table = args.read_conf_file()?;

        let bundle = Index::info_from_config(&args.conf_file()?, &config_table)?;

        let ret = Arc::new(Index {
            threads: Mutex::new(vec![]),
            config_table: ArcSwap::new(Arc::new(config_table)),
            path_and_index: ArcSwap::new(Arc::new(bundle)),
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
        hash: MD5Hash,
        gitoid: String,
        initial_body: Item,
        tx: PipeWriter,
    },
}

#[repr(u8)]
#[derive(Debug, Clone, PartialEq)]
pub enum IndexLoc {
    Loc { offset: u64, file_hash: u64 },
    Chain(Vec<IndexLoc>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntryOffset {
    pub hash: [u8; 16],
    pub loc: IndexLoc,
}

impl EntryOffset {
    // pub fn write<W: Write>(&self, writer: &mut W) -> Result<()> {
    //     writer.write(&self.hash)?;
    //     writer.write(&self.loc.to_be_bytes())?;
    //     Ok(())
    // }

    pub fn read<R: Read>(reader: &mut R) -> Result<EntryOffset> {
        let mut hash_bytes = u128::default().to_be_bytes();
        let mut file_bytes = u64::default().to_be_bytes();
        let mut loc_bytes = u64::default().to_be_bytes();
        let hl = reader.read(&mut hash_bytes)?;
        let fl = reader.read(&mut file_bytes)?;
        let ll = reader.read(&mut loc_bytes)?;
        if hl != hash_bytes.len() || ll != loc_bytes.len() || fl != file_bytes.len() {
            bail!("Failed to read enough bytes for EntryOffset")
        }
        Ok(EntryOffset {
            hash: hash_bytes,
            loc: IndexLoc::Loc {
                offset: u64::from_be_bytes(loc_bytes),
                file_hash: u64::from_be_bytes(file_bytes),
            },
        })
    }

    pub fn build_from_index_file(file_name: &str) -> Result<Vec<EntryOffset>> {
        // make sure the buffer is a multiple of 24 (the length of the u128 + u64 + u64)
        let mut reader = BufReader::with_capacity(32 * 4096, File::open(file_name)?);
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
