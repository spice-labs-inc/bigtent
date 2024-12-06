use anyhow::Result;
use arc_swap::ArcSwap;
use flume::{Receiver, Sender};
#[cfg(not(test))]
use log::{error, info};
use pipe::PipeWriter;
use scopeguard::defer;
use std::{
  collections::HashSet,
  io::{BufWriter, Write},
  sync::{atomic::AtomicU32, Arc, Mutex},
  thread::{self, JoinHandle},
  time::Instant,
}; // Use log crate when building application

#[cfg(test)]
use std::{println as error, println as info};
use thousands::Separable;
use toml::{map::Map, Table};
// use num_traits::ops::bytes::ToBytes;
use crate::{
  config::Args,
  index_file::{IndexLoc, ItemOffset},
  rodeo::GoatRodeoCluster,
  structs::{EdgeType, Item},
  util::cbor_to_json_str,
};

pub type MD5Hash = [u8; 16];

#[derive(Debug)]
pub struct RodeoServer {
  threads: Mutex<Vec<JoinHandle<()>>>,
  cluster: Arc<ArcSwap<GoatRodeoCluster>>,
  args: Args,
  config_table: ArcSwap<Table>,
  sender: Sender<IndexMsg>,
  receiver: Receiver<IndexMsg>,
}

impl RodeoServer {
  /// get the cluster arc swap so that the cluster can be substituted
  pub fn get_cluster_arcswap(&self) -> Arc<ArcSwap<GoatRodeoCluster>> {
    self.cluster.clone()
  }

  /// Get the current GoatRodeoCluster from the the server
  pub fn get_cluster(&self) -> GoatRodeoCluster {
    let the_cluster: GoatRodeoCluster = (&(**self.get_cluster_arcswap().load())).clone();
    the_cluster
  }

  pub fn find(&self, hash: MD5Hash) -> Result<Option<ItemOffset>> {
    self.cluster.load().find(hash)
  }

  pub fn entry_for(&self, file_hash: u64, offset: u64) -> Result<Item> {
    self.cluster.load().entry_for(file_hash, offset)
  }

  pub fn data_for_entry_offset(&self, index_loc: &IndexLoc) -> Result<Item> {
    self.cluster.load().data_for_entry_offset(index_loc)
  }

  pub fn data_for_hash(&self, hash: MD5Hash) -> Result<Item> {
    self.cluster.load().data_for_hash(hash)
  }

  pub fn data_for_key(&self, data: &str) -> Result<Item> {
    self.cluster.load().data_for_key(data)
  }

  pub fn antialias_for(&self, data: &str) -> Result<Item> {
    self.cluster.load().antialias_for(data)
  }

  pub fn bulk_serve(&self, data: Vec<String>, dest: PipeWriter, start: Instant) -> Result<()> {
    self
      .sender
      .send(IndexMsg::Bulk(data, dest, start))
      .map_err(|e| e.into())
  }

  pub fn do_north_serve(
    &self,
    gitoids: Vec<String>,
    purls_only: bool,
    dest: PipeWriter,
    start: Instant,
  ) -> Result<()> {
    self
      .sender
      .send(IndexMsg::North {
        gitoids,
        purls_only,
        tx: dest,
        start,
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
      if edge.0 == EdgeType::ContainedBy
        || edge.0 == EdgeType::AliasTo
        || edge.0 == EdgeType::BuildsTo
      {
        ret.insert(edge.1.clone());

      }
    }

    ret
  }

  fn north_send(
    &self,
    gitoids: Vec<String>,
    purls_only: bool,
    tx: PipeWriter,
    start: Instant,
  ) -> Result<()> {
    let mut br = BufWriter::new(tx);
    let mut found = HashSet::new();
    let mut to_find = HashSet::new();
    let mut found_purls = HashSet::<String>::new();
    to_find.extend(gitoids);
    let mut first = true;

    br.write_all(if purls_only { b"[" } else { b"{" })?;

    let cnt = AtomicU32::new(0);

    defer! {
      info!("North: Sent {} items in {:?}", cnt.load(std::sync::atomic::Ordering::Relaxed).separate_with_commas(),
      start.elapsed());
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
            if purls_only {
              for purl in item.find_purls() {
                if !found_purls.contains(&purl) {
                  let json_str = serde_json::to_string(&purl)?;
                  br.write_fmt(format_args!(
                    "{}{}",
                    if !first { ",\n" } else { "" },
                    json_str
                  ))?;
                  found_purls.insert(purl);
                  first = false;
                }
              }
            } else {
              br.write_fmt(format_args!(
                "{}\n \"{}\": {}\n",
                if !first { "," } else { "" },
                this_oid,
                cbor_to_json_str(&item)?
              ))?;
              first = false;
            }
            let and_then = RodeoServer::contained_by(&item);
            to_find = to_find.union(&and_then).map(|s| s.clone()).collect();
          }
          _ => {
            // don't write "not found" items if we're just writing pURLs
            if !purls_only {
              br.write_fmt(format_args!(
                "{}\n \"{}\": null\n",
                if !first { "," } else { "" },
                this_oid,
              ))?;
            }
          }
        }
        cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
      }
    }

    br.write_all(if purls_only { b"]" } else { b"}\n" })?;
    Ok(())
  }

  fn bulk_send(&self, data: Vec<String>, dest: PipeWriter) -> Result<()> {
    let mut br = BufWriter::new(dest);
    let mut first = true;
    br.write_all(b"{\n")?;
    for v in data {
      if !first {
        br.write_all(b",")?;
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
    br.write_all(b"}\n")?;

    Ok(())
  }

  fn thread_handler_loop(index: Arc<RodeoServer>) -> Result<()> {
    loop {
      match index.receiver.recv()? {
        IndexMsg::End => {
          break;
        }
        IndexMsg::Bulk(data, dest, start) => {
          let data_len = data.len();
          match index.bulk_send(data, dest) {
            Ok(_) => {}
            Err(e) => {
              error!("Bulk failure {:?}", e);
            }
          }
          info!("Bulk send of {} items took {:?}", data_len, start.elapsed());
        }
        IndexMsg::North {
          gitoids,
          tx,
          start,
          purls_only,
        } => {
          match index.north_send(gitoids.clone(), purls_only, tx, start) {
            Ok(_) => {}
            Err(e) => {
              error!("Bulk failure {:?}", e);
            }
          }
          info!("North of {:?} took {:?}", gitoids, start.elapsed());
        }
      }
    }
    Ok(())
  }

  pub fn get_config_table(&self) -> Arc<Map<String, toml::Value>> {
    self.config_table.load().clone()
  }

  pub fn rebuild(&self) -> Result<()> {
    let config_table = self.args.read_conf_file()?;
    let new_cluster = GoatRodeoCluster::cluster_from_config(self.args.conf_file()?, &config_table)?;
    self.cluster.store(Arc::new(new_cluster));
    Ok(())
  }

  pub fn the_args(&self) -> Args {
    self.args.clone()
  }

  /// Create a new Index based on an existing GoatRodeo instance with the number of threads
  /// and an optional set of args. This is meant to be used to create an Index without
  /// a well defined set of Args
  pub fn new_from_cluster(
    cluster: Arc<ArcSwap<GoatRodeoCluster>>,
    num_threads: u16,
    args_opt: Option<Args>,
  ) -> Result<Arc<RodeoServer>> {
    let (tx, rx) = flume::unbounded();

    // do this in a block so the index is released at the end of the block
    if false {
      // dunno if this is a good idea...
      info!("Loading full index...");
      cluster.load().get_index()?;
      info!("Loaded index")
    }

    let args = args_opt.unwrap_or_default();
    let config_table = args.read_conf_file().unwrap_or_default();

    let ret = Arc::new(RodeoServer {
      threads: Mutex::new(vec![]),
      config_table: ArcSwap::new(Arc::new(config_table)),
      cluster: cluster,
      args: args,
      receiver: rx.clone(),
      sender: tx.clone(),
    });

    // let mut handles = vec![];
    for _x in 0..num_threads {
      let my_rodeo_server = ret.clone();
      let handle = thread::spawn(move || {
        RodeoServer::thread_handler_loop(my_rodeo_server).unwrap();
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

  pub fn new(args: Args) -> Result<Arc<RodeoServer>> {
    let config_table = args.read_conf_file()?;

    let cluster = Arc::new(ArcSwap::new(Arc::new(GoatRodeoCluster::cluster_from_config(args.conf_file()?, &config_table)?)));

    RodeoServer::new_from_cluster(cluster, args.num_threads(), Some(args))
  }
}
#[derive(Clone)]
enum IndexMsg {
  End,
  Bulk(Vec<String>, PipeWriter, Instant),
  North {
    #[allow(dead_code)]
    gitoids: Vec<String>,
    purls_only: bool,
    tx: PipeWriter,
    start: Instant,
  },
}
