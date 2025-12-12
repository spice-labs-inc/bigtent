use clap::Parser;
use std::{
  net::{SocketAddr, ToSocketAddrs},
  path::PathBuf,
};

#[derive(Parser, Debug, Default, Clone, PartialEq)]
#[command(version, about, long_about = None, arg_required_else_help = true)]
pub struct Args {
  /// hostname to bind to
  #[arg(long)]
  pub host: Vec<String>,

  /// the port to bind to
  #[arg(long, short)]
  pub port: Option<u16>,

  /// if there's just a single Goat Rodeo Cluster `.grc` file to serve, use this option
  #[arg(long, short)]
  pub rodeo: Option<Vec<PathBuf>>,

  /// to merge many directories containing `.grc` files into
  /// an entirely new cluster without preserving history.
  /// Note that merging a lot of clusters will require having
  /// a lot of files open. If you get a 'Too many open files'
  /// error, please run 'ulimit -n 4096'
  #[arg(long, num_args=1..)]
  pub fresh_merge: Vec<PathBuf>,

  /// Use threads when possible
  #[arg(long)]
  pub threaded: Option<bool>,

  /// the destination for `mergenew`
  #[arg(long)]
  pub dest: Option<PathBuf>,

  /// For `rodeo` mode, should the full index be read and cached in
  /// memory or should it be
  /// accessed lazily. By default, lazy. For large clusters, the
  /// full index read/cache can take a long time and uses a lot of memory
  #[arg(long)]
  pub cache_index: Option<bool>,

  /// Applies to `fresh_merge` jobs: set a limit for the number of concurrent items in the merge
  /// queue.  Adjust this according to your system's memory limits.
  #[arg(long, short, default_value_t = 10_000)]
  pub buffer_limit: usize,
}

impl Args {
  pub fn port(&self) -> u16 {
    self.port.unwrap_or(3000)
  }

  pub fn to_socket_addrs(&self) -> Vec<SocketAddr> {
    let lh = vec!["localhost".to_string()];
    let the_port = self.port();
    let hosts = if self.host.is_empty() {
      &lh
    } else {
      &self.host
    };
    let sa: Vec<SocketAddr> = hosts
      .iter()
      .flat_map(|host_name| {
        let ma = format!("{}:{}", host_name, the_port);
        let addr = ma.to_socket_addrs().ok();
        addr
      })
      .flatten()
      .collect();
    sa
  }

  pub fn pre_cache_index(&self) -> bool {
    self.cache_index.unwrap_or(false)
  }
}
