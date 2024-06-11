use anyhow::{bail, Result};
use clap::Parser;
use std::{
  fs::File,
  io::Read,
  net::{SocketAddr, ToSocketAddrs},
  path::{Path, PathBuf},
};
use toml::Table;

#[derive(Parser, Debug, Default, Clone, PartialEq)]
#[command(version, about, long_about = None, arg_required_else_help = true)]
pub struct Args {
  /// Location of the configuration TOML file
  #[arg(short, long)]
  pub conf: Option<PathBuf>,

  /// Number of threads to allow for servicing.
  /// A small number if on spinning disks, larger
  /// for SSD
  #[arg(short, long)]
  pub threads: Option<u16>,

  /// hostname to bind to
  #[arg(long)]
  pub host: Vec<String>,

  /// the port to bind to
  #[arg(long, short)]
  pub port: Option<u16>,

  /// if there's just a single Goat Rodeo Bundle `.grb` file to serve, use this option
  #[arg(long, short)]
  pub rodeo: Option<PathBuf>,

  /// to merge many directories containing `.grb` files into
  /// an entirely new bundle without preserving history
  #[arg(long, num_args=1..)]
  pub fresh_merge: Vec<PathBuf>,

  /// Use threads when possible
  #[arg(long, short)]
  pub threaded: Option<bool>,

  /// the destination for `mergenew`
  #[arg(long)]
  pub dest: Option<PathBuf>,
}

impl Args {
  pub fn read_conf_file(&self) -> Result<Table> {
    let mut conf_file = File::open(match &self.conf {
      Some(path) if path.exists() && path.is_file() => path,
      _ => {
        bail!("A configuration file must be supplied")
      }
    })?;
    let mut contents = String::new();
    conf_file.read_to_string(&mut contents)?;
    match contents.parse::<Table>() {
      Ok(v) => Ok(v),
      Err(e) => bail!("Must provide a valid toml file: {:?}", e),
    }
  }

  pub fn num_threads(&self) -> u16 {
    self.threads.unwrap_or(7)
  }

  pub fn conf_file(&self) -> Result<PathBuf> {
    match &self.conf {
      Some(n) if n.exists() && n.is_file() => Ok(
        Path::new(
          &shellexpand::tilde(match n.to_str() {
            Some(s) => s,
            None => bail!("Couldn't convert {:?} into a string", n),
          })
          .to_string(),
        )
        .to_path_buf(),
      ),
      _ => bail!("Configuration file not specified with `--conf` options"),
    }
  }

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
}
