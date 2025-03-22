use anyhow::{bail, Result};
use clap::Parser;
use std::{
  net::{SocketAddr, ToSocketAddrs},
  path::{Path, PathBuf},
};
use tokio::fs::File;
use toml::Table;

#[derive(Parser, Debug, Default, Clone, PartialEq)]
#[command(version, about, long_about = None, arg_required_else_help = true)]
pub struct Args {
  /// Location of the configuration TOML file
  #[arg(short, long)]
  pub conf: Option<PathBuf>,

  /// hostname to bind to
  #[arg(long)]
  pub host: Vec<String>,

  /// the port to bind to
  #[arg(long, short)]
  pub port: Option<u16>,

  /// if there's just a single Goat Rodeo Cluster `.grc` file to serve, use this option
  #[arg(long, short)]
  pub rodeo: Option<PathBuf>,

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
}

impl Args {
  pub async fn read_conf_file(&self) -> Result<Table> {
    use tokio::io::AsyncReadExt;
    let mut conf_file = File::open(match &self.conf {
      Some(path) if path.exists() && path.is_file() => path,
      _ => {
        bail!("A configuration file must be supplied")
      }
    })
    .await?;
    let mut contents = String::new();
    conf_file.read_to_string(&mut contents).await?;
    match contents.parse::<Table>() {
      Ok(v) => Ok(v),
      Err(e) => bail!("Must provide a valid toml file: {:?}", e),
    }
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
