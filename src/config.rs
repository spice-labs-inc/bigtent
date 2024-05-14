use anyhow::{bail, Result};
use clap::Parser;
use std::{
    fs::File,
    io::Read,
    net::{SocketAddr, ToSocketAddrs},
};
use toml::Table;

#[derive(Parser, Debug, Default, Clone, PartialEq)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Location of the configuration TOML file
    #[arg(short, long)]
    conf: Option<String>,

    /// Number of threads to allow for servicing.
    /// A small number if on spinning disks, larger
    /// for SSD
    #[arg(short, long)]
    threads: Option<u16>,

    /// hostname to bind to
    #[arg(long)]
    host: Vec<String>,

    /// the port to bind to
    #[arg(long, short)]
    port: Option<u16>,
}

impl Args {
    pub fn read_conf_file(&self) -> Result<Table> {
        let mut conf_file = File::open(self.conf_file()?)?;
        let mut contents = String::new();
        conf_file.read_to_string(&mut contents)?;
        contents.parse::<Table>().map_err(|e| e.into())
    }

    pub fn num_threads(&self) -> u16 {
        self.threads.unwrap_or(7)
    }

    pub fn conf_file(&self) -> Result<String> {
        match &self.conf {
            Some(n) => Ok(shellexpand::tilde(n).to_string()),
            None => bail!("Configuration file not specified with `--conf` options"),
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
