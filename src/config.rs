//! # CLI Configuration
//!
//! This module defines the command-line interface configuration for BigTent
//! using the `clap` crate for argument parsing.
//!
//! ## Configuration Options
//!
//! BigTent supports two primary modes of operation, configured via CLI arguments:
//!
//! ### Server Mode (`--rodeo`)
//! - `--rodeo <paths>` - Path(s) to `.grc` cluster files or directories
//! - `--host <hostname>` - Hostname(s) to bind to (default: localhost)
//! - `--port <port>` - Port to bind to (default: 3000)
//! - `--cache-index` - Pre-load index into memory (uses more RAM but faster queries)
//!
//! ### Merge Mode (`--fresh-merge`)
//! - `--fresh-merge <paths>...` - Directories containing clusters to merge
//! - `--dest <path>` - Output directory for merged cluster
//! - `--buffer-limit <n>` - Max items in merge queue (default: 10,000)
//!
//! ### Lookup Mode (`--rodeo` + `--lookup`)
//! - `--rodeo <paths>` - Path(s) to `.grc` cluster files or directories
//! - `--lookup <path>` - JSON file containing array of identifiers to look up
//! - `--output <path>` - Output file for results (default: stdout)
//! - `--cache-index` - Pre-load index into memory (uses more RAM but faster queries)
//!
//! ## Performance Tuning
//!
//! - **Memory vs Speed**: Use `--cache-index` for faster queries at the cost of
//!   higher memory usage during startup.
//! - **Merge Performance**: Adjust `--buffer-limit` based on available memory.
//!   Higher values allow more parallel processing but use more RAM.
//!
//! ## Environment Variables
//!
//! - `RUST_LOG` - Controls log verbosity (e.g., `RUST_LOG=info`)

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

    /// Path to a JSON file containing an array of identifier strings to look up
    /// in the loaded cluster(s).
    ///
    /// Requires `--rodeo` to specify which cluster(s) to load. When provided,
    /// Big Tent performs a batch lookup instead of starting the web server.
    ///
    /// The JSON file must contain an array of strings, e.g.:
    /// ```json
    /// ["gitoid:blob:sha256:abc123...", "pkg:npm/lodash@4.17.21"]
    /// ```
    ///
    /// Results are written as a JSON object mapping each identifier to its
    /// [`Item`](crate::item::Item) (as JSON) or `null` if not found:
    /// ```json
    /// {"gitoid:blob:sha256:abc123...": {...}, "pkg:npm/lodash@4.17.21": null}
    /// ```
    ///
    /// Output goes to stdout by default, or to a file specified by `--output`.
    #[arg(long, short)]
    pub lookup: Option<PathBuf>,

    /// Output file path for `--lookup` results.
    ///
    /// When specified, the lookup results JSON is written to this file
    /// instead of stdout. The file is created if it doesn't exist,
    /// or overwritten if it does.
    #[arg(long, short)]
    pub output: Option<PathBuf>,
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
