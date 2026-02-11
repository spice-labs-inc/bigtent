//! # BigTent CLI Entry Point
//!
//! This module contains the main entry point for the BigTent binary.
//!
//! ## Execution Modes
//!
//! The binary supports three mutually exclusive modes:
//!
//! ### Rodeo Mode (`--rodeo <paths>`)
//! Loads cluster files from the specified paths and starts an HTTP server
//! to serve queries against the loaded data.
//!
//! ```bash
//! bigtent --rodeo /path/to/clusters --port 3000
//! ```
//!
//! ### Fresh Merge Mode (`--fresh-merge <paths>... --dest <output>`)
//! Merges multiple clusters from different directories into a single new cluster.
//! This is a batch operation that produces new cluster files without preserving
//! merge history.
//!
//! ```bash
//! bigtent --fresh-merge /path/to/cluster1 /path/to/cluster2 --dest /output
//! ```
//!
//! ### Lookup Mode (`--rodeo <paths>` + `--lookup <file>`)
//! Loads cluster files and performs a batch identifier lookup without starting
//! a server. Results are written to stdout or to a file specified by `--output`.
//!
//! ```bash
//! bigtent --rodeo /path/to/clusters --lookup identifiers.json
//! bigtent --rodeo /path/to/clusters --lookup identifiers.json --output results.json
//! ```
//!
//! ## Threading
//!
//! The binary runs on a Tokio multi-threaded runtime with 100 worker threads
//! to handle concurrent requests and I/O operations efficiently.

use anyhow::{Result, bail};
use arc_swap::ArcSwap;
use bigtent::{
    config::Args,
    fresh_merge::merge_fresh,
    rodeo::{
        goat::GoatRodeoCluster,
        goat_herd::GoatHerd,
        goat_trait::GoatRodeoTrait,
        holder::ClusterHolder,
        member::{HerdMember, member_core},
        robo_goat::ClusterRoboMember,
    },
    server::run_web_server,
};
use clap::{CommandFactory, Parser};
#[cfg(not(test))]
use log::info; // Use log crate when building application

#[cfg(test)]
use std::println as info;
use std::{io::Write, path::PathBuf, sync::Arc, time::Instant};

async fn run_rodeo(path_vec: &Vec<PathBuf>, args: &Args) -> Result<()> {
    let mut clusters: Vec<Arc<HerdMember>> = vec![];
    for path in path_vec.iter() {
        if path.exists() {
            for b in
                GoatRodeoCluster::cluster_files_in_dir(path.clone(), args.pre_cache_index(), vec![])
                    .await?
            {
                info!("Loaded cluster {}", b.name());
                clusters.push(member_core(b));
            }
        } else {
            bail!("Path to `.grc` does not point to a file: {:?}", path)
        }
    }

    let herd = GoatHerd::new(clusters);
    let cluster_holder =
        ClusterHolder::new_from_cluster(ArcSwap::new(Arc::new(herd)), Some(Arc::new(args.clone())))
            .await?;

    info!("Build cluster... about to run it",);

    run_web_server(cluster_holder).await?;
    Ok(())
}

async fn run_merge(paths: Vec<PathBuf>, args: Args) -> Result<()> {
    for p in &paths {
        if !p.exists() || !p.is_dir() {
            bail!("Paths must be directories. {:?} is not", p);
        }
    }
    let dest = match &args.dest {
        Some(d) => d.clone(),
        None => bail!("A `--dest` must be supplied"),
    };

    let start = Instant::now();

    info!("Loading clusters...");
    let mut clusters: Vec<Arc<HerdMember>> = vec![];
    for p in &paths {
        for b in GoatRodeoCluster::cluster_files_in_dir(p.clone(), false, vec![]).await? {
            clusters.push(member_core(b));
        }
    }
    info!(
        "Finished loading {} clusters at {:?}",
        clusters.len(),
        Instant::now().duration_since(start)
    );
    if clusters.len() < 2 {
        bail!(
            "There must be at least 2 clusters to merge... only got {}",
            clusters.len()
        );
    }

    let ret = merge_fresh(clusters, args.buffer_limit, dest).await;
    info!(
        "Finished merging at {:?}",
        Instant::now().duration_since(start)
    );
    ret
}

/// Perform a batch identifier lookup against loaded cluster(s).
///
/// This function implements the "lookup mode" of Big Tent. It:
///
/// 1. Reads and parses a JSON file containing an array of identifier strings
/// 2. Loads cluster(s) from the paths specified by `--rodeo`
/// 3. Looks up each identifier using [`GoatRodeoTrait::item_for_identifier`]
/// 4. Outputs a JSON object mapping each identifier to its
///    [`Item`](bigtent::item::Item) (serialized via [`Item::to_json`]) or `null`
///    if the identifier was not found
///
/// Output is written to stdout by default, or to a file if `--output` is specified.
///
/// # Arguments
///
/// * `path_vec` - Cluster directory paths from the `--rodeo` CLI argument
/// * `args` - Parsed CLI arguments (must include `lookup`; optionally `output`)
///
/// # Errors
///
/// Returns an error if:
/// - The lookup file does not exist or is not a file
/// - The lookup file cannot be parsed as a JSON array of strings
/// - A cluster path does not exist
/// - The output file cannot be written
///
/// # Example CLI Usage
///
/// ```bash
/// bigtent -r /path/to/clusters --lookup identifiers.json
/// bigtent -r /path/to/clusters --lookup identifiers.json --output results.json
/// ```
async fn run_lookup(path_vec: &Vec<PathBuf>, args: &Args) -> Result<()> {
    // Extract and validate the lookup file path
    let lookup_path = match &args.lookup {
        Some(p) => p.clone(),
        None => bail!("A `--lookup` path must be supplied"),
    };

    if !lookup_path.exists() || !lookup_path.is_file() {
        bail!(
            "Lookup file does not exist or is not a file: {:?}",
            lookup_path
        );
    }

    // Read the JSON file and parse it as an array of identifier strings
    let lookup_content = std::fs::read_to_string(&lookup_path)?;
    let identifiers: Vec<String> = serde_json::from_str(&lookup_content).map_err(|e| {
        anyhow::anyhow!(
            "Failed to parse lookup file as JSON array of strings: {}",
            e
        )
    })?;

    info!("Lookup: {} identifiers to resolve", identifiers.len());

    // Load clusters using the same pattern as run_rodeo
    let mut clusters: Vec<Arc<HerdMember>> = vec![];
    for path in path_vec.iter() {
        if path.exists() {
            for b in
                GoatRodeoCluster::cluster_files_in_dir(path.clone(), args.pre_cache_index(), vec![])
                    .await?
            {
                info!("Loaded cluster {}", b.name());
                clusters.push(member_core(b));
            }
        } else {
            bail!("Path to `.grc` does not point to a file: {:?}", path)
        }
    }
    let herd = GoatHerd::new(clusters);

    // Look up each identifier and build the result map.
    // Uses serde_json::Map to preserve insertion order (matches input order).
    let mut result_map = serde_json::Map::new();
    for identifier in &identifiers {
        let value = match herd.item_for_identifier(identifier) {
            Some(item) => item.to_json(),    // Serialize the found Item to JSON
            None => serde_json::Value::Null, // Not found → null
        };
        result_map.insert(identifier.clone(), value);
    }
    let result_json = serde_json::Value::Object(result_map);

    // Write the result JSON to the output destination
    let output_string = serde_json::to_string_pretty(&result_json)?;
    match &args.output {
        Some(output_path) => {
            // Write to the specified output file
            std::fs::write(output_path, &output_string)?;
            info!("Lookup results written to {:?}", output_path);
        }
        None => {
            // Write to stdout
            let stdout = std::io::stdout();
            let mut handle = stdout.lock();
            handle.write_all(output_string.as_bytes())?;
            handle.write_all(b"\n")?;
            handle.flush()?;
        }
    }

    info!(
        "Lookup complete: {} identifiers processed",
        identifiers.len()
    );
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 100)]
async fn main() -> Result<()> {
    // use tracing_subscriber in JSON mode
    // rather than the previous env logger
    tracing_subscriber::fmt().json().init();

    info!("Starting big tent git sha {}", env!("VERGEN_GIT_SHA"));
    let args = Args::parse();

    match (&args.rodeo, &args.fresh_merge, &args.lookup) {
        // Lookup mode: --rodeo + --lookup (batch identifier lookup, no server)
        (Some(rodeo), v, Some(_)) if v.len() == 0 => run_lookup(rodeo, &args).await?,
        // Server mode: --rodeo only (start HTTP server)
        (Some(rodeo), v, None) if v.len() == 0 => run_rodeo(rodeo, &args).await?,
        // Merge mode: --fresh-merge (merge clusters into a new one)
        (None, v, None) if v.len() > 0 => run_merge(v.clone(), args).await?,
        _ => {
            Args::command().print_help()?;
        }
    };

    info!("Ending");

    Ok(())
}
