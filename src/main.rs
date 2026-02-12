//! # BigTent CLI Entry Point
//!
//! This module contains the main entry point for the BigTent binary.
//!
//! ## Execution Modes
//!
//! The binary supports four mutually exclusive modes:
//!
//! ### Rodeo Mode (`--rodeo <paths>`)
//! Loads cluster files from the specified paths and starts an HTTP server
//! to serve queries against the loaded data.
//!
//! ```bash
//! bigtent --rodeo /path/to/clusters --port 3000
//! ```
//!
//! ### Cluster List Mode (`--cluster-list <file>`)
//! Reads a JSON file containing directory paths and starts an HTTP server.
//! On SIGHUP, the file is re-read to pick up directory changes.
//!
//! ```bash
//! bigtent --cluster-list /path/to/dirs.json --pid-file /tmp/bt.pid
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
    cluster_list::{load_cluster_list, load_clusters_from_dirs},
    config::{Args, ClusterSource},
    fresh_merge::merge_fresh,
    pid_file::PidFile,
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
use std::{io::Write, path::PathBuf, sync::Arc, time::{Duration, Instant}};
use tokio::signal::unix::{SignalKind, signal};

async fn run_server(cluster_source: ClusterSource, args: &Args) -> Result<()> {
    // Load initial clusters
    let dirs = match &cluster_source {
        ClusterSource::Rodeo(paths) => {
            // Validate that all paths exist before loading
            for path in paths {
                if !path.exists() {
                    bail!("Path to `.grc` does not point to a file: {:?}", path);
                }
            }
            paths.clone()
        }
        ClusterSource::ClusterList(path) => load_cluster_list(path)?,
    };

    let clusters = load_clusters_from_dirs(&dirs, args.pre_cache_index()).await?;

    let herd = GoatHerd::new(clusters);
    let initial_cluster_count = herd.get_herd().len() as u64;

    let cluster_holder =
        ClusterHolder::new_from_cluster(ArcSwap::new(Arc::new(herd)), Some(Arc::new(args.clone())))
            .await?;
    cluster_holder.record_reload(initial_cluster_count);

    info!("Built cluster... about to run it");

    // Acquire PID file if requested
    let _pid_file = match &args.pid_file {
        Some(path) => Some(PidFile::create(path)?),
        None => None,
    };

    // Spawn the web server as a background task
    let holder_for_server = cluster_holder.clone();
    let server_handle = tokio::spawn(async move {
        if let Err(e) = run_web_server(holder_for_server).await {
            tracing::error!("Web server error: {}", e);
        }
    });

    // SIGHUP reload loop
    let mut sighup = signal(SignalKind::hangup())?;
    let mut last_reload = Instant::now();
    let cooldown = Duration::from_secs(5);

    loop {
        tokio::select! {
            _ = sighup.recv() => {
                if last_reload.elapsed() < cooldown {
                    tracing::warn!("SIGHUP suppressed (cooldown)");
                    continue;
                }
                // Determine dirs from source
                let reload_dirs = match &cluster_source {
                    ClusterSource::Rodeo(paths) => paths.clone(),
                    ClusterSource::ClusterList(path) => {
                        match load_cluster_list(path) {
                            Ok(dirs) => dirs,
                            Err(e) => {
                                tracing::error!("Failed to read cluster list: {}", e);
                                continue;
                            }
                        }
                    }
                };
                // Load new clusters
                let old_count = cluster_holder.get_cluster().node_count();
                let old_cluster_count = cluster_holder.get_cluster().get_herd().len();
                let start = Instant::now();
                match load_clusters_from_dirs(&reload_dirs, args.pre_cache_index()).await {
                    Ok(members) if members.is_empty() => {
                        tracing::error!("Reload produced zero clusters, retaining existing data");
                    }
                    Ok(members) => {
                        let new_herd = GoatHerd::new(members);
                        let new_count = new_herd.node_count();
                        let new_cluster_count = new_herd.get_herd().len();
                        cluster_holder.update_cluster(Arc::new(new_herd));
                        cluster_holder.record_reload(new_cluster_count as u64);
                        tracing::info!(
                            event = "reload",
                            status = "success",
                            duration_ms = start.elapsed().as_millis() as u64,
                            old_cluster_count,
                            new_cluster_count,
                            old_node_count = old_count,
                            new_node_count = new_count,
                        );
                        last_reload = Instant::now();
                    }
                    Err(e) => {
                        tracing::error!("Reload failed: {}, retaining existing data", e);
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Received SIGINT, shutting down");
                break;
            }
        }
    }

    server_handle.abort();
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

    // Load clusters using the same pattern as run_server
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

/// Validate cluster files and exit.
///
/// Loads clusters from the given source and reports any errors.
/// Returns `true` if all clusters are valid, `false` if errors were found.
async fn run_check(cluster_source: &ClusterSource, args: &Args) -> bool {
    let dirs = match cluster_source {
        ClusterSource::Rodeo(paths) => {
            let mut errors = false;
            for path in paths {
                if !path.exists() {
                    eprintln!("ERROR: Path does not exist: {:?}", path);
                    errors = true;
                }
            }
            if errors {
                return false;
            }
            paths.clone()
        }
        ClusterSource::ClusterList(path) => {
            if !path.exists() {
                eprintln!("ERROR: Cluster list file does not exist: {:?}", path);
                return false;
            }
            match load_cluster_list(path) {
                Ok(dirs) => dirs,
                Err(e) => {
                    eprintln!("ERROR: Failed to read cluster list {:?}: {}", path, e);
                    return false;
                }
            }
        }
    };

    if dirs.is_empty() {
        eprintln!("ERROR: No cluster directories specified");
        return false;
    }

    let mut total_clusters = 0u64;
    let mut total_nodes = 0u64;
    let mut errors = false;

    for dir in &dirs {
        if !dir.exists() {
            eprintln!("ERROR: Directory does not exist: {:?}", dir);
            errors = true;
            continue;
        }

        match load_clusters_from_dirs(&[dir.clone()], args.pre_cache_index()).await {
            Ok(members) => {
                if members.is_empty() {
                    eprintln!("WARNING: No cluster files found in {:?}", dir);
                } else {
                    for member in &members {
                        total_clusters += 1;
                        let node_count = member.node_count();
                        total_nodes += node_count;
                        println!(
                            "  OK: cluster in {:?} — {} nodes",
                            dir, node_count
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!("ERROR: Failed to load cluster from {:?}: {}", dir, e);
                errors = true;
            }
        }
    }

    println!();
    if errors {
        eprintln!(
            "FAILED: Found errors. {} clusters loaded, {} total nodes.",
            total_clusters, total_nodes
        );
        false
    } else {
        println!(
            "OK: All clusters valid. {} clusters, {} total nodes.",
            total_clusters, total_nodes
        );
        true
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 100)]
async fn main() -> Result<()> {
    // use tracing_subscriber in JSON mode
    // rather than the previous env logger
    tracing_subscriber::fmt().json().init();

    info!("Starting big tent git sha {}", env!("VERGEN_GIT_SHA"));
    let args = Args::parse();

    // Warn if --pid-file is used with non-server modes
    if args.pid_file.is_some() && args.rodeo.is_none() && args.cluster_list.is_none() {
        tracing::warn!("--pid-file is only meaningful in server mode (--rodeo or --cluster-list)");
    }

    // Check for cluster-list mode first (it's mutually exclusive with rodeo via cluster_source)
    let cluster_source = args.cluster_source().map_err(|e| anyhow::anyhow!(e))?;

    match (&cluster_source, &args.fresh_merge, &args.lookup, args.check) {
        // Check mode: --rodeo/--cluster-list + --check (validate and exit)
        (Some(source), v, None, true) if v.is_empty() => {
            if !run_check(source, &args).await {
                std::process::exit(1);
            }
        }
        // Lookup mode: --rodeo + --lookup (batch identifier lookup, no server)
        (Some(ClusterSource::Rodeo(rodeo)), v, Some(_), false) if v.is_empty() => {
            run_lookup(rodeo, &args).await?
        }
        // Server mode: --rodeo or --cluster-list (start HTTP server with SIGHUP support)
        (Some(source), v, None, false) if v.is_empty() => {
            run_server(source.clone(), &args).await?
        }
        // Merge mode: --fresh-merge (merge clusters into a new one)
        (None, v, None, false) if !v.is_empty() => run_merge(v.clone(), args).await?,
        _ => {
            Args::command().print_help()?;
        }
    };

    info!("Ending");

    Ok(())
}
