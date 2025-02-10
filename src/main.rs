use anyhow::{bail, Result};
use arc_swap::ArcSwap;
use bigtent::{
  config::Args, merger::merge_fresh, rodeo::GoatRodeoCluster, rodeo_server::RodeoServer,
  server::run_web_server,
};
use clap::{CommandFactory, Parser};
use env_logger::Env;
#[cfg(not(test))]
use log::{error, info}; // Use log crate when building application

use signal_hook::{consts::SIGHUP, iterator::Signals};
use std::{path::PathBuf, sync::Arc, thread, time::Instant};
#[cfg(test)]
use std::{println as info, println as error};

async fn run_rodeo(path: &PathBuf, args: &Args) -> Result<()> {
  if path.exists() && path.is_file() {
    let whole_path = path.clone().canonicalize()?;
    let dir_path = whole_path.parent().unwrap().to_path_buf();
    let index_build_start = Instant::now();
    let cluster = Arc::new(ArcSwap::new(Arc::new(
      GoatRodeoCluster::new(&dir_path, &whole_path).await?,
    )));

    let index = RodeoServer::new_from_cluster(cluster, Some(args.clone())).await?;

    info!(
      "Initial index build in {:?}",
      Instant::now().duration_since(index_build_start)
    );

    run_web_server(index).await?;
  } else {
    bail!("Path to `.grc` does not point to a file: {:?}", path)
  }
  Ok(())
}

async fn run_full_server(args: Args) -> Result<()> {
  let index_build_start = Instant::now();
  let index = RodeoServer::new(args).await?;

  info!(
    "Initial index build in {:?}",
    Instant::now().duration_since(index_build_start)
  );

  let mut sig_hup = Signals::new([SIGHUP])?;
  let i2 = index.clone();
  let (tx, mut rx) = tokio::sync::mpsc::channel::<bool>(10);

  // Async closures are not supported, so to live in async-land
  // we need to spawn a Tokio task that waits for the MPSC message
  // and does the rebuild
  tokio::spawn(async move {
    loop {
      match rx.recv().await {
        Some(_) => {
          info!("Got rebuild signal");
          let start = Instant::now();
          match i2.rebuild().await {
            Ok(_) => {
              info!(
                "Done rebuilding. Took {:?}",
                Instant::now().duration_since(start)
              );
            }
            Err(e) => {
              error!("Rebuild error {:?}", e);
            }
          }
        }
        None => {break;}
      }
    }
  });
  
  // and we spawn a real thread to wait on `sig_hup`
  // and when there's a sighup, send a message into the channel
  // which will cause the async tasks to do the rebuild... sigh
  thread::spawn(move || {
    for _ in sig_hup.forever() {
      match tx.blocking_send(true) {
        Ok(_) => {}
        Err(_) => {break;}
      }
    }
  });

  run_web_server(index).await?;
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
  let mut clusters: Vec<GoatRodeoCluster> = vec![];
  for p in &paths {
    for b in GoatRodeoCluster::cluster_files_in_dir(p.clone()).await? {
      clusters.push(b);
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

  let ret = merge_fresh(clusters, dest).await;
  info!(
    "Finished merging at {:?}",
    Instant::now().duration_since(start)
  );
  ret
}

#[tokio::main(flavor = "multi_thread", worker_threads = 100)]
async fn main() -> Result<()> {
  let env = Env::default()
    .filter_or("MY_LOG_LEVEL", "info")
    .write_style_or("MY_LOG_STYLE", "always");

  env_logger::init_from_env(env);

  info!("Starting");
  let args = Args::parse();

  match (&args.rodeo, &args.conf, &args.fresh_merge) {
    (Some(rodeo), None, v) if v.len() == 0 => run_rodeo(rodeo, &args).await?,
    (None, Some(_conf), v) if v.len() == 0 => run_full_server(args).await?,

    // normally there'd be a generic here, but because this function is `main`, it's necessary
    // to specify the concrete type (in this case `ItemMetaData`) rather than the generic
    // type
    (None, None, v) if v.len() > 0 => run_merge(v.clone(), args).await?,
    _ => {
      Args::command().print_help()?;
    }
  };

  info!("Ending");

  Ok(())
}
