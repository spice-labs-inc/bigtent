use anyhow::{bail, Result};
use bigtent::{
  config::Args, merger::merge_fresh, rodeo::GoatRodeoBundle, rodeo_server::RodeoServer,
  server::run_web_server,
};
use clap::{CommandFactory, Parser};
use env_logger::Env;
#[cfg(not(test))]
use log::{error, info}; // Use log crate when building application

use signal_hook::{consts::SIGHUP, iterator::Signals};
use std::{path::PathBuf, thread, time::Instant};
#[cfg(test)]
use std::{println as info, println as error};

fn run_rodeo(path: &PathBuf, args: &Args) -> Result<()> {
  if path.exists() && path.is_file() {
    let whole_path = path.clone().canonicalize()?;
    let dir_path = whole_path.parent().unwrap().to_path_buf();
    let index_build_start = Instant::now();
    let bundle = GoatRodeoBundle::new(&dir_path, &whole_path)?;

    let index = RodeoServer::new_from_bundle(bundle, args.num_threads(), Some(args.clone()))?;

    info!(
      "Initial index build in {:?}",
      Instant::now().duration_since(index_build_start)
    );

    run_web_server(index);
  } else {
    bail!("Path to `.grb` does not point to a file: {:?}", path)
  }
  Ok(())
}

fn run_full_server(args: Args) -> Result<()> {
  let index_build_start = Instant::now();
  let index = RodeoServer::new(args)?;

  info!(
    "Initial index build in {:?}",
    Instant::now().duration_since(index_build_start)
  );

  let mut sig_hup = Signals::new([SIGHUP])?;
  let i2 = index.clone();

  thread::spawn(move || {
    for _ in sig_hup.forever() {
      info!("Got rebuild signal");
      let start = Instant::now();
      match i2.rebuild() {
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
  });

  run_web_server(index);
  Ok(())
}

fn run_merge(paths: Vec<PathBuf>, args: Args) -> Result<()> {
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

  info!("Loading bundles...");
  let mut bundles = vec![];
  for p in &paths {
    for b in GoatRodeoBundle::bundle_files_in_dir(p.clone())? {
      bundles.push(b);
    }
  }
  info!(
    "Finished loading {} bundles at {:?}",
    bundles.len(),
    Instant::now().duration_since(start)
  );
  if bundles.len() < 2 {
    bail!(
      "There must be at least 2 bundles to merge... only got {}",
      bundles.len()
    );
  }

  let ret = merge_fresh(bundles, args.threaded.unwrap_or(false), dest);
  info!(
    "Finished merging at {:?}",
    Instant::now().duration_since(start)
  );
  ret
}

fn main() -> Result<()> {
  let env = Env::default()
    .filter_or("MY_LOG_LEVEL", "info")
    .write_style_or("MY_LOG_STYLE", "always");

  env_logger::init_from_env(env);

  info!("Starting");
  let args = Args::parse();

  info!("{:?}, threads {} ", args, args.num_threads(),);

  match (&args.rodeo, &args.conf, &args.fresh_merge) {
    (Some(rodeo), None, v) if v.len() == 0 => run_rodeo(rodeo, &args)?,
    (None, Some(_conf), v) if v.len() == 0 => run_full_server(args)?,
    (None, None, v) if v.len() > 0 => run_merge(v.clone(), args)?,
    _ => {
      Args::command().print_help()?;
    }
  };

  info!("Ending");

  Ok(())
}
