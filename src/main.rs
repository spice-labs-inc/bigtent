use anyhow::{Result, bail};
use arc_swap::ArcSwap;
use bigtent::{
    config::Args,
    fresh_merge::merge_fresh,
    rodeo::{
        goat::GoatRodeoCluster,
        goat_herd::GoatHerd,
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
use std::{path::PathBuf, sync::Arc, time::Instant};

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

#[tokio::main(flavor = "multi_thread", worker_threads = 100)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().json().init();

    // let env = Env::default()
    //     .filter_or("MY_LOG_LEVEL", "info")
    //     .write_style_or("MY_LOG_STYLE", "always");

    // env_logger::init_from_env(env);

    info!("Starting big tent git sha {}", env!("VERGEN_GIT_SHA"));
    let args = Args::parse();

    match (&args.rodeo, &args.fresh_merge) {
        (Some(rodeo), v) if v.len() == 0 => run_rodeo(rodeo, &args).await?,
        // (None, Some(_conf), v) if v.len() == 0 => run_full_server(args).await?,

        // normally there'd be a generic here, but because this function is `main`, it's necessary
        // to specify the concrete type (in this case `ItemMetaData`) rather than the generic
        // type
        (None, v) if v.len() > 0 => run_merge(v.clone(), args).await?,
        _ => {
            Args::command().print_help()?;
        }
    };

    info!("Ending");

    Ok(())
}
