use anyhow::{bail, Result};
use bigtent::{config::Args, index::Index,server::run_web_server};
use clap::Parser;
use signal_hook::{consts::SIGHUP, iterator::Signals};
use std::{ thread, time::Instant};

fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    println!("Args {:?}, threads {} ", args, args.num_threads(),);

 if args.conf_file().is_ok() {
        let index_build_start = Instant::now();
        let index = Index::new_arc(args)?;

        println!(
            "Initial index build in {:?}",
            Instant::now().duration_since(index_build_start)
        );

        let mut sig_hup = Signals::new([SIGHUP])?;
        let i2 = index.clone();

        thread::spawn(move || {
            for _ in sig_hup.forever() {
                println!("Got rebuild signal");
                let start = Instant::now();
                match i2.rebuild() {
                    Ok(_) => {
                        println!(
                            "Done rebuilding. Took {:?}",
                            Instant::now().duration_since(start)
                        );
                    }
                    Err(e) => {
                        println!("Rebuild error {:?}", e);
                    }
                }
            }
        });

        run_web_server(index);
    } else {
        bail!("run the server with the `--conf` option")
    }

    Ok(())
}
