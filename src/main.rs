use anyhow::{bail, Result};

use bigtent::{config::Args, index::Index, indexer::FileReader, server::run_web_server};
use clap::Parser;

use scopeguard::defer;
use signal_hook::{consts::SIGHUP, iterator::Signals};
use std::{fs::File, io::BufWriter, thread, time::Instant};
use thousands::Separable;

fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    println!("Args {:?}, threads {} ", args, args.num_threads(),);

    if args.build_index() {
        let start = Instant::now();
        defer! {
          println!("Building index took {:?}", Instant::now().duration_since(start));
        }
        let file = args.index_source()?;
        let mut indexer = FileReader::new(file);
        let out = File::create(args.index_dest()?)?;
        let all = indexer.build_index()?;
        println!("Build index of {}", all.len().separate_with_commas());
       
        let mut bw = BufWriter::new(out);

        for eo in all {
            eo.write(&mut bw)?;
        }
    } else if args.conf_file().is_ok() {
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
        bail!("There must either be indexing with the `--index` option or run the server with the `--conf` option")
    }

    Ok(())
}
