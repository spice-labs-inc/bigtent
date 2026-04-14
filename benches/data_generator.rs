//! Synthetic Test Data Generator for Merge Benchmarks
//!
//! This standalone CLI tool generates synthetic BigTent clusters with controlled
//! properties for benchmarking merge operations.
//!
//! Usage:
//!   cargo run --bin data_generator -- --size 100k --clusters 3 --overlap 50 --output benchmarks/test_data/test

use anyhow::Result;
use std::path::PathBuf;
use std::time::Instant;
use thousands::Separable;

use bigtent::bench_util::{OverlapStrategy, SyntheticItemGenerator, generate_synthetic_cluster};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

const DEFAULT_SIZE: usize = 100_000;
const DEFAULT_CLUSTERS: usize = 3;
const DEFAULT_OVERLAP: f64 = 50.0;
const DEFAULT_SEED: u64 = 42;

fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer().pretty())
        .with(EnvFilter::from_default_env())
        .init();

    let args = parse_args()?;

    println!("Generating test data:");
    println!("  Size: {} items", args.size.separate_with_commas());
    println!("  Clusters: {}", args.clusters);
    println!("  Overlap: {}%", args.overlap);
    println!("  Output: {}", args.output.display());
    println!();

    let start = Instant::now();

    let generator = SyntheticItemGenerator::new(OverlapStrategy::Pattern, args.seed);

    // Generate base items
    println!("Generating base items...");
    let base_start = Instant::now();
    let base_items = generator.generate_base_items(args.size);
    println!(
        "  Generated {} base items in {:?}",
        base_items.len().separate_with_commas(),
        base_start.elapsed()
    );

    // Generate clusters
    println!("\nGenerating clusters...");
    let mut cluster_files = vec![];

    for i in 0..args.clusters {
        let cluster_start = Instant::now();
        println!("  Generating cluster {}/{}...", i + 1, args.clusters);

        let cluster_items = generator.generate_overlap_cluster(&base_items, i, args.overlap);
        println!(
            "    Generated {} items (expected: {})",
            cluster_items.len().separate_with_commas(),
            args.size.separate_with_commas()
        );

        let cluster_dir = args.output.join(format!("cluster_{}", i));
        let cluster_file = tokio::runtime::Runtime::new()?.block_on(generate_synthetic_cluster(
            &format!("cluster_{}", i),
            cluster_items,
            cluster_dir.clone(),
        ))?;

        cluster_files.push(cluster_file);
        println!(
            "    Wrote to {} in {:?}",
            cluster_dir.display(),
            cluster_start.elapsed()
        );
    }

    println!("\n✓ Generation complete!");
    println!("  Total time: {:?}", start.elapsed());
    println!("  Output directory: {}", args.output.display());
    println!("  Clusters created: {}", cluster_files.len());

    Ok(())
}

struct Args {
    size: usize,
    clusters: usize,
    overlap: f64,
    seed: u64,
    output: PathBuf,
}

fn parse_args() -> Result<Args> {
    let args: Vec<String> = std::env::args().collect();

    let mut size = DEFAULT_SIZE;
    let mut clusters = DEFAULT_CLUSTERS;
    let mut overlap = DEFAULT_OVERLAP;
    let mut seed = DEFAULT_SEED;
    let mut output = PathBuf::from("./benches/test_data/test");

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--size" | "-s" => {
                if i + 1 >= args.len() {
                    anyhow::bail!("--size requires a value");
                }
                size = parse_size(&args[i + 1])?;
                i += 2;
            }
            "--clusters" | "-c" => {
                if i + 1 >= args.len() {
                    anyhow::bail!("--clusters requires a value");
                }
                clusters = args[i + 1].parse()?;
                if clusters < 2 {
                    anyhow::bail!("--clusters must be at least 2");
                }
                i += 2;
            }
            "--overlap" | "-o" => {
                if i + 1 >= args.len() {
                    anyhow::bail!("--overlap requires a value");
                }
                overlap = args[i + 1].parse()?;
                if overlap < 0.0 || overlap > 100.0 {
                    anyhow::bail!("--overlap must be between 0 and 100");
                }
                i += 2;
            }
            "--seed" => {
                if i + 1 >= args.len() {
                    anyhow::bail!("--seed requires a value");
                }
                seed = args[i + 1].parse()?;
                i += 2;
            }
            "--output" | "-d" => {
                if i + 1 >= args.len() {
                    anyhow::bail!("--output requires a value");
                }
                output = PathBuf::from(&args[i + 1]);
                i += 2;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            unknown => {
                anyhow::bail!("Unknown argument: {}", unknown);
            }
        }
    }

    Ok(Args {
        size,
        clusters,
        overlap,
        seed,
        output,
    })
}

fn parse_size(s: &str) -> Result<usize> {
    let s = s.to_lowercase();
    if s.ends_with('k') {
        let num = s[..s.len() - 1].parse::<usize>()?;
        Ok(num * 1_000)
    } else if s.ends_with('m') {
        let num = s[..s.len() - 1].parse::<usize>()?;
        Ok(num * 1_000_000)
    } else {
        s.parse()
            .map_err(|e| anyhow::anyhow!("Invalid size: {}", e))
    }
}

fn print_help() {
    println!("Synthetic Test Data Generator for BigTent Merge Benchmarks");
    println!();
    println!("Usage:");
    println!("  data_generator [OPTIONS]");
    println!();
    println!("Options:");
    println!("  -s, --size <N>          Number of items per cluster (e.g., 10k, 100k, 1m)");
    println!("                           Default: {}", DEFAULT_SIZE);
    println!("  -c, --clusters <N>       Number of clusters to generate");
    println!("                           Default: {}", DEFAULT_CLUSTERS);
    println!("  -o, --overlap <PCT>      Percentage of items that overlap between clusters");
    println!("                           Default: {}%", DEFAULT_OVERLAP);
    println!("      --seed <N>           Random seed for reproducible data");
    println!("                           Default: {}", DEFAULT_SEED);
    println!("  -d, --output <PATH>      Output directory for generated clusters");
    println!("                           Default: ./benches/test_data/test");
    println!("  -h, --help              Show this help message");
    println!();
    println!("Examples:");
    println!("  # Generate 100k items, 3 clusters, 50% overlap");
    println!("  data_generator --size 100k --clusters 3 --overlap 50");
    println!();
    println!("  # Generate 1M items, 5 clusters, 20% overlap");
    println!("  data_generator -s 1m -c 5 -o 20 -d benches/test_data/large");
    println!();
}
