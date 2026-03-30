//! Merge Benchmarks
//!
//! Comprehensive benchmark suite for BigTent merge operations using Criterion.

use bigtent::fresh_merge::merge_fresh;
use bigtent::rodeo::goat::GoatRodeoCluster;
use bigtent::rodeo::member::member_core;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::path::PathBuf;
use std::sync::Arc;

fn load_clusters(path: &str) -> Result<Vec<Arc<GoatRodeoCluster>>, anyhow::Error> {
    tokio::runtime::Runtime::new()?.block_on(async {
        let clusters =
            GoatRodeoCluster::cluster_files_in_dir(PathBuf::from(path), false, vec![]).await?;
        Ok(clusters)
    })
}

fn to_herd_members(
    clusters: Vec<Arc<GoatRodeoCluster>>,
) -> Vec<Arc<bigtent::rodeo::member::HerdMember>> {
    clusters.into_iter().map(member_core).collect()
}

fn format_size(size: usize) -> String {
    if size >= 1_000_000 {
        format!("{}m", size / 1_000_000)
    } else if size >= 1_000 {
        format!("{}k", size / 1_000)
    } else {
        format!("{}", size)
    }
}

fn run_merge(clusters: Vec<Arc<bigtent::rodeo::member::HerdMember>>, buffer_limit: usize) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let dest = tempfile::tempdir().unwrap().path().to_path_buf();
    rt.block_on(async {
        black_box(
            merge_fresh(
                black_box(clusters),
                black_box(buffer_limit),
                black_box(dest),
            )
            .await
            .unwrap(),
        );
    });
}

fn bench_scale(c: &mut Criterion) {
    let sizes = vec![10_000, 50_000, 100_000];

    for size in sizes {
        let size_str = format_size(size);
        let cluster_dir = format!("benches/test_data/clusters_{}", size_str);
        let dir_path = PathBuf::from(&cluster_dir);

        if !dir_path.exists() {
            eprintln!("Warning: Test data not found at {}. Skipping.", cluster_dir);
            continue;
        }

        let measurement_time = match size {
            10_000 => 18.0,
            50_000 => 95.0,
            100_000 => 180.0,
            _ => 30.0,
        };

        let mut group = c.benchmark_group("scale");
        group.measurement_time(std::time::Duration::from_secs_f64(measurement_time));

        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, _| {
            let clusters = load_clusters(&cluster_dir).unwrap();
            let herd_members = to_herd_members(clusters);
            b.iter(|| run_merge(black_box(herd_members.clone()), 10_000));
        });

        group.finish();
    }
}

fn bench_overlap(c: &mut Criterion) {
    let mut group = c.benchmark_group("overlap");

    let overlap_pcts = vec![0, 10, 50, 90];

    for overlap_pct in overlap_pcts {
        let cluster_dir = format!("benches/test_data/overlap_{}pct", overlap_pct);
        let dir_path = PathBuf::from(&cluster_dir);

        if !dir_path.exists() {
            eprintln!("Warning: Test data not found at {}. Skipping.", cluster_dir);
            continue;
        }

        group.bench_with_input(
            BenchmarkId::new("overlap", overlap_pct),
            &overlap_pct,
            |b, _| {
                let clusters = load_clusters(&cluster_dir).unwrap();
                let herd_members = to_herd_members(clusters);
                b.iter(|| run_merge(black_box(herd_members.clone()), 10_000));
            },
        );
    }

    group.finish();
}

fn bench_buffer_limit(c: &mut Criterion) {
    let cluster_dir = "benches/test_data/clusters_100k";
    let dir_path = PathBuf::from(cluster_dir);

    if !dir_path.exists() {
        eprintln!("Warning: Test data not found at {}. Skipping.", cluster_dir);
    } else {
        let buffer_limits = vec![1_000, 10_000, 50_000, 100_000];

        for buffer_limit in buffer_limits {
            let measurement_time = match buffer_limit {
                1_000 | 50_000 | 100_000 => 180.0,
                _ => 30.0,
            };

            let mut group = c.benchmark_group("buffer_limit");
            group.measurement_time(std::time::Duration::from_secs_f64(measurement_time));

            group.bench_with_input(
                BenchmarkId::from_parameter(buffer_limit),
                &buffer_limit,
                |b, &limit| {
                    let clusters = load_clusters(cluster_dir).unwrap();
                    let herd_members = to_herd_members(clusters);
                    b.iter(|| run_merge(black_box(herd_members.clone()), black_box(limit)));
                },
            );

            group.finish();
        }
    }
}

fn bench_cluster_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("cluster_count");

    let cluster_counts = vec![2, 5, 10];

    for count in cluster_counts {
        let cluster_dir = format!("benches/test_data/{}_clusters", count);
        let dir_path = PathBuf::from(&cluster_dir);

        if !dir_path.exists() {
            eprintln!("Warning: Test data not found at {}. Skipping.", cluster_dir);
            continue;
        }

        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            let clusters = load_clusters(&cluster_dir).unwrap();
            let herd_members = to_herd_members(clusters);
            b.iter(|| run_merge(black_box(herd_members.clone()), 10_000));
        });
    }

    group.finish();
}

fn bench_realistic(c: &mut Criterion) {
    let mut group = c.benchmark_group("realistic");

    // Medium workload
    let cluster_dir_medium = "benches/test_data/workload_medium";
    if PathBuf::from(cluster_dir_medium).exists() {
        group.bench_function("medium", |b| {
            let clusters = load_clusters(cluster_dir_medium).unwrap();
            let herd_members = to_herd_members(clusters);
            b.iter(|| run_merge(black_box(herd_members.clone()), 10_000));
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_scale,
    bench_overlap,
    bench_buffer_limit,
    bench_cluster_count,
    bench_realistic
);
criterion_main!(benches);
