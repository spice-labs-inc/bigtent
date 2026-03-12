# BigTent Merge Benchmarks

This directory contains the merge benchmarking suite for BigTent.

## Quick Start

### 1. Generate Test Data

Generate synthetic clusters for benchmarking:

```bash
# Scale tests (different cluster sizes)
cargo run --bin data_generator -- --size 10k --clusters 3 --overlap 50 --output benches/test_data/clusters_10k
cargo run --bin data_generator -- --size 50k --clusters 3 --overlap 50 --output benches/test_data/clusters_50k
cargo run --bin data_generator -- --size 100k --clusters 3 --overlap 50 --output benches/test_data/clusters_100k

# Overlap tests
cargo run --bin data_generator -- --size 100k --clusters 3 --overlap 0 --output benches/test_data/overlap_0pct
cargo run --bin data_generator -- --size 100k --clusters 3 --overlap 10 --output benches/test_data/overlap_10pct
cargo run --bin data_generator -- --size 100k --clusters 3 --overlap 50 --output benches/test_data/overlap_50pct
cargo run --bin data_generator -- --size 100k --clusters 3 --overlap 90 --output benches/test_data/overlap_90pct

# Cluster count tests
cargo run --bin data_generator -- --size 100k --clusters 2 --overlap 30 --output benches/test_data/2_clusters
cargo run --bin data_generator -- --size 100k --clusters 5 --overlap 30 --output benches/test_data/5_clusters
cargo run --bin data_generator -- --size 100k --clusters 10 --overlap 30 --output benches/test_data/10_clusters

# Realistic workloads
cargo run --bin data_generator -- --size 100k --clusters 3 --overlap 30 --output benches/test_data/workload_medium
cargo run --bin data_generator -- --size 500k --clusters 5 --overlap 20 --output benches/test_data/workload_large
```

### 2. Run Benchmarks

```bash
# Run all benchmarks
cargo bench --bench merge_benchmarks

# Run specific benchmark group
cargo bench --bench merge_benchmarks scale
cargo bench --bench merge_benchmarks overlap
cargo bench --bench merge_benchmarks buffer_limit
cargo bench --bench merge_benchmarks cluster_count
cargo bench --bench merge_benchmarks realistic
```

### 3. View Results

```bash
# Open HTML report
open target/criterion/report/index.html  # macOS
xdg-open target/criterion/report/index.html  # Linux
```

## Baseline Management

### Save a Baseline

```bash
# Run benchmarks first
cargo bench --bench merge_benchmarks

# Save as baseline
./manage_baselines.sh save
```

### Compare Against Baseline

```bash
# Run current benchmarks
cargo bench --bench merge_benchmarks

# Compare with latest baseline
./manage_baselines.sh compare
```

### List Baselines

```bash
./manage_baselines.sh list
```

### Clean Current Results

```bash
./manage_baselines.sh clean
```

## Test Data Storage

Test data is generated once and stored in `benches/test_data/`:
- **Not tracked in git** (added to .gitignore)
- **Reproducible** (same seed produces same data)
- **Portable** (can be shared across machines)

### Regenerating Test Data

If you need to regenerate test data:

```bash
rm -rf benches/test_data/
# Re-run generation commands from step 1
```

## Benchmark Groups

### Scale (`bench_scale`)
Tests merge performance with different cluster sizes:
- 10K items
- 50K items
- 100K items

### Overlap (`bench_overlap`)
Tests merge performance with different overlap percentages:
- 0% overlap (all unique items)
- 10% overlap
- 50% overlap
- 90% overlap

### Buffer Limit (`bench_buffer_limit`)
Tests performance impact of `--buffer-limit`:
- 1,000 items (memory constrained)
- 10,000 items (default)
- 50,000 items
- 100,000 items

### Cluster Count (`bench_cluster_count`)
Tests merge performance with different cluster counts:
- 2 clusters
- 5 clusters
- 10 clusters

### Realistic (`bench_realistic`)
Real-world workload simulations:
- **Medium**: 100K items, 3 clusters, 30% overlap, buffer 10K
- **Large**: 500K items, 5 clusters, 20% overlap, buffer 50K

## Troubleshooting

### Benchmark hangs or takes too long
- Reduce `measurement_time` in `criterion.toml`
- Run specific benchmark groups instead of all

### Out of memory errors
- Reduce `--buffer-limit` in benchmarks
- Use smaller cluster sizes for testing

### Test data corrupted
```bash
rm -rf benches/test_data/
cargo run --bin data_generator -- [options]
```

### Missing test data warning
Benchments will skip groups if test data is not found. Generate test data first.
