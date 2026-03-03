# Merge Benchmarking Guide

## Overview

BigTent includes a comprehensive benchmarking suite for merge operations using Criterion.

## Quick Start

### 1. Generate Test Data

Run the standalone data generator to create test clusters:

```bash
# Generate all test datasets
for size in 10k 50k 100k; do
    cargo run --bin data_generator -- \
        --size $size --clusters 3 --overlap 50 \
        --output benches/test_data/clusters_$size
done

# Generate overlap test data
for overlap in 0 10 50 90; do
    cargo run --bin data_generator -- \
        --size 100k --clusters 3 --overlap $overlap \
        --output benches/test_data/overlap_${overlap}pct
done

# Generate cluster count test data
for count in 2 5 10; do
    cargo run --bin data_generator -- \
        --size 100k --clusters $count --overlap 30 \
        --output benches/test_data/${count}_clusters
done

# Generate realistic workloads
cargo run --bin data_generator -- \
    --size 100k --clusters 3 --overlap 30 \
    --output benches/test_data/workload_medium
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
# Open HTML report in browser
open target/criterion/report/index.html  # macOS
xdg-open target/criterion/report/index.html  # Linux
```

## Baseline Management

### Save a Baseline

```bash
# Run benchmarks first
cargo bench --bench merge_benchmarks

# Save as baseline
./benches/manage_baselines.sh save
```

### Compare Against Baseline

```bash
# Run current benchmarks
cargo bench --bench merge_benchmarks

# Compare with latest baseline
./benches/manage_baselines.sh compare

# Or compare with specific baseline
cargo bench --bench merge_benchmarks -- --baseline benches/baselines/main/20240303_123456
```

### List Available Baselines

```bash
./benches/manage_baselines.sh list
```

## Benchmark Groups

### Scale (bench_scale)
Tests merge performance with different cluster sizes:
- 10K items
- 50K items
- 100K items
- 500K items
- 1M items

### Overlap (bench_overlap)
Tests merge performance with different overlap percentages:
- 0% overlap (all unique items)
- 10% overlap
- 50% overlap
- 90% overlap

### Buffer Limit (bench_buffer_limit)
Tests performance impact of `--buffer-limit`:
- 1,000 items (memory constrained)
- 10,000 items (default)
- 50,000 items
- 100,000 items

### Cluster Count (bench_cluster_count)
Tests merge performance with different cluster counts:
- 2 clusters
- 5 clusters
- 10 clusters

### Realistic (bench_realistic)
Real-world workload simulations:
- **Medium**: 100K items, 3 clusters, 30% overlap, buffer 10K
- **Large**: 500K items, 5 clusters, 20% overlap, buffer 50K

## Test Data Storage

Test data is generated once and stored in `benches/test_data/`:
- **Not tracked in git** (added to .gitignore)
- **Reproducible** (same seed produces same data)
- **Portable** (can be shared across machines)

### Regenerating Test Data

```bash
rm -rf benches/test_data/
# Re-run generation commands from Step 1
```

## Troubleshooting

### Benchmark hangs or takes too long
- Reduce `measurement_time` in `benches/criterion.toml`
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
Benchmarks will skip groups if test data is not found. Generate test data first using the commands in Step 1.

## Data Generator CLI

The `data_generator` binary creates synthetic BigTent clusters:

```bash
cargo run --bin data_generator -- [OPTIONS]

Options:
  -s, --size <N>          Number of items per cluster (e.g., 10k, 100k, 1m)
  -c, --clusters <N>       Number of clusters to generate
  -o, --overlap <PCT>      Percentage of items that overlap between clusters
  -d, --output <PATH>      Output directory for generated clusters
  -h, --help              Show help message
```

## Performance Metrics

The benchmarks measure:
- **Execution time**: Total merge duration
- **Throughput**: Items processed per second (logged during merge)
- **Memory**: Peak RSS during merge (not implemented in current version)
- **Disk I/O**: Bytes read/written (not implemented in current version)
- **CPU time**: User and system time (not implemented in current version)

Note: Currently, metrics are primarily captured via logs during merge operations. Additional system-level metrics can be added to the benchmark suite if needed.

## Best Practices

1. **Generate test data once** - The synthetic clusters can be reused across multiple benchmark runs
2. **Use consistent environments** - Run benchmarks on the same machine with similar load
3. **Monitor system resources** - Watch for memory or I/O bottlenecks
4. **Review HTML reports** - Criterion generates detailed statistical analysis in HTML format
5. **Compare against baselines** - Track performance changes over time

## See Also

- [benches/README.md](benches/README.md) - Detailed benchmark documentation
- [PERFORMANCE.md](PERFORMANCE.md) - Performance tuning guide
- [info/config.md](info/config.md) - Configuration reference
