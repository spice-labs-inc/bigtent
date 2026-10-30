#!/bin/bash
set -e

# Test Data Generation Script for BigTent Merge Benchmarks
# This script generates all synthetic test clusters needed for benchmarking

echo "=========================================="
echo "BigTent Test Data Generation"
echo "=========================================="
echo ""

# 1. Generate scale test datasets (different sizes)
echo "Step 1/4: Generating scale test datasets..."
for size in 10k 50k 100k; do
    echo "  Generating ${size} items..."
    cargo run --bin data_generator -- \
        --size $size --clusters 3 --overlap 50 \
        --output benches/test_data/clusters_$size
    echo "  ✓ ${size} complete"
    echo ""
done

# 2. Generate overlap test data
echo "Step 2/4: Generating overlap test datasets..."
for overlap in 0 10 50 90; do
    echo "  Generating ${overlap}% overlap..."
    cargo run --bin data_generator -- \
        --size 100k --clusters 3 --overlap $overlap \
        --output benches/test_data/overlap_${overlap}pct
    echo "  ✓ ${overlap}% overlap complete"
    echo ""
done

# 3. Generate cluster count test data
echo "Step 3/4: Generating cluster count datasets..."
for count in 2 5 10; do
    echo "  Generating ${count} clusters..."
    cargo run --bin data_generator -- \
        --size 100k --clusters $count --overlap 30 \
        --output benches/test_data/${count}_clusters
    echo "  ✓ ${count} clusters complete"
    echo ""
done

# 4. Generate realistic workloads
echo "Step 4/4: Generating realistic workload datasets..."
echo "  Generating medium workload..."
cargo run --bin data_generator -- \
    --size 100k --clusters 3 --overlap 30 \
    --output benches/test_data/workload_medium
echo "  ✓ medium workload complete"
echo ""

echo "=========================================="
echo "All test data generation complete!"
echo "=========================================="
echo ""
echo "Generated datasets:"
ls -la benches/test_data/ | grep "^d" | grep -v "^\.$" | grep -v "^\.\.$" | awk '{print "  - " $NF}'
echo ""
echo "To run benchmarks:"
echo "  cargo bench --bench merge_benchmarks"
