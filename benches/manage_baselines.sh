#!/bin/bash

# Baseline Management Script for BigTent Merge Benchmarks
#
# Usage:
#   ./manage_baselines.sh {save|compare|list|clean}

set -e

BASELINE_DIR="$(dirname "$0")/baselines/main"

case "$1" in
  save)
    DATE=$(date +%Y%m%d_%H%M%S)
    echo "Saving baseline as $DATE"

    # Ensure target directory exists
    if [ ! -d "target/criterion" ]; then
        echo "Error: No benchmark results found. Run benchmarks first:"
        echo "  cargo bench --bench merge_benchmarks"
        exit 1
    fi

    # Create baseline directory
    mkdir -p "$BASELINE_DIR"
    cp -r target/criterion "$BASELINE_DIR/$DATE"

    echo "✓ Baseline saved to: $BASELINE_DIR/$DATE"
    ;;

  compare)
    LATEST=$(ls -t "$BASELINE_DIR" 2>/dev/null | head -1)

    if [ -z "$LATEST" ]; then
        echo "Error: No baseline found. Run 'save' first."
        exit 1
    fi

    echo "Comparing against baseline: $LATEST"
    echo "To view comparison report, open: target/criterion/report/index.html"
    echo ""
    echo "To manually compare against specific baseline:"
    echo "  cargo bench --bench merge_benchmarks -- --baseline $BASELINE_DIR/$LATEST"
    ;;

  list)
    if [ ! -d "$BASELINE_DIR" ]; then
        echo "No baselines found."
        exit 0
    fi

    echo "Available baselines:"
    ls -lh "$BASELINE_DIR" | awk '{print $9, $5, $6, $7, $8}'
    ;;

  clean)
    echo "Cleaning current benchmark results..."
    rm -rf target/criterion
    echo "✓ Cleaned target/criterion"
    ;;

  *)
    echo "Usage: $0 {save|compare|list|clean}"
    echo ""
    echo "Commands:"
    echo "  save     - Save current benchmark results as a baseline"
    echo "  compare  - Display instructions for comparing with latest baseline"
    echo "  list     - List all saved baselines"
    echo "  clean    - Clean current benchmark results"
    exit 1
    ;;
esac
