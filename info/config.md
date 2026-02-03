# Configuration Reference

This document describes all configuration options for BigTent.

## Command-Line Arguments

BigTent is configured primarily via command-line arguments.

### Server Mode Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--rodeo <paths>` | Path(s) | - | Path(s) to `.grc` cluster files or directories containing them |
| `--host <hostname>` | String | `localhost` | Hostname(s) to bind the HTTP server to |
| `--port <port>` | u16 | `3000` | Port to bind the HTTP server to |
| `--cache-index <bool>` | Boolean | `false` | Pre-load entire index into memory |

#### Examples

```bash
# Basic server
bigtent --rodeo /data/clusters/

# Multiple hosts and custom port
bigtent --rodeo /data/clusters/ --host 0.0.0.0 --host 127.0.0.1 --port 8080

# Pre-cache index for faster queries
bigtent --rodeo /data/clusters/ --cache-index true
```

### Merge Mode Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--fresh-merge <paths>...` | Path(s) | - | Directories containing clusters to merge |
| `--dest <path>` | Path | Required | Output directory for merged cluster |
| `--buffer-limit <n>` | usize | `10000` | Max items in merge queue |

#### Examples

```bash
# Basic merge
bigtent --fresh-merge /data/cluster1/ /data/cluster2/ --dest /data/merged/

# Memory-constrained merge
bigtent --fresh-merge /data/cluster1/ /data/cluster2/ \
    --dest /data/merged/ \
    --buffer-limit 5000
```

### General Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--threaded <bool>` | Boolean | - | Use threads when possible |
| `--help` | - | - | Display help information |
| `--version` | - | - | Display version information |

## Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `RUST_LOG` | Log level filter | `RUST_LOG=info`, `RUST_LOG=bigtent=debug` |
| `RUST_BACKTRACE` | Enable backtraces | `RUST_BACKTRACE=1` |

## Configuration Options Explained

### `--cache-index`

Controls how the index is loaded:

| Value | Memory | Startup | Query Latency |
|-------|--------|---------|---------------|
| `false` (default) | Low | Fast | Variable (may hit disk) |
| `true` | High | Slow | Consistent (all in RAM) |

**Recommendation**:
- Use `false` for clusters > 1GB or memory-constrained systems
- Use `true` for latency-sensitive applications with sufficient RAM

### `--buffer-limit`

Controls backpressure during merge operations:

- **Higher values**: More parallel processing, higher memory usage
- **Lower values**: Less memory, potentially slower merges

**Recommendation**:
- Default (10,000) works well for most systems
- Reduce to 1,000-5,000 for systems with < 8GB RAM
- Increase to 50,000+ for high-memory systems doing large merges

## Legacy Configuration File

> **Note**: The `config.toml` file format is deprecated. Use CLI arguments instead.

The legacy `config.toml` file supported:

```toml
# Path to the root of data storage
root_dir = "/data/bigtent"

# Path to cluster directory within root_dir
cluster_path = "clusters/production"
```

## Performance Tuning

### For Low Latency

```bash
bigtent --rodeo /data/clusters/ \
    --cache-index true \
    --host 0.0.0.0 \
    --port 3000
```

### For Memory Efficiency

```bash
bigtent --rodeo /data/clusters/ \
    --cache-index false
```

### For Large Merges

```bash
# Increase file descriptor limit first
ulimit -n 4096

bigtent --fresh-merge /data/cluster1/ /data/cluster2/ \
    --dest /data/merged/ \
    --buffer-limit 50000
```

## Magic Numbers and File Versions

BigTent uses food-themed magic numbers for file identification:

| File Type | Extension | Magic Number | Name | Version |
|-----------|-----------|--------------|------|---------|
| Cluster | `.grc` | `0xba4a4a` | "Banana" | 3 |
| Index | `.gri` | `0x54154170` | "Shishito" | 3 |
| Data | `.grd` | `0x00be1100` | "Bell" | 3 |

These constants are defined in:
- `src/rodeo/cluster.rs` - `ClusterFileMagicNumber`
- `src/rodeo/index.rs` - `IndexFileMagicNumber`
- `src/rodeo/data.rs` - `DataFileMagicNumber`

## File Size Limits

| Limit | Value | Constant Location |
|-------|-------|-------------------|
| Max data file size | 15 GB | `src/rodeo/writer.rs::MAX_DATA_FILE_SIZE` |
| Max index entries per file | 25M | `src/rodeo/writer.rs::MAX_INDEX_CNT` |

When these limits are reached during writes, new files are created automatically.
