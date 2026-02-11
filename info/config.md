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

### Lookup Mode Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--rodeo <paths>` | Path(s) | - | Path(s) to `.grc` cluster files or directories (required) |
| `--lookup <path>` | Path | - | JSON file containing array of identifier strings to look up |
| `--output <path>` | Path | stdout | Output file for lookup results |
| `--cache-index <bool>` | Boolean | `false` | Pre-load entire index into memory |

#### Examples

```bash
# Basic lookup, output to stdout
bigtent --rodeo /data/clusters/ --lookup identifiers.json

# Lookup with file output
bigtent --rodeo /data/clusters/ --lookup identifiers.json --output results.json

# Lookup with pre-cached index for faster queries
bigtent --rodeo /data/clusters/ --lookup identifiers.json --cache-index true
```

#### Input Format

The lookup file must be a JSON array of identifier strings:

```json
["gitoid:blob:sha256:abc123...", "pkg:npm/lodash@4.17.21", "gitoid:blob:sha256:def456..."]
```

#### Output Format

The output is a JSON object mapping each identifier to its Item (as JSON) or `null` if not found:

```json
{
  "gitoid:blob:sha256:abc123...": {
    "identifier": "gitoid:blob:sha256:abc123...",
    "connections": [["contained:up", "gitoid:blob:sha256:..."]],
    "body_mime_type": "application/vnd.cc.goatrodeo",
    "body": { ... }
  },
  "pkg:npm/lodash@4.17.21": null,
  "gitoid:blob:sha256:def456...": { ... }
}
```

### General Arguments

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `--threaded <bool>` | Boolean | - | Use threads when possible |
| `--help` | - | - | Display help information |
| `--version` | - | - | Display version information |

## Environment Variables

BigTent uses only two runtime environment variables. All other configuration
is via CLI arguments -- no `config.toml` or other files are needed.

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `RUST_LOG` | Log level filter for `tracing-subscriber` | none (no logs) | `RUST_LOG=info` |
| `RUST_BACKTRACE` | Show stack traces on panics/errors | `0` (off) | `RUST_BACKTRACE=1` |

### `RUST_LOG` Filter Syntax

`RUST_LOG` uses the [`env_filter` directive syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html):

```bash
# Info-level from all crates
RUST_LOG=info

# Debug only from bigtent, info from everything else
RUST_LOG=info,bigtent=debug

# Warnings and errors only (quietest useful level)
RUST_LOG=warn

# Trace-level for the rodeo storage layer
RUST_LOG=bigtent::rodeo=trace
```

### Build-time Variables

These are embedded at compile time and cannot be changed at runtime:

| Variable | Description | Set By |
|----------|-------------|--------|
| `VERGEN_GIT_SHA` | Git commit SHA of the build | `vergen` build script |

The git SHA is logged at startup (`Starting big tent git sha ...`) and
embedded in cluster metadata during merges.

## Logging

BigTent outputs **structured JSON logs** to stderr via `tracing-subscriber`.
Each line is a self-contained JSON object:

```json
{"timestamp":"2025-04-19T17:10:26Z","level":"INFO","message":"Starting big tent git sha abc1234"}
{"timestamp":"2025-04-19T17:10:27Z","level":"INFO","message":"Loaded cluster production_2025"}
{"timestamp":"2025-04-19T17:10:28Z","level":"INFO","message":"Served /item/gitoid:blob:sha256:... response 200 OK time 1.234ms"}
```

### What Gets Logged

| Event | Level | Content |
|-------|-------|---------|
| Startup | INFO | Git SHA, loaded cluster names |
| HTTP request | INFO | URI, response status, elapsed time |
| Cluster load | INFO | Cluster name and file paths |
| Merge progress | INFO | Item counts, timing |
| Lookup progress | INFO | Identifier count, output path |
| File errors | ERROR | Path, error details |

### Metrics and Health Checks

BigTent does not expose a Prometheus or metrics endpoint. To collect metrics:
- Parse the structured JSON logs with a log aggregator (ELK, Datadog,
  Fluentd, CloudWatch, etc.)
- Use `GET /node_count` as a lightweight health check endpoint

## Authentication and Authorization

BigTent does **not** implement authentication or authorization. All HTTP
endpoints are open to any client that can reach the server.

For production deployments:
- Place BigTent behind a reverse proxy (nginx, Caddy, cloud ALB) for TLS
  and authentication
- Bind to `localhost` (the default) and use firewall rules
- In Docker/Kubernetes, use network policies to restrict access

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

> **Note**: The `config.toml` file format is **deprecated and no longer read
> by BigTent**. All configuration is now done via CLI arguments. If you have
> an existing `config.toml`, migrate its settings to CLI flags:

| `config.toml` field | CLI equivalent |
|---------------------|---------------|
| `root_dir` + `cluster_path` | `--rodeo /data/bigtent/clusters/production` |

The old format was:

```toml
# DEPRECATED - use CLI arguments instead
root_dir = "/data/bigtent"
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

## Complete CLI Quick Reference

```
bigtent [OPTIONS]

MODES (mutually exclusive):
  --rodeo <paths>                    Server mode: load clusters and start HTTP API
  --rodeo <paths> --lookup <file>    Lookup mode: batch identifier lookup, no server
  --fresh-merge <paths>...           Merge mode: combine clusters into a new one

SERVER OPTIONS:
  -r, --rodeo <paths>        Path(s) to .grc cluster files or directories
  -p, --port <port>          Port to bind to [default: 3000]
      --host <hostname>      Hostname(s) to bind to [default: localhost]
      --cache-index <bool>   Pre-load index into memory [default: false]

LOOKUP OPTIONS:
  -l, --lookup <path>        JSON file with array of identifier strings
  -o, --output <path>        Output file for results [default: stdout]

MERGE OPTIONS:
      --fresh-merge <paths>  Directories containing clusters to merge (2+ required)
      --dest <path>          Output directory for merged cluster (required)
  -b, --buffer-limit <n>     Max items in merge queue [default: 10000]

GENERAL:
      --threaded <bool>      Use threads when possible
  -h, --help                 Print help information
  -V, --version              Print version information

ENVIRONMENT:
  RUST_LOG=<filter>          Log level (e.g., info, bigtent=debug, warn)
  RUST_BACKTRACE=1           Enable stack traces on errors
```
