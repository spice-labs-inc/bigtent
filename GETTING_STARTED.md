# Getting Started with BigTent

This guide will help you build, run, and start developing with BigTent.

## Prerequisites

- **Rust**: Version 1.86.0 or later (only needed when building from source)
- **Git**: For cloning the repository
- **Docker**: Alternative to building from source (see [Docker Deployment](#docker-deployment))
- **System Requirements**:
  - Linux, macOS, or Windows with WSL2
  - At least 4 GB RAM (more for large clusters)
  - SSD recommended for performance

### Installing Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env
rustup update
```

Verify your Rust version:
```bash
rustc --version  # Should be 1.86.0 or later
```

## Building BigTent

### Clone the Repository

```bash
git clone https://github.com/spice-labs-inc/bigtent.git
cd bigtent
```

### Build in Development Mode

```bash
cargo build
```

### Build for Production

```bash
cargo build --release
```

The binary will be at `target/release/bigtent`. This is a statically linked,
optimized binary suitable for production deployment. There is no separate
"install" step -- copy the binary to your server and run it directly.

### Run Tests

```bash
cargo test
```

## Docker Deployment

BigTent publishes multi-architecture Docker images (linux/amd64, linux/arm64)
to Docker Hub as `spicelabs/bigtent`.

### Using a Pre-built Image

```bash
# Pull the latest release
docker pull spicelabs/bigtent:latest

# Or pull a specific version
docker pull spicelabs/bigtent:0.13.0

# Run the server, mounting your cluster data
docker run -p 3000:3000 \
    -v /path/to/your/clusters:/data \
    spicelabs/bigtent --rodeo /data --host 0.0.0.0 --port 3000

# Run a batch lookup
docker run \
    -v /path/to/your/clusters:/data \
    -v /path/to/identifiers.json:/identifiers.json \
    spicelabs/bigtent --rodeo /data --lookup /identifiers.json

# Merge clusters
docker run \
    -v /path/to/cluster1:/cluster1 \
    -v /path/to/cluster2:/cluster2 \
    -v /path/to/output:/output \
    spicelabs/bigtent --fresh-merge /cluster1 /cluster2 --dest /output
```

### Building the Docker Image Locally

```bash
docker build -t bigtent .
docker run -p 3000:3000 -v /path/to/clusters:/data \
    bigtent --rodeo /data --host 0.0.0.0 --port 3000
```

The Dockerfile uses a multi-stage Alpine build: the first stage compiles
the release binary, and the second stage copies only the binary into a
minimal Alpine image.

## Understanding Identifiers

BigTent uses two types of identifiers as primary keys for Items:

### GitOIDs (Git Object Identifiers)

A [GitOID](https://www.iana.org/assignments/uri-schemes/prov/gitoid) is a
content-addressable identifier based on hashing a software artifact the same
way Git hashes objects. The format is:

```
gitoid:<object_type>:<hash_algorithm>:<hex_digest>
```

For example:
```
gitoid:blob:sha256:fee53a18d32820613c0527aa79be5cb30173c823a9b448fa4817767cc84c6f03
```

Where:
- `gitoid` -- the URI scheme
- `blob` -- the Git object type (almost always `blob` for file content)
- `sha256` -- the hash algorithm
- `fee53a...` -- the full SHA-256 hex digest (64 characters)

GitOIDs are the foundation of the [OmniBOR](https://omnibor.io/) specification
for tracking software artifact identity.

### Package URLs (PURLs)

BigTent also supports [Package URLs](https://github.com/package-url/purl-spec)
as identifiers for well-known software packages. The format is:

```
pkg:<type>/<namespace>/<name>@<version>
```

For example:
```
pkg:npm/lodash@4.17.21
pkg:maven/org.apache.logging.log4j/log4j-core@2.17.0
pkg:docker/red-kibble-1@1.0.0
```

PURLs typically appear as aliases that resolve to their underlying GitOID.
Use the `/aa` (anti-alias) endpoint to resolve a PURL to its canonical Item.

## Cluster Directory Structure

BigTent reads data from **cluster directories** -- directories containing a
set of related `.grc`, `.gri`, and `.grd` files that together form an
OmniBOR Corpus.

### What a Valid Cluster Directory Looks Like

```
my_cluster/
├── 2025_04_19_17_10_26_012a73d9c40dc9c0.grc   # Cluster file (entry point)
├── 674f1be2154b27d1.gri                         # Index file
├── 432e473a3c3e5630.grd                         # Data file
├── purls.txt                                     # Package URLs (optional)
└── history.jsonl                                 # Cluster history (optional)
```

**Required files:**
- **`.grc` (Cluster file)** -- the entry point that references all index and
  data files. Named with a timestamp prefix
  (`YYYY_MM_DD_HH_MM_SS_<hash>.grc`) so the most recent cluster is easy to
  identify. A directory may contain multiple `.grc` files; BigTent loads
  the most recent.
- **`.gri` (Index files)** -- sorted indexes that map MD5 hashes of
  identifiers to data file locations. Named `<hash>.gri`.
- **`.grd` (Data files)** -- CBOR-encoded Items stored at specific offsets.
  Named `<hash>.grd`.

**Optional files:**
- **`purls.txt`** -- a plain text file listing all Package URLs in the
  cluster, one per line. Served by the `GET /purls` endpoint.
- **`history.jsonl`** -- JSON Lines file tracking the merge/creation
  history of the cluster.

A cluster can contain multiple index and data files. Large datasets are
automatically split across multiple files during creation and merging.

### Creating Clusters

Clusters are created by [Goat Rodeo](https://github.com/spice-labs-inc/goatrodeo),
which scans software artifacts and produces the `.grc`/`.gri`/`.grd` files.
BigTent itself creates new clusters only during merge operations
(`--fresh-merge`).

For detailed file format specifications, see
[info/files_and_formats.md](info/files_and_formats.md).

## Running BigTent

BigTent has three modes of operation:

### 1. Server Mode (Rodeo)

Serve an existing cluster over HTTP:

```bash
# Serve a single cluster directory
./target/release/bigtent --rodeo /path/to/cluster/

# Specify host and port
./target/release/bigtent --rodeo /path/to/cluster/ --host 0.0.0.0 --port 8080

# Pre-cache index for faster queries (uses more memory)
./target/release/bigtent --rodeo /path/to/cluster/ --cache-index true
```

### 2. Merge Mode

Merge multiple clusters into a new one:

```bash
./target/release/bigtent \
    --fresh-merge /path/to/cluster1/ /path/to/cluster2/ \
    --dest /path/to/output/

# Adjust buffer limit for memory-constrained systems
./target/release/bigtent \
    --fresh-merge /path/to/cluster1/ /path/to/cluster2/ \
    --dest /path/to/output/ \
    --buffer-limit 5000
```

### 3. Lookup Mode

Look up a batch of identifiers against loaded clusters without starting a server:

```bash
# Create a JSON file with identifiers to look up
echo '["gitoid:blob:sha256:abc123...", "unknown_id"]' > identifiers.json

# Look up identifiers, output to stdout
./target/release/bigtent --rodeo /path/to/cluster/ --lookup identifiers.json

# Look up identifiers, output to file
./target/release/bigtent --rodeo /path/to/cluster/ \
    --lookup identifiers.json \
    --output results.json

# With pre-cached index for faster lookups
./target/release/bigtent --rodeo /path/to/cluster/ \
    --lookup identifiers.json \
    --cache-index true
```

## Your First API Query

Once the server is running, you can query it:

### Get Server Info

```bash
# Get total item count
curl http://localhost:3000/node_count
```

### Retrieve an Item

```bash
# Get item by GitOID
curl http://localhost:3000/item/gitoid:blob:sha256:abc123...

# Or use query parameter
curl "http://localhost:3000/item?identifier=gitoid:blob:sha256:abc123..."
```

### Resolve Aliases

```bash
# Follow alias chains to find canonical item
curl http://localhost:3000/aa/gitoid:blob:sha256:abc123...
```

### Graph Traversal

```bash
# Find what contains/builds this item (traverse "north")
curl http://localhost:3000/north/gitoid:blob:sha256:abc123...

# Find what this item contains (traverse "south"/flatten)
curl http://localhost:3000/flatten/gitoid:blob:sha256:abc123...
```

### Bulk Operations

```bash
# Retrieve multiple items at once
curl -X POST http://localhost:3000/bulk \
    -H "Content-Type: application/json" \
    -d '["gitoid:blob:sha256:abc...", "gitoid:blob:sha256:def..."]'
```

### OpenAPI Documentation

```bash
# Get the full API specification
curl http://localhost:3000/openapi.json
```

## Using BigTent as a Library

Add BigTent to your `Cargo.toml`:

```toml
[dependencies]
bigtent = { git = "https://github.com/spice-labs-inc/bigtent.git" }
```

### Example: Loading and Querying a Cluster

```rust
use bigtent::rodeo::goat::GoatRodeoCluster;
use bigtent::rodeo::goat_trait::GoatRodeoTrait;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load clusters from a directory
    let path = PathBuf::from("/path/to/cluster/");
    let clusters = GoatRodeoCluster::cluster_files_in_dir(
        path,
        false,  // don't pre-cache index
        vec![], // no exclusions
    ).await?;

    // Query for an item
    for cluster in clusters {
        if let Some(item) = cluster.item_for_identifier("gitoid:blob:sha256:...") {
            println!("Found: {:?}", item);
        }
    }

    Ok(())
}
```

### Example: Accessing the OpenAPI Spec

```rust
use bigtent::ApiDoc;
use utoipa::OpenApi;

fn main() {
    let spec = ApiDoc::openapi();
    let json = serde_json::to_string_pretty(&spec).unwrap();
    println!("{}", json);
}
```

## Project Structure

```
bigtent/
├── src/
│   ├── lib.rs          # Library entry point
│   ├── main.rs         # Binary entry point
│   ├── config.rs       # CLI configuration
│   ├── item.rs         # Core data model (Item, Edge)
│   ├── server.rs       # HTTP API (Axum)
│   ├── fresh_merge.rs  # Cluster merging
│   ├── util.rs         # Utilities
│   └── rodeo/          # Storage layer
│       ├── goat.rs         # File-backed cluster
│       ├── goat_herd.rs    # Multi-cluster aggregation
│       ├── goat_trait.rs   # Core trait
│       ├── robo_goat.rs    # In-memory cluster
│       ├── holder.rs       # Runtime management
│       ├── cluster.rs      # .grc file format
│       ├── index.rs        # .gri file format
│       ├── data.rs         # .grd file format
│       ├── writer.rs       # Cluster writing
│       └── member.rs       # HerdMember enum
├── info/
│   ├── files_and_formats.md  # Detailed file format docs
│   └── config.md             # Configuration reference
├── ARCHITECTURE.md    # Architecture overview
├── CONTRIBUTING.md    # Contribution guidelines
└── README.md          # Project overview
```

## Common Issues

### "Too many open files"

When merging many clusters:
```bash
ulimit -n 4096
```

### High Memory Usage

- Use `--cache-index false` (default) for large clusters
- Reduce `--buffer-limit` during merges
- Consider sharding data across multiple clusters

### Slow Queries

- Enable index pre-caching: `--cache-index true`
- Ensure data is on SSD
- Check if binary search is hitting disk (memory pressure)

## Authentication and Security

BigTent does **not** include built-in authentication or authorization.
All HTTP endpoints are open and accessible to any client that can reach the
server. (All endpoints are read-only — even the POST endpoints only perform lookups.)

For production deployments, secure BigTent at the infrastructure level:

- **Reverse proxy**: Place BigTent behind nginx or Caddy for TLS termination
  and authentication. Ready-to-use configs are provided in
  [`examples/nginx.conf`](examples/nginx.conf) and
  [`examples/Caddyfile`](examples/Caddyfile).
- **Network isolation**: Bind to `localhost` (the default) or a private
  network interface, and use firewall rules to restrict access.
- **Container networking**: When running in Docker/Kubernetes, use network
  policies to control which services can reach BigTent.
- **Docker Compose**: The included [`docker-compose.yml`](docker-compose.yml)
  bundles BigTent with Caddy for automatic HTTPS.

For detailed TLS configuration, mTLS guidance, and authentication examples,
see the [Operations Guide](info/operations.md#tls-and-authentication).

## Environment Variables

BigTent is configured entirely via CLI arguments. No `config.toml` or other
configuration files are needed. Only two environment variables are recognized
at runtime:

| Variable | Description | Example |
|----------|-------------|---------|
| `RUST_LOG` | Controls log verbosity via `tracing-subscriber` | `RUST_LOG=info` |
| `RUST_BACKTRACE` | Enables stack traces on errors | `RUST_BACKTRACE=1` |

`RUST_LOG` filter examples:

```bash
# Show info-level messages from all crates
RUST_LOG=info ./target/release/bigtent --rodeo /data/clusters/

# Show debug messages only from bigtent code
RUST_LOG=bigtent=debug ./target/release/bigtent --rodeo /data/clusters/

# Show warnings and above (quietest useful level)
RUST_LOG=warn ./target/release/bigtent --rodeo /data/clusters/
```

> **Note**: The `config.toml` file format referenced in older documentation
> is deprecated and no longer used. All configuration is via CLI arguments.

## Logging

BigTent outputs structured JSON logs to stderr via `tracing-subscriber`.
Each log line is a JSON object suitable for ingestion by log aggregation
tools (ELK, Datadog, CloudWatch, etc.).

Example log output:

```json
{"timestamp":"2025-04-19T17:10:26Z","level":"INFO","message":"Starting big tent git sha abc1234"}
{"timestamp":"2025-04-19T17:10:27Z","level":"INFO","message":"Loaded cluster my_cluster"}
{"timestamp":"2025-04-19T17:10:28Z","level":"INFO","message":"Served /item/gitoid:blob:sha256:... response 200 OK time 1.234ms"}
```

Every HTTP request is logged with its URI, response status code, and elapsed time.

### Metrics and Health

BigTent exposes a `GET /metrics` endpoint in Prometheus text exposition format,
providing request counts, latency histograms, node/cluster gauges, and process
metrics (RSS, CPU, open FDs).

Health endpoints for load balancers and orchestrators:

- `GET /healthz` — Liveness probe (always 200 if the process is running)
- `GET /readyz` — Readiness probe (200 if clusters are loaded, 503 otherwise)
- `GET /health` — Detailed JSON with version, git SHA, node count, uptime, and status

For log volume estimates, Docker log driver configuration, logrotate setup,
monitoring/alerting rules, and a complete operational runbook, see the
[Operations Guide](info/operations.md).

## Next Steps

- Read [ARCHITECTURE.md](ARCHITECTURE.md) for system design details
- See [info/files_and_formats.md](info/files_and_formats.md) for file format specifications
- Check [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines
- Browse the [OpenAPI spec](/openapi.json) for full API documentation
