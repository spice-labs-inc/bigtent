# Getting Started with BigTent

This guide will help you build, run, and start developing with BigTent.

## Prerequisites

- **Rust**: Version 1.86.0 or later
- **Git**: For cloning the repository
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

The binary will be at `target/release/bigtent`.

### Run Tests

```bash
cargo test
```

## Running BigTent

BigTent has two primary modes of operation:

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

## Environment Variables

- `RUST_LOG` - Log level (e.g., `RUST_LOG=info`, `RUST_LOG=debug`)
- `RUST_BACKTRACE` - Enable backtraces on errors (`RUST_BACKTRACE=1`)

## Next Steps

- Read [ARCHITECTURE.md](ARCHITECTURE.md) for system design details
- See [info/files_and_formats.md](info/files_and_formats.md) for file format specifications
- Check [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines
- Browse the [OpenAPI spec](/openapi.json) for full API documentation
