# Big Tent

An opinionated Graph Database for serving millions of GitOIDs (Git Object Identifiers).

Big Tent stores software artifacts as **Items** connected by typed **Edges**, enabling
efficient queries about software composition, provenance, and dependencies.

## Quick Start

```bash
# Build
cargo build --release

# Run server with a cluster
./target/release/bigtent --rodeo /path/to/cluster/ --port 3000

# Query an item
curl http://localhost:3000/item/gitoid:blob:sha256:abc123...

# Get OpenAPI documentation
curl http://localhost:3000/openapi.json
```

For detailed setup instructions, see [GETTING_STARTED.md](GETTING_STARTED.md).

## Documentation

| Document | Description |
|----------|-------------|
| [GETTING_STARTED.md](GETTING_STARTED.md) | Installation, building, and first steps |
| [ARCHITECTURE.md](ARCHITECTURE.md) | System design, components, and data flow |
| [PERFORMANCE.md](PERFORMANCE.md) | Performance tuning and optimization |
| [info/config.md](info/config.md) | Configuration reference |
| [info/files_and_formats.md](info/files_and_formats.md) | File format specifications |

## Using BigTent

Big Tent is both a Rust crate (usable to read ADGs from files) and a server with a REST API.

### As a Server

```bash
# Serve a cluster directory
bigtent --rodeo /path/to/cluster/

# With custom host and port
bigtent --rodeo /path/to/cluster/ --host 0.0.0.0 --port 8080

# Pre-cache index for faster queries (uses more memory)
bigtent --rodeo /path/to/cluster/ --cache-index true
```

### As a Library

```rust
use bigtent::rodeo::goat::GoatRodeoCluster;
use bigtent::rodeo::goat_trait::GoatRodeoTrait;

let clusters = GoatRodeoCluster::cluster_files_in_dir(path, false, vec![]).await?;
if let Some(item) = clusters[0].item_for_identifier("gitoid:blob:sha256:...") {
    println!("Found: {:?}", item);
}
```

## API Endpoints

All endpoints are available at both `/` and `/omnibor/` prefixes.

### OpenAPI Specification

* `GET /openapi.json` - Full OpenAPI 3.1 specification (auto-generated from code)

### Item Retrieval

* `GET /item/{gitoid}` or `GET /item?identifier=...` - Get a single Item
* `POST /bulk` - Get multiple Items (POST array of GitOID strings)

### Alias Resolution

* `GET /aa/{gitoid}` or `GET /aa?identifier=...` - Resolve alias to canonical Item
* `POST /aa` - Bulk alias resolution

### Graph Traversal

* `GET /north/{gitoid}` - Find containers/builders (traverse upward via `build:up`, `alias:to`, `contained:up`)
* `POST /north` - Bulk north traversal
* `GET /north_purls/{gitoid}` - Same as north, but return only Package URLs
* `GET /flatten/{gitoid}` - Find contained items (traverse downward)
* `GET /flatten_source/{gitoid}` - Flatten with source information

### Metadata

* `GET /node_count` - Total items in the cluster
* `GET /purls` - Download all Package URLs as text file

### URL Encoding Note

In path parameters (`{gitoid}`), identifiers are **not** URL encoded. This allows
copy/pasting Package URLs directly without escaping.

## Merging Clusters

Combine multiple clusters into one:

```bash
bigtent --fresh-merge /path/to/cluster1/ /path/to/cluster2/ --dest /output/
```

See [PERFORMANCE.md](PERFORMANCE.md) for tuning merge operations.

## Creating Clusters

To create an OmniBOR Corpus (cluster files), use [Goat Rodeo](https://github.com/spice-labs-inc/goatrodeo).

## Hot Reload

Send `SIGHUP` to reload cluster files without restarting:

```bash
kill -HUP <bigtent_pid>
```

## Community

* [Matrix Discussion](https://matrix.to/#/#spice-labs:matrix.org)
* [GitHub Issues](https://github.com/spice-labs-inc/bigtent/issues)

## License

Apache 2.0
