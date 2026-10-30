# BigTent Architecture

This document describes the architecture of BigTent, an opinionated graph database
for software artifacts.

## Overview

BigTent stores software artifacts as **Items** connected by typed **Edges** in a
graph structure. Items are identified by GitOIDs (Git Object Identifiers) -
content-addressable hashes that uniquely identify each artifact.

```
┌─────────────────────────────────────────────────────────────────┐
│                        BigTent Server                           │
├──────────────────────────────┬──────────────────────────────────┤
│    REST API (Axum)           │      Lookup CLI                  │
│  V1: /item /aa /north        │   --lookup identifiers.json      │
│      /flatten /bulk /purls   │   --output results.json          │
│  V2: /v2/query /v2/health    │                                  │
│      /v2/node_count /v2/...  │                                  │
├──────────────────────────────┴──────────────────────────────────┤
│  api_v2 (V2 router)   highwire (query compiler + walk engine)   │
│  barker (Starlark programs)   ← depends on →   highwire         │
├──────────────────────────────────────────────────────────────────┤
│                   ClusterHolder<GRT> / GoatHerd                 │
│              (Hot-reload via ArcSwap)                           │
├─────────────────────────────────────────────────────────────────┤
│                   GoatRodeoTrait                                │
│            (Unified query interface)                            │
├──────────────┬──────────────────────┬───────────────────────────┤
│ GoatRodeo    │     GoatHerd         │    RoboticGoat            │
│ Cluster      │  (Multi-cluster)     │   (In-memory)             │
│ (File-based) │                      │                           │
└──────────────┴──────────────────────┴───────────────────────────┘
        │                │                      │
        ▼                ▼                      ▼
   .grc/.gri/.grd    Aggregates           Vec<Item>
      files          clusters             in memory
```

## Core Components

### 1. Data Model (`src/item.rs`)

#### Item

The fundamental unit of storage. Each Item represents a software artifact.

```rust
pub struct Item {
    pub identifier: String,           // GitOID (e.g., "gitoid:blob:sha256:...")
    pub connections: BTreeMap<String, BTreeSet<String>>, // ordered map: edge type → target set
    pub body_mime_type: Option<String>,
    pub body: Option<Value>,          // CBOR-encoded metadata
}
```

#### Edge Types

Edges connect Items and express relationships:

| Edge Type | Direction | Meaning |
|-----------|-----------|---------|
| `alias:to` | Forward | This item is an alias for the target |
| `alias:from` | Reverse | The target is an alias for this item |
| `contained:up` | Up | This item is contained by the target (e.g., file in package) |
| `contained:down` | Down | This item contains the target |
| `build:up` | Up | This item was built into the target |
| `build:down` | Down | This item was built from the target |
| `tag:to` | Forward | This item tags the target |
| `tag:from` | Reverse | This item is tagged by the target |

### 2. Storage Layer (`src/rodeo/`)

#### File Format Hierarchy

```
cluster_20240101_120000.grc    # Cluster file (root)
├── index_a1b2c3d4.gri         # Index file(s)
│   └── [key hash → data location]  (algorithm declared in the files)
└── data_e5f6g7h8.grd          # Data file(s)
    └── [CBOR-encoded Items]
```

#### Cluster File (.grc)

The root file containing metadata and references to index/data files.

```
┌────────────────────────────────┐
│ Magic: 0xba4a4a ("Banana")     │  4 bytes
├────────────────────────────────┤
│ Envelope Length                │  2 bytes
├────────────────────────────────┤
│ CBOR ClusterFileEnvelope       │  Variable
│  - version: u32                │
│  - data_files: Vec<u64>        │  SHA256 hashes
│  - index_files: Vec<u64>       │  SHA256 hashes
│  - info: Map<String, String>   │
└────────────────────────────────┘
```

#### Index File (.gri)

Maps identifier hashes to data file locations for O(log n) lookups.

```
┌────────────────────────────────┐
│ Magic: 0x54154170 ("Shishito") │  4 bytes
├────────────────────────────────┤
│ CBOR IndexEnvelope             │  Variable
├────────────────────────────────┤
│ Entry 1: [key][file][offset]   │  32 bytes each
│ Entry 2: ...                   │  (sorted by key bytes)
│ Entry N: ...                   │
└────────────────────────────────┘
```

Each entry: 16 bytes key + 8 bytes file hash + 8 bytes offset = 32 bytes
(version 4 keys are the first 16 bytes of BLAKE3; version 3 keys are MD5;
the algorithm is declared in the `.grc` and `.gri` files)

#### Data File (.grd)

Stores CBOR-encoded Items at specific offsets.

```
┌────────────────────────────────┐
│ Magic: 0x00be1100 ("Bell")     │  4 bytes
├────────────────────────────────┤
│ CBOR DataFileEnvelope          │  Variable
├────────────────────────────────┤
│ [len][CBOR Item 1]             │  Length-prefixed
│ [len][CBOR Item 2]             │
│ ...                            │
└────────────────────────────────┘
```

### 3. Query Layer (`src/rodeo/goat_trait.rs`)

The `GoatRodeoTrait` provides a unified interface for all cluster types:

```rust
pub trait GoatRodeoTrait: Send + Sync {
    fn item_for_identifier(&self, id: &str) -> Option<Item>;
    fn antialias_for(self: Arc<Self>, id: &str) -> Option<Item>;
    fn north_send(...) -> Receiver<Either<Item, String>>;
    fn stream_flattened_items(...) -> Receiver<Either<Item, String>>;
    fn roots(self: Arc<Self>) -> Receiver<Item>;
    // ...
}
```

#### Implementations

| Type | Description | Use Case |
|------|-------------|----------|
| `GoatRodeoCluster` | File-backed single cluster | Production serving |
| `GoatHerd` | Aggregates multiple clusters | Sharded data |
| `RoboticGoat` | In-memory synthetic cluster | Testing, computed data |

### 4. HTTP API Layer (`src/server.rs`)

Built on Axum with streaming support for large responses.

```
┌─────────────────────────────────────────────────┐
│                 Axum Router                      │
├─────────────────────────────────────────────────┤
│  /item/{gitoid}  ──────► serve_gitoid()         │
│  /aa/{gitoid}    ──────► serve_anti_alias()     │
│  /north/{gitoid} ──────► serve_north()          │
│  /bulk           ──────► serve_bulk()           │
│  ...                                            │
├─────────────────────────────────────────────────┤
│           ClusterHolder<GRT>                    │
│         (shared state via Arc)                  │
└─────────────────────────────────────────────────┘
```

## Query System

The query system is three tiers, each with its own module, and the dependency
direction is one-way: `api_v2 → barker → highwire → sansho + the accessors`.

### High Wire (`src/highwire/`) — the grammar and the walk engine

A query is a document of **stages**. A stage is either a **walk** — a traversal
of the graph — or a **barker** — Starlark source. The traits are in
`src/highwire/grammar.rs` (one per semantic level); the JSON reading of every
trait is in `src/highwire/grammar_impl_serde.rs`.

The walk engine (`src/highwire/walk.rs`) runs one walk: a stream of root
identifiers in, rows out, following every connection type at every item of the
frontier — a BFS over the union of the named edge types. Every channel is
`bounded(256)`, so a slow consumer backpressures its producer.

The compiler (`src/highwire/compiler.rs`) turns a document into a
`CompiledQuery`: every level is read and compiled once (a syntax error anywhere is
a located failure naming the stage and the level), the stages are ordered by their
dependencies (a cycle is a compile error), and the wires — a channel per edge, a
`tee` node per stage that fans out to every destination — are built. `CompiledQuery`
is `Send + Sync`, held as an `Arc` in a `QueryCache`, and runnable many times
concurrently.

Roots come from three forms: named outright (`"roots": ["…"]`), derived from
another stage's rows (`"roots": {"jars": "id"}`), or the caller's input stream
(`"roots": {"from-input": true}`) — which is what lets a query document be written
once and served many times.

### Barker (`src/barker/`) — Starlark at a pipeline boundary

Barker is [Starlark](https://github.com/facebook/starlark-rust), the hermetic
Python dialect: no recursion, no `while`, no `import`, no I/O. A barker stage
reads a stream of rows (`input`), runs arbitrary logic, and emits rows (`emit`).

It is an element in the pipeline and a starter of pipelines: `highwire.walk`,
`highwire.barker`, `highwire.from_stage`, `highwire.connection` and `highwire.run`
are natives a script can call to build and run a query of its own, against the
same cluster its parent was given. (See [Writing a Query](info/writing_queries.md).)

The namespaces: `math`, `stats`, `collections`, `patterns`, `digest` (gitoids and
Software Heritage ids via the `coordinates` crate), `dates`. Embedding
applications add their own natives — a `#[starlark_module]` function passed to
`barker::run_with`.

### The two HTTP surfaces

V1 (`src/server.rs`) is identifier-at-a-time: `/item`, `/aa`, `/north`,
`/north_purls`, `/flatten`, `/flatten_source`, `/bulk` — each backed by an
accessor, with bulk variants that fan over a list.

V2 (`src/api_v2.rs`) is graph-at-a-time: `GET /v2/query?identifier=…&template=…`
and `POST /v2/query {"roots": [...], "template"|"query"}` stream a query's rows as
JSON lines. The pre-canned templates replicate the V1 endpoints, and the two
surfaces are proven to agree: `tests/walk_over_a_graph.rs` and
`tests/serve_both_apis.rs` compare the V1 accessors' output sets with the V2
queries' output sets over the same fixture cluster, and every pair agrees.

V2 also carries the operational endpoints — `/v2/health`, `/v2/healthz`,
`/v2/readyz`, `/v2/metrics`, `/v2/node_count`, `/v2/purls` — and its own OpenAPI
document at `/v2/openapi.json`. There is no authentication: exposing the query
surface runs arbitrary traversal and arbitrary Starlark against the cluster, and
that is a trust decision for whoever serves it.

## Data Flow

### Read Path

```
1. HTTP Request: GET /item/gitoid:blob:sha256:abc123
                          │
2. Route Handler          ▼
   serve_gitoid() ───► ClusterHolder.get_cluster()
                          │
3. Trait Method           ▼
   item_for_identifier() ◄── GoatRodeoTrait
                          │
4. Index Lookup           ▼
   Binary search in .gri file (memory-mapped)
                          │
5. Data Fetch             ▼
   Read CBOR from .grd at offset
                          │
6. Response               ▼
   JSON serialized Item
```

### Write Path (Merge)

```
1. Load clusters from directories
                          │
2. Initialize ClusterWriter
                          │
3. Coordinator finds next item(s) to merge
   (items with same hash across clusters)
                          │
4. Worker threads fetch and merge items
                          │
5. Main thread writes to output:
   - Accumulate in buffer
   - Flush when size limit reached
   - Write .grd (data), .gri (index)
                          │
6. Finalize .grc (cluster) with all references
```

### Lookup Path (Batch CLI)

```
1. CLI: bigtent --rodeo /path --lookup ids.json
                          │
2. Parse Input            ▼
   Read JSON file → Vec<String> of identifiers
                          │
3. Load Clusters          ▼
   GoatRodeoCluster::cluster_files_in_dir() → GoatHerd
                          │
4. Batch Lookup           ▼
   For each identifier:
     GoatHerd.item_for_identifier(id) → Option<Item>
                          │
5. Serialize              ▼
   Build JSON map: {id: item.to_json() | null}
                          │
6. Output                 ▼
   Write to stdout or --output file
```

## Threading Model

### Query Pipelines

Every stage of a compiled query runs on its own thread, with a `flume` channel
per edge — `bounded(256)`, so a slow stage backpressures its producer rather than
buffering the stream. A walk blocks on its input stream; a barker runs on its own
thread because the Starlark evaluator is arena-bound and not `Send`. The caller's
collection runs against the output channel, which closes when the last stage
finishes.

### Server Mode

- Tokio multi-threaded runtime (100 worker threads)
- Each request handled asynchronously
- Memory-mapped files enable lock-free concurrent reads
- Hot-reload via `ArcSwap` for atomic cluster replacement

### Merge Mode

```
┌─────────────────┐
│   Coordinator   │  Finds items with matching hashes
│     Thread      │  across all input clusters
└────────┬────────┘
         │ sends (hash, clusters) via channel
         ▼
┌─────────────────┐
│  Worker Pool    │  20 threads fetch and merge items
│   (20 threads)  │  concurrently
└────────┬────────┘
         │ sends merged Items via channel
         ▼
┌─────────────────┐
│  Main Thread    │  Writes items to output cluster
│   (Writer)      │  with backpressure control
└─────────────────┘
```

## Memory Management

### Index Loading Strategies

| Strategy | Memory | Latency | Use Case |
|----------|--------|---------|----------|
| Lazy (default) | Low | Variable | Large clusters, memory-constrained |
| Pre-cached | High | Consistent | Small clusters, latency-sensitive |

### Memory-Mapped Files

- Index and data files are memory-mapped via `memmap2`
- OS manages page caching automatically
- Enables larger-than-RAM datasets
- Thread-safe concurrent access

### Merge Buffer Limit

The `--buffer-limit` parameter controls backpressure during merges:
- Limits items queued for writing
- Prevents memory exhaustion on large merges
- Default: 10,000 items

## Logging and Observability

### Logging Infrastructure

BigTent uses `tracing-subscriber` configured for JSON output. All logs go
to stderr as structured JSON, one object per line:

```
┌──────────────┐     ┌──────────────────────┐     ┌──────────┐
│  Application │────►│ tracing-subscriber   │────►│  stderr  │
│  log macros  │     │ (JSON formatter)     │     │  (JSON)  │
│  info!()     │     │                      │     │          │
│  error!()    │     │ Controlled by        │     │          │
│              │     │ RUST_LOG env var     │     │          │
└──────────────┘     └──────────────────────┘     └──────────┘
```

### Request Logging

The HTTP server includes a middleware (`request_log_middleware` in
`src/server.rs`) that logs every request:
- Request URI
- Response HTTP status code
- Elapsed time from request receipt to response

### Observability

BigTent provides:
- Prometheus `/metrics` endpoint (text exposition; `test_metrics_endpoint_responds`)
- Built-in health endpoints (`/health`, `/healthz`, `/readyz`)
- Structured JSON logs for log aggregation

BigTent does **not** currently provide OpenTelemetry tracing spans.

### Authentication

BigTent does not implement authentication or authorization at the
application level. All endpoints are publicly accessible. Access control
must be handled at the infrastructure layer (reverse proxy, firewall,
network policy).

## Error Handling

BigTent uses `anyhow::Result` for error propagation with context.

Common error scenarios:
- **Invalid file format**: Magic number mismatch, version incompatibility
- **Corrupted data**: Hash verification failure
- **Missing files**: Referenced index/data file not found
- **I/O errors**: Disk full, permission denied

## Extension Points

### Adding New Edge Types

1. Add constants to `src/item.rs`
2. Implement `EdgeType` trait methods
3. Update traversal logic if needed

### Custom Cluster Types

1. Implement `GoatRodeoTrait`
2. Optionally implement `ClusterRoboMember`
3. Wrap in `HerdMember` for herd compatibility

### New API Endpoints

1. Add handler function in `src/server.rs`
2. Add utoipa path annotation
3. Register route in `build_route()`
4. Add to `ApiDoc` paths list
