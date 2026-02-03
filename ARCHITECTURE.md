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
├─────────────────────────────────────────────────────────────────┤
│                     REST API (Axum)                             │
│   /item  /aa  /north  /flatten  /bulk  /purls  /openapi.json   │
├─────────────────────────────────────────────────────────────────┤
│                   ClusterHolder<GRT>                            │
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
    pub connections: BTreeSet<Edge>,  // Typed edges to other items
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
│   └── [MD5 hash → data location]
└── data_e5f6g7h8.grd          # Data file(s)
    └── [CBOR-encoded Items]
```

#### Cluster File (.grc)

The root file containing metadata and references to index/data files.

```
┌────────────────────────────────┐
│ Magic: 0xba4a4a ("Banana")     │  4 bytes
├────────────────────────────────┤
│ Envelope Length                │  4 bytes
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
│ Entry 1: [MD5][file][offset]   │  32 bytes each
│ Entry 2: ...                   │  (sorted by MD5)
│ Entry N: ...                   │
└────────────────────────────────┘
```

Each entry: 16 bytes MD5 + 8 bytes file hash + 8 bytes offset = 32 bytes

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

## Threading Model

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
