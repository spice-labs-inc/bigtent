# Performance Guide

This document describes BigTent's performance characteristics and tuning options.

## Memory Usage

### Index Loading Strategies

| Strategy | Flag | Memory | Startup | Query Latency |
|----------|------|--------|---------|---------------|
| Lazy (default) | `--cache-index false` | O(1) | Fast | Variable |
| Pre-cached | `--cache-index true` | O(n) | Slow | Consistent |

**Lazy Loading** (default):
- Index files are memory-mapped
- Pages loaded on demand by OS
- Best for large clusters or memory-constrained systems
- Query latency depends on OS page cache state

**Pre-cached Loading**:
- Entire index loaded into HashMap at startup
- Startup time proportional to index size
- Consistent O(1) lookup after startup
- Best for latency-sensitive applications with sufficient RAM

### Memory Estimates

| Component | Memory Usage |
|-----------|-------------|
| Index entry | 32 bytes |
| 1M items (lazy) | ~32 MB mapped (actual usage varies) |
| 1M items (cached) | ~100-150 MB |
| Item in memory | ~500 bytes - 10 KB (varies with metadata) |

### Merge Memory Usage

During merges, memory is consumed by:
- Source cluster index mappings
- In-flight items (controlled by `--buffer-limit`)
- Output buffers (data + index)

**Recommendations**:
| System RAM | `--buffer-limit` |
|------------|------------------|
| 4 GB | 1,000 - 2,000 |
| 8 GB | 5,000 - 10,000 |
| 16 GB | 10,000 - 25,000 |
| 32+ GB | 25,000 - 100,000 |

## Query Performance

### Single Item Lookup

| Operation | Complexity | Typical Latency |
|-----------|------------|-----------------|
| `item_for_identifier()` | O(log n) | < 1ms (cached) |
| Index binary search | O(log n) | ~10-100µs |
| Data file read | O(1) | ~100µs - 1ms |

### Graph Traversal

| Operation | Complexity | Notes |
|-----------|------------|-------|
| `north_send()` | O(k) | k = reachable items |
| `flatten()` | O(k) | k = contained items |
| `antialias_for()` | O(d) | d = alias chain depth |
| `roots()` | O(n) | **Expensive**: scans all items |

### Bulk Operations

Bulk endpoints (`/bulk`, `/aa` POST) are more efficient than multiple
single requests due to:
- Reduced HTTP overhead
- Better connection reuse
- Streaming responses

## Disk I/O

### File Access Patterns

BigTent uses memory-mapped files for efficient I/O:

```
Index lookup:  Sequential scan with binary search jumps
Data read:     Random access at specific offsets
```

**Optimization**: Place cluster files on SSD for best random read performance.

### File Size Limits

| File Type | Max Size | Rationale |
|-----------|----------|-----------|
| Data (.grd) | 15 GB | Manageable memory mapping |
| Index (.gri) | ~800 MB (25M entries) | Fits in RAM for pre-caching |

When limits are reached, new files are created automatically.

## Threading

### Server Mode

- Tokio runtime with 100 worker threads
- Each request handled asynchronously
- Memory-mapped files enable lock-free concurrent reads

### Merge Mode

| Thread Type | Count | Role |
|-------------|-------|------|
| Coordinator | 1 | Finds items to merge |
| Workers | 20 | Fetch and merge items |
| Writer | 1 (main) | Write output cluster |

Communication via bounded channels prevents memory exhaustion.

## Benchmarking

### Quick Performance Check

```bash
# Time a bulk query
time curl -s -X POST http://localhost:3000/bulk \
    -H "Content-Type: application/json" \
    -d '["gitoid1", "gitoid2", ...]' > /dev/null

# Check node count (verifies cluster is loaded)
curl http://localhost:3000/node_count
```

### Profiling with perf

```bash
# Record CPU profile during operation
perf record -g ./target/release/bigtent --rodeo /data/clusters/

# Analyze results
perf report
```

### Memory Profiling

```bash
# Using heaptrack
heaptrack ./target/release/bigtent --rodeo /data/clusters/
heaptrack_gui heaptrack.bigtent.*.gz
```

## Optimization Checklist

### For Low Latency

- [ ] Use SSD storage
- [ ] Enable index pre-caching (`--cache-index true`)
- [ ] Ensure sufficient RAM for working set
- [ ] Use bulk endpoints for multiple items

### For High Throughput

- [ ] Place hot clusters on fast storage
- [ ] Monitor OS page cache hit rate
- [ ] Consider sharding across multiple instances

### For Large Merges

- [ ] Increase file descriptor limit: `ulimit -n 4096`
- [ ] Tune `--buffer-limit` for available RAM
- [ ] Use fast storage for output directory
- [ ] Monitor for OOM conditions

### For Memory-Constrained Systems

- [ ] Use lazy index loading (default)
- [ ] Reduce `--buffer-limit` during merges
- [ ] Consider smaller cluster shards
- [ ] Monitor swap usage

## Monitoring

### Key Metrics

| Metric | How to Check | Target |
|--------|--------------|--------|
| Response latency | Application logs | < 100ms p99 |
| Memory usage | `top` / `htop` | < 80% available |
| Open files | `lsof -p <pid> | wc -l` | < ulimit |
| Disk I/O | `iostat` | < 80% utilization |

### Log Output

BigTent logs include timing information:

```json
{"timestamp":"...","level":"INFO","message":"Served /item response 200 time 1.234ms"}
{"timestamp":"...","level":"INFO","message":"North: Sent 1,234 items in 567ms"}
```

Use `RUST_LOG=info` or `RUST_LOG=bigtent=debug` for varying detail levels.
