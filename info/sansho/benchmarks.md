# Sansho: benchmarks — how to read them

Sansho's benchmarks (`engine/sansho/benches/projection.rs`, run with
`cargo bench -p sansho`) measure selective evaluation over the CBOR byte
slice against alternatives, on the Appendix-A-shaped item class scaled
to real sizes (1,000 and 4,000 `file_names` entries; every third entry
gitoid-prefixed).

## The pipeline comparison (the serde-materialize-then-query versus the direct path)

`cargo bench -p sansho --bench comparison` runs the three-pipeline
comparison at four serialized-CBOR sizes (~10 KB, ~100 KB, ~1 MB,
~10 MB; the Appendix-A-shaped item): the outputs of all three are
asserted identical first (`tests/comparison_equality.rs`). The medians
from a reference run:

| size | expression | CBOR→serde→jmespath crate | CBOR→serde→sansho-materialized | sansho-cursor (direct) |
|------|-----------|---------------------------|-------------------------------|------------------------|
| 10 KB | prefix filter | 26.6µs | 16.4µs | 26.1µs |
| 10 KB | `identifier` | 17.5µs | 8.3µs | 3.8µs |
| 10 KB | `length(...)` | 18.1µs | 9.5µs | 6.9µs |
| 100 KB | prefix filter | 313µs | 176µs | 249µs |
| 100 KB | `identifier` | 197µs | 81µs | 32.5µs |
| 100 KB | `length(...)` | 190µs | 82.5µs | 75.1µs |
| 1 MB | prefix filter | 3.25ms | 1.58ms | 2.16ms |
| 1 MB | `identifier` | 1.92ms | 784µs | 315µs |
| 1 MB | `length(...)` | 1.87ms | 815µs | 655µs |
| 10 MB | prefix filter | 39.7ms | 17.7ms | 22.3ms |
| 10 MB | `identifier` | 23.9ms | 7.85ms | 2.94ms |
| 10 MB | `length(...)` | 26.1ms | 8.36ms | 5.66ms |

What this says:

1. **The simple projection from the byte slice is the direct path's
   win**: `identifier` runs 2–8× faster than the
   materialize-then-query pipelines, and the gap grows
   with the document size — the pipelines decode everything; the byte
   source walks the keys and stops.
2. **The complex filter over a huge array is where the byte source pays**:
   the prefix filter is slower than the sansho-materialized path at the
   large sizes (22.3ms against 17.7ms at 10 MB), though still faster than
   the `jmespath` crate (39.7ms), which pays the same bulk parse *and* an
   interpreter. The materialize-pipeline's cost is dominated by
   serde_cbor's bulk parse (a tight, optimized loop over
   the whole document); the byte source's positional walk (per-header,
   per-element) cannot match a bulk parser's throughput, even though
   it materializes a third of the nodes.
3. **Sansho's walk program beats the `jmespath` crate's interpreter**
   on the same materialized input at every size and expression
   (1.6–3.1× in these runs).

The engineering conclusion: choose the cursor for the
select-the-small-things-from-big-documents pattern (the memory and the
wire wins, plus the fastest simple projections); a bulk-materialize
pipeline remains competitive for whole-array filters — and the memory
advantage (the unselected payloads never materialize — as demonstrated
by `selective_materialization_budget`) holds regardless.

## Running them (the profile discipline)

- `cargo bench -p sansho` — the benches compile with the **bench
  profile** (optimized); the timings are release-profile timings.
- The `source_paths` suite (the plan's Phase 0 baseline + Phase 5's
  extension) compares the data paths across the scan, seek, and
  full-item shapes at 10 / 1k / 25k / 100k / 500k connections (the
  500k arm = the log4shell-scale item class). The recorded baselines
  and gate ratios live in the execution records.
- The GATE tests (`benchmark_gate.rs`, `phase5_gates.rs`) run under
  `cargo test` (the debug profile) — their assertions are
  ratio-and-structure based (both sides share the profile), and they
  are verified under release too: `cargo test -p sansho --release`.
  Both profiles green (as demonstrated by the full test-suite and the
  release run of the gate files).
- The worst-case-legal-expression's one-second bound holds under BOTH
  profiles (the debug run and the release run — the release is three
  orders of magnitude under it).

## The four measurements

| Benchmark | What it measures |
|-----------|-----------------|
| `prefix_filter/cursor/N` | the canonical use case: the gitoid-prefix filter over the byte slice |
| `prefix_filter/materialized/N` | the same expression through the materialized backend (the input pre-parsed — the parse cost is NOT in this number) |
| `full_item/cursor/N` | the `@` projection: everything materialized (one bulk decode) |
| `no_selection/cursor/N` | a selection that skips the huge array (the walk plus the machinery, minimal materialization) |

## What the numbers say (and what they do NOT say)

1. **The selectivity is the point, not the stopwatch.** The cursor's
   win is ALLOCATION and WIRE: the prefix filter's output is ~35% of the
   full item's materialized nodes, and the unselected payloads never
   materialize at all (as demonstrated by
   `benchmark_gate_prefix_filter_selectivity` and
   `selective_materialization_budget`). The byte-slice input means the
   caller never pays to decode what it will not ship.
2. **A CBOR walk is not free.** A CBOR map carries element COUNTS, not
   byte lengths: reaching a late key means walking past the earlier
   values' headers. At the 4,000-entry scale that walk costs about as
   much as a bulk decode. This is the format's inherent positional cost,
   measured by the benchmarks — not a defect, and not what the gates
   bound.
3. **The gates bound the machinery, not the format.** The selectivity
   ratio (the output-nodes of the prefix filter against the full path —
   as demonstrated by `benchmark_gate_prefix_filter_selectivity`), and
   the zero-copy baseline itself: the current-node expression `@`
   compiles to an empty spine that decodes nothing, so its cost must not
   track the document it is evaluated against (as demonstrated by
   `benchmark_gate_no_selection_scale_invariance`). A single-key
   selection is asserted to be at least five times slower than that
   baseline (as demonstrated by
   `benchmark_gate_no_selection_not_slower`). That ratio is a LOWER
   bound by design: a baseline sitting at the timer's noise floor cannot
   support an upper bound, because an upper bound would measure the
   clock rather than the evaluator. A lower bound is tripped when the
   walk gets FASTER, so a failure after an optimization means the
   baseline moved, not that the evaluator broke.

## The protocol

The gate tests time 21 samples and take the MEDIAN (robust to machine
noise). Where one evaluation would be below the timer's resolution, the
evaluation is repeated until a time budget elapses and the total is
divided by the iterations; the rounds are interleaved across the sizes
being compared, so every size sees the same conditions. Both measures
are demonstrated by `benchmark_gate_no_selection_scale_invariance`. The
worst-case legal expression (the deepest legal nesting) must evaluate
promptly (as demonstrated by
`benchmark_gate_worst_case_legal_expression`). Criterion records the
full distributions under `target/criterion/` for before-and-after
comparison across changes.
