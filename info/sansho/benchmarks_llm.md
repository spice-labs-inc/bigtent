# Sansho: benchmarks — LLM copy

LLM-readable counterpart of `benchmarks.md`. Same content, structured for
retrieval: the suites, the measurements, the gates, and what the numbers do
and do not support.

## Suites and how to run them

- `cargo bench -p sansho` — the criterion suites, under the bench profile.
- `cargo bench -p sansho --bench comparison` — the three-pipeline comparison.
- The GATE tests live in `tests/benchmark_gate.rs` and `tests/phase5_gates.rs`
  and run under `cargo test`; their assertions are ratio- and structure-based,
  so both sides share the profile.

## The three-pipeline comparison

`tests/comparison_equality.rs` first asserts that all three pipelines produce
identical output; only then are the timings meaningful. Four serialized-CBOR
sizes (~10 KB, ~100 KB, ~1 MB, ~10 MB), two expressions. Reference medians:

| size | expression | CBOR→serde→`jmespath` | CBOR→serde→sansho | sansho direct |
|------|-----------|----------------------|-------------------|---------------|
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

Three conclusions, in order of importance:

1. **Simple projections from the byte slice win.** `identifier` is 2–8× faster
   than either materialize-then-query pipeline, and the gap grows with the
   document. The pipelines decode the whole document; the byte source walks
   the keys and stops.
2. **Whole-array filters are where the byte source loses.** The prefix filter
   is slower than the sansho-materialized path at the large sizes (22.3ms
   against 17.7ms at 10 MB), though still faster than the `jmespath` crate
   (39.7ms), which pays the same bulk parse *and* an interpreter. The
   pipelines pay serde_cbor's bulk parse, a tight loop over the whole
   document; the byte source's positional walk is per-header and per-element
   and cannot match a bulk parser's throughput even though it materializes a
   third of the nodes.
3. **Sansho's compiled program beats the `jmespath` crate's interpreter** on
   the same materialized input, at every size and expression (1.6–3.1×).

Engineering conclusion: choose the byte source for select-the-small-things-
from-big-documents (memory and wire wins, plus the fastest simple
projections); a bulk-materialize pipeline stays competitive for whole-array
filters. The memory advantage holds regardless — unselected payloads never
materialize (`selective_materialization_budget`).

## The gates

| test | asserts |
|------|---------|
| `benchmark_gate_prefix_filter_selectivity` | the prefix filter's materialized-node count is at most 35% of the full path's (the representative shape measures 0.333) |
| `benchmark_gate_no_selection_not_slower` | a single-key selection is at least **5×** slower than `@`, the zero-copy identity |
| `benchmark_gate_no_selection_scale_invariance` | `@`'s cost does not track the document: 1,000 → 64,000 entries, spread ≤ 2× |
| `benchmark_gate_worst_case_legal_expression` | a depth-60 legal expression evaluates promptly (under 1 second) |

Two properties of the second gate that are easy to misread:

- The bound is a **lower** bound on a ratio, not a performance target. `@`
  compiles to an empty spine and decodes nothing, so its cost is at the
  timer's noise floor and an upper bound would measure the clock.
- A lower bound is tripped when the walk gets **faster** — an index, a smarter
  skip. A failure after an optimization means the baseline moved, not that the
  evaluator broke.

## The protocol

- Median of 21 samples (robust to machine noise).
- Where one evaluation is below the timer's resolution, the evaluation is
  repeated until a time budget elapses and the total is divided by the
  iterations; a single `@` evaluation is ~100 ns, which is at the timer's
  resolution.
- The rounds are **interleaved** across the sizes being compared. Measuring
  each size in sequence made the first size look 3.4× slower than the last
  (CPU frequency ramp, not evaluation cost).

## What the numbers do NOT say

- A CBOR map carries element **counts**, not byte lengths, so reaching a late
  key costs the walk past every earlier value's header. At the 4,000-entry
  scale that walk costs about as much as a bulk decode. This is the format's
  inherent positional cost, measured by the benchmarks, not bounded by the
  gates.
- The selectivity metric is the **materialized-node** count of the outputs,
  not the number of byte positions read. A bulk decode of `@` is one position
  but ~485K nodes, so a position count cannot express selectivity.

## Reading the criterion output

Criterion writes the full distributions under `target/criterion/`, so a change
can be compared against a previous run rather than against a single number.
The numbers in the tables above are reference medians from one machine, not
thresholds: only the gate tests assert.
