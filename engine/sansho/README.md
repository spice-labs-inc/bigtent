# Sansho — a JMESPath projection engine for single CBOR documents

Sansho evaluates a [JMESPath](https://jmespath.org) expression against
one CBOR document and returns a JSON value containing only the parts of
the document the expression selects. It is specified by
`specs/0001-jmespath-projection.md` and implements the complete
JMESPath language, conformance-gated by the vendored JMESPath compliance
corpus.

Sansho is developed as a stand-alone crate in this repository
(`engine/sansho/`) and will be extracted to its own project; it depends
on nothing in this repository (architecture decision record 0001:
`plans/adr/0001_sansho_workspace.md`).

## The one-minute version

```rust
use sansho::{compile, evaluate_cbor, parse};

// the item's stored CBOR bytes (a byte slice; no decode has happened)
let item_bytes: &[u8] = /* the item's CBOR */;

let program = sansho::compile(&parse("body.file_names[?starts_with(@, 'gitoid:')]")?)?;
let selected = evaluate_cbor(&program, item_bytes)?;
```

The expression is parsed once and compiled once; programs are cached by
the canonicalized expression and the engine version. Evaluation reads
only the parts of the document the expression selects: a projection over
a 4,000-entry `file_names` array that selects one small field per entry
never materializes the unselected payloads (as demonstrated by
`selective_materialization_budget`).

## What Sansho guarantees

- **Complete JMESPath conformance** — the official compliance corpus
  (pinned commit, vendored under `tests/jmespath-corpus/`) runs green
  through every tree-view backend, including the corpus's
  invalid-expression cases (as demonstrated by
  `corpus_slice1_materialized` and `corpus_cursor_matches_materialized`).
- **Deterministic evaluation** — identical bytes and an identical
  expression always produce identical results (as demonstrated by
  `evaluation_is_deterministic`).
- **Single-pass input** — each input byte is consumed at most once per
  evaluation; identical navigations share one decode (as demonstrated
  by `shared_navigation_decodes_once`).
- **Selective materialization** — memory tracks the output, not the
  input (as demonstrated by `selective_materialization_budget`).
- **No input/output in the evaluation path** — parse, compile,
  evaluate, and the program cache do no network, filesystem, or
  clock work; the corpus-driver module (the test harness's entry
  point) reads the vendored corpus files only when invoked.
- **Bounded resources** — every limit (expression length, nesting
  depth, instruction budget, output nodes, output bytes, in-flight
  aggregation bytes) is configurable, and exceeding any limit aborts
  the whole evaluation with a structured error; results are never
  partial (as demonstrated by the `limit_boundary_*` tests).

## Where to read next

- Getting started: `docs/sansho/getting_started.md`
- Architecture (the walk program, the tree view, the cache):
  `docs/sansho/architecture.md`
- Interpreting benchmark results: `docs/sansho/benchmarks.md`
- Troubleshooting: `docs/sansho/troubleshooting.md`
- The specification: `specs/0001-jmespath-projection.md`
- The design dialog (why it is the way it is):
  `discussions/2026_09_11_sansho_spec.md`
