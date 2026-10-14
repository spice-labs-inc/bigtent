# Sansho: architecture overview

This page explains how Sansho works — the pipeline, the tree view, the
walk program, the evaluator's flow model, and the cache — for engineers
and for LLMs. The normative statement of *what* Sansho is lives in
`specs/0001-jmespath-projection.md`; this page explains *how* it works.

## The pipeline

```
expression text
      │  parse (nom; the complete grammar; the depth limit is
      │  enforced by an iterative pre-scan BEFORE the recursion runs)
      ▼
  syntax tree                    ── canonical form ──► cache key
      │  compile                                        (plus the
      ▼                                                 engine version)
  walk program (a linear edge list in source order)
      │  evaluate (the flow-threading evaluator, generic over the backend)
      ▼
  JSON value (the projection)
```

The canonical form is a deterministic re-serialization of the syntax
tree: it is a fixed point under re-parsing (as demonstrated by
`canonical_is_fixed_point_over_corpus_expressions`), it distinguishes
expressions of different meaning (as demonstrated by
`canonical_distinguishes_distinct_meanings`), and it collapses
insignificant whitespace (as demonstrated by
`canonical_collapses_insignificant_whitespace`).

## The tree view: one evaluator, two backends

The evaluator is generic over a read-only tree-view trait
(`src/view.rs`); nothing in the evaluator knows which backend it walks:

| Backend | Input | Node = | Materialization |
|---------|-------|--------|-----------------|
| Materialized (`src/materialized.rs`) | in-memory JSON | a reference into the value | none: a member is a reference into the value |
| Byte source (`src/source.rs`) | a raw CBOR byte slice | a subslice of the document | none: a member is `&document[start..end]` |

The backends must produce identical results — the corpus runs through
BOTH, and the per-feature score must be identical (as demonstrated by
`corpus_cursor_matches_materialized`), with the same agreement at
generated scale (as demonstrated by
`differential_property_over_generated_documents`).

The byte source walks container HEADERS to navigate (key lookup, index,
iteration) without decoding the unselected values; the minicbor spike
(`tests/spike_minicbor.rs`) proved the decoder supports this surface.

## The flow model (the evaluator's heart)

Evaluation threads a FLOW through the program's edges:

- `One(node)` — a backend position (lazy; materialization is the
  backend's memoized decode);
- `Value(json)` — a computed scalar or aggregate (functions,
  comparators, logic, literals, multi-selects);
- `Proj(items)` — a projection's collection.

The rules, all corpus-refereed:

1. **Every edge after a projection applies per element.** The mapping
   RECURSES into nested projections preserving shape: a nested
   projection is a VALUE (an array) in the parent collection — the
   corpus's `[*].[*].{...}` stays nested.
2. **Null element-results drop** out of projections.
3. **The flatten operator (`[]`) consumes the whole flow**: a
   projection's collected arrays dissolve one level, scalars pass
   through, a plain non-array flattens to null. It is the ONLY
   flattener — wildcards nest.
4. **Wildcard kinds are strict**: `.*` reads object values; `[*]`
   reads array elements; the wrong kind yields null.
5. **Navigation misses evaluate to null**; a multi-select applied to
   null is null.
6. **Truthiness**: false, null, empty string, empty array, and empty
   object are false-y; numbers (zero included) and other values are
   true.
7. **Comparators across kinds yield null** (never an error).

## The memo is the fusion

The byte source's decode memo guarantees each boundary position materializes at most
once per evaluation. When a filter predicate and an emission both
consume the same subtree, they share the one decode — the fusion that
makes the byte-once requirement (SPEC-0001 §5.1) true without any
deferred-emission buffering (as demonstrated by
`shared_navigation_decodes_once`; the decode-count is the
instrumentation).

## The limits

Two parse-time bounds, both constants:

- **Expression length** — 16_384 characters;
- **Nesting depth** — 64, checked by an iterative scan over the
  expression's structural characters before the recursive parser runs,
  so hostile nesting is rejected without recursing at all.

Exceeding either rejects the expression with a structured Limit error,
so there is never a partial result (as demonstrated by
`boundary_max_expression_length` and `boundary_max_depth`).

## The CBOR-to-JSON mapping

The mapping is normative (SPEC-0001 §4, per RFC 8949 §4.2): definite
lengths only; text strings map to strings; byte strings map to
base64url-encoded strings; integers keep their exact integer
representation (the precision loss is the comparison-level double
reduction); bignums (tags 2 and 3) reduce to doubles; duplicate map
keys resolve last-wins; unknown tags and `undefined` are evaluation
errors; indefinite-length items and trailing bytes are input errors.
The half-precision floats decode manually (IEEE 754 half).
