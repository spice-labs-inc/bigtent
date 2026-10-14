# Sansho: architecture overview (LLM copy)

LLM-readable counterpart of `architecture.md`. Same content, compressed;
the claims' test references are identical.

- Pipeline: parse (nom, complete grammar; the depth limit enforced by
  an ITERATIVE pre-scan before recursion) → syntax tree → canonical
  form (the cache key's input) → walk program (a linear edge list in
  source order) → flow-threading evaluator → JSON value.
- Canonical form: a fixed point under re-parsing, distinct across
  meanings, whitespace-collapsing (`canonical_is_fixed_point_over_corpus_expressions`,
  `canonical_distinguishes_distinct_meanings`,
  `canonical_collapses_insignificant_whitespace`).
- Tree view: a generic read-only trait; two backends — materialized
  (in-memory JSON references) and cursor (byte positions over CBOR).
  Identical results REQUIRED: the whole corpus through both with
  identical per-feature scores (`corpus_cursor_matches_materialized`),
  plus the generated differential property (`differential_property_over_generated_documents`).
- Cursor: a node = a byte POSITION; navigation walks container headers
  without decoding values; materialization = the MEMOIZED decode (each
  position decodes at most once per evaluation — the byte-once
  requirement's enforcement); the decode-count = the instrumentation
  (`shared_navigation_decodes_once`).
- Flow model: One(node) | Value(json) | Proj(collection). Rules:
  (1) edges after a projection apply per element; the mapping recurses
  into nested projections SHAPE-PRESERVING (nested projections are
  values); (2) null element-results drop; (3) the flatten operator `[]`
  consumes the WHOLE flow (dissolves arrays one level; scalars pass; a
  plain non-array → null) — the ONLY flattener; (4) wildcard kinds are
  strict: `.*` = object values, `[*]` = array elements, the wrong kind
  → null; (5) misses → null; multi-select on null → null; (6)
  truthiness: false/null/empty-string/empty-array/empty-object are
  false-y, numbers (zero included) true; (7) comparators across kinds
  → null.
- Limits: seven configurable bounds (the expression length, the depth,
  the instruction budget, the output nodes, the output bytes, the
  aggregation bytes, the cache entries); the parse-time bounds reject
  before recursion; the evaluation bounds accrue through TWO ledgers —
  account_output (the final result, once) and account_aggregation (the
  in-flight materializations); exceeding the parse-time bounds = the
  structured Limit error, never partial results
  (`boundary_max_expression_length`, `boundary_max_depth`).
- Cache: LRU keyed by the canonical expression + the engine version;
  errors not cached; cached == fresh INCLUDING post-eviction
  (`cache_hit_identity_including_eviction`, `cache_key_stability`,
  `compile_errors_are_not_cached`).
- CBOR→JSON mapping: RFC 8949 §4.2; definite-only; base64url byte
  strings; integers keep their exact representation (the precision loss
  = the comparison-level reduction); bignums → doubles; duplicate keys
  last-wins; unknown tags/undefined → evaluation errors; indefinite/
  trailing-bytes → input errors; f16 decoded manually.
- Hostile input: no panics, no hangs, structured errors only
  (`fuzz_expressions_no_panics`, `fuzz_cbor_no_panics`,
  `fuzz_cbor_structured_hostiles`); NO capacity reservation from
  declared container counts.
