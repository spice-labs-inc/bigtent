# Sansho: troubleshooting (LLM copy)

LLM-readable counterpart of `troubleshooting.md`; the test references
are identical.

- Null for an existing-looking field: (a) the lookup is byte-exact (no
  normalization) — missing keys evaluate to null
  (`null_semantics_missing_paths_are_null`); (b) the wildcard kinds are
  strict: `.*` = object values, `[*]` = array elements, the wrong kind
  → null; (c) the wildcards NEST — `[]` is the flattener.
- Expression rejected: parse errors carry a byte position; the filter's
  `?` must immediately follow `[`; unknown functions/wrong arities =
  compile errors (the table: `src/program.rs::function_arity`); the
  sort/sort_by/max_by/min_by keys must be uniform numbers-or-strings
  (null/boolean/container/mixed = invalid-type — the corpus's referee).
- Limit errors: the two parse-time bounds — the expression's length
  (16_384 characters) and its nesting depth (64) — reject the expression
  with `SanshoError::Limit` before the recursive parser runs (never
  partial: `boundary_max_expression_length`, `boundary_max_depth`).
  Constants, not per-call configuration.
- Backend disagreement = a defect: the corpus runs through both
  backends with identical scores (`corpus_cursor_matches_materialized`);
  the differential property covers generated pairs
  (`differential_property_over_generated_documents`).
- A fixture decode failure after a dependency upgrade = the intended
  tripwire: the golden fixture pins the item encoding
  (`golden_layout_fixture_decodes_to_json_twin`); regenerate via
  `cargo run -p sansho --example gen_fixture` (from `engine/sansho`),
  review the diff as a format change, re-commit.
