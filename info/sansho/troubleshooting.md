# Sansho: troubleshooting

Symptoms, causes, and the tests that pin each behavior. The
human-readable version is this file; the LLM-readable copy is
`troubleshooting_llm.md`.

## "My expression returns null but the field exists"

- **The identifier or key does not match byte-for-byte.** Lookup is
  exact-match; no case-folding, no normalization. Check the document's
  actual keys (a missing key evaluates to null — as demonstrated by
  `null_semantics_missing_paths_are_null`).
- **You used the wrong wildcard kind.** `.*` reads object values only;
  `[*]` reads array elements only; the wrong kind on the wrong
  container yields null (as demonstrated by the corpus's wildcard
  cases and `corpus_cursor_matches_materialized`).
- **You used a wildcard where you wanted flatten.** Wildcards nest;
  `[]` dissolves arrays one level (see `architecture.md`'s flow-model
  rules).

## "My expression was rejected"

- **Parse errors carry a byte position** — look there; the usual causes
  are an unterminated literal, an unbalanced bracket, or a stray
  character. A filter's `?` must follow `[` with no whitespace between
  them (`foo[ ?x]` is invalid).
- **Unknown function or wrong arity** is a compile error; the built-in
  function list and arities are in `src/program.rs`
  (`function_arity`).
- **`sort`/`sort_by`/`max_by`/`min_by` require uniform keys** (all
  numbers or all strings); mixed types, null by-values, and boolean
  keys are invalid-type — by the corpus's referee decisions.

## "My evaluation returned a Limit error"

Two parse-time bounds are enforced: the expression's length (16_384
characters) and its nesting depth (64). Exceeding either rejects the
expression with `SanshoError::Limit` before the recursive parser runs,
so there is never a partial result (as demonstrated by
`boundary_max_expression_length` and `boundary_max_depth`). The bounds
are constants rather than per-call configuration: narrow the expression.

## "The byte source and the value source disagree"

They must not. The entire compliance corpus runs through both backends
with identical per-feature scores (as demonstrated by
`corpus_cursor_matches_materialized`), and generated documents are
differentially tested (as demonstrated by
`differential_property_over_generated_documents`). If you see a
disagreement, it is a defect: capture the document and the expression
and file it with both backends' outputs.

## "My fixture stopped decoding after a dependency upgrade"

The cursor decoder depends on the fixture ENCODER's output stability:
the golden fixture `tests/fixtures/item_a.cbor` pins the item encoding;
a serializer upgrade changes the bytes and fails
`golden_layout_fixture_decodes_to_json_twin` — deliberately. Re-generate
the fixture with `cargo run -p sansho --example gen_fixture` (from the
`engine/sansho` directory), review the diff as a format change, and
re-commit.
