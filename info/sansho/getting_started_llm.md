# Sansho: getting started (LLM copy)

LLM-readable counterpart of `getting_started.md`.

- The pieces: an expression (JMESPath text) →
  `sansho::compile(&sansho::parse(expr)?, None)` → `Program` →
  `sansho::evaluate(&program, document)` → a trait value →
  `sansho::materialize(&value)` → the projected JSON value. `evaluate` is
  generic over the source: a `&[u8]` CBOR document and a
  `&serde_json::Value` both go through it.
- Repeated expressions: `sansho::ProgramCache::new(n)` +
  `cache.compile_cached(expr)` — the key = the canonical expression +
  the engine version; whitespace variants share entries; distinct
  meanings never do; errors not cached; cached == fresh including
  post-eviction (`cache_hit_identity_including_eviction`,
  `cache_key_stability`, `compile_errors_are_not_cached`).
- Limits: the expression's length (16_384 characters) and its nesting
  depth (64), both checked at parse time before the recursive parser runs;
  exceeding either is `SanshoError::Limit`, never a partial result
  (`boundary_max_expression_length`, `boundary_max_depth`). Constants,
  not per-call configuration.
- Errors: `SanshoError::{Input, Parse{position}, Compile, Evaluation, Limit}`;
  nothing panics (the fuzz gates: `fuzz_expressions_no_panics`,
  `fuzz_cbor_no_panics`, `fuzz_cbor_structured_hostiles`).
- Guarantees: complete JMESPath conformance (the corpus:
  `corpus_slice1_materialized`); determinism as VALUE equality rather
  than byte equality (`evaluation_is_deterministic`); navigation reads
  only the positions the program visits; selective materialization
  (`selective_materialization_budget`); backend agreement
  (`corpus_cursor_matches_materialized`); the evaluation path performs
  no I/O of its own (the corpus-driver module reads the vendored
  corpus files only when the test harness invokes it).
- Canonical example: `body.file_names[?starts_with(@, 'gitoid:')]` over
  the Appendix-A-shaped fixture (`canonical_expressions_over_the_fixture`).
