# Barker — LLM reference

Every claim is backed by a named test. The human page is `info/barker.md`.

## The three phases

| phase | signature | claim | test |
|---|---|---|---|
| parse | `barker::parse(&str) -> Result<Program>` | a syntax error is returned before anything runs | `a_program_that_does_not_parse_is_refused_before_it_runs` |
| run | `barker::run(Program, Receiver<Value>) -> Result<Rows>` | rows in, rows out, streamed | `a_program_takes_rows_in_and_puts_rows_out` |
| run_with | `run_with(program, input, lookup, natives)` | the embedder's natives compose | `an_embedders_natives_are_callable_from_the_program` |

## Streaming

- The input is `StreamingInput`, a Starlark value wrapping a `flume::Receiver`:
  `iter_next` pulls one row and converts it; the channel closing ends the
  iteration. Nothing is collected first.
  Tests: `ten_ints_stream_in_and_double_out` (produces after the fork, collects
  on the calling thread), `two_stages_take_roots_from_the_input_and_both_see_everything`
  (two stages, both fed — the top tee).
- The output is `Rows`, an iterator over a flume receiver; `run` forks the
  program's thread (the evaluator is not Send) and returns before any row exists.
- A failure emits a row `{"error": "…"}`, not the end of the stream.
  Tests: `a_failing_program_reports_its_failure_as_a_row`,
  `a_program_with_a_static_error_emits_nothing`.

## Boundedness

- `load` cannot reach the filesystem: the dialect enables it, no loader is
  configured. Test: `a_program_cannot_load_another_file`.
- No determinism promise; the deterministic tiers are Sansho and the walk engine.
- Resource limits: not yet wired into the run loop.

## The namespaces

Registered in `run_with` with `with_namespace`. Tests by namespace:

| namespace | test |
|---|---|
| `math` | `every_math_function_computes` |
| `stats` | `a_stream_is_summarised_in_one_pass` (one pass over a stream), `tally_counts` |
| `collections` | `the_collections_shape` |
| `patterns` | `the_patterns_match` |
| `digest` | `the_digests_produce_the_identifier_forms` |
| `dates` | `dates_are_canonical_and_compare_chronologically`, `the_dates_compute` |

Distinctions that are load-bearing:

- `trunc(-1.5)` is -1; `floor(-1.5)` is -2. Test: `every_math_function_computes`.
- Population vs sample variance differ by the denominator.
  Test: `a_stream_is_summarised_in_one_pass`.
- `gitoid_*` hashes `"blob <len>\0"` + content; `swh_*` is the SAME hash under
  `swh:1:cnt:`. Test: `the_digests_produce_the_identifier_forms`.
- A date is canonical RFC 3339, so string comparison is chronological.
  Test: `dates_are_canonical_and_compare_chronologically`.
- `as_number` reads a float through `StarlarkFloat`, NOT through JSON — JSON has
  no NaN, so a guard against NaN could not receive one otherwise.
  Test: `a_stream_is_summarised_in_one_pass`.

## The `highwire` namespace

A script can build and run a query. See `info/writing_queries_llm.md` for the
tests; the natives are `walk`, `barker`, `from_stage`, `connection`, `run`.
