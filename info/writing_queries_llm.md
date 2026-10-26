# Writing a query — LLM reference

Every claim here is backed by a named test. Invariant 8 requires the pairing.

## Structure

A query is a document with `phases`: an array of **stages**. A stage is one of two
kinds, selected by a key:

| key present | kind | trait |
|---|---|---|
| `barker` | program | `Barker` |
| anything else | walk | `Walk` |

The selector is implemented in `src/highwire/grammar_impl_serde.rs`; the traits
are in `src/highwire/grammar.rs`. A single-stage query is a walk document with no
`phases` wrapper.

## Edges

- A walk's `roots` is either a list (`Enumerated`) or an object naming upstream
  stages (`Predicates`).
- A program's `inputs` is a list of stage names.
- **The root expression is applied to the ROW.** A stage emitting a bare string
  yields rows that are the identifier, so a downstream stage reads it with `"@"`;
  a stage emitting an object reads it with a field name. A wrong projection yields
  nothing and the downstream stage starts empty — silently.
  Test: `a_phase_draws_its_roots_from_the_phase_before_it`,
  `one_stages_rows_reach_every_consumer`.
- Both kinds of stage feed both kinds. A walk consumer gets identifiers derived
  through the edge's projection; a program consumer gets the rows themselves.
  Test: `two_program_stages_chain_through_rows`.

## `tee`

A stage's rows reach its downstream stages only, unless `tee` is true, in which
case they also reach the caller. The last stage always reaches the caller.
Test: `tee_decides_whether_a_stage_is_visible`.

## Programs

`input` is a stream (iterate once; no `len()`). `emit(row)` appends a row.
Test: `a_program_takes_rows_in_and_puts_rows_out`,
`ten_ints_stream_in_and_double_out`.

A failure emits `{"error": "<message>"}` as a row.
Test: `a_failing_program_reports_its_failure_as_a_row`,
`a_program_with_a_static_error_emits_nothing`.

A parse error is refused before anything runs, returning an error rather than a
row. Test: `a_program_that_does_not_parse_is_refused_before_it_runs`.

`unnest: true` makes each element of an emitted list its own row, one level.
Compiled in `compile_barker`; the walk side's equivalent is tested by
`unnest_decides_what_a_row_is` in `engine/sansho/src/ast.rs` (the analysis), and
the wiring applies it in the program arm of `run`.

## Namespaces registered

`math`, `stats`, `collections`, `patterns`, `digest`, `dates` — registered in
`run_with` via `with_namespace`. Test coverage:

| namespace | test |
|---|---|
| `math` | `every_math_function_computes` |
| `stats` | `a_stream_is_summarised_in_one_pass`, `tally_counts` |
| `collections` | `the_collections_shape` |
| `patterns` | `the_patterns_match` |
| `digest` | `the_digests_produce_the_identifier_forms` |
| `dates` | `dates_are_canonical_and_compare_chronologically`, `the_dates_compute` |

### Distinctions that are load-bearing

- `trunc(-1.5)` is -1, `floor(-1.5)` is -2.
  Test: `every_math_function_computes`.
- Population vs sample variance differ by the denominator. For 2,4,4,4,5,5,7,9:
  population stdev is 2, sample stdev is sqrt(32/7).
  Test: `a_stream_is_summarised_in_one_pass`.
- `sha256(s)` is the digest of the content; `gitoid_sha256(s)` is the digest of
  `"blob <len>\0"` + the content. For the empty content they differ.
  Test: `the_digests_produce_the_identifier_forms`.
- `swh_sha1(s)` carries the SAME hash as `gitoid_sha1(s)` under a different
  prefix: the hash of a SWHID content object is the git blob digest (`sha1_git`
  in Software Heritage's terms), not the bare digest. For `"hello\n"` both are
  `ce013625…`; the bare `sha1` is `f572d396…` and is not the SWHID.
  Test: `the_digests_produce_the_identifier_forms` asserts the two hashes are
  equal and that neither is the bare digest.
- All hashing is routed through the `coordinates` crate (`spice-coordinates`)
  except `blake3`, which that crate does not define and which is computed by the
  `blake3` crate directly. The tests pin the crate's output against the canonical
  vectors (`e3b0c442…` for the empty content, `f572d396…` for `"hello\n"`) and
  BLAKE3 against its own empty-input vector (`af1349b9…`).
  Test: `the_digests_produce_the_identifier_forms`.
- A date is canonical RFC 3339 UTC, so string comparison is chronological
  comparison; `2026-10-16T13:00:00+02:00` canonicalises to `11:00:00.000Z` and
  sorts before `12:00:00Z`.
  Test: `dates_are_canonical_and_compare_chronologically`.
- Durations are integers of milliseconds; `now_ms() - t0` is a duration.
  Test: `the_dates_compute`.

### The NaN path

`serde_json` has no NaN and no infinity, so a JSON round trip cannot carry them.
`as_number` in `src/barker/values.rs` therefore reads a float through
`StarlarkFloat` and only falls back to the JSON form for integers — otherwise
`stats.is_nan` could never receive the value it exists to detect.
Test: `a_stream_is_summarised_in_one_pass` asserts `stats.is_nan(math.sqrt(-1))` is true.

An infinity cannot be produced by division in Starlark (it raises); it comes from
`float("inf")`. Same test.

## `dedup`

`dedup` is a stage-level flag beside `tee` and `unnest`, on both `Walk` and `Barker`
(`src/highwire/grammar.rs`). It is applied in the `tee` node (`src/highwire/compiler.rs`),
so the set is per stage: one stage's repeats are not suppressed by another's.

The key is `row_key`: the first 16 bytes of `blake3`. A `Value::String` is hashed
over its own bytes with no rendering and no copy — a string's rendering IS the
string — while every other row is rendered canonically first (object keys sorted).
Both paths compute the same digest for the same value, so the distinction is
PERFORMANCE, not accuracy: it saves a `String` clone on the common case, an
identifier. 16 bytes is 128 bits of an evenly distributed digest, the same collision
budget the index uses.

Tests: `dedup_emits_a_repeated_row_once` (off, then on), `dedup_is_per_stage`,
`dedup_handles_long_strings` (the no-copy path).

`walk.seen` deduplicates VISITS; `dedup` deduplicates ROWS. They do not overlap.

## The V2 HTTP surface

`src/api_v2.rs`. `GET /v2/query?identifier=…&template=<name>` and
`POST /v2/query` with `{"roots": [...], "template": name}` or
`{"roots": [...], "query": {...}}`; neither field means the DEFAULT template.
Output is JSON lines (`StreamBodyAs::json_nl`), streamed from a blocking run
through a bounded(256) channel.

| claim | test |
|---|---|
| a custom template and a designated default answer an unspecified request | `a_custom_template_and_a_designated_default` |
| an unknown default is refused at construction | `an_unknown_default_is_refused_at_construction` |
| a caller's template replaces a built-in of the same name | `a_caller_template_replaces_a_builtin` |
| the document describes exactly the served routes | `the_openapi_document_matches_the_routes` |

`TemplateRegistry` is EMPTY by default: `new()` holds nothing and has no default.
`with_builtins()` adds the six endpoint replications plus the passthrough;
`with_template(name, text)` adds or replaces; `without_template(name)` removes —
a built-in included, and removing the default leaves it undesignated.
`with_default(name)` designates and is fail-fast (an unknown name errors at
construction). An unspecified request with no default designated is a 404 naming
the omission, not a silent empty answer.

Tests: `an_empty_registry_has_no_default`, `a_removed_builtin_is_not_served`,
`a_replaced_builtin_can_be_the_default`,
`a_custom_template_and_a_designated_default`,
`an_unknown_default_is_refused_at_construction`.

The V2 state is `V2State { holder, queries, templates }`; the `QueryCache` is
built once in `run_web_server`, so the expression programs live for the process.
`CompiledQuery` is `Send + Sync` (pinned by `a_compiled_query_is_send_and_sync`),
held as `Arc` in the cache, and runnable many times concurrently
(`a_cached_query_runs_more_than_once`).

| claim | test |
|---|---|
| the same document compiles once | `the_same_document_compiles_once` |
| different documents do not collide | `different_documents_do_not_collide` |
| a document that does not parse is not cached | `a_document_that_does_not_parse_is_not_cached` |
| the expression cache spans queries | `the_expression_cache_spans_queries` |

## The `highwire` namespace

`src/barker/highwire.rs`. Builders produce a `Stage` value carrying the stage's
document; `run` renders `{"phases": [...]}` and hands it to the same compiler the
JSON reader feeds — there is no second grammar.

| claim | test |
|---|---|
| a DSL-built query runs | `a_dsl_built_query_runs` |
| stages are referenced by value, not by name | `stages_are_referred_to_by_value` |
| malformed builds are refused at construction (no `emit`, mixed roots, duplicate name) | `the_dsl_refuses_malformed_stages` |
| nothing happens until the rows are iterated | `a_nested_query_does_nothing_until_it_is_read` |
| the output is the stage with no consumers, not the last declared | `the_output_is_the_stage_with_no_consumers_not_the_last_declared` |
| `tee` publishes a consumed stage in addition | `a_teed_middle_stage_publishes_as_well_as_feeding` |

`run_nested` in `src/highwire/compiler.rs` is lazy: it spawns a thread that
compiles and runs only when the returned stream is first pulled, and a compile
failure arrives as the stream's first item because it has not happened yet when
`run` returns.

A nested query needs the cluster: `run_with` carries `Option<HerdOrCluster>` in the
script's `Context`, and a nested run with no cluster reports that as an error item.

## Not available

`load` cannot reach the filesystem — the dialect enables it, no loader is
configured. Test: `a_program_cannot_load_another_file`.

## Compile-time failures

Located by stage and level, e.g.
`at the query -> phase \`classes\` -> its root: a root draws on \`jars\`, and no
stage is named that`. Tests: `a_root_drawing_on_a_missing_phase_names_the_phase_it_could_not_find`,
`a_rendered_failure_carries_the_phase_the_level_and_the_detail`,
`a_duplicate_phase_name_is_refused_with_the_name`,
`a_cycle_is_refused_and_names_a_phase_in_it`,
`a_walk_with_no_emit_is_refused_with_its_phase_and_level`,
`a_malformed_gate_is_refused_at_the_level_it_was_read`.
