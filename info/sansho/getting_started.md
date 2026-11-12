# Sansho: getting started

Sansho projects fields out of a single CBOR document with a JMESPath
expression. This page gets a caller from zero to a first projection;
the guarantees and the architecture live in the other pages of this
directory.

## The pieces

| Piece | What it is | Where |
|-------|-----------|-------|
| An expression | JMESPath text, e.g. `body.file_names[?starts_with(@, 'gitoid:')]` | the caller |
| A program | The compiled walk program (parse once, evaluate many) | `sansho::compile`, `sansho::Program` |
| A document | One CBOR value in a byte slice (definite lengths only) | the caller's item bytes |
| The result | A JSON value | `sansho::evaluate`, then `sansho::materialize` |

## First projection

```rust
let program = sansho::compile(&sansho::parse("body.mime_type[0]")?, None)?;
let value = sansho::evaluate(&program, item_bytes)?;   // item_bytes: &[u8]
let mime = sansho::materialize(&value)?;
// mime == "text/x-java-source" for the Appendix-A-shaped item
// (as demonstrated by canonical_expressions_over_the_fixture)
```

## Repeated expressions: use the cache

The cache keys programs by the canonicalized expression and the engine
version; repeated expressions (the common case for a query surface) pay
the parse and compile once:

```rust
let cache = sansho::ProgramCache::new(1_024);
let program = cache.compile_cached("body.file_names[?starts_with(@, 'gitoid:')]")?;
// cached and freshly compiled programs behave identically, including
// after eviction (as demonstrated by cache_hit_identity_including_eviction)
```

Whitespace-only expression differences share one cache entry;
expressions of different meaning never do (as demonstrated by
`cache_key_stability`). Compile errors are not cached (as demonstrated
by `compile_errors_are_not_cached`).

## In-memory documents

`evaluate` is generic over the source, so a document that is already a
`serde_json::Value` is evaluated directly, with no CBOR step:

```rust
let value = sansho::evaluate(&program, &json_value)?;
let result = sansho::materialize(&value)?;
```

The two sources (the byte source over raw CBOR bytes and the value source over in-memory JSON) produce identical results (as demonstrated by
`corpus_cursor_matches_materialized` and
`differential_property_over_generated_documents`).

## The parse-time bounds

The evaluation path carries no limits or data caps. The parser's two
parse-time expression bounds (length 16,384, nesting depth 64) are
one-time checks rejecting hostile queries before parsing (as
demonstrated by `boundary_max_expression_length` and
`boundary_max_depth`); a `SanshoError::Limit` names the bound
exceeded.

## Errors

Every failure is a structured `SanshoError`: `Input` (the bytes are not
exactly one CBOR document), `Parse` (the expression is not valid
JMESPath, with the byte position), `Compile` (the expression is
invalid per the specification, or names an unknown function or a wrong
arity), `Evaluation` (the document's contents defeat evaluation), and
`Limit` (a resource bound was exceeded). Nothing panics.
