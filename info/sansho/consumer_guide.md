# Sansho as a consumer

The generic evaluation entry, the backing contract, and the derive —
what a consumer (an embedding engine) uses.

## The entry points

- `sansho::evaluate(&program, document)` — the one entry point, generic
  over `SanshoTrait`: a `&[u8]` CBOR document and a `&serde_json::Value`
  both evaluate through it.
- `sansho::materialize(&value)` — the boundary conversion to a
  `serde_json::Value`, run after the evaluation has completed.

## The backing contract

The byte source is a PURE BORROWER: the byte slice must be immutable
for the evaluation's lifetime (the mmap'd clusters satisfy this); the
node never owns its backing, and there is no unsafe. The consumer
holds the backing (an `Arc<Mmap>`, an in-memory item) and evaluates
over it — as demonstrated by
`consumer_backends_agree_through_lookup` (the byte and value sources
agree, one row per pkg: entry) and
`consumer_byte_source_borrows_in_place` (the borrowed string lies
within the input slice).

## The embedding type

An embedding crate implements `SanshoTrait` for its own item type
(`src/item_sansho_impl.rs` implements it for the consumer's `Item`) and
evaluates over it directly. The evaluation carries trait values, so it
never sees the concrete type.

## The materialization rule

Values are materialized only at the row boundary: the evaluation
returns a JSON value, and the streaming consumer can serialize that
value directly. No per-element materialization happens inside the
evaluation (the leaf fast paths read borrowed leaves).
