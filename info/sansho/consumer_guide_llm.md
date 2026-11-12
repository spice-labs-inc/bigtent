# Sansho as a consumer — LLM copy

- `sansho::evaluate(&program, document)` — the one generic entry over
  `SanshoTrait`; `sansho::materialize(&value)` converts the result at the
  boundary.
- The backing contract: the byte source is a pure borrower — the byte
  slice is immutable for the evaluation's lifetime (mmap'd clusters); the
  consumer owns the backing (Arc<Mmap> / in-memory item). No unsafe.
- An embedding crate implements `SanshoTrait` for its own item type; the
  evaluation never sees that type.
- Materialization: at the row boundary only; the evaluation never
  materializes per element (the leaf fast paths).
- Verified: `consumer_backends_agree_through_lookup`,
  `consumer_byte_source_borrows_in_place`.
