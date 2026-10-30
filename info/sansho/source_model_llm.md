# Sansho's source model — LLM copy

LLM-readable counterpart of `source_model.md`.

- A program evaluates against a SOURCE through `SanshoTrait<'a>` — that
  trait is the only type the evaluator carries. Navigation returns trait
  values (`SanshoCarrier<'a>`, refcounted), never a source-specific node.
  Its members: `kind`; `get_key`/`get_index`; `as_object` (the members
  keyed by name, a `HashMap`); `as_elements` (document order, one VALUE
  per element); `as_array_str` (document order, `&str` — the LENIENT
  array read: it skips every non-text kind, a byte string included, and
  every element whose text is not valid UTF-8, where `as_elements` plus
  `as_str` answers an input error instead; it holds nothing, so it has no
  default and a non-array answers an error); `container_len`; the leaf
  reads `as_str`, `as_bytes`, `as_sansho_number`, `as_f64`, `as_bool`,
  `is_null`; and `to_string_sansho` (debugging; may copy).
- THREE sources, each of which runs the compliance corpus:
  - the byte source (`src/source.rs`, `&[u8]`): raw CBOR bytes (mmap'd
    clusters); zero-copy — a text string is `&str` over the slice (CBOR
    text is raw UTF-8), a member is a subslice, numbers decode from
    headers, byte strings are `&[u8]` rendered base64url at the
    boundary. A field lookup is a raw-key-bytes scan, linear in the map's
    size, with no key-string materialization.
    Corpus: `corpus_green_through_cbor_node`.
  - the JSON source (`src/materialized.rs`, `serde_json::Value` and
    `&serde_json::Value`): merged herd item bodies, invocation inputs,
    proxied items. Corpus: `corpus_slice1_materialized`.
  - the in-memory CBOR source (`src/cbor_value.rs`,
    `&serde_cbor::Value`). Corpus: `corpus_green_through_cbor_value`.
- The three AGREE as VALUES (differential-pinned) — not byte for byte.
  Member order is not part of a value: `as_object` returns a
  `HashMap<&str, SanshoCarrier>`, its iteration order is not a contract,
  and nothing may depend on it.
- Determinism means value equality: identical bytes plus an identical
  expression always yield results that compare equal as JSON
  (`evaluation_is_deterministic`). It does NOT mean identical
  serialization — a result carried in a hash-backed container may
  serialize its members in a different order and still be the same
  result.
- Validation happens where text is read: a document that is not valid
  CBOR is an input error at that point, never a wrong value
  (`escape_case_matrix_pins_the_mapping`).
- The leaf fast paths: string-predicate arguments and the
  `[?starts_with(@, 'prefix')]` filter shape read borrowed leaves — no
  per-element recursion. Measured: scan 103.6 → 23.4 ms at 100k; seek
  12.2 → 1.5 ms (recorded baselines against the `cbor_node` arms);
  `leaf_nodes_answer_no_navigation` pins the leaf reads.
- `#[derive(SanshoObject)]` (the `sansho-derive` crate, workspace member;
  sansho never depends on it): generates `impl<'a> SanshoTrait<'a> for
  &'a MyStruct` — the direct form, no wrapper type — implementing `kind`,
  `get_key`, `as_object`, `container_len`, `to_string_sansho`, with the
  rest by default. Each named field is a member carried by reference
  (`Rc::new(&value.field)`); a Copy scalar (`bool`, `u64`, `i64`, `f64`)
  or a reference field is copied instead (the trait's impls for those are
  on the owned forms). `#[serde(rename = "…")]` renames the member,
  `#[serde(skip)]` drops it, `#[serde(rename_all = "…")]` is a compile
  error. Tests: `derived_struct_is_a_jmespath_source`,
  `the_member_set_is_the_field_set`, `the_rendering_names_the_members`,
  `navigation_and_iteration_agree`.
- NO limits or data caps in the evaluation path; the parser's parse-time
  expression bounds (length 16,384, depth 64) are one-time checks
  (`boundary_max_expression_length`, `boundary_max_depth`).
