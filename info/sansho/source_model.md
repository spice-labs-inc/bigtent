# Sansho's source model

The zero-copy evaluation layer.

Sansho evaluates a program against a **source** through the value-node
abstraction: a node is a cheap, copyable position value (a byte position
for the byte source; a reference for the in-memory sources), and
evaluation touches only the fields the program reads.

## The three sources

- **The byte source** (`src/source.rs`, implemented for `&[u8]`): over a
  raw CBOR byte slice — the mmap'd cluster items. Zero-copy by
  construction: a text string's `&str` IS the bytes in the slice (CBOR
  text strings are raw UTF-8, with no escape sequences), and a member is
  a subslice of the document rather than a copy. Numbers decode from
  their headers; byte strings (major type 2) render base64url at the
  boundary (pinned by `escape_case_matrix_pins_the_mapping`).
- **The JSON source** (`src/materialized.rs`, implemented for
  `serde_json::Value` and `&serde_json::Value`): over an in-memory
  `serde_json::Value` — the merged herd item's body, invocation inputs,
  proxied items.
- **The in-memory CBOR source** (`src/cbor_value.rs`, implemented for
  `&serde_cbor::Value`): over a decoded CBOR value.

All three produce equal results, and each runs the compliance corpus —
`corpus_green_through_cbor_node` (byte source),
`corpus_green_through_cbor_value` (in-memory CBOR) and
`corpus_slice1_materialized` (JSON).

## The trait surface

A source implements `SanshoTrait<'a>`, and that trait is the only type
the evaluator carries. Navigation returns trait values
(`SanshoCarrier<'a>`, a refcounted value), never a source-specific node:

| method | what it answers |
|--------|-----------------|
| `kind` | which of null/bool/number/string/bytes/array/object this is |
| `get_key`, `get_index` | one member by name, or one element by position |
| `as_object` | the object's members, keyed by name (a `HashMap`) |
| `as_elements` | the array's elements, in document order — a value per element (see below) |
| `as_array_str` | the array's TEXT elements, in document order, skipping every other kind |
| `container_len` | the member or element count |
| `as_str`, `as_bytes`, `as_sansho_number`, `as_f64`, `as_bool`, `is_null` | the leaf reads |
| `to_string_sansho` | a human-friendly rendering (debugging; may copy) |

**Member order is not part of a value.** A JSON object is an unordered
collection of name/value pairs, so the iteration order of the map that
`as_object` returns is not a contract, and no caller may depend on it.
Two results that compare equal as JSON are the same result, whatever
order their members happen to serialize in.

**Two reads of an array's text, and they differ on purpose.**
`as_array_str` is the LENIENT read: it yields the text elements in
document order, skips every element of another kind — a byte string is
octets and is never text — and skips an element whose text is not valid
UTF-8 rather than reporting it. `as_elements` followed by `as_str` is the
STRICT read: an invalid element is an input error. Scanning for a prefix
wants to pass over what it cannot read; producing a value must not
silently drop data. `as_array_str` also holds nothing: nothing is
refcounted and nothing is allocated, because each string borrows the
source — which is why it cannot have a default implementation (a general
one would have to borrow `&str` out of carriers it owns) and answers an
error for a source that is not an array.

## Navigation and validation

Navigation parses headers and does position arithmetic: a field lookup is
a raw-key-bytes scan, linear in the map's size, with no key-string
materialization. Decoding a leaf's text — and therefore validating it as
UTF-8 — happens where the text is read, so a document that is not valid
CBOR is an input error at that point rather than a wrong value (pinned by
`escape_case_matrix_pins_the_mapping`).

## The leaf fast paths

The evaluator's hot paths read string leaves without materializing them:
the string-predicate functions (`starts_with`/`ends_with`/`contains`)
evaluate a current-node string argument from the node's borrowed
`as_str`, and the dominant filter shape `[?starts_with(@, 'prefix')]` is
hoisted into the filter loop as a borrowed read plus a boolean per
element. The measured result: the scan shape at 100k connections drops
from 103.6 ms to 23.4 ms, and the seek shape from 12.2 ms to 1.5 ms (the
recorded benchmark baselines against the `scan/cbor_node` and
`seek/cbor_node` arms). The fall-through semantics of the fast paths are
pinned by the corpus and the escape matrix, and
`leaf_nodes_answer_no_navigation` pins the leaf reads.

## The derive

`#[derive(SanshoObject)]` (the `sansho-derive` crate) generates a
field-walking `SanshoTrait` implementation for an item-shaped struct, so
the struct is itself a source: `sansho::evaluate(&program, &my_struct)`
walks its fields. No wrapper type is generated — the implementation goes
on `&'a MyStruct`, the same shape the hand-written
`src/item_sansho_impl.rs` uses, and the same shape a consumer would write
by hand.

Each named field becomes a member carried by reference
(`Rc::new(&value.field)`). A `Copy` scalar (`bool`, `u64`, `i64`, `f64`)
or a reference field (`&str`, `&[u8]`) is copied into its carrier
instead, because the engine implements the trait for those in their owned
form. `#[serde(rename = "name")]` renames the member,
`#[serde(skip)]` removes it, and `#[serde(rename_all = "...")]` is a
compile error (the derive does not implement serde's case conversions).

Demonstrated by `derived_struct_is_a_jmespath_source`,
`the_member_set_is_the_field_set`, `the_rendering_names_the_members` and
`navigation_and_iteration_agree`.

## No limits

The evaluation path carries no limits or data caps: the
instruction/output/aggregation accounting was removed. The parser's two
parse-time expression bounds (length 16,384; nesting depth 64) remain as
one-time parse checks, demonstrated by `boundary_max_expression_length`
and `boundary_max_depth`.
