# Sansho's source model

The zero-copy evaluation layer (the research spike's outcome).

Sansho evaluates a program against a **source** through the value-node
abstraction: a node is a cheap, copyable position value (a byte
position for the byte source; a reference wrapper for the in-memory
sources), and evaluation touches only the fields the program reads.

## The two sources

- **The byte source** (`src/source.rs`): over a raw CBOR byte slice — the
  mmap'd cluster items. Zero-copy by construction: a text string's
  `&str` IS the bytes in the slice (CBOR text strings are raw UTF-8,
  no escape sequences); numbers decode from headers (as demonstrated
  by `text_string_borrows_bytes` and
  `consumer_byte_source_borrows_in_place`). Byte strings (major type
  2) render base64url — an owned `Cow` at the leaf read and at the
  boundary (pinned by `escape_case_matrix_pins_the_mapping`).
- **The value source** (`MaterializedNode`): over an in-memory
  `serde_json::Value` — the merged herd item's body, invocation
  inputs, proxied items.

The two backends produce identical results — the differential tests
pin the agreement (`cbor_node_matches_cursor_engine`, the corpus green
through both — `corpus_green_through_cbor_node`).

## The boundary decode memo

Navigation never decodes: field lookup is a raw-key-bytes scan — linear in the map's size, with
no key-string materialization (`raw_key_scan_finds_keys`; the seek
bench: 12.2 ms → 1.5 ms at 100k) — and iteration is position
arithmetic.
Materialization happens only at the row boundary, through the decode
memo — each position decodes at most once per evaluation (the byte-once
contract, as demonstrated by `boundary_memo_decodes_once` and
`scan_decodes_once_per_position_at_the_boundary`).

## The leaf fast paths

The evaluator's hot paths read string leaves without materializing:
the string-predicate functions (`starts_with`/`ends_with`/`contains`)
evaluate a current-node string argument from the node's borrowed
`as_str` (the value-argument leaf path), and the dominant filter shape
`[?starts_with(@, 'prefix')]` is hoisted into the filter loop (a
borrowed read + a boolean per element). The measured result: the scan
shape at 100k connections drops from 103.6 ms to 23.4 ms (the recorded
benchmark baseline vs the `scan/cbor_node` arm), and the seek shape
from 12.2 ms to 1.5 ms — the raw-key scan and the hoisted path
(`leaf_nodes_answer_no_navigation`, the fast paths' fall-through
semantics pinned by the corpus and the escape matrix).

## The derive

`#[derive(SanshoNode)]` (the `sansho-derive` crate) generates a
field-walking `SanshoTrait` implementation for item-shaped structs: `String`/`&str` fields
become string leaves, `Option<String>` optional strings,
`serde_json::Value` fields walk their tree, and any other field is a
null `Skip`. `#[serde(rename)]` is a compile error (the derive cannot
honor it) — as demonstrated by `derive_matches_hand_written` and the
crate's compile-fail doc-test.

## No limits

The evaluation path carries no limits or data caps (the owner's
directive): the instruction/output/aggregation accounting was removed.
The parser's two parse-time expression bounds (length 16,384, nesting
depth 64) remain as one-time parse checks.