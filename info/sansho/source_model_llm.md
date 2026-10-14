# Sansho's source model — LLM copy

- Sansho evaluates programs over a SOURCE via the value-node
  abstraction (`sansho::view::Node`): nodes are cheap copyable
  positions; evaluation touches only what the program reads.
- TWO sources:
  - the byte source (`src/source.rs`): over raw CBOR bytes (mmap'd clusters);
    zero-copy — text strings are `&str` over the slice (CBOR text is
    raw UTF-8), numbers decode from headers, byte strings are `&[u8]`
    (base64url at the result boundary only). Seek = single-pass
    raw-key-bytes scan (O(depth), no key-string materialization).
  - `MaterializedNode` (value source): over in-memory
    `serde_json::Value` (merged herd items, invocation inputs).
- The backends AGREE byte-for-byte (differential-pinned).
- The boundary decode memo: navigation never decodes; each position
  materializes at most once per evaluation (the byte-once contract).
- The leaf fast paths: string-predicate args and the
  `[?starts_with(@, 'prefix')]` filter shape read borrowed leaves —
  no memo, no per-element recursion. Measured: scan 103.6 → 23.4 ms at
  100k; seek 12.2 → 1.5 ms (recorded baselines vs the `cbor_node`
  bench arms).
- `#[derive(SanshoNode)]` (the `sansho-derive` crate, workspace
  member; sansho never depends on it): `String`/`&str` → string leaf,
  `Option<String>` → optional string, `serde_json::Value` → tree walk,
  other → null `Skip`; `#[serde(rename)]` is a compile error.
- NO limits or data caps in the evaluation path (owner directive);
  the parser's parse-time expression bounds (length 16,384, depth 64)
  are one-time checks.
