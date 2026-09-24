# Phase 1: Hash Primitives, Safety Fix, and Dead API Removal

Status: planned. No on-disk behavior change in this phase: existing
clusters still use MD5 keys until phase 2 flips the format.

## Requirements addressed

* D2: BLAKE3 truncated to the first 16 bytes, compared as unsigned byte
  strings.
* D9: removal of the dead API surface listed in the scoping discussion.
* H1 (owner-accepted 2026-09-16): remove undefined behavior in CBOR error
  logging.

## Deliverables

### 1. Hashing primitives (`src/util.rs`, `Cargo.toml`)

* Add `blake3 = "1"` with minimal features to `[dependencies]`. Record the
  lock-file diff review and, if available in the environment, run
  `cargo audit`/`cargo deny`; record the exact commands and results in
  execution state. If neither tool is available, note that and rely on the
  lock diff review.
* Replace `pub type MD5Hash = [u8; 16]` with
  `pub type KeyHash = [u8; 16]` and rename every use in the crate.
* Add:

  ```rust
  /// The key-space algorithm used by a file format version.
  pub enum KeyAlg { Md5, Blake3Truncated128 }

  impl KeyAlg {
      /// Hash an identifier into the algorithm's 16-byte key space.
      pub fn hash_identifier(&self, identifier: &str) -> KeyHash;
  }
  ```

  `Blake3Truncated128` computes BLAKE3 over the UTF-8 bytes of the
  identifier and keeps bytes 0..16 of the 32-byte digest.
* Keep `md5hash_str` and add `blake3hash_str` as the single
  implementations; `KeyAlg` delegates.
* Delete `check_md5`, `millis_now`, `current_date_string`, `read_all`,
  `traverse_value`, `as_obj`, `as_array`; update the module-level doc list
  that mentions them.
* Delete the uncompiled dead module `src/mod_share.rs` (not referenced by
  `lib.rs`; it references a type that no longer exists). If the owner
  prefers to keep the file, it must not reference removed types and must
  be excluded from the removal-proof search.

### 2. H1 safety fix (`src/util.rs`)

* Replace both `unsafe { String::from_utf8_unchecked(buffer) }` call sites
  in `read_cbor` and `read_cbor_sync` with safe escaped or hex formatting
  (not plain `from_utf8_lossy`, so attacker bytes cannot forge log lines
  with control characters). These functions log the raw payload when a
  typed parse fails and a generic parse succeeds; with attacker-controlled
  bytes this is undefined behavior today.

### 3. Dead API removal

* `src/rodeo/goat_trait.rs`: remove `is_empty` and `item_for_hash` from
  the trait.
* `src/rodeo/goat.rs`: remove `is_empty`, `get_blob`, `get_data_files`,
  `get_index_files`, `common_parent_dir`, `find_data_file_from_sha256`;
  make `hash_to_item_offset` crate-private; `item_for_identifier`
  computes the key directly; keep `get_sha`, `get_cluster_file_hash`,
  `roots`, `has_identifier`. Replace `get_directory` with a narrower
  `cluster_directory()` accessor: phase 3 needs each source cluster's
  directory for temporary-root overlap validation.
* `src/rodeo/robo_goat.rs`, `src/rodeo/goat_herd.rs`,
  `src/rodeo/member.rs`: remove the corresponding trait implementations
  and forwarding methods, explicitly including `HerdMember::get_blob` and
  `HerdMember::get_directory`; `member.rs` keeps `get_sha`.
* `src/rodeo/writer.rs`: remove `add_index`.
* `src/item.rs`: remove `Item::is_same`.
* `src/item.rs`: remove `EdgeType::is_from`, `is_to`, `is_down`,
  `is_builds_to`; keep everything else, including `is_up`.
* Adapt `test_antialias` (`src/rodeo/goat.rs`) to fetch items through
  `item_from_item_offset`, with identical assertions. Record a before/after
  assertion inventory for every edited test in execution state, because a
  search cannot prove that no assertion was weakened.

## Tests (write first, expect red)

1. `test_blake3_known_answer_vectors` (unit).
   * Requirement: D2.
   * Theory: official BLAKE3 test vectors at several input lengths
     (empty, short, one block boundary, multi-block) exercised through
     `KeyAlg::Blake3Truncated128`, asserting the first 16 bytes of the
     digest. This pins the construction (unkeyed BLAKE3 over UTF-8 bytes,
     prefix truncation) at more than one length; a single short input
     cannot distinguish truncated BLAKE3 from BLAKE2 or a folded digest.
2. `test_md5_known_answer_vector` (unit).
   * Requirement: D2.
   * Theory: a fixed KAT for `KeyAlg::Md5` proves the dispatch still
     selects MD5 for version 3 and that the primitive is unchanged.
3. `test_on_disk_index_entry_is_32_bytes` (integration).
   * Requirement: D2, D3.
   * Theory: after writing a cluster with two items, decode the raw `.gri`
     and assert the entry stride is 32 bytes (16 key + 8 file hash + 8
     offset) and each 16-byte key equals the expected algorithm's key.
     This is the actual compatibility constraint; a `[u8; 16]` type-size
     assertion would be a compile-time tautology.
4. `prop_hash_is_deterministic` (property).
   * Requirement: D2.
   * Theory: for arbitrary generated strings, repeated hashing with the
     same algorithm returns identical keys, and the two algorithms do not
     return the same key for the same input. Distribution properties are
     not asserted by tests; they follow from the BLAKE3 and MD5 designs
     as recorded in ADR 0002.
5. `test_cbor_error_logging_does_not_panic_on_non_utf8` (unit).
   * Requirement: H1.
   * Theory: a synthetic CBOR value with non-UTF-8 bytes drives the typed
     parse failure path and must produce an error, not UB, and the
     diagnostic must escape control characters rather than emitting them
     raw. Runs under Miri where available; the execution state records
     whether Miri ran.
6. Existing suite compiles and passes unchanged apart from the enumerated
   test adaptations in the call-site inventory.
   * Requirement: D9.
   * Theory: removal without behavior change is proven by the existing
     suite, especially the version 3 corpus tests. Any red test means a
     removal removed behavior.
   * Note under D10: the phase 1 on-disk index layout test is expected to
     be adapted in phase 2 when the writer flips to BLAKE3. That
     adaptation is inventoried like every other edited test, with its
     semantic meaning preserved (the assertion changes from
     "MD5-keyed entry" to "the then-current algorithm's keyed entry").

## Documentation

No `info/` change: this phase has no user-observable runtime behavior. The
public API removals are recorded in execution state and described in the
downstream change description produced by the final documentation phase;
`info/` maintains no public API reference.

## Exit review (HS-2)

1. Gap review: every D9 symbol gone; search proves no call sites remain;
   per-test assertion inventory shows preserved meaning; H1 fixed at both
   call sites.
2. Claims verification: every new test run, read, and confirmed to test
   its claim.
3. Hostile reviewer: "Is any removed symbol actually used by a downstream
   consumer or in a way the compiler cannot see?"
4. Full suite regression: full `cargo test` summary with the exact count
   reconciled against the pre-change baseline recorded at plan approval in
   `execution_state/`; no ignored, skipped, or deselected tests.

## Adversarial review (rule 9)

Independent sub-agent review of implementation against this phase file;
remediate all gaps and repeat until clean.
