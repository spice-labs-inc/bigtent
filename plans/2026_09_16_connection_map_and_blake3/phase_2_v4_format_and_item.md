# Phase 2: Version 4 File Format and the New Item Shape

Status: planned. One atomic format flip: new `Item`, version 4 writer,
version-dispatching reader, and H3 (partial), H4, and H5 together, so no
intermediate state writes a mixed-format cluster.

## Requirements addressed

D1-D6, D11, H3 (partial), H4, and H5.

## Deliverables

### 1. `src/item.rs`

* `connections: BTreeMap<String, BTreeSet<String>>`, documented shape; the
  legacy pair set folds in by inserting each target under its edge type
  (ADR 0001).
* Serde adapter accepting: the map shape; the legacy pair-array shape
  (deduplicated); and a missing field (empty map). Rejects wrong arity,
  non-string elements, and nested arrays with an entry-naming error.
  Serialization emits the map as stored, with sorted keys and sorted
  target sets.
* `PartialEq` for `Item` and `ItemV3` with documented semantics.
* `pub struct ItemV3` with legacy `BTreeSet<(String, String)>`
  connections, `Serialize`, `Deserialize`, `ToSchema`; plus
  `Item::to_v3()`, `From<&Item> for ItemV3`, `From<ItemV3> for Item`.
* Update `is_root_item`, `find_purls`, `contained_by`, `is_alias`, and
  `merge` for the map.
* Fix the module doctest.

### 2. Key algorithm declaration and version authority

Per approved ADR 0002, the index key algorithm is declared in the files:

* The constant `BLAKE3[0..16]/Long/Long` names the actual transformation:
  the first 16 bytes (128 bits) of the BLAKE3 digest over the UTF-8 bytes
  of the identifier, followed by the two big-endian u64 fields of the
  index entry.
* Version 4 behavior: the constant is carried in **both** the `.grc` and
  the `.gri` files. The writer always emits it in both.
* Version 3 clusters: the constant is not in the `.grc` (the checked-in
  V3 `.grc` files carry no algorithm declaration). Readers look in the
  **first `.gri` file** for the hash description - the checked-in V3
  corpus carries `MD5/Long/Long` there.
* Readers use the algorithm the files declare; BigTent neither assumes an
  algorithm from the version number nor validates items against the
  declared algorithm at load time (that would require hashing every
  item's identifier - a full pass over potentially multi-terabyte
  clusters).
* `GoatRodeoCluster` exposes `key_alg()` resolved from the `.grc`
  declaration when present, otherwise from the first `.gri`.
* Data-envelope version requirements stay as versioning rules of the
  file format: version 4 clusters carry data envelope 2, validated as
  envelope metadata at load. Unknown cluster versions are rejected on
  load, as today, extended to accept version 4.

### 3. H3 structural validation (partial)

* `DataFile::read_item_at`: reject an `item_len` larger than the remaining
  mapped bytes (log and return no item) before allocating. Rationale: a u32
  length inside a large mapping otherwise permits a 4 GiB allocation per
  request.
* Reject short `.grc` filenames instead of slicing blindly.
* Validate `magic` fields in the data and index envelopes, not only the
  file-level magic and the cluster envelope magic.
* Not in scope (owner-rejected): a configurable maximum item length, index
  `size * 32 == data_len` validation, zero-size index handling, per-entry
  data-file hash validation, and converting panics on corrupt files into
  errors. A corrupted index or data file is catastrophic and must not be
  silently swallowed.
* `--check` loads clusters through the same readers, so it covers the
  envelope magic validation; there is no deep mode and no binding check
  (ADR 0002 amendment).

### 4. (H4 removed)

Alias-cycle termination was removed by owner disposition
(2026-09-18): alias cycles are not a real problem, and a visited set
plus depth bound would cost compute and memory on every antialias
traversal. No cycle guard is implemented and no cycle test exists.

### 5. Read path

* `item_for_identifier`, `has_identifier`, `identifier_to_item_offset`
  use the cluster `key_alg`.
* `hash_to_item_offset` becomes crate-private; `item_from_item_offset`
  remains public and algorithm-agnostic.
* `RoboticGoat` keys offsets with BLAKE3.

### 6. Write path and H5 determinism

* `ClusterWriter` writes version 4 everything; tracks the last appended
  key and errors immediately on regression.
* The data envelope's `previous` and `depends_on` fields are inert in
  version 4: the writer always emits `previous: 0` and an empty
  `depends_on`; BigTent neither maintains nor consults the chain. The
  `previous_hash` atomic is removed. This keeps multi-file output
  byte-deterministic instead of depending on task scheduling.
* `IndexEnvelope.data_files` becomes `BTreeSet`.
* Add a constructor or option for a bounded writer buffer capacity, used
  by conversion; the default writer behavior is unchanged.
* Document the determinism scope: identical items, options, and key order
  yield byte-identical output.

### 7. Call-site migration and fixtures

* `fresh_merge.rs`, `goat_trait.rs`, `robo_goat.rs`, `bench_util.rs`,
  `benches/*`, tests: map literals and BLAKE3.
* `data_generator` gains `--max-data-file-size` (bytes) so multi-file
  determinism tests are possible.
* Generate small deterministic version 4 clusters (fixed seed and size)
  into `test_data/v4/`; record the command in execution state.
* Keep all version 3 clusters untouched.
* Check a neutral legacy-item JSON fixture into `test_data/` (no consumer
  names) for the dual-shape JSON test; tests never depend on untracked
  directories.
* Declare two test helpers: a raw version 4 cluster assembler (bytes, not
  the writer) for golden-bytes tests, and note that the version 3
  assembler arrives in phase 3.

## Tests (write first, expect red)

Item shape: 1 `test_item_v4_cbor_round_trip`; 2
`test_item_legacy_pairs_cbor_deserialize`; 3
`test_item_legacy_pairs_json_deserialize` (using the neutral fixture);
4 `test_item_serialize_canonical_deterministic`; 5
`test_item_missing_connections_field_is_empty_map`; 6
`test_item_v3_round_trip`; 7
`test_flattened_map_order_equals_legacy_pair_order`; 8
`test_item_merge_unions_connection_map`; 9
`test_block_list_retain_removes_blocked_targets`; 10
`test_legacy_connections_malformed_rejected`; 11
`prop_item_v4_cbor_round_trip`; 12
`prop_item_v3_pair_conversion_no_loss`; 13
`prop_connection_merge_is_union`; 14
`prop_legacy_pair_deserialization_is_permutation_invariant` (property:
any ordering of the same legacy pairs yields identical bytes); 15
`prop_item_merge_algebra` (property: connection union is commutative,
associative, and idempotent for generated items).

Format and algorithms: 16 `test_writer_emits_version_4_envelopes`; 17
`test_v3_fixture_clusters_load_and_resolve`; 18
`test_checked_in_v4_fixtures_load` (fails if fixtures are empty); 19
`test_reader_follows_declared_algorithm` (a v4 cluster carrying
`BLAKE3[0..16]/Long/Long` in the `.grc` and `.gri` resolves identifiers
with BLAKE3 keys; a v3 cluster resolves via the first `.gri`'s hash
description - MD5 for the checked-in corpus); 20
`test_envelope_version_cross_check` (cluster/data-envelope version
validation only: unknown cluster versions rejected; v4 requires data
envelope 2; no algorithm-bearing assertions); 21
`test_cluster_envelope_rejects_unknown_versions`; 22
`test_envelope_magic_validated` (wrong magic in the data and index
envelopes fails naming the file); 23
`test_grd_item_length_bounds` (length beyond remaining bytes returns
an Err naming the file, without allocating); 24
`test_short_grc_filename_rejected`.

Cycle and dispatch: 26
`test_v4_cluster_lookup_traversal_and_roots`; 27
`test_mixed_herd_lookup_resolves_both_versions` (a version 3 fixture
whose identifier also appears in a writer-built version 4 fixture is the
stated overlap construction); 28 `test_key_algorithm_dispatch_per_cluster`
(dispatch follows the `.grc` declaration, not the version number) in a
`#[cfg(test)]` module.

Determinism: 29 `test_writer_output_is_deterministic_multi_file`
(integration target using `CARGO_BIN_EXE_data_generator` with
`--max-data-file-size`; two child processes; compare `.grd`/`.gri` bytes
and names exactly, and `.grc` contents and hash suffix with the timestamp
prefix excluded; envelopes carry the inert `previous: 0` and empty
`depends_on`); 30 `test_writer_rejects_out_of_order_append`; 31
`test_hand_assembled_v4_cluster_golden_bytes` (handassembled v4 cluster
loads; used by the Goat Rodeo guide as external-producer interop).

Boundary: 32 `test_empty_and_single_item_clusters`.

### Test intent (requirement and theory; repeated in test source comments)

* Shape tests 1-15: the map must round-trip through CBOR and JSON, accept
  every legacy shape including duplicates and permutations, serialize
  deterministically (the same content yields the same bytes regardless of
  insertion history), convert to and from the legacy shape without loss,
  reject malformed input with entry-level errors, and preserve
  connection-union algebra. These are the format contract; any silent loss
  or nondeterminism corrupts content addressing.
* Format tests 16-24: version 4 files must carry the right versions,
  magic, and the `.grc` algorithm declaration; readers follow the
  declared algorithm per ADR 0002; version, magic, and length violations
  fail with file-naming errors and no oversized allocation for the
  item-length case. There is no algorithm validation against item keys
  at load.
* Dispatch tests 26-28: version 4
  clusters answer lookups, traversal, and roots; mixed herds resolve both
  versions; algorithms are selected per cluster.
* Determinism tests 29-31: multi-file writer output is byte-stable across
  processes so content-addressed names mean something, and a hand-assembled
  cluster proves third-party byte layouts are accepted.
* Boundary test 32: empty and single-item clusters are legal and must not
  special-case into failures.

## Documentation updates for this phase

Phase 2 owns `info/files_and_formats.md` format sections and creates
`info/llm/files_and_formats_llm.md`; updates `info/README.md`; updates the
`Item` struct and index diagrams in `ARCHITECTURE.md`, the example JSON in
`README.md`, and the schema annotation. Every claim links to a test above.

## Exit review (HS-2)

1. Gap review against this file; every deliverable and test present.
2. Claims verification: run each referenced test, read it, confirm it
   tests the claim.
3. Hostile reviewer: "Does any test pass for the wrong reason; is the
   legacy corpus actually exercised; can crafted files bypass version,
   encoding, magic, or length checks?"
4. Full suite regression with exact test-count reconciliation; no skipped
   tests.

## Adversarial review (rule 9)

Independent sub-agent review; remediate; repeat until clean.
