# Files and Formats — LLM companion

Machine-usable summary of `info/files_and_formats.md`. Every claim names
the test that validates it (H12: user-facing documentation never
references execution evidence).

## Version matrix

| Artifact | Version 3 | Version 4 (current, writer default) |
|----------|-----------|--------------------------------------|
| `.grc` cluster envelope `version` | 3 | 4 |
| `.grc` `encoding` declaration | absent (None) | `"BLAKE3[0..16]/Long/Long"` |
| `.grd` data envelope `version` | 1 | 2 |
| `.gri` index envelope `version` | 1 | 1 (unchanged; `encoding` names the algorithm) |
| `.gri` `encoding` | `"MD5/Long/Long"` | `"BLAKE3[0..16]/Long/Long"` |
| Index key derivation | MD5(identifier UTF-8), 16 bytes | BLAKE3(identifier UTF-8) digest bytes [0..16] (128 bits), end-exclusive slice |
| Item `connections` wire shape | array of `[edge_type, target]` pairs | map of edge_type to array of targets |
| Reader acceptance | yes | yes |
| Writer | never written by BigTent (test fixtures only) | the only format BigTent writes |

Tests: `test_writer_emits_version_4_envelopes` (v4 envelopes carry
versions 4/2, the `.grc`+`.gri` encoding constant, inert chain fields),
`test_v3_fixture_clusters_load_and_resolve` (v3 corpus reads via the
first-`.gri` declaration), `test_envelope_version_cross_check` (v4
requires data envelope 2; unknown cluster versions rejected),
`test_cluster_envelope_rejects_unknown_versions`,
`test_reader_follows_declared_algorithm` (readers follow the declared
algorithm; a v4 cluster declaring MD5 resolves with MD5 keys).

## Algorithm declaration resolution (ADR 0002)

1. If the `.grc` envelope has `encoding`, that is the algorithm.
2. Else read the first `.gri` envelope's `encoding`.
3. Unknown constant: cluster load fails naming the constant.
4. Readers never validate items against the declared algorithm at load
   (that would hash every identifier — a full pass).

## Item shape (version 4)

* `identifier`: required string.
* `connections`: required (default empty) map: edge type string → array
  of target identifier strings; keys sorted, target arrays sorted and
  deduplicated; empty target arrays are preserved as given.
* `body_mime_type`, `body`: optional.
* Dual-shape reading: legacy pair arrays fold into the map (targets
  inserted under their edge type; duplicates deduplicated); missing
  field = empty map; wrong arity / non-strings / nested arrays are
  rejected with entry-naming errors.
* Legacy view: `ItemV3` (public) — `connections` is the sorted pair set.

Tests: `test_item_v4_cbor_round_trip`,
`test_item_legacy_pairs_cbor_deserialize`,
`test_item_legacy_pairs_json_deserialize`,
`test_item_missing_connections_field_is_empty_map`,
`test_item_v3_round_trip`, `test_item_serialize_canonical_deterministic`,
`test_legacy_connections_malformed_rejected`,
`test_item_merge_unions_connection_map`,
`test_block_list_retain_removes_blocked_targets`,
`test_flattened_map_order_equals_legacy_pair_order`.

## Framing (all three file types)

Big-endian u32 magic, then big-endian u16 envelope byte length, then the
CBOR envelope. `.grd` items follow as big-endian u32 byte length plus
CBOR item. An item length beyond the file's remaining bytes fails with
an `Err` naming the file (`test_grd_item_length_bounds`); a too-short
`.grc` name is rejected (`test_short_grc_filename_rejected`); envelope
`magic` fields are validated (`test_envelope_magic_validated`).

## Writer determinism (H5)

Identical items, options, and key order yield byte-identical output:
chain fields inert (`previous: 0`, empty `depends_on`), `data_files` an
ordered set, keys appended strictly non-decreasing (regression rejected
immediately). Tests: `test_writer_output_is_deterministic_multi_file`,
`test_writer_rejects_out_of_order_append`,
`test_empty_and_single_item_clusters`.

## Index entry

32 bytes: 16 key + 8 data-file hash (mangled SHA256, big-endian u64) + 8
offset. Test: `test_on_disk_index_entry_is_32_bytes`.

## Mixed herds and merges

Version 3 and version 4 clusters may coexist in one herd
(`test_mixed_herd_lookup_resolves_both_versions`); merges produce
version 4 output (`test_mixed_merge_output_is_version_4`, phase 3).

## HTTP wire shapes (D8)

* Default = map shape (`connections` object of arrays); `?item_format=v3`
  = legacy pair array; `?item_format=v4` = explicit default.
* Rejected (400, static message): unknown/empty/wrong-case values,
  conflicting duplicate params; identical duplicates accepted.
* Applies to: `/item/{gitoid}`, `/item`, `POST /bulk`, all `/aa` forms,
  full-item `/north` forms. No effect on: `/flatten*` (identifiers),
  `/north_purls` (identifiers), `/purls`, `/node_count`, `/health`.
* Errors are static strings; nothing internal is echoed.
* Spec: `/openapi.json` from a running server only; documents both
  `Item` and `ItemV3` schemas plus the parameter on applicable paths.
* Tests: `test_item_default_shape_is_map`,
  `test_item_format_v3_shape_is_legacy_pairs`,
  `test_item_format_explicit_v4`, `test_item_format_invalid_rejected`,
  `test_item_format_applies_to_bulk`,
  `test_item_format_applies_to_aa_endpoints`,
  `test_item_format_applies_to_north_full_items`,
  `test_flatten_returns_identifiers_regardless_of_item_format`,
  `test_identifier_streams_unaffected_by_item_format`,
  `test_openapi_schema_contains_both_shapes`,
  `prop_default_and_v3_responses_are_semantically_equal`.
