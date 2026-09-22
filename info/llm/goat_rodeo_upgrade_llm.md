# Goat Rodeo Producer Upgrade — LLM companion

Machine-usable summary of `info/goat_rodeo_upgrade.md` (claims name
their tests).

## Constants

| Constant | Value | Where |
|----------|-------|-------|
| V4 cluster version | 4 | `.grc` envelope `version` |
| V4 data envelope version | 2 | `.grd` envelope `version` |
| V4 algorithm declaration | `"BLAKE3[0..16]/Long/Long"` | `.grc` envelope `encoding` + `.gri` envelope `encoding` |
| V3 algorithm declaration | `"MD5/Long/Long"` | `.gri` envelope `encoding` (no `.grc` declaration in v3) |
| V4 key derivation | BLAKE3(identifier UTF-8) digest bytes [0..16], end-exclusive, 16 bytes = 128 bits | index keys |
| Key comparison | unsigned byte strings, big-endian binary search unchanged | index lookups |
| Index entry | 16 key + 8 data-file hash + 8 offset = 32 bytes | `.gri` entries |

## Item shape

| Aspect | V3 | V4 |
|--------|----|----|
| `connections` | array of `[edge_type, target]` pairs | map of edge_type → array of targets |
| ordering | sorted pair set | sorted keys, sorted deduplicated target arrays |
| reader acceptance | yes | yes (dual-shape) |

## Version acceptance

Reader accepts cluster versions 3 and 4; everything else rejected. V4
requires data envelope 2. The `.gri` envelope version is unchanged (1).

## Error semantics

* Unknown cluster version: cluster load fails.
* Unknown algorithm declaration: cluster load fails.
* Readers never validate items against the declared algorithm at load.

## Claim → test table

| Claim | Test |
|-------|------|
| v3 stays readable | `test_v3_fixture_clusters_load_and_resolve` |
| v4 writer output carries versions 4/2 + declarations + inert chain fields | `test_writer_emits_version_4_envelopes` |
| envelope versions enforced | `test_envelope_version_cross_check`, `test_cluster_envelope_rejects_unknown_versions` |
| readers follow the declared algorithm | `test_reader_follows_declared_algorithm` |
| BLAKE3[0..16] derivation correct at 4 input lengths | `test_blake3_known_answer_vectors` |
| on-disk entry stride 32 with real keys | `test_on_disk_index_entry_is_32_bytes` |
| dual-shape reading | `test_item_legacy_pairs_cbor_deserialize`, `test_item_missing_connections_field_is_empty_map` |
| v4 round trip | `test_item_v4_cbor_round_trip` |
| externally assembled bytes interop | `test_hand_assembled_v4_cluster_golden_bytes` |
| checked-in v4 fixtures resolve | `test_checked_in_v4_fixtures_load` |
