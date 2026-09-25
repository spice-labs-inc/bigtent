# Goat Rodeo Producer Upgrade Guide: Writing Version 4

This guide is for the producers of Big Tent cluster files — notably Goat
Rodeo — and describes **only what changes when moving from version 3 to
version 4**. It is format-level and producer-side: no changes to Big
Tent are required, and Big Tent keeps reading version 3
(`test_v3_fixture_clusters_load_and_resolve`).

Four things change:

## 1. `Item.connections` changes shape

Version 3 stores `connections` as an array of `(edge type, target)`
pairs. Version 4 stores it as a **map of edge type to an array of target
identifiers**:

```text
V3:  "connections": [["contained:up", "gitoid:..."], ["alias:from", "pkg:..."]]
V4:  "connections": {"contained:up": ["gitoid:..."], "alias:from": ["pkg:..."]}
```

Map keys and target arrays are in sorted order; target arrays are
deduplicated. Big Tent's readers accept **both** shapes, so the change
can ship independently of cluster rewrites
(`test_item_legacy_pairs_cbor_deserialize`,
`test_item_v4_cbor_round_trip`).

## 2. The version numbers change

Version 4 clusters carry: cluster envelope `version: 4` (in the `.grc`)
and data file envelope `version: 2` (in each `.grd`). Big Tent rejects
other versions for version 4 clusters
(`test_writer_emits_version_4_envelopes`,
`test_envelope_version_cross_check`).

## 3. The index key hashing changes

Version 3 index keys are the MD5 digest of the identifier. Version 4
index keys are the **first 16 bytes (128 bits) of the BLAKE3 digest**
computed over the UTF-8 bytes of the identifier — the digest is 32 bytes
and bytes 0..16 (end-exclusive slice) are kept. Keys are compared as
unsigned byte strings; the 32-byte index entry layout (16 key + 8
data-file hash + 8 offset) is unchanged
(`test_blake3_known_answer_vectors`, `test_on_disk_index_entry_is_32_bytes`).

## 4. The algorithm is declared in the `.grc`

Version 4 clusters carry the constant `"BLAKE3[0..16]/Long/Long"` in the
cluster envelope's `encoding` field and in each index envelope's
`encoding` field. Big Tent's readers use the algorithm the files declare
(`test_reader_follows_declared_algorithm`).

## Verification checklist

1. Run `bigtent --rodeo <dir-with-produced-clusters> --check` — the
   produced clusters must validate.
2. Resolve identifiers against the produced clusters — the lookups must
   hit (`test_checked_in_v4_fixtures_load` proves BigTent-written v4
   fixtures resolve; the same construction applies to yours).
3. Compare a produced cluster against a BigTent-written reference:
   envelope versions, the `encoding` declaration, and the resolved items
   must agree. `test_hand_assembled_v4_cluster_golden_bytes` proves
   Big Tent's reader accepts externally assembled bytes beyond its own
   writer's output — that test is the interop contract.
