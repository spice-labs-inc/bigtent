# ARCHITECTURE — LLM companion

Machine-usable summary of `ARCHITECTURE.md` deltas under the version 4
migration (claims name their tests; full context in the human document).

## Data model

* `Item.connections`: `Connections(BTreeMap<String, BTreeSet<String>>)` —
  ordered map of edge type → target set. Tests:
  `test_item_v4_cbor_round_trip`, `test_item_serialize_canonical_deterministic`.
* Legacy view: `ItemV3` (pair set) with lossless conversions. Tests:
  `test_item_v3_round_trip`, `test_flattened_map_order_equals_legacy_pair_order`.
* Deserialization is dual-shape (map or legacy pairs; missing field =
  empty map). Tests: `test_item_legacy_pairs_cbor_deserialize`,
  `test_item_missing_connections_field_is_empty_map`.

## Index keys

* Version 4 keys: BLAKE3 over the identifier's UTF-8 bytes, digest bytes
  [0..16] (128 bits, end-exclusive). Test: `test_blake3_known_answer_vectors`.
* Version 3 keys: MD5. Both 16 bytes; the 32-byte index entry is
  unchanged. Test: `test_on_disk_index_entry_is_32_bytes`.
* The algorithm is declared per cluster: `.grc` `encoding` when present
  (v4), else the first `.gri`'s `encoding`. Unknown declarations fail
  load; readers never validate items against the declaration at load.
  Tests: `test_reader_follows_declared_algorithm`,
  `test_envelope_magic_validated`.

## Versions

* Readers accept 3 and 4; writers write 4 only. Tests:
  `test_cluster_envelope_rejects_unknown_versions`,
  `test_writer_emits_version_4_envelopes`.
* `.grc` framing: u32 magic + u16 envelope length + CBOR envelope (the
  envelope length prefix is 2 bytes). The inner envelope `magic` fields
  are validated. Tests: `test_envelope_magic_validated`,
  `test_short_grc_filename_rejected`.
* Mixed-version herds are supported; merges output v4; conversion
  re-keys v3 sources (byte-copy) before the coordinator. Tests:
  `test_mixed_herd_lookup_resolves_both_versions`,
  `test_mixed_merge_output_is_version_4`,
  `test_convert_v3_cluster_matches_source_items`.

## Threading (unchanged claims)

* Tokio multi-threaded runtime, `worker_threads = 100` (main.rs).
* Merge workers default to 75% of cores, cgroup-aware. Tests:
  `test_default_merge_worker_count_is_75_percent_of_cores`,
  `test_default_merge_worker_count_is_at_least_one`.

## Observability

* `GET /metrics` serves Prometheus text exposition. Test:
  `test_metrics_endpoint_responds`.
* Health endpoints: `/health`, `/healthz`, `/readyz`; `/openapi.json` is
  the only API specification. Test: `test_openapi_schema_contains_both_shapes`.
