# GETTING_STARTED — LLM companion

Machine-usable summary of `GETTING_STARTED.md` claims affected by the
version 4 migration (claims name their tests).

* `.gri` index files map 16-byte index keys — BLAKE3-derived for version
  4 clusters, MD5 for version 3 — to data file locations. Tests:
  `test_blake3_known_answer_vectors`, `test_on_disk_index_entry_is_32_bytes`,
  `test_v3_fixture_clusters_load_and_resolve`.
* `.grc` cluster files are version 4 with the algorithm declaration
  `BLAKE3[0..16]/Long/Long`; version 3 clusters carry no declaration
  (readers fall back to the first `.gri`). Tests:
  `test_writer_emits_version_4_envelopes`,
  `test_reader_follows_declared_algorithm`.
* `Item.connections` in any fetched JSON defaults to the map shape;
  `?item_format=v3` selects the legacy pair shape on item-emitting HTTP
  endpoints. Tests: `test_item_default_shape_is_map`,
  `test_item_format_v3_shape_is_legacy_pairs`.
* Merging version 3 and version 4 clusters is supported; output is
  version 4. Test: `test_mixed_merge_output_is_version_4`.
