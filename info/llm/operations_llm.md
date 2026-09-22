# Operations Guide — LLM companion

Machine-usable summary of `info/operations.md` merge-relevant sections.
Claims name their tests; execution evidence lives only in
`execution_state/`.

## Mixed-version merge (ADR 0003)

* Readers accept v3 and v4; writers write v4 only; mixed merges output v4
  always. Tests: `test_mixed_merge_output_is_version_4`,
  `test_v3_fixture_clusters_load_and_resolve`,
  `test_checked_in_v4_fixtures_load`.
* v3 sources are converted (re-keyed) before the coordinator: item bytes
  copied verbatim, keys = `BLAKE3[0..16]`, chunks sorted by
  `(key, old grd file hash, old offset)`; no duplicate detection anywhere;
  out-of-order/colliding data surfaces during the merge. Tests:
  `test_convert_v3_cluster_matches_source_items`,
  `test_conversion_output_is_deterministic`,
  `test_conversion_chunk_count_is_controlled`,
  `test_mixed_merge_unions_duplicate_identifier`,
  `test_existing_two_version_3_merge_semantics_preserved`.
* Conversion chunking shares the writer's split limits (15 GB per data
  file / 25M entries per index); a single item larger than the budget
  gets its own chunk. Tests: `test_conversion_chunk_count_is_controlled`,
  `test_single_item_larger_than_split_limit`,
  `prop_split_limits_invariance_on_synthetic_clusters`.
* History contract: output history = verbatim original histories + one
  `convert_v3_to_v4` marker per v3 input (`source_cluster`,
  `big_tent_commit`, `date`) + the merge marker listing original names;
  temporary chunk names never appear. Test:
  `test_merge_history_is_opaque_to_conversion`.
* Block lists work across mixed versions:
  `test_block_list_mixed_versions`.

## Temporary directory contract (H6)

* Default: random 0700 `bigtent-merge-*` dir under the system temp dir;
  removed on success, error, and panic. Tests:
  `test_merge_temp_dir_cleaned_on_success`,
  `test_merge_temp_dir_cleaned_after_conversion_failure`,
  `test_merge_temp_dir_cleaned_on_panic`.
* `--merge-temp-dir`: canonicalized containment validation (inputs/dest),
  ownership + mode checks with `--force-temp-dir` logged override,
  explicit roots never deleted (only run dirs inside). Tests:
  `test_temp_root_overlap_validation`,
  `test_temp_root_ownership_predicate`,
  `test_merge_temp_dir_cleaned_on_success`,
  `test_temp_root_rejects_destination_overlap`.
* Free-space preflight (about 2x converted input, labeled estimate):
  `test_free_space_preflight`.
* SIGKILL stranding: sweep `bigtent-merge-*` by prefix and age
  (runbook command in operations.md).
* Verify-once at the trust boundary: written chunks hash-verify against
  their names. Test: `test_converted_file_tamper_detected_by_verify_helper`.

## CLI flags

`--merge-temp-dir <path>` (default None → system temp),
`--force-temp-dir` (default false). Test: `test_merge_temp_dir_flags_parse`.
Worker/buffer invariance: `prop_worker_count_and_buffer_limit_invariance`.
Large corpus: `test_large_corpus_merge` (corpus-gated; env vars
`BIGTENT_LARGE_MERGE_CORPUS`, `BIGTENT_REQUIRE_LARGE_MERGE_CORPUS`).
