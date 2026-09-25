# PERFORMANCE — LLM companion

Machine-usable summary of `PERFORMANCE.md` claims affected by the
version 4 migration (claims name their tests).

* Fresh-merge worker threads default to 75% of available cores,
  cgroup-aware, minimum 1; `--merge-worker-count` overrides. Tests:
  `test_default_merge_worker_count_is_75_percent_of_cores`,
  `test_default_merge_worker_count_is_at_least_one`.
* Tokio runtime: `worker_threads = 100` (server mode; unchanged).
* Mixed-version merges convert version 3 sources first (re-keying pass,
  items copied verbatim); conversion chunk budgets share the writer's
  split limits (15 GB per data file / 25M entries per index) and do not
  change the merge result. Tests: `test_convert_v3_cluster_matches_source_items`,
  `prop_split_limits_invariance_on_synthetic_clusters`,
  `test_many_chunk_merge`.
* The writer's output is byte-deterministic for identical items, options,
  and key order. Test: `test_writer_output_is_deterministic_multi_file`.
* No new performance numbers are claimed by the migration; observed run
  metrics live only in `execution_state/` (H12).
