# Configuration Reference — LLM companion

Machine-usable summary of the merge-related flags (full CLI reference in
`info/config.md`).

## Merge flags

| Flag | Default | Meaning | Test |
|------|---------|---------|------|
| `--fresh-merge <paths>...` | required | Input cluster directories | (product path) |
| `--dest <path>` | required | Output directory | (product path) |
| `--buffer-limit <n>` | 10000 | Max items in the merge queue | `prop_worker_count_and_buffer_limit_invariance` |
| `--merge-worker-count <n>` | 75% of cores | Parallel fetch/merge workers | `prop_worker_count_and_buffer_limit_invariance` |
| `--merge-buffer-size <gb>` | 15 | Per-file data buffer size (GB) | `test_new_with_max_size_respects_limit` |
| `--merge-temp-dir <path>` | random 0700 dir under system temp | Temporary conversion scratch root; explicit roots never deleted | `test_merge_temp_dir_flags_parse`, `test_merge_temp_dir_cleaned_on_success` |
| `--force-temp-dir` | off | Override temp-root ownership/mode checks (logged) | `test_temp_root_ownership_predicate` |
| `--block-list <path>` | none | Identifiers to exclude | `test_block_list_mixed_versions` |

## Test-only environment variables (never for production use)

| Variable | Meaning | Test |
|----------|---------|------|
| `BIGTENT_LARGE_MERGE_CORPUS` | Corpus directory for the large merge test | `test_large_corpus_merge` |
| `BIGTENT_REQUIRE_LARGE_MERGE_CORPUS=1` | Fail (don't punt) when the corpus is missing | `test_large_corpus_merge` |
