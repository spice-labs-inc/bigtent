# Phase 3: Mixed-Version Merge (Version 3 + Version 4)

Status: planned. Implements the approved ADR 0003 architecture: readers
accept version 3 and version 4, writers write version 4 only, and
mixed-version merges are supported with output always version 4.

## Requirements addressed

D6, D7, and D11 as conformed on 2026-09-18, plus the temporary-directory
safety (H6), large-corpus gate honesty (H9), and bounded-writer items
below.

## Design

The version 4 format changes the item shape and the index keys. Version 3
sources cannot enter the merge coordinator directly, because the
coordinator groups duplicates by equal index keys and a version 3
source's keys are in a different key space.

Conversion is a **re-keying pass**, not a decode/re-encode pass. For each
version 3 source cluster:

1. Walk the source `.gri` in ascending order. For each entry, go to the
   `.grd` file and offset, read the item length, and deserialize **only
   the identifier** (a one-field serde struct; serde skips the remaining
   fields without materializing them). Hash the identifier with
   `BLAKE3[0..16]` and track the tuple `(key, old grd file hash, old
   offset)`. Keep a running total of the framed item bytes.
2. At the existing writer split limits (15 GB of item bytes per data
   file, 25M entries per index file — one set of limits, shared with the
   normal writer), sort the tuples by `(key, old grd file hash, old
   offset)` and write the chunk.
3. Writing a chunk writes the `.grd`, `.gri`, and `.grc` **through the
   same writer code path the normal merge uses**, so a fix in one place
   fixes both. Item bytes are **copied verbatim** from the old `.grd` to
   the new `.grd` — no item deserialization, no re-encoding.
4. The chunk writer runs on a separate thread so the main thread can
   continue skimming the next batch.

The temporary cluster files therefore carry the **source's version and
item shape** (for a version 3 source: version 3 `.grc`, legacy pair items
in the `.grd`) with the index re-keyed to `BLAKE3[0..16]/Long/Long`, which
is declared in the `.gri`. Nothing about the item is converted; item
up-conversion happens at read time through dual-shape deserialization
(D5), including during the merge itself.

Read cost: `.gri` and `.grd` files are memory-mapped; both passes touch
pages in ascending order, so pages are warm or prefetched and the item
bytes are physically read once. Sort memory is 32 bytes per entry (at the
25M entry cap, about 800 MB). No conversion chunk-size flag exists: the
existing split limits bound chunking.

## Deliverables

### 1. Conversion module (`src/rodeo/convert.rs`, new)

```rust
pub struct ConversionOptions {
    pub temp_root: PathBuf,
}

/// Members plus the temporary directory that owns their files.
/// The merge must keep the guard alive until every member is gone.
pub struct ConvertedClusters {
    members: Vec<Arc<HerdMember>>,
    guard: tempfile::TempDir,
}

impl ConvertedClusters {
    pub fn members(&self) -> &[Arc<HerdMember>];
}

pub async fn convert_cluster_for_merge(
    cluster: &GoatRodeoCluster,
    options: &ConversionOptions,
) -> Result<Option<ConvertedClusters>>;
```

* `members` is private; callers borrow. The guard is closed explicitly
  with `TempDir::close()` so cleanup errors are observable, in addition
  to drop-on-unwind.
* Chunk files are verified (SHA256 against name) directly after each
  chunk write. This is verify-once protection at the trust boundary; the
  claim is scoped accordingly (no continuous protection).
* Writer errors inside chunk flushes are propagated with path context;
  conversion verifies the finalized chunk loads and has the expected
  entry count before returning.
* Conversion uses the same writer code path and the same split limits as
  the normal merge writer. Test-only injection (`#[cfg(test)]`) may
  shrink the split limits so chunk boundaries are exercisable in tests;
  there is no user-facing conversion size flag.

### 2. Merge integration (`src/fresh_merge.rs`)

* New entry point `merge_fresh_with_options(..., temp_dir)`.
  `merge_fresh` keeps its exact signature and delegates.
* The merge owns every `ConvertedClusters` guard for the whole run and
  joins the coordinator and all worker threads before closing the guards.
  No temporary file is closed while a thread can still read it.
* Originals' names and histories are captured before replacement, so the
  output history is opaque to conversion: verbatim original histories,
  one `convert_v3_to_v4` marker per converted input (`source_cluster`,
  `big_tent_commit`, `date`), then the normal merge marker listing
  original names. Temporary chunk clusters never appear in
  `history.jsonl` or the merge marker (ADR 0003 provenance contract).
* `max_merge_len` is computed after conversion.

### 3. Temporary root handling (H6)

* Default: random 0700 run directory under the system temporary directory
  (owner decision).
* Explicit `--merge-temp-dir`: canonicalize the root, the destination,
  and each input cluster directory (via the `cluster_directory()`
  accessor) before containment checks. Reject a root inside an input
  directory or inside the destination. A symlinked root is allowed
  (`/tmp` is a symlink on some systems); the per-run directory is always
  a fresh, real, 0700 path. Reject roots not owned by the effective user
  or writable by group/other, with an explicit `--force-temp-dir` override
  recorded in logs. An explicit root is never deleted; only its run
  subdirectories are.
* Free-space preflight from the summed version 3 input file sizes; fail
  fast with computed numbers.
* Cleanup guarantees: success, in-process errors, and panics (unwind).
  SIGKILL/abort/machine loss can strand `bigtent-merge-*`; operations
  guide documents a prefix-and-age stale sweep.
* Per-run cleanup runs through `spawn_blocking`; `close()` errors are
  logged and surfaced in the merge result.

### 4. Test-only fault injection

A `#[cfg(test)]` hook inside the conversion/merge pipeline supports:
* fail after writing N chunks (deterministic conversion failure);
* panic on the task that owns the `ConvertedClusters` guard (cleanup on
  unwind);
* fail after finalize and before verification (exercises the verify
  helper with a tampered file);
* shrink the split limits (chunk boundary exercise).
No environment heuristics (`tmpfs where available`, permissions under
root) are used by tests.

### 5. CLI (`src/main.rs`, `src/config.rs`)

* `--merge-temp-dir <path>`, `--force-temp-dir`. No conversion size flag:
  conversion shares the writer's existing split limits.
* Documented and tested for defaults and parsing.

### 6. Synthetic version 3 test cluster writer (test only)

Writes `.grd` (legacy pair CBOR, envelope 1), `.gri` (MD5, envelope 1,
`"MD5/Long/Long"`), `.grc` (version 3, correct file names). Production
never writes version 3.

## Tests (write first, expect red)

Conversion correctness: 1 `test_convert_cluster_matches_source_items`
(items byte-identical to the source; index keys equal
`BLAKE3[0..16]` of each identifier); 2
`test_conversion_chunk_count_is_controlled` (test-only split-limit
injection; observed counts 1, 2, many); 3
`test_split_limits_do_not_change_merge_result` (item sets); 4
`test_conversion_output_is_deterministic` (same fixture converted twice
yields byte-identical chunk files); 5
`test_converted_members_are_file_backed_in_temp_root` (direct conversion
result held by the test, plus a merge-level hook that observes files
while thread handles are live).

Merge semantics: 6 `test_mixed_merge_unions_duplicate_identifier`; 7
`test_mixed_merge_keeps_v3_only_and_v4_only_items`; 8
`test_mixed_merge_output_is_version_4`; 9
`test_existing_two_version_3_merge_semantics_preserved`; 10
`test_merge_history_is_opaque_to_conversion` (verbatim original
histories, one conversion marker per converted input with source name and
`big_tent_commit`, no temporary chunk names anywhere, merge marker lists
original names); 11 `test_block_list_mixed_versions` (blocked targets
removed, other targets and edge types intact).

Temp handling: 12 `test_merge_temp_dir_cleaned_on_success`; 13
`test_merge_temp_dir_cleaned_after_conversion_failure` (deterministic
chunk-N hook; asserts non-empty root before failure); 14
`test_merge_temp_dir_cleaned_on_panic` (panic injected on the
guard-owning task); 15 `test_explicit_temp_root_is_preserved`; 16
`test_temp_root_rejects_destination_overlap` and
`test_temp_root_rejects_input_overlap` (canonicalized); 17
`test_symlinked_explicit_temp_root_allowed`; 18
`test_temp_root_rejects_foreign_owned_or_world_writable` (override
honored and logged; the ownership predicate is unit-tested over
synthetic metadata so the branch is exercised unprivileged, and the
real-permissions integration branch is marked privileged-only and
documented); 19
`test_converted_file_tamper_detected_by_verify_helper` (verify-once
scope; unit test of the verify routine); 20
`test_conversion_write_error_surfaces_cause` (deterministic writer hook,
not filesystem heuristics).

Scale and robustness: 21 `test_many_chunk_merge` (about 100 chunks; file
descriptor count recorded); 22
`test_single_item_larger_than_split_limit`; 23
`test_empty_v3_cluster_conversion`; 24
`test_single_item_v3_cluster_conversion`; 25
`prop_split_limits_invariance_on_synthetic_clusters` (generator asserts
coverage: several edge types, multiple targets, unicode); 26
`prop_worker_count_and_buffer_limit_invariance`; 27
`test_large_corpus_merge` (corpus-gated; see below).

Corpus test 27 behavior:
* `BIGTENT_REQUIRE_LARGE_MERGE_CORPUS=1` with a missing, unset, or
  undersized corpus fails.
* Minimum scale is fixed before phase 6 begins (owner-visible value;
  initial proposal: at least 100 clusters and the owner-stated total byte
  size).
* Verification mode is chosen before the run and recorded: default is a
  full check of every index entry; if a sample is used, it is a
  pre-agreed deterministic sample (every Nth entry plus first and last per
  cluster), never a runtime budget decision.
* Assertions: output loads; sampled or full source identifiers resolve;
  per-input-cluster identifiers unique; output count between largest
  input and sum of inputs; `purls.txt` and `history.jsonl` exist and name
  original clusters; temporary root empty; destination contains no
  temporary clusters.
* Absent corpus without require-mode logs a loud punt marker and returns.

### Test intent (repeated in source comments)

* Conversion tests prove fidelity (byte-identical items, BLAKE3 keys),
  chunk control through the shared split limits, determinism, and spill
  visibility.
* Merge tests prove union, survival, version of output, preserved
  existing semantics, and conversion-opaque provenance per the ADR 0003
  history contract.
* Temp tests prove cleanup in every in-process termination mode, root
  validation, and tamper detection at the verify boundary.
* Scale tests prove many chunks work and resource behavior is observable.

## Documentation updates for this phase

Phase 3 owns `info/operations.md` merge sections: mixed-version merge,
flags, scratch estimate (~2x input, labeled estimate), cleanup guarantee
and SIGKILL exception, stale sweep, and the large-corpus procedure
including both corpus environment variables as test-only, punt/fail
semantics, minimum scale, and sampling mode. It updates `info/config.md`
for the new flags, creates `info/llm/operations_llm.md` and
`info/llm/config_llm.md`, and updates `info/README.md`. Observed run
metrics never appear in `info/`; they live in execution state. Every
claim links to a test above.

## Exit review (HS-2)

1. Gap review; all deliverables and tests present.
2. Claims verification: run each referenced test, read it, confirm it
   tests the claim, especially crash cleanup, tamper scope, and history.
3. Hostile reviewer: "Can a temp cluster outlive or be altered; is output
   independent of chunk boundaries, worker counts, and queue limits?"
4. Full suite regression with exact test-count reconciliation; no skipped
   tests.

## Adversarial review (rule 9)

Independent sub-agent review; remediate; repeat until clean.
