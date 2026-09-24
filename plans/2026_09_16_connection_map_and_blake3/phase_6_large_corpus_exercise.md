# Phase 6: Large Corpus Merge Exercise

Status: planned. Depends on phase 3 (merge) and phase 5 (documentation).
The owner supplies the corpus; the test is corpus-gated with a require
mode, and phase completion requires a recorded real run.

## Requirements addressed

* D11: 100+ clusters of roughly 16 GB each, produced by Goat Rodeo, merged
  by BigTent with a recorded result.
* H9: corpus gate honesty.
* H12: documentation carries functional claims only, each naming its
  test; the run and its numbers live only in `execution_state/`.

## Preconditions

* A scratch volume with room for the temporary conversion clusters and the
  output: budget roughly 2x the input bytes plus a safety margin (one
  input-sized temporary copy, plus output up to approximately input size).
  The run records actual peak usage.
* A destination filesystem for the merged output.
* The owner-supplied corpus directory.
* Before the run: the minimum scale (cluster count and total bytes) and
  the verification mode (full or a pre-agreed deterministic sample) are
  recorded here, then used by the test. Neither is chosen at runtime.
* A release build.

## Procedure

1. Enumerate the corpus:
   `bigtent --rodeo <corpus-dir> --check` and record the cluster count and
   node count in execution state.
2. Run the merge through the product path:

   ```bash
   bigtent --fresh-merge <corpus-dir> \
       --dest <output-dir> \
       --merge-temp-dir <scratch-dir> \
       --buffer-limit <n> \
       --merge-worker-count <n>
   ```

3. Run the corpus-gated test in require mode:

   ```bash
   BIGTENT_LARGE_MERGE_CORPUS=<corpus-dir> \
   BIGTENT_REQUIRE_LARGE_MERGE_CORPUS=1 \
   cargo test --release test_large_corpus_merge -- --nocapture
   ```

4. Verify the output: `bigtent --rodeo <output-dir> --check`, then load a
   herd over the output and resolve identifiers sampled from every input
   cluster.
5. Assert and record:
   * every source identifier resolves (full check, or the documented
     deterministic sample: every Nth index entry plus first and last per
     input cluster; the mode used is recorded),
   * per-input-cluster identifiers are unique,
   * output count is between the largest input and the sum of inputs,
   * `purls.txt` and `history.jsonl` exist and name every original input
     cluster,
   * the temporary root is empty afterwards and no `bigtent-merge-*`
     directory remains,
   * the destination contains no temporary clusters.
6. Measure with named tooling so numbers are auditable: `/usr/bin/time -v`
   for wall clock and peak RSS, periodic `df`/`du` samples on the scratch
   and destination volumes for peak disk use, and the process file
   descriptor count observed during conversion.
7. Record commands, environment, timings, resource peaks, test output,
   and results in
   `execution_state/2026_09_16_connection_map_and_blake3_phase_6_large_corpus.md`.

## Tests

* `test_large_corpus_merge` from phase 3 is the test executed here. This
  phase adds no behavior; it exercises and records. Defects found during
  the run are fixed with a new test first, then rerun.
* Run the full suite in both modes and record both: with the corpus
  variable unset (documented punt path) and with require mode on the
  corpus (real assertions).

## Documentation

Phase 6 appends to `info/operations.md` the "Large merge runbook" section:
resource estimation and its measurement basis expressed as a labeled
estimate, command lines, verification steps, cleanup checks, failure
recovery, and the corpus environment variables
(`BIGTENT_LARGE_MERGE_CORPUS`, `BIGTENT_REQUIRE_LARGE_MERGE_CORPUS`)
marked test-only with their punt/fail semantics, the minimum scale agreed
before the run, and the chosen verification mode. Every claim links to
`test_large_corpus_merge`. Observed wall-clock, memory, disk, descriptor,
and run-occurrence numbers live only in `execution_state/`. The companion
`info/llm/operations_llm.md` is updated in lockstep.

## Exit review (HS-2)

1. Gap review against this file and the runbook; no step skipped.
2. Claims verification: each documentation claim resolves to the
   corpus-gated test; the execution record is reviewed separately for the
   run's sampling mode and require-mode behavior.
3. Hostile reviewer: "Does the execution record convince a hostile
   engineer; were any identifiers unchecked and is the sampling stated;
   was the temporary root really empty; did anything depend on a warm
   cache?"
4. Full suite regression with exact test-count reconciliation, covering
   both corpus modes.

## Adversarial review (rule 9)

Independent sub-agent review of recorded evidence against the plan and
documentation; remediate all gaps; repeat until no gaps.
