# Plan: Connections Map and BLAKE3 Index Keys (File Format Version 4)

Date: 2026-09-16
Status: DRAFT, pending owner approval
Author: assistant, under owner direction

## Executive summary

`Item.connections` becomes `BTreeMap<String, BTreeSet<String>>` so that
"all connections of type X" is a single map lookup instead of a full set
walk. The index key widens to a fast modern hash: BLAKE3 truncated to the
first 16 bytes, replacing MD5. Both changes are on-disk format changes, so
the plan introduces file format version 4, keeps version 3 readable,
supports mixed-version herds, and implements a chunked, disk-spilled
upgrade path so version 3 clusters can be merged with version 4 clusters.

Three invariants govern the work:

1. Determinism. Ordered maps and sets only; serialized bytes must be
   reproducible because file names are content hashes.
2. Semantic preservation. Every existing test keeps its meaning; no test
   may become an automatic pass.
3. No non-open-source names in tracked artifacts. Downstream consumers are
   referred to as "downstream consumers". Goat Rodeo is open source and
   may be named. `/workspace/` is added to `.gitignore` when the
   downstream change description is produced, so that file cannot be
   committed accidentally.

## Locked decisions

Full rationale and provenance are in
`discussions/2026_09_16_connections_map_and_blake3_scoping.md`.

| # | Decision |
|---|----------|
| D1 | `connections: BTreeMap<String, BTreeSet<String>>`; the legacy pair set folds in by inserting each target under its edge type (ADR 0001) |
| D2 | Index key = first 16 bytes of BLAKE3, compared as unsigned byte strings; the encoding constant is `BLAKE3[0..16]/Long/Long` |
| D3 | New clusters: cluster `version: 4`, data envelope `version: 2` (the item shape changed). The `.gri` envelope version is **unchanged** — nothing in the `.gri` format changes; the `encoding` string's value (`BLAKE3[0..16]/Long/Long`) already distinguishes the key algorithm. V4 carries the constant in **both** the `.grc` and the `.gri`. V3 carries no algorithm declaration in the `.grc`; readers fall back to the first `.gri`'s hash description (the checked-in V3 corpus carries `MD5/Long/Long` there). Readers use the algorithm the files declare; writers write 4 only |
| D4 | Legacy shape is public `ItemV3`; `Item::to_v3()`, `From<&Item> for ItemV3`, `From<ItemV3> for Item` |
| D5 | `Item` deserialization accepts legacy pairs and new map (CBOR and JSON); serialization emits the map unless legacy is requested |
| D6 | Mixed-version herds allowed; one version per cluster; up-conversion after read |
| D7 | Mixed-version merge converts version 3 clusters to temporary, disk-spilled clusters re-keyed to BLAKE3 through the shared writer code path, in chunks bounded by the existing writer split limits; item bytes are copied verbatim (no conversion of the Item); `--merge-temp-dir`, default random directory under the system temp directory, cleaned up on success and on in-process failure |
| D8 | HTTP: map shape is default; `?item_format=v3` selects legacy shape on every item-emitting endpoint; invalid values rejected; OpenAPI documents both |
| D9 | Dead API removal list (see discussion record, decision 9) |
| D10 | Existing tests may be edited en masse only with preserved semantics; never an automatic pass |
| D11 | Keep all version 3 fixtures; add `test_data/v4/`; large corpus is owner-supplied and corpus-gated |
| D12 | No non-open-source names in plans, `info/`, or code comments |
| D13 | Public format-level Goat Rodeo producer upgrade guide under `info/` with an LLM companion |

## Hardening decisions (owner disposition, 2026-09-16; revised 2026-09-18)

Each item was presented and decided in isolation. Descriptions are
self-contained. Rejected/removed items are out of the phases.

| # | Item | Disposition |
|---|------|-------------|
| H1 | The CBOR error-logging path (`read_cbor`/`read_cbor_sync` in `src/util.rs`) logs raw payload bytes using `String::from_utf8_unchecked`, which is undefined behavior when the bytes are not valid UTF-8 (attacker-controllable). Fix: log safely escaped or hex-formatted bytes instead. | Accepted |
| H2 | On lookup, verify that the item's identifier actually hashes to the index key it was found under. | Rejected. Item lookups keep current behavior |
| H3 | Structural validation of cluster files before allocating memory: (a) an item length in a `.grd` larger than the remaining mapped bytes must return an `Err` naming the file, before any allocation; (b) reject too-short `.grc` file names instead of slicing them blindly; (c) validate the `magic` field inside the data and index envelopes, not just the file-level magic. | Partial. (a), (b), (c) accepted. Rejected: a configurable maximum item length, an index `size * 32 == data_len` check, zero-size handling, per-entry data-file hash validation, and converting panics on corrupt files into errors — a corrupted index or data file is catastrophic and must not be silently swallowed |
| H4 | Terminate alias-chain traversal on cycles (a visited set plus depth bound in the antialias walk). | Removed 2026-09-18: alias cycles are not a real problem, and the guard would cost compute and memory on every traversal |
| H5 | Writer determinism: the data-file chain fields (`previous`, `depends_on`) are inert — the writer always emits `previous: 0` and an empty `depends_on`; `IndexEnvelope.data_files` becomes a `BTreeSet`; `data_generator` gains `--max-data-file-size` (a cap on each output `.grd` file, enabling multi-file output tests). | Accepted |
| H6 | Merge scratch-directory safety: the temporary directory defaults to a random 0700 directory; `--merge-temp-dir` overrides it; roots overlapping inputs or the destination are rejected after canonicalization; explicit roots are never deleted; cleanup runs on success, in-process errors, and panics; the operations guide documents a stale sweep for SIGKILL. | Accepted |
| H7 | Pre-merge validation: every source's index keys strictly ascending; error when two sources contribute equal keys with different identifiers; a conversion duplicate scan across chunk boundaries. | Removed 2026-09-18: out-of-order indexes are detected during the merge; the equal-key/different-identifier check and the conversion duplicate scan are rejected outright. No preflight machinery exists |
| H8 | Bound conversion memory with a dedicated conversion chunk-size flag and byte budget. | Superseded 2026-09-18: conversion shares the writer's existing split limits (15 GB per data file, 25M entries per index) — one set of limits, no new flag |
| H9 | Large-corpus test honesty: with `BIGTENT_REQUIRE_LARGE_MERGE_CORPUS=1`, a missing, unset, or undersized corpus fails the test instead of silently skipping; without the variable, a documented punt. | Accepted |
| H10 | Repository specification hygiene: delete the stale `openapi.yaml`; `/openapi.json` from a running server is the only specification; no code-generated artifacts checked in. | Accepted |
| H11 | Verify that downstream consumers still compile after the API changes. | Rejected: compile verification is the consumer's responsibility; the change description only states what changed |
| H12 | Documentation claims reference the tests that validate them. Execution evidence — test counts, timings, run logs — is **never** referenced from user-facing documentation and lives only in `execution_state/`. | Accepted (wording corrected 2026-09-18) |

## Non-goals

* Writing version 3 files. The writer emits version 4 only; version 3 test
  clusters are hand-built by test helpers.
* Changing SHA256 file naming, `history.jsonl`, or `purls.txt` formats.
  The data-file chain fields (`previous`, `depends_on`) become inert
  constants in version 4 under H5.
* Making Goat Rodeo a reader of BigTent files; the guide is producer-side.
* Changing identifier strings or PURL semantics.
* Performance benchmarking beyond the existing benchmarks; no performance
  claim is made or documented.

## Constraints

* Test-first: each phase starts by implementing the tests listed for that
  phase, in the order listed, and the tests must fail for the expected
  reason before the implementation lands.
* Every test carries, in its source comments: the requirement it tests, the
  theory of why the test is meaningful, and a note suitable for humans and
  LLMs.
* Every new or changed public item carries rustdoc explaining shape,
  ordering, and version applicability.
* Every documentation claim is backed by a named test. Claims are
  functional statements; test counts, timings, peak memory or disk,
  descriptor counts, and any evidence that a test or run executed are
  forbidden in documentation and live only in `execution_state/`.
* Each phase ends with the four-step phase exit review: gap review, claims
  verification, hostile reviewer check, full-suite regression with no
  skipped tests and test-count reconciliation against the previous phase.
* Each phase gets an independent adversarial review of the implementation
  against the plan and documentation.
* Binary-level tests use a declared `tests/` integration target with
  `env!("CARGO_BIN_EXE_bigtent")` and `env!("CARGO_BIN_EXE_data_generator")`
  where a child process is needed; fixtures live under `test_data/`, never
  in untracked directories.
* No test may compare source or documentation text. Generated artifacts
  come from a generator command, never a document-diffing test.

## Testing strategy

1. **Unit and boundary tests** for every changed function: malformed
   input, empty input, unicode, duplicate legacy pairs, unknown edge
   types, wrong versions, wrong encodings, crafted files, and injection
   hooks.
2. **Property tests** with `proptest` for round trips, conversion and
   merge equivalence, merge algebra, determinism, split-limit and
   worker-count invariance, and generator coverage assertions.
3. **Corpus tests** against checked-in version 3 clusters, generated
   version 4 fixtures under `test_data/v4/`, and the owner-supplied large
   corpus with require-mode gating.

## Phase index

| Phase | File | Deliverable | Depends on |
|-------|------|-------------|------------|
| 1 | `phase_1_hash_primitives_and_removals.md` | BLAKE3 primitives, `KeyAlg`, dead API removals, H1 | plan approval |
| 2 | `phase_2_v4_format_and_item.md` | New `Item`, `ItemV3`, dual-shape serde, v4 writer, v3 reader, H3 partial/H4/H5, fixtures | 1 |
| 3 | `phase_3_mixed_version_merge.md` | Conversion, temp spill, H6/H7/H8/H9 test, merge flags | 2 |
| 4 | `phase_4_http_item_format.md` | `?item_format=v3`, endpoint audit, H10 removal, route tests | 2 |
| 5 | `phase_5_documentation_and_goat_rodeo_guide.md` | `info/` rewrite (human + LLM), Goat Rodeo guide, downstream change description, claims sweep | 2, 3, 4 |
| 6 | `phase_6_large_corpus_exercise.md` | Corpus-gated large merge executed and recorded, runbook | 3, 5 |

Phases 3 and 4 may run in parallel; both only need phase 2.

## Review log (required by project rules)

* Round 1 (2026-09-16): QA, principal engineer, red-team security, and
  technical writer. Remediated into revision 2: H1-H12, endpoint audit,
  temp-guard ownership, corpus require-mode, encoding authority, binding,
  determinism, Goat Rodeo guide completeness, documentation ownership.
* Round 2 (2026-09-16): all four reviewers re-ran. Residuals remediated
  into revision 3: requested-identifier equality and merge-path binding,
  cross-chunk duplicate authority, strict ascending v4 validation,
  deterministic fault-injection hooks, thread join and `TempDir::close`,
  canonicalized temp-root checks, container-safe chunk default, bounded
  writer buffer, `--check-deep` definition, `RawQuery` duplicate handling,
  neutral legacy fixtures, H11 checkpoint, explicit per-item H approval,
  guide magic/offset/required-field completeness, and observed-metrics
  placement.
* Round 3 (2026-09-16): QA and principal engineer flagged residuals;
  red-team security and technical writer reported no further feedback.
  Remediated: index envelope version is producer-specific and accepted
  as 1 or 2 regardless of cluster version (the checked-in version 3
  corpus uses 2; `encoding` remains the sole algorithm authority), test
  20 expanded accordingly, phase 5 duplicate stale-claim row removed,
  phase 2a regression gate restored, and the temp-root ownership test
  split into a unit-tested predicate plus a documented privileged
  integration branch.
* Round 4 (2026-09-16): confirmation re-review of revision 4. QA and
  principal engineer both report no further feedback; red-team security
  and technical writer reported no further feedback in round 3. The
  mandated review cycle is complete with all reviewer findings
  remediated.
* Owner dispositions (2026-09-16, after the review cycle): each hardening
  item H1-H12 was presented and decided in isolation; outcomes are in the
  Hardening decisions table above. H2 was rejected and ADR 0002 was
  amended to remove binding; H3 was partially accepted; H5 item 1 became
  inert `previous`/`depends_on`; H10 became removal; H11 was rejected;
  H12 was revised to forbid execution evidence in documentation. The
  phases were updated to match, and phase 2a was removed.
* Re-review (2026-09-18): the owner reopened all three ADRs and rewrote
  them to architecture-only content (no implementation mechanics, no
  version-to-algorithm coupling, no transient references); all three were
  re-approved on 2026-09-18. The plan was conformed: the algorithm
  constant is `BLAKE3[0..16]/Long/Long`, carried in both `.grc` and
  `.gri` for V4, with the reader falling back to the first `.gri` for V3;
  conversion became a byte-copy re-keying pass sorted by `(key, old grd
  file hash, old offset)` through the shared writer; H4 and H7 were
  removed; H8 was superseded by the existing split limits; H12 wording
  was corrected (execution evidence is never referenced from
  user-facing documentation); the phase 5 Goat Rodeo guide was shrunk to
  the conversion description. The oversized-item check returns `Err`.

## Architectural decision records

* A1: Connections stored as an ordered map of edge type to target set.
  **Approved 2026-09-18** (re-reviewed from the 2026-09-16 draft); see
  `plans/adr/0001-connections-ordered-map.md`.
* A2: BLAKE3-128 index keys. **Approved 2026-09-18** (re-reviewed from
  the 2026-09-16 draft); see `plans/adr/0002-blake3-index-keys.md`.
* A3: Version 4 compatibility and mixed-version merge.
  **Approved 2026-09-18** (re-reviewed from the 2026-09-16 draft); see
  `plans/adr/0003-version-4-compatibility-and-mixed-version-merge.md`.

## Risk register

| Risk | Mitigation |
|------|------------|
| Version 3 corpus silently stops being exercised | Phase 2/3 assertions against checked-in clusters; require-mode large test; claims verification |
| Conversion reorders, drops, or substitutes items | Deterministic `(key, old grd file hash, old offset)` sort; item bytes copied verbatim; property tests across split limits and worker counts |
| Temporary files leak, are tampered with, or outlive the merge | H6 guard joined to threads and closed explicitly; SHA256 verify-once after write; tamper unit test; cleanup on success, error, and panic; stale-sweep guidance |
| Scratch exhaustion at scale | Existing writer split limits bound chunks; free-space preflight; configurable temp root; ~2x input estimate documented |
| Downstream breakage | Dual-shape deserialization; neutral checked-in legacy fixture; compile verification is the consumer's responsibility (H11 rejected) |
| Non-open-source naming | `.gitignore` first; per-phase rule; exit-review check |
| Test edits erode meaning | Owner semantics rule; per-test assertion inventory; exit review reads every edited test |
| Writer nondeterminism | H5 inert fields (constant `previous`/`depends_on`); BTreeSet; multi-file cross-process byte test with timestamp-normalized comparison |

## Success criteria

1. Version 3 clusters load and answer identifier lookups with MD5 keys.
2. Version 3 and version 4 clusters coexist in one herd.
3. Version 3 + version 4 merge to a correct version 4 output, including
   duplicates, with a configurable temporary directory and cleanup on
   success and in-process failure.
4. `Item` accepts both shapes; `ItemV3` yields the legacy shape.
5. `?item_format=v3` returns the legacy JSON shape on every item-emitting
   endpoint; default returns the map.
6. All previous test semantics remain, plus the new tests, with no
   automatic passes.
7. `info/` documents the v4 format, mixed-version merge, and Goat Rodeo
   producer upgrade, every claim linked to a test.
8. The large corpus merges in a recorded run satisfying the corpus test's
   assertions, with no temporary files left behind.
