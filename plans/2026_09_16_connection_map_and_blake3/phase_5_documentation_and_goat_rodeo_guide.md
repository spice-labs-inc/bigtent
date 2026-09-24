# Phase 5: Documentation, LLM Companions, and Goat Rodeo Producer Guide

Status: planned. Depends on phases 2, 3, and 4.

## Requirements addressed

* Invariant 8: complete, correct user documentation with human and LLM
  copies, every claim linked to a test.
* D12: the non-open-source naming rule.
* D13: public Goat Rodeo producer upgrade guide.
* H12 (approved): documentation carries functional claims only; test
  counts, timings, and any evidence that a test or run executed are
  forbidden in documentation and live only in `execution_state/`.

## Deliverables

### 1. Goat Rodeo producer upgrade guide

`info/goat_rodeo_upgrade.md` plus `info/llm/goat_rodeo_upgrade_llm.md`,
linked from `info/README.md`. Format-level, producer-side, no changes to
BigTent required. The documentation is **just the conversion to V4** —
the four things that change:

* **`Item.connections` changes shape**: from an array of
  `(edge type, target)` pairs to a map of edge type to target array.
  Deserialization accepts both shapes.
* **The version number changes**: new clusters are cluster version 4,
  data envelope version 2.
* **The hashing changes**: index keys are `BLAKE3[0..16]/Long/Long` —
  the first 16 bytes (128 bits) of the BLAKE3 digest over the UTF-8
  bytes of the identifier, compared as unsigned byte strings.
* **The algorithm constant in the `.grc`**: `BLAKE3[0..16]/Long/Long`,
  also carried in the `.gri`.

Plus a short verification checklist: run `bigtent --check` with
`--rodeo` over produced clusters, resolve identifiers, and compare
against a BigTent-written reference cluster (per
`test_hand_assembled_v4_cluster_golden_bytes`).

### 2. Documentation ownership and sweep

Single owner per document section, no phase rewrites another phase's
section:

* Phase 2 owns `info/files_and_formats.md` format sections.
* Phase 3 owns `info/operations.md` merge sections and `info/config.md`
  merge flags.
* Phase 4 owns the API sections and the `info/README.md` pointer to
  `/openapi.json`.
* Phase 5 adds the Goat Rodeo guide, creates every missing LLM companion,
  updates `info/README.md`, and performs the claims sweep across all
  touched documents.
* Phase 6 appends the runbook section to `info/operations.md`.

### 3. LLM companion template

Every LLM companion, wherever it lives (`info/llm/`, a `_llm` file, or
the repository root for root documents), uses the same machine-usable
structure: one section per artifact with constants/versions, field tables
(name, type, required value, meaning), byte-offset tables, ordering and
encoding rules, accepted/rejected version matrix, error strings and
meaning, and a `claim -> test` table. Companions are verified by reading
during the exit review; automated text comparison of documents is
prohibited (rule 10).

### 4. Claims sweep and stale claims

For every claim in every touched document: identify the test that proves
it, run it, read it, confirm it tests the claim, and place the test name
next to the claim. Claims without a test get one (written first) or are
removed with a recorded disposition. The sweep output is a
`[verified] claim -> test` checklist in execution state.

Stale claims found by review, with proposed disposition:

| Claim | Location | Disposition |
|-------|----------|-------------|
| Cluster version is 1 | `info/files_and_formats.md` | Fix as part of the phase 2 rewrite |
| Index/data envelope version statements | `info/config.md` magic/version table | Fix: data envelope 2 for version 4; the algorithm constant `BLAKE3[0..16]/Long/Long` is carried in the `.grc` and `.gri` (V4), with the reader falling back to the first `.gri` for V3 |
| Merged cluster format version 3 | `info/operations.md` | Fix to 4 |
| `.gri` maps MD5 hashes | `GETTING_STARTED.md` | Fix to the declared-algorithm rule (ADR 0002) |
| Envelope length is 4 bytes | `ARCHITECTURE.md` | Fix to 2 bytes, test-backed by structural tests |
| `/metrics` does not exist | `info/config.md`, `ARCHITECTURE.md`, `README.md` | Fix here; the phase 4 route harness makes a route test possible, and the corrected claim links to it |
| Default merge workers is 20 | `PERFORMANCE.md` | Fix here to 75% of cores and link the claim to the existing `test_default_merge_worker_count_is_75_percent_of_cores` |
| `config.toml` listed as a file type | `info/files_and_formats.md` | Fix during the phase 2 rewrite |
| `--lookup` example shows legacy pairs | `info/config.md` | Fix to the map shape |
| `--merge-buffer-size` missing from the merge flags table | `info/config.md` | Fix with the correct units and default |
| `openapi.yaml` documents schemas that do not exist | repository root | Deleted in phase 4 (H10); `info/README.md` points at `/openapi.json` |

No claim is deferred to a separate plan: every row above is fixed in
phase 5 with a test link where a test exists. Claims that cannot be
tested are removed with a recorded justification and owner visibility.

`BENCHMARKING.md` is explicitly untouched: it references `data_generator`
usage and benchmark invocation, none of which changes in meaning; the
phase 5 gap review records that finding.

The sweep also confirms that observed run numbers (timings, RSS, disk
peaks, file descriptors) appear only in `execution_state/`; `info/`
carries labeled estimates and links to tests.

### 5. Root documentation

Root documents touched by this plan (`README.md`, `ARCHITECTURE.md`,
`GETTING_STARTED.md`, `PERFORMANCE.md`) get `_llm` companions in the
repository root when a claim changes, or an explicit recorded decision to
exclude them.

### 6. Downstream change description

At the end of this phase, after every code phase has landed, produce
`workspace/external_migration.md` and add `/workspace/` to `.gitignore`.
The description exists only to say what changed:

* **In-memory API**: `Item.connections` is now
  `BTreeMap<String, BTreeSet<String>>`; the legacy pair view is `ItemV3`
  with `Item::to_v3()`, `From<&Item> for ItemV3`, and
  `From<ItemV3> for Item`; deserialization accepts both shapes. List the
  removed symbols: the `GoatRodeoTrait` methods `is_empty` and
  `item_for_hash`, `Item::is_same`, `HerdMember::get_blob`,
  `HerdMember::get_directory`, `GoatRodeoCluster::get_blob`,
  `GoatRodeoCluster::get_directory`, `GoatRodeoCluster::get_data_files`,
  `GoatRodeoCluster::get_index_files`,
  `GoatRodeoCluster::common_parent_dir`,
  `GoatRodeoCluster::find_data_file_from_sha256`,
  `ClusterWriter::add_index`, `EdgeType::is_from`, `EdgeType::is_to`,
  `EdgeType::is_down`, `EdgeType::is_builds_to`, and the unused `util`
  functions `check_md5`, `millis_now`, `current_date_string`, `read_all`,
  `traverse_value`, `as_obj`, and `as_array`.
* **File format**: version 4 - cluster envelope 4, data envelope 2, the
  algorithm constant `BLAKE3[0..16]/Long/Long` carried in both the
  `.grc` and the `.gri`, index keys = first 16 bytes of BLAKE3 over the
  identifier's UTF-8 bytes compared as unsigned byte strings, map-shaped
  item connections, inert data-envelope chain fields (`previous` always
  0, `depends_on` always empty). Version 3 remains readable via the
  first-`.gri` hash description.
* **Wire format**: HTTP defaults to the map shape; `?item_format=v3`
  returns the legacy pair shape.
* One sentence: this document describes changes only; downstream code
  changes are outside this plan.

The description is not a plan, an inventory, or a verification checklist,
and it must not instruct anyone to change downstream code. It is written
last precisely because it must not be stale: if any later phase, including
the corpus exercise, changes behavior, the description is refreshed before
that phase exits.

## Tests

No new behavior. Any test created by the claims sweep is written first,
with requirement and theory comments. Existing tests referenced by claims
are run as part of the sweep.

## Exit review (HS-2)

1. Gap review: every document and section above updated or explicitly
   untouched with a recorded reason; every new flag documented; the
   downstream change description present and matching the implemented
   behavior at this point.
2. Claims verification: full checklist output; every claim has a passing,
   meaningful test; no test counts or execution metrics in `info/`.
3. Hostile reviewer: "Is any documented capability unproven; does any doc
   name a non-open-source system; do companions match their human
   counterparts in content; is the downstream description a description
   only and not a plan to modify other code?"
4. Full suite regression with exact test-count reconciliation; no skipped
   tests.

## Adversarial review (rule 9)

Independent sub-agent review of documentation against implementation and
tests; remediate; repeat until no gaps.
