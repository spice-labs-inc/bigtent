# Phase 4: HTTP `?item_format=v3` and OpenAPI

Status: planned. Depends only on phase 2; may run in parallel with phase 3.

## Requirements addressed

* D5: dual shape available at the boundary.
* D8: map shape is the default HTTP JSON; `?item_format=v3` returns the
  legacy pair shape on every item-emitting endpoint; invalid values are
  rejected; OpenAPI documents both.
* H10 (approved as removal): `openapi.yaml` is deleted; `/openapi.json`
  from `ApiDoc::openapi()` is the only specification; no code-generated
  artifacts are checked into the repository.

## Endpoint audit

The audit below was performed against `build_route` and the handlers. The
result is part of this phase's contract.

| Endpoint | Emits | `item_format` |
|----------|-------|---------------|
| `GET /item/{gitoid}`, `GET /item` | one Item | applies |
| `POST /bulk` | stream of Items | applies |
| `GET /aa/{gitoid}`, `GET /aa`, `POST /aa` | one or many Items | applies |
| `GET /north/{gitoid}`, `GET /north`, `POST /north` (query param `identifier`, body identifiers) | stream of Items | applies |
| `/north_purls` variants | PURL strings | no effect |
| `/flatten`, `/flatten_source` variants | identifiers (runtime emits `Either::Right` strings) | no effect; OpenAPI annotations are corrected to identifier strings in this phase |
| `/purls`, `/node_count`, `/health`, `/metrics`, history | scalar or text | no effect |

## Deliverables

### 1. Parameter plumbing (`src/server.rs`)

* `enum ItemFormat { V4, V3 }`:
  * absent or `v4` -> `V4`;
  * `v3` -> `V3`;
  * anything else, wrong case, or duplicate parameters with conflicting
    values -> 400 with a static message naming accepted values. Because
    the handlers currently use `Query<HashMap<String, String>>`, which is
    silently last-wins, duplicate detection requires parsing the raw query
    (`RawQuery` or a small custom extractor). Identical duplicates
    (for example `item_format=v3&item_format=v3`) are accepted as `v3`
    and pinned by a test.
* Every applicable handler accepts the parameter and serializes `Item` or
  `ItemV3` accordingly. `ItemV3` gains `ToSchema`.
* Passing the parameter to endpoints with no item shape is accepted and
  has no effect; a test proves byte-identical output.

### 2. Error hygiene

* Invalid-format and not-found responses are static strings; they never
  include parser output, filesystem paths, or internal type names. The
  existing north error path that returns `{:?}` of an arbitrary error is
  corrected to a sanitized message.

### 3. H10 specification handling

* Delete `openapi.yaml` from the repository; it is stale and documents
  schemas that do not exist in the code.
* `info/README.md` points readers at `/openapi.json` from a running server
  as the only specification.
* No code-generated artifact is checked in.

### 4. CLI

* `bigtent --lookup` keeps emitting the new shape; no flag is added. This
  is an explicit non-goal recorded in the plan.

## Test harness

Route-level tests need `tower` (`ServiceExt`) and `http-body-util` in
`[dev-dependencies]`, plus a `ClusterHolder` fixture built from a small
version 4 cluster. This harness is a deliverable of this phase; without
it, the tests below would degrade into private-helper unit tests.

## Tests (write first, expect red)

1. `test_item_default_shape_is_map` (integration). Requirement D8.
   Theory: default response has `connections` as a JSON object of arrays.
2. `test_item_format_v3_shape_is_legacy_pairs` (integration).
   Requirement D8. Theory: `?item_format=v3` returns an array of
   two-element arrays in the old canonical order, byte-shaped like the
   legacy output.
3. `test_item_format_explicit_v4` (boundary). Requirement D8. Theory:
   `?item_format=v4` is accepted and equals the default output.
4. `test_item_format_invalid_rejected` (boundary). Requirement D8.
   Theory: unknown, empty, wrong-case, and conflicting duplicate values
   return 400 with a static message; identical duplicates are accepted;
   no internal details leak.
5. `test_item_format_applies_to_bulk` (integration). Requirement D8.
   Theory: `POST /bulk` honors the parameter, including for streamed
   elements, and the response contains at least one item.
6. `test_item_format_applies_to_aa_endpoints` (integration).
   Requirement D8. Theory: single, query, and bulk antialias endpoints
   honor the parameter in nested item values; responses are non-empty.
7. `test_item_format_applies_to_north_full_items` (integration).
   Requirement D8. Theory: the full-item north stream honors the
   parameter for `GET /north/{gitoid}`, the query form (`identifier`), and
   the bulk body; streams are non-empty.
8. `test_flatten_returns_identifiers_regardless_of_item_format`
   (boundary). Requirement D8. Theory: flatten output is identifiers with
   and without the parameter; this also corrects the OpenAPI annotations.
9. `test_identifier_streams_unaffected_by_item_format` (boundary).
   Requirement D8. Theory: PURL and count endpoints return identical bytes
   with and without the parameter.
10. `test_openapi_schema_contains_both_shapes` (integration).
    Requirement D8, H10. Theory: `ApiDoc::openapi()` contains the map
    schema, the `ItemV3` schema, and the parameter on every applicable
    path; this reads the generated structure, not the static file text.
11. `prop_v3_json_round_trip` (property). Requirement D4, D8. Theory:
    arbitrary items serialize as `ItemV3` JSON and parse back equal.
12. `prop_default_and_v3_responses_are_semantically_equal` (property).
    Requirement D8. Theory: for arbitrary items, the parsed default and
    v3 responses contain the same edge multiset.

### Documentation updates for this phase

Phase 4 owns the HTTP section of `info/files_and_formats.md` (or a new
`info/api.md` if the section grows too large) and the `info/README.md`
pointer to `/openapi.json` from a running server; `openapi.yaml` does not
exist after this phase (H10), and no code-generated artifact is checked
in. Every claim links to the tests above. The LLM companion for the
updated document is created and `info/README.md` is updated.

## Exit review (HS-2)

1. Gap review: every applicable endpoint handled; audit table matches the
   router; `openapi.yaml` removed and `info/README.md` points at
   `/openapi.json`.
2. Claims verification: run each referenced test, read it, confirm it
   tests the claim, especially shape fidelity and error hygiene.
3. Hostile reviewer: "Are there routes that leak the new shape to legacy
   clients or return 200 with the wrong shape; can query parsing be
   confused by duplicates or casing?"
4. Full suite regression with exact test-count reconciliation; no skipped
   tests.

## Adversarial review (rule 9)

Independent sub-agent review; remediate; repeat until clean.
