# README — LLM companion

Machine-usable summary of `README.md` claims affected by the version 4
migration (claims name their tests).

* Big Tent stores software artifacts as Items connected by typed Edges;
  `Item.connections` is an ordered map of edge type to target set (v4
  shape; legacy pair shape still readable). Tests:
  `test_item_v4_cbor_round_trip`, `test_item_legacy_pairs_cbor_deserialize`.
* Cluster files are version 4 (readers also accept version 3); the index
  key algorithm is declared in the files. Tests:
  `test_writer_emits_version_4_envelopes`,
  `test_v3_fixture_clusters_load_and_resolve`.
* The `--lookup` batch output maps each identifier to its Item in the
  default map shape. (No `?item_format=v3` flag exists for `--lookup` —
  an explicit non-goal.)
* `GET /metrics` serves Prometheus text exposition metrics. Test:
  `test_metrics_endpoint_responds`.
* The HTTP API's only machine-readable specification is `/openapi.json`
  from a running server. Test: `test_openapi_schema_contains_both_shapes`.
