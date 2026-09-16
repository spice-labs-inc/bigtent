//! The pipeline-comparison's VALIDITY premise: the three paths produce
//! the same outputs (the CBOR → the serde intermediate + the `jmespath`
//! crate; the serde intermediate + Sansho's materialized backend;
//! Sansho's cursor over the byte slice). The timings live in
//! benches/comparison.rs; this file asserts they are timings OF THE
//! SAME computation.

use serde_json::Value as J;

fn build_item(entries: usize) -> J {
    let file_names: Vec<String> = (0..entries)
        .map(|i| {
            if i % 3 == 0 {
                format!(
                    "gitoid:blob:sha256:{:064x}!$org/apache/logging/log4j/core/lookup/JndiLookup.java",
                    i
                )
            } else {
                format!("logging-log4j2-log4j-2.10.0/log4j-core/src/main/java/org/apache/logging/log4j/core/lookup/JndiLookup.java{i}")
            }
        })
        .collect();
    serde_json::json!({
        "identifier": "gitoid:blob:sha1:30e65ad24f4b4d799e52cfd70fcbebc0490b7343",
        "connections": [
            ["alias:from", "md5:4d921bf0ce238d1a96bdf65000fde565"],
            ["alias:from", "sha1:e9f38efe19210f3b63a72699a69eb261176d251e"]
        ],
        "body_mime_type": "application/vnd.cc.goatrodeo",
        "body": {
            "extra": {},
            "file_names": file_names,
            "file_size": 3050,
            "mime_type": ["text/x-java-source"]
        }
    })
}

// Requirement: the comparison benchmarks' validity premise (the plan's
// Phase 6 extension, the owner's request). What: for two sizes and four
// expressions, all three paths produce IDENTICAL results. Why: the
// timings are only meaningful if they measure the same computation.
//
// LLM section: path A = serde_cbor→Value + the jmespath crate; path B =
// serde_cbor→Value + Sansho's materialized backend; path C = Sansho's
// cursor over the raw bytes. The outputs serialize identically.
#[test]
fn path_outputs_agree() {
    for target in [10_000usize, 100_000, 1_000_000] {
        let entries = (target / 130).max(1);
        let document = build_item(entries);
        let bytes = serde_cbor::to_vec(&document).unwrap();
        let jmespath_variable = jmespath::Variable::from_serializable(&document).unwrap();
        for expression in [
            "body.file_names[?starts_with(@, 'gitoid:')]",
            "body.file_size",
            "length(body.file_names)",
            "identifier",
            "body.mime_type[0]",
            "body.file_names[0:5]",
        ] {
            let jmespath_expression = jmespath::compile(expression).unwrap();
            let jmespath_result = jmespath_expression
                .search(&jmespath_variable)
                .unwrap_or_else(|e| panic!("{expression}: jmespath errors: {e}"));
            let via_jmespath: J = serde_json::from_str(&jmespath_result.to_string())
                .unwrap_or_else(|e| panic!("{expression}: jmespath output: {e}"));

            let parsed_value: J = serde_cbor::from_slice(&bytes).unwrap();
            let program = sansho::compile(&sansho::parse(expression).unwrap()).unwrap();
            let via_materialized = sansho::evaluate_json(&program, &parsed_value)
                .unwrap_or_else(|e| panic!("{expression}: sansho materialized: {e}"));
            let via_cursor = sansho::evaluate_cbor(&program, &bytes)
                .unwrap_or_else(|e| panic!("{expression}: sansho cursor: {e}"));

            let canonical_a = canonicalize(&via_jmespath);
            let canonical_b = canonicalize(&via_materialized);
            let canonical_c = canonicalize(&via_cursor);
            assert_eq!(
                canonical_a, canonical_b,
                "{expression} @ ~{target}B: jmespath vs sansho-materialized"
            );
            assert_eq!(
                canonical_b, canonical_c,
                "{expression} @ ~{target}B: sansho-materialized vs sansho-cursor"
            );
        }
    }
}

/// The object key-orders may differ between the engines (the multiselect
/// hashes = the insertion-order vs the sorted); the comparison
/// canonicalizes: the objects' keys sort before comparing.
fn canonicalize(value: &J) -> String {
    match value {
        J::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let body: Vec<String> = keys
                .iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        k,
                        canonicalize(map.get(*k).unwrap())
                    )
                })
                .collect();
            format!("{{{}}}", body.join(","))
        }
        J::Array(items) => {
            format!(
                "[{}]",
                items.iter().map(canonicalize).collect::<Vec<String>>().join(",")
            )
        }
        other => other.to_string(),
    }
}
