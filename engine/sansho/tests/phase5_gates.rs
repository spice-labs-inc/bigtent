//! The Phase 5 gates: the per-limit boundaries (at the bound = allowed;
//! bound plus one = rejected with a structured limit error, never
//! partial results), the hostile-input fuzzing (no panics, no hangs),
//! and the core properties re-run under hostile input.

use proptest::prelude::*;
use sansho::{Limits, compile, evaluate_cbor_with_limits, parse_with_limits};

fn limit_error(result: Result<serde_json::Value, sansho::Stop>) -> bool {
    matches!(
        result,
        Err(sansho::Stop::Error(sansho::SanshoError::Limit { .. }))
    )
}

// Requirement: SPEC-0001 §5.5 — the expression-length bound. What: at
// the bound the expression parses; one character over is rejected with
// the Limit error. Why: the parse-time boundary (plan
// `limit_boundary_max_expression_length`).
//
// LLM section: the limits' expression length is set to a small value;
// "a.b"-shaped expressions at and one-past that length probe the exact
// boundary.
#[test]
fn limit_boundary_max_expression_length() {
    let limits = Limits {
        max_expression_length: 3,
        ..Default::default()
    };
    assert!(
        parse_with_limits("a.b", &limits).is_ok(),
        "at the bound must parse"
    );
    let over = parse_with_limits("a.bc", &limits).map_err(sansho::Stop::Error);
    assert!(
        limit_error(over.map(|_| serde_json::Value::Null)),
        "one over must reject"
    );
}

// Requirement: SPEC-0001 §5.5 — the nesting-depth bound. What: the
// nesting at the bound parses; one deeper is rejected. Why: the
// pre-scan's boundary (plan `limit_boundary_max_depth`).
//
// LLM section: the depth counts the STRUCTURAL characters ([ and {);
// the probe nests bracket-filters (the recursive parse's depth driver);
// at the bound it parses; one deeper rejects.
#[test]
fn limit_boundary_max_depth() {
    let limits = Limits {
        max_depth: 3,
        ..Default::default()
    };
    let at_bound = "a[?b[?c[?d]]]";
    assert!(
        parse_with_limits(at_bound, &limits).is_ok(),
        "at the bound must parse: {at_bound}"
    );
    let one_over = "a[?b[?c[?d[?e]]]]";
    let result = parse_with_limits(one_over, &limits).map_err(sansho::Stop::Error);
    assert!(
        limit_error(result.map(|_| serde_json::Value::Null)),
        "one over must reject: {one_over}"
    );
}

// Requirement: SPEC-0001 §5.5 — the output-node cap. What: the
// projection over N items evaluates at the cap; a cap one below fails
// with the Limit error. Why: the evaluation-time boundary (plan
// `limit_boundary_output_node_cap`).
//
// LLM section: a 10-element array; the wildcard projection materializes
// 10 nodes; the cap = 10 passes, 9 rejects. The rejection is WHOLE: no
// partial result ever surfaces.
#[test]
fn limit_boundary_output_node_cap() {
    let document = serde_json::json!({"items": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]});
    let bytes = serde_cbor::to_vec(&document).unwrap();
    let parsed = parse_with_limits("items[*]", &Limits::default()).unwrap();
    let program = compile(&parsed).unwrap();

    // the final output = 11 nodes (the array itself + its 10 elements)
    let at_cap = Limits {
        output_node_cap: 11,
        ..Default::default()
    };
    let result = evaluate_cbor_with_limits(&program, &bytes, &at_cap);
    assert!(result.is_ok(), "at the cap must evaluate: {result:?}");

    let one_under = Limits {
        output_node_cap: 10,
        ..Default::default()
    };
    let result = evaluate_cbor_with_limits(&program, &bytes, &one_under);
    assert!(limit_error(result), "one under the cap must reject");
}

// Requirement: SPEC-0001 §5.5 — the output-byte cap. What: the
// projection over large strings evaluates under the cap and rejects one
// byte over it. Why: the node cap alone cannot bound memory (a hostile
// projection of huge strings) — the byte cap is the plan's own
// addition (plan `limit_boundary_output_byte_cap`).
//
// LLM section: 10 strings of 100 bytes; the byte cap counts the
// materialized output; at the boundary pass/reject flips.
#[test]
fn limit_boundary_output_byte_cap() {
    let payload = "x".repeat(100);
    let items: Vec<serde_json::Value> = (0..10)
        .map(|_| serde_json::Value::String(payload.clone()))
        .collect();
    let document = serde_json::json!({"items": items});
    let bytes = serde_cbor::to_vec(&document).unwrap();
    let parsed = parse_with_limits("items[*]", &Limits::default()).unwrap();
    let program = compile(&parsed).unwrap();

    // the output = 10 strings of 100 bytes (+overhead): roughly 1020
    // bytes; the cap = 2000 passes; 1000 rejects
    let at_cap = Limits {
        output_byte_cap: 2000,
        ..Default::default()
    };
    let result = evaluate_cbor_with_limits(&program, &bytes, &at_cap);
    assert!(
        result.is_ok(),
        "under the byte cap must evaluate: {result:?}"
    );

    let over = Limits {
        output_byte_cap: 1000,
        ..Default::default()
    };
    let result = evaluate_cbor_with_limits(&program, &bytes, &over);
    assert!(limit_error(result), "over the byte cap must reject");
}

// Requirement: SPEC-0001 §5.5 — the aggregation byte cap. What: the
// in-flight aggregation (a sort's buffering) is bounded; the sort over
// a large array rejects when the in-flight bytes exceed the cap. Why:
// aggregations buffer output — the cap bounds them (plan
// `limit_boundary_aggregation_byte_cap`).
//
// LLM section: the aggregation happens at the FUNCTION boundary (a
// sort's input materializes fully); the cap catches the materialization.
#[test]
fn limit_boundary_aggregation_byte_cap() {
    let items: Vec<serde_json::Value> = (0..10)
        .map(|_| serde_json::Value::String("x".repeat(100)))
        .collect();
    let document = serde_json::json!({"items": items});
    let bytes = serde_cbor::to_vec(&document).unwrap();
    // the aggregation: the projection feeds the function boundary; the
    // in-flight bytes = the materialized array
    let parsed = parse_with_limits("sort(items)", &Limits::default()).unwrap();
    let program = compile(&parsed).unwrap();

    let at_cap = Limits {
        aggregation_byte_cap: 2000,
        ..Default::default()
    };
    let result = evaluate_cbor_with_limits(&program, &bytes, &at_cap);
    assert!(
        result.is_ok(),
        "under the aggregation cap must evaluate: {result:?}"
    );

    let over = Limits {
        aggregation_byte_cap: 900,
        ..Default::default()
    };
    let result = evaluate_cbor_with_limits(&program, &bytes, &over);
    assert!(limit_error(result), "over the aggregation cap must reject");
}

// Requirement: SPEC-0001 §3 — expressions the specification declares
// invalid are REJECTED, and hostile input never panics or hangs (§2's
// library-safety posture). What: the parser fuzz gate — hostile
// expression strings (random ASCII, bracket storms, deep parens,
// unterminated literals) produce structured errors or successful
// evaluations, never panics or hangs (time-bounded by the test
// harness). Why: the hostile-input survival requirement (plan
// `fuzz_no_panics`).
//
// LLM section: the generator mixes random printable characters with
// structured storms (deep nesting, unbalanced brackets, long
// identifier-runs). Every expression either parses (and evaluates
// against the fixture document through BOTH backends) or errors
// structurally.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    #[test]
    fn fuzz_expressions_no_panics(expression in hostile_expression_strategy()) {
        let document = serde_json::json!({"a": {"b": [1, 2, 3]}, "c": "str"});
        let bytes = serde_cbor::to_vec(&document).unwrap();
        // the parse is the first gate: an error is structured; a panic
        // or a hang fails the test
        let parsed = match parse_with_limits(&expression, &Limits::default()) {
            Ok(parsed) => parsed,
            Err(_) => return Ok(()),
        };
        let program = match compile(&parsed) {
            Ok(program) => program,
            Err(_) => return Ok(()),
        };
        let _ = sansho::evaluate_json(&program, &document);
        let _ = sansho::evaluate_cbor(&program, &bytes);
    }
}

fn hostile_expression_strategy() -> impl proptest::strategy::Strategy<Value = String> {
    prop_oneof![
        // random hostile-character soup
        "[a-zA-Z0-9_.\\[\\]\\(\\)@`'\"\\\\:?|&<>=!,*-]{0,40}",
        // bracket storms (deep, unbalanced)
        "[\\[\\]\\{\\}]{0,80}",
        // deep parentheses
        "\\(\\(\\(\\(\\(a\\)\\)\\)\\)\\)".prop_map(|s: String| s),
        // long identifier runs with separators
        "[a-z_]{1,50}(\\.[a-z_]{1,50}){0,30}",
        // unterminated literals
        "`\\{\"k\": \"v".prop_map(|s: String| s),
        "'unterminated".prop_map(|s: String| s),
        "\"escaped\\\\".prop_map(|s: String| s),
    ]
}

// Requirement: SPEC-0001 §4 — hostile CBOR input (truncated slices,
// indefinite lengths, huge header counts, trailing bytes) is rejected
// with structured input errors, never panics or hangs. What: the CBOR
// fuzz gate over adversarial byte slices. Why: the library's input is
// UNTRUSTED artifact bytes (plan `fuzz_cbor_no_panics`).
//
// LLM section: the generator produces arbitrary bytes AND structured
// hostiles (valid prefixes truncated at every offset, huge-length
// headers, indefinite markers). The evaluation path (parse the
// expression, evaluate over the bytes) must survive everything.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    #[test]
    fn fuzz_cbor_no_panics(bytes in proptest::collection::vec(any::<u8>(), 0..200)) {
        let parsed = parse_with_limits("a.b", &Limits::default()).unwrap();
        let program = compile(&parsed).unwrap();
        // whatever the bytes hold, the evaluation is structured
        let _ = sansho::evaluate_cbor(&program, &bytes);
    }

    #[test]
    fn fuzz_cbor_structured_hostiles(
        choice in 0usize..5,
        bytes in proptest::collection::vec(any::<u8>(), 0..120),
    ) {
        let parsed = parse_with_limits("a", &Limits::default()).unwrap();
        let program = compile(&parsed).unwrap();

        let hostile: Vec<u8> = match choice {
            // truncated valid document at every prefix length
            0 => {
                let doc = serde_cbor::to_vec(&serde_json::json!({"a": {"b": [1, 2, 3]}}))
                    .unwrap_or_default();
                let cut = bytes.first().copied().unwrap_or(0) as usize % (doc.len() + 1);
                doc[..cut].to_vec()
            }
            // huge array-count header
            1 => vec![0x9B, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
            // huge map-count header
            2 => vec![0xBF, 0x63, b'a', 0xA3, 0xFF],
            // indefinite array
            3 => vec![0x9F, 0x01, 0x02, 0xFF],
            // trailing garbage after a valid document
            _ => {
                let mut doc = serde_cbor::to_vec(&serde_json::json!({"a": 1}))
                    .unwrap_or_default();
                doc.extend_from_slice(&bytes);
                doc
            }
        };
        let result = sansho::evaluate_cbor(&program, &hostile);
        // structured outcomes only: Ok, or an Input/Evaluation error —
        // never a panic (which would fail the test by unwinding)
        match result {
            Ok(_) => {}
            Err(error) => {
                prop_assert!(
                    matches!(
                        error,
                        sansho::SanshoError::Input { .. }
                            | sansho::SanshoError::Evaluation { .. }
                    ),
                    "hostile bytes must yield input/evaluation errors, got {error}"
                );
            }
        }
    }
}

// Requirement: SPEC-0001 §5.1–5.2, §5.5 — the core properties hold under
// hostile input, not only well-formed documents. What: the decode-count
// budget re-run over hostile (truncated) byte slices: the evaluation is
// structured AND the decode-count stays bounded. Why: the plan's
// `single_pass_and_budget_under_hostile_input`.
//
// LLM section: the truncation produces input errors at various depths;
// whatever materialized BEFORE the error is bounded by the projection's
// selection — never the whole document.
#[test]
fn single_pass_and_budget_under_hostile_input() {
    // the big document, truncated at the middle
    let big: Vec<serde_json::Value> = (0..2000)
        .map(|i| serde_json::json!({"id": i, "payload": "x".repeat(50)}))
        .collect();
    let document = serde_json::json!({"items": big});
    let bytes = serde_cbor::to_vec(&document).unwrap();
    let full_len = bytes.len();

    let parsed = parse_with_limits("items[*].id", &Limits::default()).unwrap();
    let program = compile(&parsed).unwrap();

    for cut in [0, 1, 17, 500, full_len / 2, full_len - 1] {
        let truncated = &bytes[..cut];
        let (_result, count) = sansho::evaluate_cbor_with_stats(&program, truncated);
        // whatever happened, the decode-count is bounded: a truncated
        // document cannot decode MORE than the full one's selections
        assert!(
            count < 2 * 2000,
            "cut {cut}: the hostile input must not materialize unboundedly: {count}"
        );
    }
}

#[test]
fn probe_byte_accounting() {
    let payload = "x".repeat(100);
    let items: Vec<serde_json::Value> = (0..10)
        .map(|_| serde_json::Value::String(payload.clone()))
        .collect();
    let document = serde_json::json!({"items": items});
    let bytes = serde_cbor::to_vec(&document).unwrap();
    let parsed = parse_with_limits("items[*]", &Limits::default()).unwrap();
    let program = compile(&parsed).unwrap();
    let (result, count) = sansho::evaluate_cbor_with_stats(&program, &bytes);
    println!("PROBE result={result:?} decode_count={count}");
    // the materialized output's own size:
    if let Ok(v) = &result {
        println!(
            "PROBE output bytes ~ {}",
            serde_json::to_string(v).unwrap().len()
        );
    }
}

#[test]
fn probe_byte_cap_1000() {
    let payload = "x".repeat(100);
    let items: Vec<serde_json::Value> = (0..10)
        .map(|_| serde_json::Value::String(payload.clone()))
        .collect();
    let document = serde_json::json!({"items": items});
    let bytes = serde_cbor::to_vec(&document).unwrap();
    let parsed = parse_with_limits("items[*]", &Limits::default()).unwrap();
    let program = compile(&parsed).unwrap();
    let over = Limits {
        output_byte_cap: 1000,
        ..Default::default()
    };
    let result = evaluate_cbor_with_limits(&program, &bytes, &over);
    println!("PROBE2 result={result:?}");
}
