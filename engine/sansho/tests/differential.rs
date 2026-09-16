//! The differential tests: the cursor backend (raw CBOR bytes) against
//! the materialized backend (in-memory JSON) — the corpus's
//! backend-agreement requirement (SPEC-0001 §5.3), plus the byte-level
//! properties: single-pass consumption, shared-navigation decode-once,
//! and the selective-materialization budget (SPEC-0001 §5.1–5.2).

use proptest::prelude::*;
use sansho::corpus::{load_corpus, run_corpus, Driver, DriverOutcome};
use sansho::{compile, evaluate_cbor, parse};
use std::path::Path;

/// The cursor driver: the corpus's JSON givens encode to CBOR (they are
/// pure JSON data) and evaluate over the byte slice.
struct CursorEngine;

impl Driver for CursorEngine {
    fn evaluate(&self, given: &serde_json::Value, expression: &str) -> DriverOutcome {
        let bytes = match serde_cbor::to_vec(given) {
            Ok(bytes) => bytes,
            Err(e) => return DriverOutcome::Error(sansho::SanshoError::Input {
                message: format!("fixture encoding: {e}"),
            }),
        };
        let parsed = match parse(expression) {
            Ok(parsed) => parsed,
            Err(e) => return DriverOutcome::Error(e),
        };
        let program = match compile(&parsed) {
            Ok(program) => program,
            Err(e) => return DriverOutcome::Error(e),
        };
        match sansho::evaluate_cbor_stopped(&program, &bytes) {
            Ok(value) => DriverOutcome::Result(value),
            Err(sansho::Stop::Error(e)) => DriverOutcome::Error(e),
            Err(sansho::Stop::NotImplemented) => DriverOutcome::NotImplemented,
        }
    }
}

// Requirement: SPEC-0001 §5.3 — evaluation through every tree-view
// implementation produces identical results; §6 — the corpus gates both
// backends. What: the ENTIRE corpus runs through the cursor backend and
// scores EXACTLY as the materialized backend does (per-feature
// passed/failed/not-implemented counts identical). Why: this is the
// normative backend agreement, enforced at corpus scale (plan
// `cursor_matches_materialized_property`).
//
// LLM section: the materialized report is the reference; the cursor
// report must equal it feature by feature. The corpus's JSON givens
// encode to CBOR with serde_cbor (a development dependency, used ONLY
// for fixture construction — the engine itself decodes with minicbor).
#[test]
fn corpus_cursor_matches_materialized() {
    let files = load_corpus(Path::new("tests/jmespath-corpus/compliance"))
        .expect("vendored corpus must load");
    let materialized = run_corpus(&sansho::RealEngine, &files);
    let cursor = run_corpus(&CursorEngine, &files);
    for (reference, candidate) in materialized.iter().zip(cursor.iter()) {
        assert_eq!(
            (reference.passed, reference.failed, reference.not_implemented),
            (candidate.passed, candidate.failed, candidate.not_implemented),
            "backend disagreement on {}: materialized {reference:?} cursor {candidate:?}",
            reference.feature
        );
    }
}

// Requirement: SPEC-0001 §5.3 — the same program through both backends
// yields the same result. What: property test — generated documents
// encode to CBOR; generated expressions evaluate through both backends;
// results are identical (including the error/not-implemented category).
// Why: the differential property at random scale beyond the corpus (plan
// `cursor_matches_materialized_property`).
//
// LLM section: expression generation covers the implemented language
// slice (fields, indices, slices, wildcards, filters, multi-selects,
// pipes, the implemented functions); document generation covers nesting
// and scalars.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn differential_property_over_generated_documents(
        document in json_strategy(3),
        expression in expression_strategy(),
    ) {
        let parsed = match parse(&expression) {
            Ok(parsed) => parsed,
            // generated expressions that do not parse are skipped (the
            // generator's grammar is approximate)
            Err(_) => return Ok(()),
        };
        let program = match compile(&parsed) {
            Ok(program) => program,
            Err(_) => return Ok(()),
        };
        let materialized = sansho::evaluate_json(&program, &document);
        let bytes = serde_cbor::to_vec(&document).expect("fixture encoding");
        let cursor = evaluate_cbor(&program, &bytes);
        match (materialized, cursor) {
            (Ok(a), Ok(b)) => prop_assert_eq!(a, b),
            (Err(a), Err(b)) => {
                // both must fail in the same CATEGORY
                prop_assert_eq!(
                    sansho::SanshoError::category(&a),
                    sansho::SanshoError::category(&b)
                );
            }
            (a, b) => prop_assert!(false, "backend disagreement on {expression:?}: {a:?} vs {b:?}"),
        }
    }
}

fn expression_strategy() -> impl proptest::strategy::Strategy<Value = String> {
    use proptest::prelude::*;
    prop_oneof![
        // plain chains
        "[a-c]{1,4}(\\.[a-c]{1,4}){0,3}",
        // index chains
        "[a-c]{1,3}(\\[[0-2]\\]){0,3}",
        // wildcard projections
        "[a-c]{1,2}(\\.\\*){1,2}(\\.[a-c]{1,2}){0,2}",
        // filters
        "[a-c]{1,2}(\\[\\?[a-c]{1,3}\\]){1,2}",
        // slices
        "[a-c]{1,2}(\\[:[0-3]\\]){1,2}",
        // multi-select hashes
        "[a-c]{1,2}\\{{1}[a-c]{1,3}: [a-c]{1,3}{1}}",
        // pipes
        "[a-c]{1,3}(\\.[a-c]{1,3})(\\|\\s*[a-c]{1,3}(\\.[a-c]{1,3})){0,2}",
    ]
}

fn json_strategy(depth: u32) -> impl proptest::strategy::Strategy<Value = serde_json::Value> {
    use proptest::prelude::*;
    let leaf = prop_oneof![
        Just(serde_json::Value::Null),
        any::<bool>().prop_map(serde_json::Value::Bool),
        any::<i64>().prop_map(|n| serde_json::json!(n)),
        any::<f64>()
            .prop_filter("finite", |f: &f64| f.is_finite())
            .prop_map(|f| serde_json::json!(f)),
        "[a-z]{0,10}".prop_map(serde_json::Value::String),
    ];
    leaf.prop_recursive(depth, 24, 6, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..6).prop_map(serde_json::Value::Array),
            prop::collection::hash_map("[a-c]{1,5}", inner, 0..6)
                .prop_map(|m| serde_json::Value::Object(m.into_iter().collect())),
        ]
    })
}

// Requirement: SPEC-0001 §5.1 — an evaluation consumes each input byte
// at most once. What: the decode-memo guarantees each POSITION decodes
// once; the decode-count instrumentation proves shared navigations
// decode their target once ({a: @.x, b: @.x} counts one decode of x's
// position). Why: the fusion point of the byte-once requirement (plan
// `shared_navigation_single_decode`).
//
// LLM section: the count comes from the cursor's memo (the instrumentation
// the plan asked for); the position of x is decoded through the
// multi-select's two slots — both slots' evaluation hits the same
// position, which the memo serves once.
#[test]
fn shared_navigation_decodes_once() {
    let document = serde_json::json!({"x": {"deep": [1, 2, 3]}});
    let bytes = serde_cbor::to_vec(&document).unwrap();
    let parsed = parse(r#"{a: x.deep, b: x.deep}"#).unwrap();
    let program = compile(&parsed).unwrap();

    // the multiselect's two slots read x.deep — the shared position
    // decodes ONCE (the memo), not once per slot
    let (_result, count) = sansho::evaluate_cbor_with_stats(&program, &bytes);
    // the positions decoded: x (once, shared), the deep array (once,
    // shared), the elements' scalars at the leaf — bounded well below
    // the per-slot recount (2 x 2)
    assert!(
        count <= 5,
        "the shared navigation must not re-decode shared positions: {count} decodes"
    );
}

// Requirement: SPEC-0001 §5.2 — memory proportional to output, not
// input. What: a projection selecting NOTHING from a large document
// materializes nothing beyond the navigation (the decode-count stays
// bounded by the selected nodes, not the document size). Why: the
// selective-materialization budget (plan
// `selective_materialization_budget`).
//
// LLM section: a 4,000-element document with a projection that selects
// one small field per element decodes exactly the traversed positions —
// never the unselected siblings' subtrees.
#[test]
fn selective_materialization_budget() {
    let big: Vec<serde_json::Value> = (0..4000)
        .map(|i| serde_json::json!({"id": i, "payload": "x".repeat(100)}))
        .collect();
    let document = serde_json::json!({"items": big});
    let bytes = serde_cbor::to_vec(&document).unwrap();

    // the projection: every item's id (40,000 bytes of payload skipped)
    let parsed = parse("items[*].id").unwrap();
    let program = compile(&parsed).unwrap();

    let (result, count) = sansho::evaluate_cbor_with_stats(&program, &bytes);
    let value = result.expect("evaluates");
    assert_eq!(value.as_array().map(|a| a.len()), Some(4000));

    // the budget: the decoded positions = the items' containers + the
    // ids + the headers — NOT the 4,000 payloads (each 100+ bytes)
    assert!(
        count < 2 * 4000,
        "the payload subtrees must not materialize: {count} decodes"
    );
}

#[test]
fn probe_cursor_basic() {
    let doc = serde_json::json!({"foo": {"bar": {"baz": "correct"}}});
    let bytes = serde_cbor::to_vec(&doc).unwrap();
    println!("cbor bytes: {:?}", bytes);
    for expr in ["foo", "foo.bar", "foo.bar.baz"] {
        let parsed = parse(expr).unwrap();
        let program = compile(&parsed).unwrap();
        let result = evaluate_cbor(&program, &bytes);
        println!("probe {expr:?} -> {result:?}");
    }
}

// Requirement: SPEC-0001 §4 — the mapping is normative, and the cursor
// decoder depends on the fixture encoder's output stability: the
// checked-in fixture bytes are the golden layout. What: the fixture
// decodes to exactly its JSON twin. Why: any change in the item
// encoding (a serializer upgrade) changes the bytes and fails here
// loudly — the intended tripwire (plan `golden_layout_fixtures`).
//
// LLM section: the fixture was generated ONCE by examples/gen_fixture.rs
// (serde_cbor's encoding of the Appendix-A-shaped item); this test never
// regenerates it — it only decodes and compares against the committed
// JSON twin.
#[test]
fn golden_layout_fixture_decodes_to_json_twin() {
    let bytes = std::fs::read(Path::new("tests/fixtures/item_a.cbor"))
        .expect("the committed fixture exists");
    let expected_text =
        std::fs::read_to_string(Path::new("tests/fixtures/item_a.json"))
            .expect("the committed JSON twin exists");
    let expected: serde_json::Value = serde_json::from_str(&expected_text).unwrap();

    let parsed = parse("@").unwrap();
    let program = compile(&parsed).unwrap();
    let decoded = evaluate_cbor(&program, &bytes).expect("decodes");
    assert_eq!(decoded, expected);
}

// Requirement: SPEC-0001 §2.1 — the canonical expressions over the
// Appendix-A shape are the acceptance shape for real-world data. What:
// the prefix filter and the indexed mime type, evaluated through BOTH
// backends, produce the expected selections. Why: this is the use case
// the engine exists for (plan `real_item_fixtures`).
//
// LLM section: file_names mixes plain paths with gitoid:…!$path
// merge-disambiguated entries; the filter keeps the gitoid ones;
// mime_type is an ARRAY (the mapping's array), so [0] indexes it.
#[test]
fn canonical_expressions_over_the_fixture() {
    let bytes = std::fs::read(Path::new("tests/fixtures/item_a.cbor")).unwrap();
    let json: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(Path::new("tests/fixtures/item_a.json")).unwrap(),
    )
    .unwrap();

    for expression in [
        "body.file_names[?starts_with(@, 'gitoid:')]",
        "body.mime_type[0]",
        "length(body.file_names)",
        "body.file_size",
    ] {
        let parsed = parse(expression).unwrap();
        let program = compile(&parsed).unwrap();
        let from_json = sansho::evaluate_json(&program, &json).expect("materialized");
        let from_cbor = evaluate_cbor(&program, &bytes).expect("cursor");
        assert_eq!(from_json, from_cbor, "{expression}: backends agree");

        match expression {
            "body.file_names[?starts_with(@, 'gitoid:')]" => {
                assert_eq!(from_json.as_array().map(|a| a.len()), Some(2));
            }
            "body.mime_type[0]" => {
                assert_eq!(from_json, serde_json::json!("text/x-java-source"));
            }
            "length(body.file_names)" => {
                assert_eq!(from_json, serde_json::json!(5));
            }
            "body.file_size" => {
                assert_eq!(from_json, serde_json::json!(3050));
            }
            _ => unreachable!(),
        }
    }
}
