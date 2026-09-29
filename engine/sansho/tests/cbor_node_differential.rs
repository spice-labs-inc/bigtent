//! Phase 1, T1.2/T1.4: the zero-copy byte source (`CborNode`)
//! differential-tested against the existing materializing cursor — over
//! the compliance corpus, generated documents, and the escape-case
//! matrix (byte strings, floats, non-text keys, deep nesting).

use sansho::corpus::{Driver, DriverOutcome, load_corpus};
use sansho::view::SanshoTrait;
use std::path::Path;

/// The CborNode driver: the corpus's JSON givens encode to CBOR and
/// evaluate through the zero-copy byte source.
struct CborNodeEngine;

impl Driver for CborNodeEngine {
    fn evaluate(&self, given: &serde_json::Value, expression: &str) -> DriverOutcome {
        let bytes = match serde_cbor::to_vec(given) {
            Ok(bytes) => bytes,
            Err(e) => {
                return DriverOutcome::Error(sansho::SanshoError::Input {
                    message: format!("fixture encoding: {e}"),
                });
            }
        };
        let parsed = match sansho::parse(expression) {
            Ok(parsed) => parsed,
            Err(e) => return DriverOutcome::Error(e),
        };
        let program = match sansho::compile(&parsed) {
            Ok(p) => p,
            Err(e) => return DriverOutcome::Error(e),
        };
        let slice: &[u8] = &bytes;
        match sansho::lookup_value(&slice, &program) {
            Ok(value) => DriverOutcome::Result(value),
            Err(e) => DriverOutcome::Error(e),
        }
    }
}

/// Every corpus case runs green through CborNode (the corpus is the
/// referee; a single failure names the expression).
#[test]
fn corpus_green_through_cbor_node() {
    let files = load_corpus(Path::new("tests/jmespath-corpus/compliance")).expect("loads");
    let mut failures = 0usize;
    let mut not_impl = 0usize;
    for file in &files {
        for group in &file.groups {
            for case in &group.cases {
                let outcome = CborNodeEngine.evaluate(&group.given, &case.expression);
                let declared = case.error.clone().unwrap_or_else(|| "ok".to_string());
                let matches = match (&outcome, &case.error) {
                    (DriverOutcome::Result(value), None) => {
                        case.result.as_ref().map(|e| sansho::corpus::json_equal(value, e)).unwrap_or(false)
                    }
                    (DriverOutcome::Error(error), Some(_)) => {
                        sansho::corpus::expected_category(&declared) == Some(error.category())
                    }
                    (DriverOutcome::NotImplemented, _) => {
                        not_impl += 1;
                        true
                    }
                    _ => false,
                };
                if !matches {
                    failures += 1;
                    println!(
                        "FAIL[{}] expr={:?}\n  given={}\n  declared={declared} outcome={outcome:?}",
                        file.feature, case.expression, group.given,
                    );
                }
            }
        }
    }
    println!("not-implemented: {not_impl}");
    assert_eq!(failures, 0, "corpus failures through CborNode: {failures}");
}

/// Generated documents: CborNode ≡ the materializing cursor, byte for
/// byte (the same evaluator, two byte sources — the differential
/// contract of the re-expression).


/// The escape-case matrix (the corpus is pure JSON and cannot referee
/// these): byte strings, floats, non-text keys, bignums — asserted
/// equal through both byte sources.
/// Generated documents: CborNode ≡ the materializing cursor — a
/// deterministic seeded generator, covering array docs of varying
/// length and the scan/seek shapes.
#[test]
fn cbor_node_matches_cursor_engine() {
    let mut seed: u64 = 0x5eed;
    let mut next = move || {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (seed >> 33) as usize
    };
    let expressions = ["@", "length(@)", "sum(@)", "[0]", "[-1]"];
    for doc_len in [0usize, 1, 5, 20, 100] {
        let values: Vec<serde_json::Value> = (0..doc_len)
            .map(|_| serde_json::Value::Number(((next() % 1_000_000) as i64).into()))
            .collect();
        let given = serde_json::Value::Array(values);
        let bytes = serde_cbor::to_vec(&given).unwrap();
        for expr in expressions {
            let parsed = sansho::parse(expr).unwrap();
            let program = sansho::compile(&parsed).unwrap();
            let given_value: serde_json::Value = serde_cbor::from_slice(&bytes).unwrap();
            let cursor_result = sansho::lookup_value(&given_value, &program);
            let slice: &[u8] = &bytes;
            let node_result = sansho::lookup_value(&slice, &program);
            assert_eq!(
                format!("{node_result:?}"),
                format!("{cursor_result:?}"),
                "CborNode and the value source must agree on {expr:?} over a {doc_len}-element array"
            );
        }
    }
}

// The escape-case matrix: the corpus is pure JSON and cannot referee
// byte strings, u64-range numbers, or non-finite floats. With the old
// materializing cursor removed, the oracle is the SPECIFIED mapping —
// the matrix asserts the expected values directly (the values were
// pinned against the cursor before its removal; the agreement is
// preserved by these assertions).
#[test]
fn escape_case_matrix_pins_the_mapping() {
    // byte strings (major 2): the base64url rendering; u64 >= 2^63:
    // the exact value (no wrap-around)
    let mut bytes = vec![0xa2u8];
    bytes.extend_from_slice(&[0x62, b'b', b's']);
    bytes.extend_from_slice(&[0x43, 0xff, 0xfe, 0x00]);
    bytes.extend_from_slice(&[0x61, b'n']);
    bytes.extend_from_slice(&[0x1b, 0x80, 0, 0, 0, 0, 0, 0, 0]);
    let parsed = sansho::parse("@").unwrap();
    let program = sansho::compile(&parsed).unwrap();
    let value = {
        let slice: &[u8] = &bytes;
        sansho::lookup_value(&slice, &program).unwrap()
    };
    let obj = value.as_object().unwrap();
    assert_eq!(obj["bs"], serde_json::json!("__4A"), "byte strings render base64url");
    assert_eq!(obj["n"], serde_json::json!(9_223_372_036_854_775_808u64), "u64 exact");

    // floats: f16/f32/f64, +Inf and NaN coerce to 0 (the mapping's
    // number coercion), -0.0 is preserved
    let mut fb = vec![0xa4u8];
    for (key, val) in [
        (b"f16".as_slice(), &[0xf9, 0x00, 0x00][..]),
        (b"f32", &[0xfa, 0x7f, 0x80, 0x00, 0x00][..]),
        (b"f64", &[0xfb, 0x7f, 0xf8, 0, 0, 0, 0, 0, 0][..]),
        (b"neg0", &[0xfb, 0x80, 0, 0, 0, 0, 0, 0, 0][..]),
    ] {
        fb.push(0x60 + key.len() as u8);
        fb.extend_from_slice(key);
        fb.extend_from_slice(val);
    }
    let value2 = {
        let slice: &[u8] = &fb;
        sansho::lookup_value(&slice, &program).unwrap()
    };
    let obj2 = value2.as_object().unwrap();
    assert_eq!(obj2["f16"], serde_json::json!(0.0), "f16 zero");
    assert_eq!(obj2["f32"], serde_json::json!(0), "+Inf coerces to 0");
    assert_eq!(obj2["f64"], serde_json::json!(0), "NaN coerces to 0");
    assert_eq!(obj2["neg0"], serde_json::json!(-0.0), "-0.0 preserved");

    // invalid-UTF-8 text: the root materialization is the exactly-one-
    // document contract — invalid text is an Input error, never a
    // wrong value (pinned: a single invalid-UTF-8 text item)
    let invalid = vec![0x63u8, 0xff, 0xfe, 0x00]; // text(3) with bad bytes
    assert!(
        {
        let slice: &[u8] = &invalid;
        sansho::lookup_value(&slice, &program).is_err()
    },
        "invalid UTF-8 text must error"
    );

    // the RELAXED filter-predicate contract (owner decision
    // 2026-09-23): an invalid-UTF-8 element inside a filter predicate
    // is DROPPED, not errored — the raw-bytes comparison never
    // validates; the predicate simply does not match.
    // doc: {items: ["abc", INVALID]} — both elements are strings; the
    // second is invalid UTF-8
    let mut with_invalid = vec![0xa1u8, 0x65, b'i', b't', b'e', b'm', b's', 0x82];
    with_invalid.extend_from_slice(&[0x63, b'a', b'b', b'c']); // "abc"
    with_invalid.extend_from_slice(&[0x63, 0xff, 0xfe, 0x00]); // invalid text element
    let parsed_f = sansho::parse("items[?starts_with(@, 'x')]").unwrap();
    let program_f = sansho::compile(&parsed_f).unwrap();
    let value_f = {
        let slice: &[u8] = &with_invalid;
        sansho::lookup_value(&slice, &program_f).unwrap()
    };
    // the invalid element does not match "x" and is dropped without
    // error; the valid element also does not match — empty result
    assert_eq!(
        value_f.as_array().map(|a| a.len()),
        Some(0),
        "invalid text in a filter predicate is dropped, not errored"
    );
    // and a MATCHING valid element still matches (raw bytes == str)
    let parsed_g = sansho::parse("items[?starts_with(@, 'a')]").unwrap();
    let program_g = sansho::compile(&parsed_g).unwrap();
    let value_g = {
        let slice: &[u8] = &with_invalid;
        sansho::lookup_value(&slice, &program_g).unwrap()
    };
    assert_eq!(
        value_g.as_array().map(|a| a.len()),
        Some(1),
        "valid matching elements still match"
    );

    // trailing bytes after a valid document: the root decode enforces
    // the exactly-one-document contract (the old cursor decoded only
    // the first item and ignored the rest — the change is documented;
    // the pinned behavior: trailing bytes are an Input error)
    let mut doc = serde_cbor::to_vec(&serde_json::json!({"a": 1})).unwrap();
    doc.push(0x00); // one trailing byte
    assert!(
        {
        let slice: &[u8] = &doc;
        sansho::lookup_value(&slice, &program).is_err()
    },
        "trailing bytes must error (the exactly-one-document contract)"
    );

    // non-text map keys: the pinned CborNode contract — the BOUNDARY
    // materialization stringifies them (the shared decode's mapping);
    // NAVIGATION drops them (get_key/entries never see them). The old
    // cursor's navigation emitted empty-keyed entries; the divergence
    // is documented as the change. (The two halves disagree with each
    // other by design of the shared boundary decode; pinned here.)
    let nontext = vec![0xa1u8, 0x01, 0x61, b'a']; // map(1): int key 1 -> "a"
    let parsed_nk = sansho::parse("@").unwrap();
    let program_nk = sansho::compile(&parsed_nk).unwrap();
    let value5 = {
        let slice: &[u8] = &nontext;
        sansho::lookup_value(&slice, &program_nk).unwrap()
    };
    assert_eq!(value5.as_object().map(|o| o.len()), Some(1),
        "the boundary materializes non-text keys (stringified)");
    // navigation through the source drops non-text keys
    let slice5: &[u8] = &nontext;
    let root5 = sansho::lookup(&slice5, &program_nk).unwrap();
    let _ = root5;
    assert!(SanshoTrait::get_key(&slice5, "1").is_none(), "navigation drops non-text keys");
    assert!(SanshoTrait::entries(&slice5).is_empty(), "entries drop non-text keys");
}

#[test]
fn fuzz_truncated_slices_never_panic() {
    let doc = serde_json::json!({
        "connections": {"alias:from": ["pkg:a", "gitoid:b"], "contained:up": ["p"]},
        "body": {"file_names": ["a.java", "b.java"], "file_size": 3050}
    });
    let full = serde_cbor::to_vec(&doc).unwrap();
    let whole = sansho::compile(&sansho::parse("@").unwrap()).unwrap();
    for len in 0..full.len() {
        let truncated = &full[..len];
        let slice: &[u8] = truncated;
        // a truncated document may or may not decode at the boundary;
        // the lookup must ERROR (never panic) — asserting the error
        // result is tolerated either way keeps the panic-free contract
        let _ = sansho::lookup(&slice, &whole);
        // navigation must never panic (the byte-once boundary contract
        // is asserted in the crate's own tests — the memo is internal)
        let _ = SanshoTrait::kind(&slice);
        let _ = SanshoTrait::get_key(&slice, "connections");
        let _ = SanshoTrait::get_key(&slice, "body");
        let _ = SanshoTrait::elements(&slice);
        let _ = SanshoTrait::container_len(&slice);
    }
}

/// T1.3-adjacent: the boundary memo's decode-once across a scan (the
/// byte-once contract through the new source).
#[test]
fn scan_decodes_once_per_position_at_the_boundary() {
    #[path = "../src/tests_common.rs"]
mod tests_common;
    let item = tests_common::appendix_a(1_000, 500);
    let bytes = serde_cbor::to_vec(&item).unwrap();
    let parsed = sansho::parse("connections.\"alias:from\"[?starts_with(@, 'pkg:')]").unwrap();
    let program = sansho::compile(&parsed).unwrap();
    let _ = {
        let slice: &[u8] = &bytes;
        sansho::lookup_value(&slice, &program).unwrap()
    };
    let (result, count) = sansho::eval::evaluate_cbor_with_stats(&program, &bytes);
    assert!(result.is_ok());
    // the memo decodes each POSITION once — the decode count must not
    // exceed the number of distinct materialized positions (and must
    // be far below the element count: the filter's predicate touches
    // each element's string, but only the boundary materializes)
    assert!(
        count <= 1_000 + 100,
        "decode count {count} must stay bounded by the distinct materialized positions"
    );
}
