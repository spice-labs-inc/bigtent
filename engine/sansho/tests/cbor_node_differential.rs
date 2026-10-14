//! The zero-copy byte source
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
        let program = match sansho::compile(&parsed, None) {
            Ok(p) => p,
            Err(e) => return DriverOutcome::Error(e),
        };
        let slice: &[u8] = &bytes;
        match sansho::evaluate(&program, slice).and_then(|value| sansho::materialize(&value)) {
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
                    (DriverOutcome::Result(value), None) => case
                        .result
                        .as_ref()
                        .map(|e| sansho::corpus::json_equal(value, e))
                        .unwrap_or(false),
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

/// Generated documents: the byte source and the in-memory source must
/// agree byte for byte — one evaluator, two sources. A deterministic
/// seeded generator covers array documents of varying length and the
/// scan and seek shapes.
#[test]
fn cbor_node_matches_cursor_engine() {
    let mut seed: u64 = 0x5eed;
    let mut next = move || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
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
            let program = sansho::compile(&parsed, None).unwrap();
            let given_value: serde_json::Value = serde_cbor::from_slice(&bytes).unwrap();
            let cursor_result = sansho::evaluate(&program, given_value)
                .and_then(|value| sansho::materialize(&value));
            let slice: &[u8] = &bytes;
            let node_result =
                sansho::evaluate(&program, slice).and_then(|value| sansho::materialize(&value));
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
    let program = sansho::compile(&parsed, None).unwrap();
    let value = {
        let slice: &[u8] = &bytes;
        sansho::materialize(&sansho::evaluate(&program, slice).unwrap()).unwrap()
    };
    let obj = value.as_object().unwrap();
    assert_eq!(
        obj["bs"],
        serde_json::json!("__4A"),
        "byte strings render base64url"
    );
    assert_eq!(
        obj["n"],
        serde_json::json!(9_223_372_036_854_775_808u64),
        "u64 exact"
    );

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
        sansho::materialize(&sansho::evaluate(&program, slice).unwrap()).unwrap()
    };
    let obj2 = value2.as_object().unwrap();
    assert_eq!(obj2["f16"], serde_json::json!(0.0), "f16 zero");
    assert_eq!(obj2["f32"], serde_json::json!(0), "+Inf coerces to 0");
    assert_eq!(obj2["f64"], serde_json::json!(0), "NaN coerces to 0");
    assert_eq!(obj2["neg0"], serde_json::json!(-0.0), "-0.0 preserved");

    // invalid-UTF-8 text is invalid CBOR — major type 3 is defined as
    // UTF-8 — so it is an INPUT error, raised where the text is read. The
    // evaluation is lazy and never decodes the node, so the error surfaces
    // at the boundary materialization rather than at `evaluate`.
    let invalid = vec![0x63u8, 0xff, 0xfe, 0x00]; // text(3) with bad bytes
    assert!(
        {
            let slice: &[u8] = &invalid;
            let root = sansho::evaluate(&program, slice).expect("the evaluation returns the node");
            sansho::materialize(&root).is_err()
        },
        "invalid UTF-8 text must error at the boundary"
    );

    // an invalid-UTF-8 element inside a filter predicate: the text is
    // invalid CBOR, so it is an INPUT error wherever the predicate reads it
    // — the predicate compares text, so the filter errors rather than
    // dropping the element.
    // doc: {items: ["abc", INVALID]} — both elements are strings; the
    // second is invalid UTF-8
    let mut with_invalid = vec![0xa1u8, 0x65, b'i', b't', b'e', b'm', b's', 0x82];
    with_invalid.extend_from_slice(&[0x63, b'a', b'b', b'c']); // "abc"
    with_invalid.extend_from_slice(&[0x63, 0xff, 0xfe, 0x00]); // invalid text element
    let parsed_f = sansho::parse("items[?starts_with(@, 'x')]").unwrap();
    let program_f = sansho::compile(&parsed_f, None).unwrap();
    let slice_f: &[u8] = &with_invalid;
    assert!(
        sansho::evaluate(&program_f, slice_f).is_err(),
        "invalid text in a filter predicate is an input error"
    );
    // a predicate that WOULD match the valid element errors the same way:
    // the invalid element is read whichever predicate is applied
    let parsed_g = sansho::parse("items[?starts_with(@, 'a')]").unwrap();
    let program_g = sansho::compile(&parsed_g, None).unwrap();
    let slice_g: &[u8] = &with_invalid;
    assert!(
        sansho::evaluate(&program_g, slice_g).is_err(),
        "the invalid element is read whichever predicate is applied"
    );

    // trailing bytes after a valid document: silently ignored — the root
    // decode reads one document and does not require the slice to end at
    // it, so bytes past the document are never read
    let mut doc = serde_cbor::to_vec(&serde_json::json!({"a": 1})).unwrap();
    doc.push(0x00); // one trailing byte
    let with_trailing = {
        let slice: &[u8] = &doc;
        sansho::materialize(&sansho::evaluate(&program, slice).unwrap()).unwrap()
    };
    assert_eq!(
        with_trailing,
        serde_json::json!({"a": 1}),
        "trailing bytes are silently ignored"
    );

    // non-text map keys: CBOR permits any item as a map key, so a map keyed
    // by an integer is valid CBOR that has no JSON representation — a JSON
    // object's name is defined to be a string. Sansho is a JMESPath
    // implementation, so input outside the JSON data model is not
    // represented: the boundary materialization and navigation both drop it,
    // and the two halves agree about what the document contains.
    let nontext = vec![0xa1u8, 0x01, 0x61, b'a']; // map(1): int key 1 -> "a"
    let parsed_nk = sansho::parse("@").unwrap();
    let program_nk = sansho::compile(&parsed_nk, None).unwrap();
    let value5 = {
        let slice: &[u8] = &nontext;
        sansho::materialize(&sansho::evaluate(&program_nk, slice).unwrap()).unwrap()
    };
    assert_eq!(
        value5.as_object().map(|o| o.len()),
        Some(0),
        "a non-text key has no JSON representation and is not materialized"
    );
    // navigation through the source does not find it either
    let slice5: &[u8] = &nontext;
    let root5 = sansho::evaluate(&program_nk, slice5).unwrap();
    let _ = root5;
    assert!(
        SanshoTrait::get_key(&slice5, "1").unwrap().is_none(),
        "navigation does not find a non-text key"
    );
    assert!(
        SanshoTrait::entries(&slice5).unwrap().is_empty(),
        "entries do not report a non-text key"
    );
}

#[test]
fn fuzz_truncated_slices_never_panic() {
    let doc = serde_json::json!({
        "connections": {"alias:from": ["pkg:a", "gitoid:b"], "contained:up": ["p"]},
        "body": {"file_names": ["a.java", "b.java"], "file_size": 3050}
    });
    let full = serde_cbor::to_vec(&doc).unwrap();
    let whole = sansho::compile(&sansho::parse("@").unwrap(), None).unwrap();
    for len in 0..full.len() {
        let truncated = &full[..len];
        let slice: &[u8] = truncated;
        // a truncated document may or may not decode at the boundary;
        // the lookup must ERROR (never panic) — asserting the error
        // result is tolerated either way keeps the panic-free contract
        let _ = sansho::evaluate(&whole, slice);
        // navigation must never panic (the byte-once boundary contract
        // is asserted in the crate's own tests — the memo is internal)
        let _ = SanshoTrait::kind(&slice);
        let _ = SanshoTrait::get_key(&slice, "connections");
        let _ = SanshoTrait::get_key(&slice, "body");
        let _ = SanshoTrait::elements(&slice);
        let _ = SanshoTrait::container_len(&slice);
    }
}
