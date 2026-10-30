//! Non-finite numbers at the JSON boundary.
//!
//! REQUIREMENT (second review, finding 19, decided by the owner — option 1):
//! JSON has no spelling for `NaN`, `Infinity` or `-Infinity`, so materializing
//! one answers `null` — the JavaScript convention, and the one the evaluator's
//! text rendering already used. Before this, `lib.rs` substituted `0` while
//! `eval.rs` rendered `null`: two answers for the same value, so neither was a
//! policy.
//!
//! THEORY OF THE TEST: `null` must be what `materialize` produces for all three
//! non-finite values, and the same `null` must be what the evaluator's own text
//! rendering produces — one policy, two paths. A FINITE float is untouched, so
//! the fix cannot be "call every float null".

use sansho::evaluate;
use sansho::view::{SanshoNumber, SanshoTrait};
use std::rc::Rc;

/// Materialize a float payload from CBOR, as the pipeline does.
fn materialize_float(value: f64) -> serde_json::Value {
    let bytes = serde_cbor::to_vec(&serde_cbor::Value::Float(value)).expect("the float encodes");
    let carrier: sansho::SanshoCarrier<'_> = Rc::new(&bytes[..]);
    sansho::materialize(&carrier).expect("materializes")
}

/// The three non-finite values materialize as `null` — never as `0`.
#[test]
fn non_finite_numbers_materialize_as_null() {
    for (value, name) in [
        (f64::NAN, "NaN"),
        (f64::INFINITY, "Infinity"),
        (f64::NEG_INFINITY, "-Infinity"),
    ] {
        assert_eq!(
            materialize_float(value),
            serde_json::Value::Null,
            "{name} must materialize as null"
        );
    }
}

/// A FINITE float is still its number — the control that keeps the policy from
/// swallowing ordinary values.
#[test]
fn a_finite_number_is_untouched() {
    assert_eq!(materialize_float(2.5), serde_json::json!(2.5));
}

/// The evaluator's TEXT rendering agrees: the same three values render `null`,
/// so the two paths cannot disagree again.
#[test]
fn the_evaluator_renders_non_finite_numbers_as_null() {
    let parsed = sansho::parse("to_string(@)").expect("parses");
    let program = sansho::compile(&parsed, None).expect("compiles");
    for (value, name) in [
        (f64::NAN, "NaN"),
        (f64::INFINITY, "Infinity"),
        (f64::NEG_INFINITY, "-Infinity"),
    ] {
        let bytes = serde_cbor::to_vec(&serde_cbor::Value::Float(value)).expect("encodes");
        let result = evaluate(&program, &bytes[..]).expect("evaluates");
        let rendered = sansho::materialize(&result).expect("materializes");
        assert_eq!(
            rendered,
            serde_json::json!("null"),
            "{name} must render the text `null`"
        );
    }
}

/// The carrier still HOLDS the non-finite value: the policy is about JSON, not
/// about the engine. `is_nan`/`is_infinite` must keep answering.
#[test]
fn the_carrier_still_holds_the_value() {
    let bytes = serde_cbor::to_vec(&serde_cbor::Value::Float(f64::NAN)).expect("encodes");
    let carrier: sansho::SanshoCarrier<'_> = Rc::new(&bytes[..]);
    match carrier.as_sansho_number().expect("a number") {
        SanshoNumber::F64(value) => assert!(value.is_nan(), "the NaN survives"),
        other => panic!("expected an F64, found {other:?}"),
    }
}

/// FINDING 20 (second review): `mean` is not a JMESPath function — the spec's
/// name is `avg` — so `mean(...)` must fail as an UNKNOWN FUNCTION, not quietly
/// return an average. `avg` keeps working.
#[test]
fn mean_is_not_a_function_but_avg_is() {
    let parsed = sansho::parse("avg(@)").expect("parses");
    let program = sansho::compile(&parsed, None).expect("compiles");
    let bytes = serde_cbor::to_vec(&serde_cbor::Value::Array(vec![
        serde_cbor::Value::Integer(1.into()),
        serde_cbor::Value::Integer(3.into()),
    ]))
    .expect("encodes");
    let average = sansho::materialize(&evaluate(&program, &bytes[..]).expect("evaluates"))
        .expect("materializes");
    assert_eq!(average, serde_json::json!(2.0), "avg is the spec's function");

    // `mean` is NOT a spec function: the document is refused at COMPILE time,
    // as any unknown function is — never evaluated to an average
    let parsed = sansho::parse("mean(@)").expect("parses");
    match sansho::compile(&parsed, None) {
        Err(error) => assert!(
            format!("{error}").contains("unknown function"),
            "the refusal names the unknown function: {error}"
        ),
        Ok(program) => {
            let outcome = evaluate(&program, &bytes[..]);
            assert!(
                outcome.is_err(),
                "`mean` must not evaluate to an average: {:?}",
                outcome.map(|value| sansho::materialize(&value))
            );
        }
    }
}
