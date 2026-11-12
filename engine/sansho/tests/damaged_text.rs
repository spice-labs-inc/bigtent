// The engine's error carries its location and its source text by design; boxing it
// would add an allocation to every failure. The same expectation the library crates
// declare, restated because each test and bench is its own crate root.
#![expect(
    clippy::result_large_err,
    reason = "the error carries its location and its source by design"
)]

//! A DAMAGED ITEM IS AN ERROR, NOT AN EMPTY ONE.
//!
//! REQUIREMENT (class-C audit, the finding-46 family): a gate that collapses an
//! evaluation error into a default makes a broken document look like an empty
//! one. The slice edge had exactly that shape in two adjacent arms: a string's
//! text came from `as_str().unwrap_or_default()`, and an array's length from
//! `container_len().unwrap_or(0)`. Both turned "the engine cannot read this item"
//! into "the item is empty", which then slices to `""` or `[]` and returns a
//! perfectly ordinary answer to a query over a document nobody can read.
//!
//! THEORY OF THE TEST: a CBOR text item whose bytes are not valid UTF-8 has
//! `Kind::String` (so the string arm is taken) and `as_str()` FAILS (so the
//! swallow triggers). The query must report the failure, because every other
//! navigation in the same function maps its read error with `?`; the slice is the
//! only edge that answers a value instead.
//!
//! LLM section: `0x63 0xFF 0xFE 0x00` is text(3) with invalid UTF-8 bytes. The
//! control is the same document with valid text, which must slice normally, so a
//! blanket refusal of slicing is not what this test accepts.

use sansho::error::SanshoError;

/// Evaluate an expression over a document, materialized.
fn eval(
    document: &[u8],
    expression: &str,
) -> Result<serde_json::Value, SanshoError> {
    let parsed = sansho::parse(expression)?;
    let program = sansho::compile(&parsed, None)?;
    let value = sansho::evaluate(&program, document)?;
    sansho::materialize(&value)
}

/// The damaged text: text(3) whose bytes are not valid UTF-8.
fn damaged_text() -> Vec<u8> {
    vec![0x63, 0xff, 0xfe, 0x00]
}

/// Slicing a text the engine cannot decode must fail, not answer `""`.
#[test]
fn slicing_a_damaged_text_is_an_error_not_an_empty_string() {
    match eval(&damaged_text(), "@[0:1]") {
        Err(_) => {}
        Ok(answer) => panic!(
            "a slice of undecodable text must be an error, not {answer} — the \
             string arm swallows `as_str`'s failure into an empty string"
        ),
    }
}

/// The control: the same slice over decodable text answers the text.
#[test]
fn slicing_readable_text_still_answers() {
    // text(3) "abc"
    let document = vec![0x63, b'a', b'b', b'c'];
    assert_eq!(eval(&document, "@[0:2]").expect("a readable slice"), serde_json::json!("ab"));
}

/// `length` of undecodable text must fail, not answer 0.
///
/// REQUIREMENT (class-C audit): `string_length` was
/// `as_str().map(chars().count()).unwrap_or(0)` — a string the engine cannot
/// decode reported the length of an empty one, which is a plausible-looking
/// answer to a question nobody could ask.
///
/// THEORY OF THE TEST: the same damaged text as above has `Kind::String`, so the
/// string arm of `length` is taken and the decode fails. The control is readable
/// text of three characters, whose length is 3, so a blanket failure is refused.
#[test]
fn the_length_of_undecodable_text_is_an_error_not_zero() {
    match eval(&damaged_text(), "length(@)") {
        Err(_) => {}
        Ok(answer) => panic!("length of undecodable text must be an error, not {answer}"),
    }
    // control
    let readable = vec![0x63, b'a', b'b', b'c'];
    assert_eq!(eval(&readable, "length(@)").expect("a readable length"), serde_json::json!(3));
}

/// `to_number` of undecodable text must fail, not answer `null`.
///
/// REQUIREMENT (class-C audit): `to_number` answered `null` for a string it could
/// not DECODE, which is the specification's answer for a string that is merely not
/// numeric — two different facts collapsed into one answer. Its number arm had
/// the same shape: `as_f64().unwrap_or(0.0)` made an unreadable number zero.
///
/// THEORY OF THE TEST: undecodable text must error; the controls are a numeric
/// string (`"42"` → 42) and a non-numeric one (`"abc"` → null), which must keep
/// their specification answers.
#[test]
fn to_number_of_undecodable_text_is_an_error_not_null() {
    match eval(&damaged_text(), "to_number(@)") {
        Err(_) => {}
        Ok(answer) => panic!("to_number of undecodable text must be an error, not {answer}"),
    }
    // a numeric string still converts
    let numeric = vec![0x62, b'4', b'2'];
    assert_eq!(
        eval(&numeric, "to_number(@)").expect("a numeric string"),
        serde_json::json!(42.0)
    );
    // a non-numeric string is still null, the specification's answer
    let words = vec![0x63, b'a', b'b', b'c'];
    assert_eq!(eval(&words, "to_number(@)").expect("a word"), serde_json::json!(null));
}

/// A FILTER MUST NOT DROP AN ITEM IT CANNOT READ.
///
/// REQUIREMENT (class-C audit, approved interface change): `truthy` answered
/// `false` for a read error, so a filter over an array containing undecodable text
/// silently dropped the element — "empty" and "unreadable" collapsed into one
/// answer, and the query returned a perfectly ordinary result for an item nobody
/// could read. `truthy` now returns `Result`, and the filter propagates.
///
/// THEORY OF THE TEST: an array of [damaged text, 1] filtered by `[?@]` must FAIL
/// rather than yield [1]. The control is the same filter over ["abc", 1], which
/// must yield [1] — the damaged element is the only difference.
#[test]
fn a_filter_does_not_silently_drop_undecodable_text() {
    // array(2): text(3) invalid UTF-8, then 1
    let mut damaged = vec![0x82];
    damaged.extend(damaged_text());
    damaged.push(0x01);
    match eval(&damaged, "@[?@]") {
        Err(_) => {}
        Ok(answer) => panic!(
            "a filter over undecodable text must be an error, not {answer} — the \
             truthiness gate collapsed the read failure into `false`"
        ),
    }

    // control: the same array with readable text keeps the filter working
    let mut readable = vec![0x82, 0x63, b'a', b'b', b'c'];
    readable.push(0x01);
    assert_eq!(eval(&readable, "@[?@]").expect("a readable filter"), serde_json::json!(["abc", 1]));
}

/// REVERSING UNDECODABLE TEXT REPORTS THE DECODE FAILURE, NOT A TYPE ERROR.
///
/// REQUIREMENT (class-C sweep): `reverse` answered `type_error(..)` for a string
/// whose text could not be decoded — the kind was right and the content was not, so
/// the reported fact was wrong. It now propagates the read's own error, which is an
/// `Input` failure rather than an `Evaluation` one.
///
/// THEORY OF THE TEST: the two error kinds are distinguishable, so the assertion is
/// on the VARIANT, not the prose: a decode failure is `Input`, a type error is
/// `Evaluation`. The control reverses readable text normally.
#[test]
fn reversing_undecodable_text_reports_the_decode_failure() {
    match eval(&damaged_text(), "reverse(@)") {
        Err(SanshoError::Input { .. }) => {}
        other => panic!("a decode failure must be an Input error, got {other:?}"),
    }
    let readable = vec![0x63, b'a', b'b', b'c'];
    assert_eq!(eval(&readable, "reverse(@)").expect("a readable reverse"), serde_json::json!("cba"));
}
