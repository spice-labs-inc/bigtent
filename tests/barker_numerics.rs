//! Numeric arguments that are coerced into integers.
//!
//! REQUIREMENT (second review, findings 22 and 23), decided by the owner: a
//! value that cannot be what the argument requires is REFUSED, naming the value
//! the script actually passed. Before, `chunks` and the date functions made an
//! `as` cast:
//! * `chunks(xs, 1.9)` silently became 1;
//! * `chunks(xs, float("nan"))` became 0 and reported "a chunk size is at
//!   least 1, found 0" — the wrong message, naming a number never passed;
//! * `chunks(xs, -3)` saturated to 0, the same misleading message;
//! * `add_milliseconds(d, 1.9)` silently shifted by 1;
//! * `add_milliseconds(d, float("nan"))` silently returned `d` (a shift of 0);
//! * `from_millis(float("inf"))` saturated to `i64::MAX`, an instant never
//!   asked for.
//!
//! THEORY OF THE TEST: the error must NAME the offending value, because the
//! defect's signature is a plausible-looking answer (or a message about a
//! different number). Each case asserts the value appears in the message, and
//! each has an integral control that must still work.

use bigtent::barker::{parse, run_with};

/// Run one script; (the error rows' texts, the value rows).
fn run(script: &str) -> (Vec<String>, Vec<serde_json::Value>) {
    let program = parse(script).expect("the script parses");
    let (sender, input) = flume::unbounded();
    sender.send(serde_json::json!(1)).expect("sends");
    drop(sender);
    let rows: Vec<serde_json::Value> = run_with(program, input, None, None, None)
        .expect("the run starts")
        .filter_map(|row| row.ok())
        .collect();
    let errors: Vec<String> = rows
        .iter()
        .filter_map(|row| row.get("error").and_then(|e| e.as_str().map(str::to_string)))
        .collect();
    (errors, rows)
}

/// Assert that a script fails and that its error names `needle`.
fn refuses(script: &str, needle: &str) {
    let (errors, _) = run(script);
    assert!(!errors.is_empty(), "the script must fail: {script}");
    let joined = errors.join("\n");
    assert!(
        joined.contains(needle),
        "the error must name `{needle}`: {joined}"
    );
}

// ------------------------------------------------------------------ chunks

/// A FRACTIONAL chunk size is refused, naming the value.
#[test]
fn a_fractional_chunk_size_is_refused_by_name() {
    refuses("emit(collections.chunks([1, 2, 3], 1.9))\n", "1.9");
}

/// A NaN chunk size is refused by NAME — not as "found 0".
#[test]
fn a_nan_chunk_size_is_refused_by_name() {
    refuses(
        "emit(collections.chunks([1, 2, 3], float(\"nan\")))\n",
        "nan",
    );
}

/// A NEGATIVE chunk size is refused by name, not saturated to 0.
#[test]
fn a_negative_chunk_size_is_refused_by_name() {
    refuses("emit(collections.chunks([1, 2, 3], -3))\n", "-3");
}

/// An INFINITE chunk size is refused by name.
#[test]
fn an_infinite_chunk_size_is_refused_by_name() {
    refuses(
        "emit(collections.chunks([1, 2, 3], float(\"inf\")))\n",
        "inf",
    );
}

/// The control: an integral size still chunks.
#[test]
fn an_integral_chunk_size_still_works() {
    let (errors, rows) = run("emit(collections.chunks([1, 2, 3], 2))\n");
    assert!(errors.is_empty(), "an integral size is fine: {errors:?}");
    assert_eq!(rows.first(), Some(&serde_json::json!([[1, 2], [3]])));
    // an integral FLOAT is a whole number, so it is accepted too
    let (errors, rows) = run("emit(collections.chunks([1, 2, 3], 2.0))\n");
    assert!(errors.is_empty(), "2.0 is whole: {errors:?}");
    assert_eq!(rows.first(), Some(&serde_json::json!([[1, 2], [3]])));
}

// ------------------------------------------------------------------- dates

/// A FRACTIONAL millisecond shift is refused, naming the value.
#[test]
fn a_fractional_date_shift_is_refused_by_name() {
    refuses(
        "emit(dates.add_milliseconds(\"2026-01-01T00:00:00Z\", 1.9))\n",
        "1.9",
    );
}

/// A NaN shift is refused by name — not silently treated as 0.
#[test]
fn a_nan_date_shift_is_refused_by_name() {
    refuses(
        "emit(dates.add_milliseconds(\"2026-01-01T00:00:00Z\", float(\"nan\")))\n",
        "nan",
    );
}

/// An infinite shift is refused by name — not saturated to `i64::MAX`.
#[test]
fn an_infinite_date_shift_is_refused_by_name() {
    refuses(
        "emit(dates.add_milliseconds(\"2026-01-01T00:00:00Z\", float(\"inf\")))\n",
        "inf",
    );
}

/// `from_millis` of a non-finite value is refused by name.
#[test]
fn from_millis_of_a_non_finite_value_is_refused_by_name() {
    refuses("emit(dates.from_millis(float(\"inf\")))\n", "inf");
    refuses("emit(dates.from_millis(float(\"nan\")))\n", "nan");
}

/// The control: an integer instant still renders, and an integral shift works.
#[test]
fn integral_date_arguments_still_work() {
    let (errors, rows) = run("emit(dates.from_millis(0))\n");
    assert!(errors.is_empty(), "from_millis(0) works: {errors:?}");
    assert_eq!(
        rows.first(),
        Some(&serde_json::json!("1970-01-01T00:00:00.000Z")),
        "the epoch"
    );
    let (errors, rows) = run(
        "emit(dates.add_milliseconds(\"2026-01-01T00:00:00Z\", 1000))\n",
    );
    assert!(errors.is_empty(), "a whole millisecond shift works: {errors:?}");
    assert_eq!(rows.first(), Some(&serde_json::json!("2026-01-01T00:00:01.000Z")));
}
