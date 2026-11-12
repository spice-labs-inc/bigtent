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

// ------------------------------------------------------- unbounded numeric edges

/// A NaN BOUND MUST NOT REACH `f64::clamp`.
///
/// REQUIREMENT (third review, finding 6): `f64::clamp` panics when `low > high`
/// AND when either bound is NaN. The guard only tested `low > high`, and every
/// comparison with NaN is false, so `stats.clamp(5, float("nan"), 1)` fell
/// through to `x.clamp(…)` and panicked. The umbrella turned the panic into an
/// error row, but a validation that only renames a panic has not validated.
///
/// THEORY OF THE TEST: the NaN bound is refused, naming the requirement, while
/// an ordinary clamp still answers and still clamps. The control is what
/// distinguishes a fix from a blanket refusal of `clamp`.
#[test]
fn a_nan_bound_is_refused_instead_of_panicking() {
    // the KIND is the discriminator: a validated refusal is a `value` failure,
    // while `f64::clamp`'s panic payload happens to CONTAIN the word "NaN"
    // ("min > max, or either was NaN"), so a message match alone would accept
    // the caught panic this finding is about
    let (errors, rows) = run("emit(stats.clamp(5, float(\"nan\"), 1))\n");
    assert!(!errors.is_empty(), "a NaN bound must fail: {rows:?}");
    let kind = rows
        .iter()
        .find_map(|row| row.get("kind"))
        .and_then(|kind| kind.as_str())
        .unwrap_or("");
    assert_eq!(
        kind, "value",
        "a VALIDATED refusal (kind `value`), not a caught panic: {rows:?}"
    );

    // the control: an ordinary clamp still works, and it clamps
    let (errors, rows) = run("emit(stats.clamp(50, 1, 10))\n");
    assert!(errors.is_empty(), "an ordinary clamp does not fail: {errors:?}");
    assert_eq!(rows, vec![serde_json::json!(10.0)]);
}

/// A NON-FINITE FLOAT CANNOT BECOME A ROW, AND SAYS SO.
///
/// REQUIREMENT (third review, finding 4): `to_json_limited` converted a float
/// with `json!(number.0)`, which is `to_value(…).unwrap()`; serde_json refuses
/// NaN and infinities, so the macro PANICKED. That contradicts the function's
/// contract — an unrepresentable value is an error — and it survived only
/// because the thread-level umbrella caught the panic.
///
/// THEORY OF THE TEST: emitting a NaN or an infinity must produce an ERROR row
/// naming the non-finite number, not a panic and not a silent `null`. The
/// control is an ordinary float, which must still be emitted as itself.
#[test]
fn a_non_finite_float_is_refused_rather_than_panicking() {
    for script in ["emit(float(\"nan\"))\n", "emit(float(\"inf\"))\n"] {
        refuses(script, "non-finite");
    }

    // the control: a finite float is emitted as the number it is
    let (errors, rows) = run("emit(1.5)\n");
    assert!(errors.is_empty(), "a finite float is fine: {errors:?}");
    assert_eq!(rows, vec![serde_json::json!(1.5)]);
}

// ------------------------------------------------- Python's three failures

/// PYTHON'S SEPARATION OF TYPE, VALUE AND OVERFLOW — REPLICATED.
///
/// REQUIREMENT (S6, decided by replicating the reference dialect): Python reports
/// three DIFFERENT failures and never calls a number a "wrong type":
///
/// * `int(None)`            → `TypeError`    (wrong kind of thing)
/// * `int(float("nan"))`    → `ValueError`  ("cannot convert float NaN to integer")
/// * `int(float("inf"))`    → `OverflowError` ("cannot convert float infinity")
/// * a big int for a C integer → `OverflowError` ("Python int too large…")
///
/// Our vocabulary already mirrors it (`Type`/`Value`/`Range`), but `as_number`
/// answered a bare `Option`, so an integer too large for `i64` and a `NoneType`
/// both arrived as `None` and both left as `Type` — a number reported as the wrong
/// kind of thing.
///
/// THEORY OF THE TEST: the KIND in the error row is the discriminator, exactly as
/// it is for the clamp and non-finite tests — a message match would accept the
/// wrong diagnosis. `chunks(xs, 10**30)` must be `range`, `chunks(xs, None)` must
/// be `type`, and `chunks(xs, float("nan"))` must be `value`.
#[test]
fn python_three_failures_are_three_kinds() {
    /// The kind code of the first error row a script produces.
    fn kind_of(script: &str) -> String {
        let program = parse(script).expect("parses");
        let (sender, input) = flume::unbounded();
        sender.send(serde_json::json!(1)).expect("sends");
        drop(sender);
        let rows: Vec<serde_json::Value> = run_with(program, input, None, None, None)
            .expect("the run starts")
            .filter_map(|row| row.ok())
            .collect();
        rows.iter()
            .find_map(|row| row.get("kind"))
            .and_then(|kind| kind.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| panic!("no typed error row for {script}: {rows:?}"))
    }

    // an integer too large for i64: Python's OverflowError
    assert_eq!(
        kind_of("emit(collections.chunks([1, 2], 1000000000000000000000000000000))\n"),
        "range",
        "a number too large to carry is an OVERFLOW, not a wrong type"
    );
    // infinite: Python's OverflowError
    assert_eq!(
        kind_of("emit(collections.chunks([1, 2], float(\"inf\")))\n"),
        "range",
        "an infinite size is an OVERFLOW"
    );
    // NaN: Python's ValueError
    assert_eq!(
        kind_of("emit(collections.chunks([1, 2], float(\"nan\")))\n"),
        "value",
        "NaN is a VALUE failure, as Python's ValueError says"
    );
    // not a number at all: Python's TypeError
    assert_eq!(
        kind_of("emit(collections.chunks([1, 2], None))\n"),
        "type",
        "a non-number is a TYPE failure"
    );
}
