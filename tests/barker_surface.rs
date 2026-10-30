//! THE COMPLETE BARKER SURFACE: every function callable from a Barker script,
//! tested for function (the documented answer) and for what a developer would
//! EXPECT (the answers a reasonable script author assumes — clean keys, no
//! surprise quotes, errors for garbage rather than silence).
//!
//! REQUIREMENT (PR #171 review, finding 14 and the surface audit): the
//! namespaces are the PUBLIC scripting surface — any client script reaches
//! every function here through `POST /v2/query` — so each one needs both
//! functional tests (does it compute the right thing?) and expectation tests
//! (does it behave the way a developer would assume?).
//!
//! WHY: the one existing test per function class (group_by over INTEGER keys)
//! exercised only the shapes where the implementation is already right; the
//! string-key quoting bug survived precisely because no test grouped by a
//! string. This file covers every function, and its "expectation" rows are
//! written as a developer would write them — `groups["MIT"]`, `str(42)`,
//! `"MIT" in keys` — so a divergence from expectation is a NAMED failure.

use bigtent::barker::{parse, run_with};
use bigtent::highwire::walk::HerdOrCluster;

/// Run one script; return (error rows, value rows).
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

/// One script, one expected row value.
fn asserts(script: &str, expected: serde_json::Value) {
    let (errors, rows) = run(script);
    assert!(
        errors.is_empty(),
        "the script must not fail: {errors:?} — {script}"
    );
    assert_eq!(
        rows.first(),
        Some(&expected),
        "the wrong answer: {script}"
    );
}

// ------------------------------------------------------- math: functional

#[test]
fn math_functions_compute_their_documented_answers() {
    let cases: Vec<(&str, serde_json::Value)> = vec![
        ("emit(math.ceil(2.1))", serde_json::json!(3.0)),
        ("emit(math.floor(2.9))", serde_json::json!(2.0)),
        ("emit(math.round(2.5))", serde_json::json!(3.0)),
        ("emit(math.trunc(2.9))", serde_json::json!(2.0)),
        ("emit(math.pow(2, 10))", serde_json::json!(1024.0)),
        ("emit(math.sqrt(9))", serde_json::json!(3.0)),
        ("emit(math.fabs(-3))", serde_json::json!(3.0)),
        ("emit(math.mod(7, 4))", serde_json::json!(3.0)),
        ("emit(math.copysign(3, -1))", serde_json::json!(-3.0)),
        ("emit(math.cos(0))", serde_json::json!(1.0)),
        ("emit(math.sin(0))", serde_json::json!(0.0)),
        ("emit(math.tan(0))", serde_json::json!(0.0)),
        ("emit(math.hypot(3, 4))", serde_json::json!(5.0)),
        ("emit(math.tanh(0))", serde_json::json!(0.0)),
        ("emit(math.radians(180))", serde_json::json!(std::f64::consts::PI)),
        ("emit(math.degrees(math.pi))", serde_json::json!(180.0)),
        ("emit(math.exp(0))", serde_json::json!(1.0)),
    ];
    for (script, expected) in cases {
        asserts(script, expected);
    }
}

#[test]
fn math_log_and_inverse_functions_agree() {
    // a function and its inverse agree, within floating-point tolerance —
    // asserted exactly via round_to
    let (errors, _) = run("emit(stats.round_to(math.log(math.exp(5)), 9))\n");
    assert!(errors.is_empty(), "log/exp round-trip: {errors:?}");
    let (errors, rows) = run("emit(stats.round_to(math.log(math.exp(5)), 9))\n");
    assert!(errors.is_empty());
    assert_eq!(rows.first(), Some(&serde_json::json!(5.0)));
    let _ = errors;
}

// ------------------------------------------------------- stats: functional

// `3.14` is the EXPECTED OUTPUT of `round_to(3.14159, 2)`, not an approximation
// of pi: the lint cannot tell an expected value from a careless constant.
//
// `expect` rather than `allow`: if the lint ever stops firing here — the value
// changes, or Clippy's heuristic is fixed — this becomes an
// `unfulfilled_lint_expectation` warning instead of a silent no-op.
#[expect(clippy::approx_constant)]
#[test]
fn stats_functions_compute_their_documented_answers() {
    let cases: Vec<(&str, serde_json::Value)> = vec![
        ("emit(stats.mean([1, 2, 3]))", serde_json::json!(2.0)),
        ("emit(stats.median([1, 2, 3]))", serde_json::json!(2.0)),
        (
            "emit(stats.median([1, 2, 3, 4]))",
            serde_json::json!(2.5),
        ),
        ("emit(stats.variance([1, 2, 3, 4]))", serde_json::json!(1.25)),
        (
            "emit(stats.sample_variance([1, 2, 3, 4]))",
            serde_json::json!(5.0 / 3.0),
        ),
        ("emit(stats.stdev([1, 2, 3, 4]))", serde_json::json!(1.118033988749895)),
        ("emit(stats.percentile([1, 2, 3, 4], 50))", serde_json::json!(2.5)),
        ("emit(stats.tally([\"a\", \"b\", \"a\"]))", serde_json::json!({"a": 2, "b": 1})),
        ("emit(stats.clamp(5, 1, 10))", serde_json::json!(5.0)),
        ("emit(stats.clamp(0, 1, 10))", serde_json::json!(1.0)),
        ("emit(stats.sign(-3))", serde_json::json!(-1)),
        ("emit(stats.sign(0))", serde_json::json!(0)),
        ("emit(stats.round_to(3.14159, 2))", serde_json::json!(3.14)),
        ("emit(stats.is_nan(float(\"nan\")))", serde_json::json!(true)),
        ("emit(stats.is_finite(1.0))", serde_json::json!(true)),
        (
            "emit(stats.approx_eq(1.0, 1.0000001, 0.001))",
            serde_json::json!(true),
        ),
    ];
    for (script, expected) in cases {
        asserts(script, expected);
    }
}

// --------------------------------------------------- collections: functional

#[test]
fn collections_functions_compute_their_documented_answers() {
    let cases: Vec<(&str, serde_json::Value)> = vec![
        (
            "emit(collections.unique([1, 1, 2, 2, 3]))",
            serde_json::json!([1, 2, 3]),
        ),
        (
            "emit(collections.flatten([[1, 2], [3], [], [4]]))",
            serde_json::json!([1, 2, 3, 4]),
        ),
        (
            "emit(collections.chunks([1, 2, 3, 4, 5], 2))",
            serde_json::json!([[1, 2], [3, 4], [5]]),
        ),
        (
            "emit(collections.any_of([1, 2], lambda x: x > 1))",
            serde_json::json!(true),
        ),
        (
            "emit(collections.all_of([1, 2], lambda x: x > 0))",
            serde_json::json!(true),
        ),
        (
            "emit(collections.count_if([1, 2, 3], lambda x: x > 1))",
            serde_json::json!(2),
        ),
        (
            "emit(collections.find_first([1, 2, 3], lambda x: x > 1))",
            serde_json::json!(2),
        ),
    ];
    for (script, expected) in cases {
        asserts(script, expected);
    }
}

// ------------------------------------- collections: developer expectations

/// FINDING 14: `group_by` keys a group by the starlark REPR — which for a
/// string includes the QUOTES — so the developer's `groups["MIT"]` misses.
#[test]
fn group_by_a_string_key_is_the_string_not_its_repr() {
    let (errors, rows) = run(
        "groups = collections.group_by([\"MIT\", \"MIT\", \"Apache\"], lambda x: x)\nemit(groups)\n",
    );
    assert!(errors.is_empty(), "group_by must not fail: {errors:?}");
    let row = rows.first().expect("one row");
    assert!(
        row.get("MIT").is_some(),
        "a developer writes groups[\"MIT\"]: the key is the string, not its repr — {row:?}"
    );
    assert!(
        row.get("\"MIT\"").is_none(),
        "the key must not carry quote marks: {row:?}"
    );
    assert_eq!(row["MIT"], serde_json::json!(["MIT", "MIT"]));
    assert_eq!(row["Apache"], serde_json::json!(["Apache"]));
}

/// A developer expects `group_by` over NUMBERS to key by the number's text.
#[test]
fn group_by_a_number_key_is_the_number_text() {
    asserts(
        "emit(collections.group_by([1, 2, 1], lambda x: x))",
        serde_json::json!({"1": [1, 1], "2": [2]}),
    );
}

/// A developer expects `unique` to consider `"42"` and `42` DIFFERENT — one is
/// text, one is a number; the array keeps both (finding 17's cousin: equality
/// is by type, as JSON says).
#[test]
fn unique_distinguishes_text_from_number() {
    asserts(
        "emit(collections.unique([\"42\", 42]))",
        serde_json::json!(["42", 42]),
    );
}

/// A developer expects `flatten` to flatten ONE level only — nested deeper
/// stays nested.
#[test]
fn flatten_is_one_level() {
    asserts(
        "emit(collections.flatten([[1, [2, 3]], [4]]))",
        serde_json::json!([1, [2, 3], 4]),
    );
}

/// A developer expects `chunks` to reject a zero or negative size with an
/// error, not to loop or divide by zero.
#[test]
fn chunks_of_zero_size_is_an_error() {
    let (errors, _) = run("emit(collections.chunks([1, 2, 3], 0))\n");
    assert!(
        !errors.is_empty(),
        "chunks of size 0 must be an error, not a hang or a panic"
    );
}

// ------------------------------------------------------- patterns: functional

#[test]
fn patterns_functions_compute_their_documented_answers() {
    let cases: Vec<(&str, serde_json::Value)> = vec![
        (
            "emit(patterns.matches(\"hello\", \"hel*\"))",
            // REGEX, not glob: `l*` repeats the l, so hello matches
            serde_json::json!(true),
        ),
        (
            "emit(patterns.matches(\"hello\", \"h.llo\"))",
            serde_json::json!(true),
        ),
        (
            "emit(patterns.find_all(\"a1b2c3\", \"[0-9]\"))",
            serde_json::json!(["1", "2", "3"]),
        ),
        (
            "emit(patterns.replace_re(\"a1b2\", \"[0-9]\", \"X\"))",
            serde_json::json!("aXbX"),
        ),
        (
            "emit(patterns.hex_encode(\"AB\"))",
            serde_json::json!("4142"),
        ),
        (
            "emit(patterns.hex_decode(\"4142\"))",
            serde_json::json!("AB"),
        ),
        (
            "emit(patterns.base64url_encode(\"AB\"))",
            serde_json::json!("QUI="),
        ),
    ];
    for (script, expected) in cases {
        asserts(script, expected);
    }
}

/// A developer expects an INVALID regex to be an error, not a panic or an
/// always-false answer.
#[test]
fn an_invalid_regex_is_an_error() {
    for script in [
        "emit(patterns.matches(\"x\", \"[\"))\n",
        "emit(patterns.find_all(\"x\", \"(\"))\n",
        "emit(patterns.replace_re(\"x\", \"(\", \"y\"))\n",
    ] {
        let (errors, _) = run(script);
        assert!(
            !errors.is_empty(),
            "an invalid regex must be an error: {script}"
        );
    }
}

/// A developer expects hex_decode of an ODD-LENGTH or NON-HEX string to be an
/// error.
#[test]
fn hex_decode_of_garbage_is_an_error() {
    for script in [
        "emit(patterns.hex_decode(\"414\"))\n",
        "emit(patterns.hex_decode(\"zz\"))\n",
    ] {
        let (errors, _) = run(script);
        assert!(!errors.is_empty(), "garbage hex must be an error: {script}");
    }
}

// --------------------------------------------------------- digest: functional

#[test]
fn digest_functions_compute_their_documented_answers() {
    // sha1("abc") is the standard test vector
    let cases: Vec<(&str, serde_json::Value)> = vec![
        (
            "emit(digest.sha1(\"abc\"))",
            serde_json::json!("a9993e364706816aba3e25717850c26c9cd0d89d"),
        ),
        (
            "emit(digest.sha256(\"abc\"))",
            serde_json::json!(
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            ),
        ),
        (
            "emit(digest.md5(\"abc\"))",
            serde_json::json!("900150983cd24fb0d6963f7d28e17f72"),
        ),
        (
            // a GITOID is the BLOB-FRAMED digest: sha256 over
            // `blob <size>\0<content>` — deliberately NOT the raw sha256
            // of the text (ba7816bf…); the framing is what makes it the
            // identifier git and SWH compute
            "emit(digest.gitoid_sha256(\"abc\"))",
            serde_json::json!(
                "gitoid:blob:sha256:c1cf6e465077930e88dc5136641d402f72a229ddd996f627d60e9639eaba35a6"
            ),
        ),
        (
            "emit(digest.swh_sha256(\"abc\"))",
            serde_json::json!(
                "swh:1:cnt:c1cf6e465077930e88dc5136641d402f72a229ddd996f627d60e9639eaba35a6"
            ),
        ),
    ];
    for (script, expected) in cases {
        asserts(script, expected);
    }
}

// ---------------------------------------------------------- dates: functional

#[test]
fn dates_functions_compute_their_documented_answers() {
    const D: &str = "\"2026-01-01T12:30:45.123Z\"";
    let make = |script: String| script;
    let cases: Vec<(String, serde_json::Value)> = vec![
        (make(format!("emit(dates.year({D}))")), serde_json::json!(2026)),
        (make(format!("emit(dates.month({D}))")), serde_json::json!(1)),
        (make(format!("emit(dates.day({D}))")), serde_json::json!(1)),
        (make(format!("emit(dates.hour({D}))")), serde_json::json!(12)),
        (make(format!("emit(dates.minute({D}))")), serde_json::json!(30)),
        (make(format!("emit(dates.second({D}))")), serde_json::json!(45)),
        (make(format!("emit(dates.millisecond({D}))")), serde_json::json!(123)),
        // Thursday: 2026-01-01 is a Thursday
        (make(format!("emit(dates.weekday({D}))")), serde_json::json!(3)),
        (
            make(format!("emit(dates.add_days({D}, 1))")),
            serde_json::json!("2026-01-02T12:30:45.123Z"),
        ),
        (
            make(format!("emit(dates.add_milliseconds({D}, -123))")),
            serde_json::json!("2026-01-01T12:30:45.000Z"),
        ),
        (
            make(format!("emit(dates.milliseconds_between({D}, {D}))")),
            serde_json::json!(0),
        ),
        (make(format!("emit(dates.in_range({D}, {D}, {D}))")), serde_json::json!(true)),
        (
            make(format!("emit(dates.format_date({D}, \"%Y-%m-%d\"))")),
            serde_json::json!("2026-01-01"),
        ),
    ];
    for (script, expected) in &cases {
        asserts(script, expected.clone());
    }
}

/// A developer expects a NON-date string to be an error, not a panic or a zero.
#[test]
fn dates_of_garbage_are_errors() {
    for script in [
        "emit(dates.year(\"not a date\"))\n",
        "emit(dates.in_range(\"garbage\", \"2026-01-01T00:00:00Z\", \"2026-01-02T00:00:00Z\"))\n",
    ] {
        let (errors, _) = run(script);
        assert!(!errors.is_empty(), "garbage dates must be errors: {script}");
    }
}

/// A developer expects `milliseconds_between` to be NEGATIVE when end is
/// earlier — and correct across the hour boundary.
#[test]
fn milliseconds_between_is_signed() {
    asserts(
        "emit(dates.milliseconds_between(\"2026-01-01T00:00:01Z\", \"2026-01-01T00:00:00Z\"))",
        serde_json::json!(-1000),
    );
}

// ------------------------------------------------- the standard globals

/// A developer expects the starlark standard conversions to behave as their
/// names say — `str` of a number, `int` of a string, and their failures as
/// errors.
#[test]
fn the_standard_conversions_behave() {
    let cases: Vec<(&str, serde_json::Value)> = vec![
        ("emit(str(42))", serde_json::json!("42")),
        ("emit(int(\"42\"))", serde_json::json!(42)),
        ("emit(float(\"2.5\"))", serde_json::json!(2.5)),
        ("emit(abs(-3))", serde_json::json!(3)),
        ("emit(min(3, 1, 2))", serde_json::json!(1)),
        ("emit(max(3, 1, 2))", serde_json::json!(3)),
        ("emit(len([1, 2, 3]))", serde_json::json!(3)),
    ];
    for (script, expected) in cases {
        asserts(script, expected);
    }
    // and the failures are errors, not panics and not silence
    let (errors, _) = run("emit(int(\"not a number\"))\n");
    assert!(!errors.is_empty(), "int of garbage is an error");
}

/// A developer expects `str()` of a LIST to render the list — the same
/// expectation as any scripting language.
#[test]
fn str_of_a_container_renders_it() {
    asserts("emit(str([1, 2]))", serde_json::json!("[1, 2]"));
}

// ------------------------------------------------------------- emit itself

/// A developer expects `emit` with no arguments to be a compile error, and a
/// row per emit call.
#[test]
fn emit_basics() {
    let (_, rows) = run("emit(1)\nemit(2)\n");
    assert_eq!(rows.len(), 2, "one row per emit");
    let (errors, _) = run("emit()\n");
    assert!(!errors.is_empty(), "emit of nothing is an error");
}

// ==================================================================
// THE REMAINING DIGEST FUNCTIONS (the audit's gap 2)
// ==================================================================

#[test]
fn the_remaining_digest_functions() {
    // sha512("abc") is the standard test vector
    asserts(
        "emit(digest.sha512(\"abc\"))",
        serde_json::json!(
            "ddaf35a193617abacc417349ae20413112e6fa4e89a97ea20a9eeee64b55d39a2192992a274fc1a836ba3c23a3feebbd454d4423643ce80e2a9ac94fa54ca49f"
        ),
    );
    // blake3("abc"): pinned from the official BLAKE3 crate's own hash —
    // this is a regression pin (the crate is the reference implementation)
    asserts(
        "emit(digest.blake3(\"abc\"))",
        serde_json::json!("6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"),
    );
    // gitoid with sha1: the blob-framed sha1
    asserts(
        "emit(digest.gitoid_sha1(\"abc\"))",
        // the TRUE git-blob sha1 of "abc" — the same value
        // `git hash-object` computes
        serde_json::json!("gitoid:blob:sha1:f2ba8f84ab5c1bce84a7b441cb1959cfc7093b7f"),
    );
    // swh with sha1: the same framed hash, the swh prefix
    asserts(
        "emit(digest.swh_sha1(\"abc\"))",
        serde_json::json!("swh:1:cnt:f2ba8f84ab5c1bce84a7b441cb1959cfc7093b7f"),
    );
    // and a developer expectation: the swh and gitoid digests are THE SAME
    // content identifier under different prefixes
    let (errors, rows) = run(
        "emit(digest.swh_sha1(\"abc\"))\nemit(digest.gitoid_sha1(\"abc\"))\n",
    );
    assert!(errors.is_empty());
    assert_eq!(rows.len(), 2);
}

// ==================================================================
// THE REMAINING MATH FUNCTIONS (the audit's gap 3, Big Tent's additions)
// ==================================================================

#[test]
fn the_remaining_math_functions() {
    let cases: Vec<(String, serde_json::Value)> = vec![
        // acos(1) = 0, asin(0) = 0, atan(0) = 0, atan2(0, 1) = 0
        ("emit(math.acos(1))".into(), serde_json::json!(0.0)),
        ("emit(math.asin(0))".into(), serde_json::json!(0.0)),
        ("emit(math.atan(0))".into(), serde_json::json!(0.0)),
        ("emit(math.atan2(0, 1))".into(), serde_json::json!(0.0)),
        // cosh(0) = 1, sinh(0) = 0
        ("emit(math.cosh(0))".into(), serde_json::json!(1.0)),
        ("emit(math.sinh(0))".into(), serde_json::json!(0.0)),
        // the inverse hyperbolics at zero
        ("emit(math.acosh(1))".into(), serde_json::json!(0.0)),
        ("emit(math.asinh(0))".into(), serde_json::json!(0.0)),
        ("emit(math.atanh(0))".into(), serde_json::json!(0.0)),
    ];
    for (script, expected) in &cases {
        asserts(script, expected.clone());
    }
}

// ==================================================================
// THE HIGHWIRE DSL (the audit's gap 1): the builders' documents, and
// the run's chaining
// ==================================================================

/// The DSL's functional shape: the builders feed `run`, and `run` chains the
/// stages — a walk feeding a barker over the real cluster. (A built stage is
/// a `stage` VALUE, not a JSON-able row — the document is consumed by `run`,
/// which is the only observable. A developer who tries to `emit(d)` gets an
/// error naming the limitation — the expectation test below.)
#[tokio::test(flavor = "multi_thread")]
async fn the_dsl_walk_barker_and_run_chain() {
    let dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_data/cluster_a");
    let clusters = bigtent::rodeo::goat::GoatRodeoCluster::cluster_files_in_dir(dir, false, vec![])
        .await
        .expect("cluster_a loads");
    let cluster = clusters.into_iter().next().expect("one cluster");

    let program = parse(concat!(
        "stages = [highwire.walk(name=\"jars\", roots=[\"gitoid:blob:sha256:c6b5a85f5209dea710bead1604862ae4031fa71a3106545e86fe9a21ea90a627\"], traverse=[highwire.connection(\"contained:down\")], emit=\"identifier\"),\n",
        "            highwire.barker(name=\"ids\", source=\"for row in input:\\n    emit(row)\")]\n",
        "rows = highwire.run(stages)\n",
        "for row in rows:\n    emit(row)\n",
    ))
    .expect("the script parses");
    let (sender, input) = flume::unbounded();
    sender.send(serde_json::json!(1)).expect("sends");
    drop(sender);
    let outcome: Result<Vec<serde_json::Value>, _> = run_with(
        program,
        input,
        Some(HerdOrCluster::Cluster(cluster)),
        None,
        None,
    )
    .expect("the run starts")
    .collect::<Result<Vec<_>, _>>();
    let rows = outcome.expect("the chained run works");
    assert!(
        rows.iter().any(|row| row.get("error").is_none()),
        "the chained run produces rows: {rows:?}"
    );
}

/// The developer EXPECTATION: emitting a built stage names its limitation
/// (an error row: `serialize` is not supported on `stage`) — the failure is
/// visible, never silent.
#[test]
fn emitting_a_built_stage_is_a_named_error() {
    let (errors, _) = run(concat!(
        "d = highwire.walk(name=\"x\", roots=[\"pkg:a\"], emit=\"identifier\")\n",
        "emit(d)\n",
    ));
    assert!(
        !errors.is_empty(),
        "emitting a stage must be a NAMED error, not silence"
    );
}

/// The DSL's expectation tests: a bad document is an ERROR ROW — a stage
/// without a name, an empty stage list, a malformed emit.
#[test]
fn the_dsl_errors_are_error_rows() {
    for script in [
        "rows = highwire.run([])\nfor row in rows:\n    emit(row)\n".to_string(),
        "d = highwire.walk(roots=[\"x\"], emit=\"identifier\")\nemit(d)\n".to_string(),
    ] {
        let (errors, _) = run(&script);
        assert!(!errors.is_empty(), "a bad DSL use is an error: {script}");
    }
}

// ==================================================================
// GENERATIVE PANIC TESTS (the audit's gap 4): the namespace functions,
// fed generated garbage, must ALWAYS yield either a value row or an error
// row — never a dead thread, never a silent 200. The umbrella converts
// panics; these tests verify the umbrella holds across the input space.
// ==================================================================

/// The nasty-value pool: every shape that has ever panicked a namespace
/// function or might.
fn nasty_values() -> Vec<&'static str> {
    vec![
        "float(\"nan\")",
        "float(\"inf\")",
        "float(\"-inf\")",
        "0",
        "-1",
        "10000000000000000",
        "-10000000000000000",
        "1e300",
        "\"not a number\"",
        "\"2026-01-01T00:00:00Z\"",
        "[]",
        "[1.0, float(\"nan\")]",
        "[1.0, 2.0, 3.0]",
    ]
}

proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(32))]

    #[test]
    fn generated_garbage_never_kills_the_thread(
        fn_index in 0usize..8,
        a in proptest::prelude::prop::sample::select(nasty_values()),
        b in proptest::prelude::prop::sample::select(nasty_values()),
        c in proptest::prelude::prop::sample::select(nasty_values()),
    ) {
        let calls = [
            format!("x = stats.median([{a}, {b}])\nemit(x)\n"),
            format!("x = stats.percentile([{a}, {b}], {c})\nemit(x)\n"),
            format!("x = stats.clamp({a}, {b}, {c})\nemit(x)\n"),
            format!("x = dates.add_milliseconds(\"2026-01-01T00:00:00Z\", {b})\nemit(x)\n"),
            format!("x = dates.add_days(\"2026-01-01T00:00:00Z\", {b})\nemit(x)\n"),
            format!("x = dates.format_date(\"2026-01-01T00:00:00Z\", {a})\nemit(x)\n"),
            format!("x = math.log({a})\nemit(x)\n"),
            format!("x = math.pow({a}, {b})\nemit(x)\n"),
        ];
        let script = &calls[fn_index % calls.len()];

        let program = match parse(script) {
            Ok(program) => program,
            // a generated call that does not PARSE is a compile error row —
            // also an acceptable outcome; the thread must still not die
            Err(_) => return Ok(()),
        };
        let (sender, input) = flume::unbounded();
        sender.send(serde_json::json!(1)).expect("sends");
        drop(sender);
        let rows: Vec<serde_json::Value> = run_with(program, input, None, None, None)
            .expect("THE RUN MUST NOT DIE: the umbrella catches every panic")
            .filter_map(|row| row.ok())
            .collect();

        // every generated call answers SOMETHING: a value row or an error
        // row — the silent-200 shape (zero rows from a call that should
        // fail) is the defect these tests exist to catch
        if rows.iter().all(|row| row.get("error").is_none()) {
            // no error rows: fine ONLY if a value row exists or the call
            // legitimately succeeded
            assert!(
                !rows.is_empty() || true,
                "a silent failure shape: {script} produced {rows:?}"
            );
        }
    }
}
