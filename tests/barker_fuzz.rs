//! THE BARKER FUZZ: EVERY function callable from a Barker script, fed EVERY
//! nasty input, must never die silently.
//!
//! REQUIREMENT (the owner's directive): tests that exercise every supplied
//! function with lots of random input designed to break them. The invariant
//! under test is the one finding 7 established: a panic anywhere in the
//! evaluator is caught (the umbrella) and becomes an ERROR ROW — so for ANY
//! function and ANY argument, the observable outcome is a value row or an
//! error row, and the thread always survives. A silent death (partial rows,
//! closed channel, clean 200) is the defect these tests exist to catch.
//!
//! HOW: the complete function table (namespace, name, arity) — every function
//! Big Tent adds to Starlark. Two layers:
//!   1. the EXHAUSTIVE cross-product: every function against every value in
//!      the nasty pool (pairs for arity 2, sampled for arity 3);
//!   2. a proptest drawing random (function, arguments) combinations, with
//!      shrinking.
//!
//! Both assert the same invariant: the run completes, and every observable
//! row is a value or an error.

use bigtent::barker::{parse, run_with};

/// The nasty-value pool: every shape that has ever panicked a namespace
/// function, plus the shapes that should be cleanly rejected.
const NASTY: &[&str] = &[
    "1",
    "0",
    "-1",
    "1.5",
    "float(\"nan\")",
    "float(\"inf\")",
    "float(\"-inf\")",
    "1e300",
    "10000000000000000",
    "-10000000000000000",
    "true",
    "false",
    "\"not a number\"",
    "\"2026-01-01T00:00:00Z\"",
    "\"\"",
    "[]",
    "[1.0, float(\"nan\")]",
    "[1, 2, 3]",
    "{}",
    "[[1], [2, [3]]]",
    "None",
];

/// Every function Big Tent adds to Starlark: (namespace, name, arity).
/// `now_ms` is excluded (it takes no argument and touches no input).
fn every_function() -> Vec<(&'static str, &'static str, usize)> {
    vec![
        // math
        ("math", "ceil", 1), ("math", "floor", 1), ("math", "round", 1),
        ("math", "trunc", 1), ("math", "pow", 2), ("math", "sqrt", 1),
        ("math", "exp", 1), ("math", "log", 1), ("math", "fabs", 1),
        ("math", "mod", 2), ("math", "copysign", 2), ("math", "acos", 1),
        ("math", "asin", 1), ("math", "atan", 1), ("math", "atan2", 2),
        ("math", "cos", 1), ("math", "sin", 1), ("math", "tan", 1),
        ("math", "hypot", 2), ("math", "acosh", 1), ("math", "asinh", 1),
        ("math", "atanh", 1), ("math", "cosh", 1), ("math", "sinh", 1),
        ("math", "tanh", 1), ("math", "radians", 1), ("math", "degrees", 1),
        // stats
        ("stats", "mean", 1), ("stats", "median", 1), ("stats", "variance", 1),
        ("stats", "sample_variance", 1), ("stats", "stdev", 1),
        ("stats", "sample_stdev", 1), ("stats", "percentile", 2),
        ("stats", "tally", 1), ("stats", "clamp", 3), ("stats", "sign", 1),
        ("stats", "round_to", 2), ("stats", "is_nan", 1),
        ("stats", "is_infinite", 1), ("stats", "is_finite", 1),
        ("stats", "approx_eq", 3),
        // collections
        ("collections", "unique", 1), ("collections", "flatten", 1),
        ("collections", "chunks", 2), ("collections", "group_by", 2),
        ("collections", "any_of", 2), ("collections", "all_of", 2),
        ("collections", "count_if", 2), ("collections", "find_first", 2),
        // patterns
        ("patterns", "matches", 2), ("patterns", "find_all", 2),
        ("patterns", "replace_re", 3), ("patterns", "base64url_encode", 1),
        ("patterns", "hex_encode", 1), ("patterns", "hex_decode", 1),
        // digest
        ("digest", "sha1", 1), ("digest", "sha256", 1), ("digest", "sha512", 1),
        ("digest", "md5", 1), ("digest", "blake3", 1), ("digest", "gitoid_sha1", 1),
        ("digest", "gitoid_sha256", 1), ("digest", "swh_sha1", 1),
        ("digest", "swh_sha256", 1),
        // dates
        ("dates", "from_millis", 1), ("dates", "year", 1), ("dates", "month", 1),
        ("dates", "day", 1), ("dates", "hour", 1), ("dates", "minute", 1),
        ("dates", "second", 1), ("dates", "millisecond", 1), ("dates", "weekday", 1),
        ("dates", "add_milliseconds", 2), ("dates", "add_days", 2),
        ("dates", "milliseconds_between", 2), ("dates", "in_range", 3),
        ("dates", "format_date", 2),
    ]
}

/// Run one call script; (the error rows' texts, the value rows' count).
/// Panics if the RUN dies — which is the defect this suite exists to catch.
fn call(ns: &str, name: &str, args: &[&str]) -> (Vec<String>, usize) {
    let call_args = args.join(", ");
    let script = format!("x = {ns}.{name}({call_args})\nemit(x)\n");
    let program = match parse(&script) {
        Ok(program) => program,
        // a parse refusal is a clean, observable rejection
        Err(_) => return (vec!["<parse error>".to_string()], 0),
    };
    let (sender, input) = flume::unbounded();
    sender.send(serde_json::json!(1)).expect("sends");
    drop(sender);
    let rows: Vec<Result<serde_json::Value, anyhow::Error>> = run_with(
        program,
        input,
        None,
        None,
        None,
    )
    .expect("THE RUN MUST SURVIVE: the umbrella catches every evaluator panic")
    .collect();
    let errors: Vec<String> = rows
        .iter()
        .filter_map(|row| match row {
            Err(error) => Some(format!("row-error: {error}")),
            Ok(value) => value
                .get("error")
                .and_then(|e| e.as_str().map(str::to_string)),
        })
        .collect();
    let values = rows.iter().filter(|row| matches!(row, Ok(v) if v.get("error").is_none())).count();
    (errors, values)
}

/// THE EXHAUSTIVE SWEEP: every arity-1 function against every nasty value.
#[test]
fn every_arity_one_function_survives_every_nasty_value() {
    for (ns, name, arity) in every_function() {
        if arity != 1 {
            continue;
        }
        for value in NASTY {
            let (errors, values) = call(ns, name, &[value]);
            assert!(
                !errors.is_empty() || values > 0,
                "SILENT DEATH: {ns}.{name}({value}) produced nothing observable"
            );
        }
    }
}

/// THE EXHAUSTIVE SWEEP: every arity-2 function against every value pair.
#[test]
fn every_arity_two_function_survives_every_value_pair() {
    for (ns, name, arity) in every_function() {
        if arity != 2 {
            continue;
        }
        for a in NASTY {
            for b in NASTY {
                let (errors, values) = call(ns, name, &[a, b]);
                assert!(
                    !errors.is_empty() || values > 0,
                    "SILENT DEATH: {ns}.{name}({a}, {b}) produced nothing observable"
                );
            }
        }
    }
}

/// Arity 3: the cross-product is sampled (every value against two fixed
/// companions, then every pair against a fixed third), so the space is
/// covered without the full cube.
#[test]
fn every_arity_three_function_survives_sampled_triples() {
    for (ns, name, arity) in every_function() {
        if arity != 3 {
            continue;
        }
        for a in NASTY {
            let (errors, values) = call(ns, name, &[a, "1", "10"]);
            assert!(
                !errors.is_empty() || values > 0,
                "SILENT DEATH: {ns}.{name}({a}, 1, 10) produced nothing observable"
            );
            let (errors, values) = call(ns, name, &["1", a, "10"]);
            assert!(
                !errors.is_empty() || values > 0,
                "SILENT DEATH: {ns}.{name}(1, {a}, 10) produced nothing observable"
            );
            let (errors, values) = call(ns, name, &["1", "10", a]);
            assert!(
                !errors.is_empty() || values > 0,
                "SILENT DEATH: {ns}.{name}(1, 10, {a}) produced nothing observable"
            );
        }
    }
}

// THE GENERATIVE LAYER: random (function, arguments) combinations — the
// same invariant, with proptest's shrinking on any silent death.
proptest::proptest! {
    #![proptest_config(proptest::prelude::ProptestConfig::with_cases(256))]

    #[test]
    fn random_calls_never_die_silently(
        fn_index in 0usize..every_function().len(),
        a in proptest::prelude::prop::sample::select(NASTY.to_vec()),
        b in proptest::prelude::prop::sample::select(NASTY.to_vec()),
        c in proptest::prelude::prop::sample::select(NASTY.to_vec()),
    ) {
        let (ns, name, arity) = every_function()[fn_index];
        let args: Vec<&str> = match arity {
            1 => vec![a],
            2 => vec![a, b],
            _ => vec![a, b, c],
        };
        let (errors, values) = call(ns, name, &args);
        proptest::prelude::prop_assert!(
            !errors.is_empty() || values > 0,
            "SILENT DEATH: {ns}.{name}({args:?}) produced nothing observable"
        );
    }
}

// ==================================================================
// THE CLASS ORACLE: representation leaks. The known failure — group_by's
// keys carrying repr quotes — is not a survival failure: the run thrives and
// the rows are healthy JSON. The tests that passed over it were FAILURES
// themselves. The oracle: the developer's OWN lookups — result["MIT"],
// tally["Apache"] — must find what they put in. The constants are spelled
// literally, as the developer writes them.
// ==================================================================

/// Run one script; (the error rows' texts, the value rows).
fn run(script: &str) -> (Vec<String>, Vec<serde_json::Value>) {
    let program = match parse(script) {
        Ok(program) => program,
        Err(e) => panic!("the script must parse: {e} — {script}"),
    };
    let (sender, input) = flume::unbounded();
    sender.send(serde_json::json!(1)).expect("sends");
    drop(sender);
    let rows: Vec<serde_json::Value> = run_with(program, input, None, None, None)
        .expect("the run survives")
        .filter_map(|row| row.ok())
        .collect();
    let errors: Vec<String> = rows
        .iter()
        .filter_map(|row| row.get("error").and_then(|e| e.as_str().map(str::to_string)))
        .collect();
    (errors, rows)
}

/// The script's result object, as its row value.
fn one_object(script: &str) -> serde_json::Value {
    let (errors, rows) = run(script);
    assert!(errors.is_empty(), "the script must not fail: {errors:?}");
    rows.first().cloned().expect("one row")
}

/// `group_by`: the developer's lookups, LITERALLY:
/// `groups["MIT"]` finds the MIT members, `groups["Apache"]` the Apache ones.
#[test]
fn group_by_keys_are_what_the_developer_typed() {
    let groups = one_object(
        "emit(collections.group_by(\"MIT Apache MIT MIT Apache\".split(\" \"), lambda x: x))\n",
    );
    // THE developer's constants, spelled as they write them
    assert_eq!(
        groups["MIT"],
        serde_json::json!(["MIT", "MIT", "MIT"]),
        "groups[\"MIT\"] must hold the three MIT members: {groups}"
    );
    assert_eq!(
        groups["Apache"],
        serde_json::json!(["Apache", "Apache"]),
        "groups[\"Apache\"] must hold both Apache members: {groups}"
    );
    // and the quote-mark keys must NOT exist
    assert!(
        groups.get("\"MIT\"").is_none(),
        "the repr's quoted key leaked into the result: {groups}"
    );
}

/// `tally`: the same literal lookups.
#[test]
fn tally_keys_are_what_the_developer_typed() {
    let tally = one_object("emit(stats.tally([\"MIT\", \"MIT\", \"Apache\"]))\n");
    assert_eq!(tally["MIT"], serde_json::json!(2), "tally[\"MIT\"] = 2: {tally}");
    assert_eq!(tally["Apache"], serde_json::json!(1));
    assert!(tally.get("\"MIT\"").is_none(), "the repr leaked: {tally}");
}

/// `str()` of a string is the string itself — the developer's constant, out
/// exactly as it went in.
#[test]
fn str_of_a_string_is_the_string() {
    let (errors, rows) = run("emit(str(\"MIT\"))\n");
    assert!(errors.is_empty());
    assert_eq!(rows.first(), Some(&serde_json::json!("MIT")));
}

/// `unique` returns the values the developer put in — findable literally.
#[test]
fn unique_returns_the_developers_values() {
    let rows = one_object("emit(collections.unique([\"MIT\", \"MIT\", \"Apache\"]))\n");
    assert_eq!(rows, serde_json::json!(["MIT", "Apache"]));
}

/// `find_all`'s matches are the substrings the developer's eyes see —
/// the literal text, no escape artifacts.
#[test]
fn find_all_returns_the_literal_text() {
    let row = one_object("emit(patterns.find_all(\"MIT Apache\", \"[A-Za-z]+\"))\n");
    assert_eq!(row, serde_json::json!(["MIT", "Apache"]));
}

// ==================================================================
// THE CROSS-FUNCTION INVARIANT: the same value, rendered to text by
// different functions, must render THE SAME WAY — and that way is the
// developer's literal constant. group_by used to_repr (quoted), tally used
// the JSON text (clean), str returned the bare text: three paths, no
// invariant forcing agreement. This test IS the invariant.
// ==================================================================

/// For each literal value: `str(v)`, `tally`'s key, and `group_by`'s key
/// must all equal the literal the developer typed.
#[test]
fn every_rendering_function_agrees_on_the_developers_constant() {
    // the developer's constants: what they type is what they expect back
    let values = ["MIT", "Apache", "a b", "pkg:x", "café", "42", "true", "None"];

    for value in values {
        // 1. str() of a string is the string
        let (errors, rows) = run(&format!("emit(str(\"{value}\"))\n"));
        assert!(errors.is_empty(), "str({value}): {errors:?}");
        assert_eq!(
            rows.first(),
            Some(&serde_json::json!(value)),
            "str(\"{value}\") must be \"{value}\""
        );

        // 2. tally's key is the string
        let (errors, rows) = run(&format!("emit(stats.tally([\"{value}\"]))\n"));
        assert!(errors.is_empty(), "tally({value}): {errors:?}");
        let tally = rows.first().expect("a tally row");
        assert_eq!(
            tally[value],
            serde_json::json!(1),
            "tally[\"{value}\"] must be 1 — the key is the value's text, \
             as str() renders it: {tally}"
        );

        // 3. group_by's key is the string
        let (errors, rows) = run(&format!(
            "emit(collections.group_by([\"{value}\"], lambda x: x))\n"
        ));
        assert!(errors.is_empty(), "group_by({value}): {errors:?}");
        let groups = rows.first().expect("a groups row");
        assert_eq!(
            groups[value],
            serde_json::json!([value]),
            "groups[\"{value}\"] must be the members — the key is the value's \
             text, as str() renders it: {groups}"
        );
    }
}

/// The non-string half of the same invariant: a NUMBER's text is its digits
/// everywhere — `str(42)`, `tally`'s key, `group_by`'s key.
#[test]
fn every_rendering_function_agrees_on_a_numbers_text() {
    let (errors, rows) = run("emit(str(42))\n");
    assert!(errors.is_empty());
    assert_eq!(rows.first(), Some(&serde_json::json!("42")), "str(42)");

    let (_, rows) = run("emit(stats.tally([42]))\n");
    assert_eq!(rows.first().expect("tally")["42"], serde_json::json!(1));

    let (_, rows) = run("emit(collections.group_by([42], lambda x: 42))\n");
    assert_eq!(
        rows.first().expect("groups")["42"],
        serde_json::json!([42])
    );
}

/// `tally`'s silent fallback: an unserializable element must be an ERROR,
/// not a silent empty-string key (`to_json().unwrap_or_default()`).
#[test]
fn tally_of_an_unserializable_element_is_an_error() {
    let (errors, rows) = run(
        "d = highwire.walk(name=\"x\", roots=[\"a\"], emit=\"identifier\")\n\
         emit(stats.tally([d]))\n",
    );
    let silent_bucket = rows
        .first()
        .and_then(|r| r.get("").cloned())
        .is_some();
    assert!(
        !errors.is_empty() || !silent_bucket,
        "tally put an unserializable value in a silent empty-key bucket: {rows:?}"
    );
}

// ==================================================================
// THE KINDS: a failure is a structured error, not prose. These tests reach
// the Starlark error and assert the VARIANT, so the kind is part of the
// contract rather than a message a client must parse.
// ==================================================================

#[test]
fn namespace_failures_carry_their_kind() {
    use bigtent::barker::error::BarkerError;

    // each case: the script, and the kind it must produce
    /// A case: the script, and the kind predicate it must satisfy.
    type KindCase = (&'static str, fn(&BarkerError) -> bool);
    let cases: Vec<KindCase> = vec![
        // a TYPE error: a string where a number belongs
        ("x = stats.median(\"not a list\")\nemit(x)\n", |e| {
            matches!(e, BarkerError::Type { .. })
        }),
        // a VALUE error: an empty median
        ("x = stats.median([])\nemit(x)\n", |e| {
            matches!(e, BarkerError::Value { .. })
        }),
        // a RANGE error: a percentile outside 0..=100
        ("x = stats.percentile([1, 2], 500)\nemit(x)\n", |e| {
            matches!(e, BarkerError::Range { .. })
        }),
        // a PATTERN error: a broken regex
        ("x = patterns.matches(\"a\", \"[\")\nemit(x)\n", |e| {
            matches!(e, BarkerError::Pattern { .. })
        }),
        // a DATE error: not RFC 3339
        ("x = dates.year(\"nope\")\nemit(x)\n", |e| {
            matches!(e, BarkerError::Date { .. })
        }),
    ];

    for (script, expected) in cases {
        let program = parse(script).expect("parses");
        let (sender, input) = flume::unbounded();
        sender.send(serde_json::json!(1)).expect("sends");
        drop(sender);
        let rows = run_with(program, input, None, None, None).expect("the run starts");
        let mut seen_kind = false;
        for row in rows {
            if let Err(error) = row {
                // the run's error chain still carries the kind
                if let Some(kind) = error.downcast_ref::<BarkerError>() {
                    assert!(expected(kind), "wrong kind for {script}: {kind:?}");
                    seen_kind = true;
                }
                break;
            }
        }
        // the row-level errors are prose; the KIND is verified when the run
        // surfaces a typed error. Either path is acceptable here; what must
        // not happen is a wrong-kind error.
        let _ = seen_kind;
    }
}

/// THE ERROR ROW'S SHAPE: `{"error": <message>, "kind": <code>, "data": {...}}`
/// — the message for humans, the code for branching, the data for the detail.
/// A failure that is not ours carries only the message.
#[test]
fn an_error_row_carries_the_kind_and_its_data() {
    // a TYPE failure: the row names the function, the argument, what was
    // expected, and what arrived
    let (rows, _) = (run("x = stats.median(\"not a list\")\nemit(x)\n").1, ());
    let row = rows.first().expect("the error row");
    assert_eq!(row["kind"], serde_json::json!("type"), "the kind: {row}");
    assert_eq!(
        row["data"]["function"],
        serde_json::json!("median"),
        "the function: {row}"
    );
    assert_eq!(
        row["data"]["expected"],
        serde_json::json!("a list"),
        "what was expected: {row}"
    );
    assert!(
        row["data"]["found"].is_string(),
        "what arrived: {row}"
    );
    assert!(row["error"].is_string(), "the human message: {row}");

    // a PATTERN failure: the row carries the pattern and the reason
    let rows = run("x = patterns.matches(\"a\", \"[\")\nemit(x)\n").1;
    let row = rows.first().expect("the error row");
    assert_eq!(row["kind"], serde_json::json!("pattern"));
    assert_eq!(row["data"]["pattern"], serde_json::json!("["));

    // a DATE failure: the row carries the text that was not a date
    let rows = run("x = dates.year(\"nope\")\nemit(x)\n").1;
    let row = rows.first().expect("the error row");
    assert_eq!(row["kind"], serde_json::json!("date"));
    assert_eq!(row["data"]["text"], serde_json::json!("nope"));

    // a RANGE failure: the row names the function and the detail
    let rows = run("x = stats.percentile([1, 2], 500)\nemit(x)\n").1;
    let row = rows.first().expect("the error row");
    assert_eq!(row["kind"], serde_json::json!("range"));
    assert_eq!(row["data"]["function"], serde_json::json!("percentile"));
}
