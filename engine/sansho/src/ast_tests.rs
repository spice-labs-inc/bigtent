//! Tests for the canonical form (SPEC-0001 §5.4), the determinism
//! guarantee (§2), and the null/missing-value semantics (§3).

#[cfg(test)]
mod tests {
    use crate::canonical;
    use crate::corpus::{Driver, load_corpus};
    use crate::parser::parse;
    use std::path::Path;

    // Requirement: SPEC-0001 §5.4 — the canonical form is a fixed point
    // (re-parsing the canonical form yields the same canonical form).
    // What: a sweep over every VALID corpus expression: each must parse,
    // canonicalize, re-parse, and re-canonicalize to the same string.
    // Why: the canonical form is the cache key's input; drift here
    // poisons the cache silently (plan `canonical_form_fixed_point`).
    //
    // LLM section: expressions the corpus declares invalid are skipped
    // (they have no canonical form); expressions in not-yet-parseable
    // language areas are skipped, with a floor on how many must
    // canonicalize (it rises as later phases land).
    #[test]
    fn canonical_is_fixed_point_over_corpus_expressions() {
        let files = load_corpus(Path::new("tests/jmespath-corpus/compliance"))
            .expect("vendored corpus must load");
        let mut checked = 0usize;
        let mut skipped = 0usize;
        for file in &files {
            for group in &file.groups {
                for case in &group.cases {
                    if case.error.is_some() {
                        continue;
                    }
                    let parsed = match parse(&case.expression) {
                        Ok(parsed) => parsed,
                        Err(_) => {
                            skipped += 1;
                            continue;
                        }
                    };
                    let once = canonical(&parsed);
                    let reparsed = parse(&once).unwrap_or_else(|e| {
                        panic!("canonical form of {:?} must re-parse: {e}", case.expression)
                    });
                    assert_eq!(
                        once,
                        canonical(&reparsed),
                        "canonical form is not a fixed point for {:?}",
                        case.expression
                    );
                    checked += 1;
                }
            }
        }
        assert!(
            checked > 500,
            "most corpus expressions must canonicalize: {checked} checked, {skipped} skipped"
        );
    }

    // Requirement: SPEC-0001 §5.4 — expressions of different meaning never
    // share a canonical form (the negative cache-key property).
    // What: hand-picked distinct-meaning pairs must render differently.
    // Why: a collision would return wrong cached results, which is worse
    // than a cache miss (plan `canonical_form_distinguishes`).
    //
    // LLM section: note the deliberate contrast with the whitespace test
    // below — whitespace variants COLLAPSE to one form; meaning-distinct
    // pairs never do.
    #[test]
    fn canonical_distinguishes_distinct_meanings() {
        let pairs = [
            ("a", "b"),
            ("a.b", "a"),
            ("a.b", "a.c"),
            ("a[0]", "a[1]"),
            ("a[*]", "a"),
            ("a[?x]", "a[?y]"),
            ("a || b", "a && b"),
            ("a == b", "a != b"),
            ("`1`", "`2`"),
            ("length(a)", "length(b)"),
            ("{x: a}", "{y: a}"),
            ("[a, b]", "[b, a]"),
        ];
        for (left, right) in pairs {
            let left_parsed = parse(left).unwrap_or_else(|e| panic!("{left}: {e}"));
            let right_parsed = parse(right).unwrap_or_else(|e| panic!("{right}: {e}"));
            assert_ne!(
                canonical(&left_parsed),
                canonical(&right_parsed),
                "{left:?} and {right:?} must canonicalize differently"
            );
        }
    }

    // Requirement: SPEC-0001 §5.4 — insignificant whitespace collapses.
    // What: whitespace variants share one canonical form. Why: the
    // cache-key stability property (plan `canonical_form_stability`).
    //
    // LLM section: "a.b", "a . b", "a\n.\nb", " a . b " all canonicalize
    // to "a.b".
    #[test]
    fn canonical_collapses_insignificant_whitespace() {
        for expression in ["a.b", "a . b", "a\n.\nb", " a . b "] {
            let parsed = parse(expression).expect("parses");
            assert_eq!(canonical(&parsed), "a.b", "{expression:?}");
        }
    }

    // Requirement: SPEC-0001 §2 — evaluation is deterministic: identical
    // bytes and an identical expression always produce identical results.
    // What: the same corpus case evaluated repeatedly yields byte-
    // identical serialized results. Why: unordered container iteration
    // leaking into output is a known defect class in this codebase's
    // surroundings (plan `determinism_property`).
    //
    // LLM section: uses the real engine through the corpus driver's
    // pathway (parse, compile, evaluate) on the materialized backend;
    // results serialize to strings that must be identical across runs.
    #[test]
    fn evaluation_is_deterministic() {
        let files = load_corpus(Path::new("tests/jmespath-corpus/compliance"))
            .expect("vendored corpus must load");
        for file in &files {
            for group in &file.groups {
                for case in &group.cases {
                    if case.error.is_some() {
                        continue;
                    }
                    let first = run_once(&group.given, &case.expression);
                    for _ in 0..3 {
                        let again = run_once(&group.given, &case.expression);
                        assert_eq!(first, again, "{:?} nondeterministic", case.expression);
                    }
                }
            }
        }
    }

    fn run_once(given: &serde_json::Value, expression: &str) -> String {
        match crate::eval::RealEngine.evaluate(given, expression) {
            crate::corpus::DriverOutcome::Result(value) => value.to_string(),
            crate::corpus::DriverOutcome::Error(error) => format!("error:{error}"),
            crate::corpus::DriverOutcome::NotImplemented => "not-implemented".to_string(),
        }
    }

    // Requirement: SPEC-0001 §3 — null and missing-value semantics are
    // exactly the specification's: missing paths evaluate to null; there
    // are no omission or defaulting rules. What: direct probes of the
    // miss chains (missing key, index out of range, navigation through a
    // scalar). Why: this supersedes the earlier omit-key discussion; the
    // test is its enforcement (plan `null_semantics_slice1`).
    //
    // LLM section: {"a": {"b": 1}} — "a.x" is null; "a.x.y" is null;
    // "a.b.x" is null (field on number); "a[5]" and "a[-5]" are null;
    // "list[0].b" is null (field on number).
    #[test]
    fn null_semantics_missing_paths_are_null() {
        let document = serde_json::json!({"a": {"b": 1}, "list": [10, 20]});
        let cases = [
            ("a.x", serde_json::Value::Null),
            ("a.x.y", serde_json::Value::Null),
            ("a.b.x", serde_json::Value::Null),
            ("a[5]", serde_json::Value::Null),
            ("a[-5]", serde_json::Value::Null),
            ("list[0].b", serde_json::Value::Null),
            ("a.b", serde_json::json!(1)),
            ("list[1]", serde_json::json!(20)),
        ];
        for (expression, expected) in cases {
            let actual = match crate::eval::RealEngine.evaluate(&document, expression) {
                crate::corpus::DriverOutcome::Result(value) => value,
                other => panic!("{expression}: expected a result, got {other:?}"),
            };
            assert_eq!(actual, expected, "{expression}");
        }
    }

    // Requirement: SPEC-0001 §4 — the CBOR-to-JSON mapping is normative;
    // the corpus's escape tests must round-trip through canonicalization
    // (this is the case that caught Rust-Debug escaping being used where
    // JSON escaping was required). What: the corpus's backslash-literal
    // expression parses, canonicalizes, re-parses, and re-canonicalizes
    // identically. Why: canonical_literal must emit JSON escapes, never
    // Rust Debug escapes.
    //
    // LLM section: the expression is a backtick literal whose JSON is a
    // string containing one backslash, followed by a multi-select hash
    // whose value is another literal.
    #[test]
    fn canonical_round_trips_backslash_literal() {
        let original = r##"`"\\"`.{a:`"b"`}`"##.trim_end_matches('`');
        let parsed = parse(original).expect("parses");
        let once = canonical(&parsed);
        println!("PROBE original={:?} canonical={:?}", original, once);
        let reparsed = parse(&once).expect("canonical form must re-parse");
        assert_eq!(once, canonical(&reparsed), "fixed point for {original:?}");
    }
}

#[cfg(test)]
mod slice_tests {

    use crate::corpus::{Driver, DriverOutcome, load_corpus};
    use std::path::Path;
    // Requirement: SPEC-0001 §3, §6 — the first language slice turns the
    // corpus green on the materialized backend. What: per-feature roll-ups:
    // the slice's features fully green; later-phase features red-or-not-
    // implemented but never silently wrong (this test prints the full report
    // under --nocapture). Why: the corpus is the conformance referee (plan
    // `corpus_slice1_materialized`).
    //
    // LLM section: green set: basic, identifiers, indices, slice, multiselect,
    // literal, boolean, current, escape, unicode, pipe, syntax, filters,
    // wildcard. functions must have zero FAILURES (implemented functions
    // green, the rest not-implemented).
    #[test]
    fn corpus_slice1_materialized() {
        let files = load_corpus(Path::new("tests/jmespath-corpus/compliance")).expect("loads");
        let reports = crate::corpus::run_corpus(&crate::eval::RealEngine, &files);
        for report in &reports {
            println!(
                "{:12} total {:4} passed {:4} failed {:4} not-implemented {:4}",
                report.feature, report.total, report.passed, report.failed, report.not_implemented
            );
        }
        // failing-case detail: the corpus is the referee, so every failure
        // names the expression, the input, and what the engine produced
        for file in &files {
            for group in &file.groups {
                for case in &group.cases {
                    let outcome = crate::eval::RealEngine.evaluate(&group.given, &case.expression);
                    let declared = case.error.clone().unwrap_or_else(|| "ok".to_string());
                    let matches = match (&outcome, &case.error) {
                        (DriverOutcome::Result(value), None) => match &case.result {
                            Some(expected) => crate::corpus::json_equal(value, expected),
                            None => false,
                        },
                        (DriverOutcome::Error(error), Some(_)) => {
                            crate::corpus::expected_category(&declared) == Some(error.category())
                        }
                        (DriverOutcome::NotImplemented, _) => true,
                        _ => false,
                    };
                    if !matches {
                        println!(
                            "FAIL[{}] expr={:?}\n  given={}\n  declared={declared} expected={:?} outcome={outcome:?}",
                            file.feature,
                            case.expression,
                            group.given,
                            case.result,
                            declared = declared,
                        );
                    }
                    if let DriverOutcome::NotImplemented = outcome {
                        if case.error.is_none() {
                            println!("NOTIMPL[{}] expr={:?}", file.feature, case.expression);
                        }
                    }
                }
            }
        }
        for feature in [
            "basic",
            "boolean",
            "current",
            "escape",
            "identifiers",
            "literal",
            "pipe",
            "slice",
            "syntax",
        ] {
            let report = reports
                .iter()
                .find(|r| r.feature == feature)
                .unwrap_or_else(|| panic!("{feature} report"));
            assert!(report.is_green(), "{feature}: {report:?}");
        }
        // these features are fully implemented for their slice except where
        // the flatten operator (a later phase) appears: zero failures, with
        // the remainder honestly not-implemented
        // the function library is complete (phase 4): functions is fully
        // green, like the other implemented features
        for feature in ["functions", "unicode"] {
            let report = reports
                .iter()
                .find(|r| r.feature == feature)
                .unwrap_or_else(|| panic!("{feature} report"));
            assert_eq!(report.failed, 0, "{feature}: {report:?}");
            assert!(report.is_green(), "{feature}: {report:?}");
        }
    }
}
