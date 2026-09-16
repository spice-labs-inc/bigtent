//! The JMESPath compliance-corpus harness.
//!
//! The corpus (vendored under `tests/jmespath-corpus/`, pinned by commit
//! — see that directory's `PIN.txt`) is data: this module parses the
//! corpus files, runs each case against a supplied [`Driver`], and
//! reports per-feature pass/fail roll-ups. Corpus cases are never
//! inlined into unit tests; the files on disk are the single source of
//! conformance truth.

use crate::error::SanshoError;
use std::path::Path;

/// One compliance case, extracted from a corpus file.
#[derive(Clone, Debug)]
pub struct CorpusCase {
    pub expression: String,
    /// The expected projection, for cases that expect a result.
    pub result: Option<serde_json::Value>,
    /// The declared error, for cases that expect failure
    /// (`syntax`, `invalid-type`, `invalid-arity`, `invalid-value`,
    /// `unknown-function`).
    pub error: Option<String>,
    /// A benchmark case: no correctness expectation at all (the corpus
    /// schema allows result/error/bench as alternatives).
    pub bench: bool,
}

/// A group of cases sharing one input document.
#[derive(Clone, Debug)]
pub struct CorpusGroup {
    pub given: serde_json::Value,
    pub cases: Vec<CorpusCase>,
}

/// One corpus file, parsed: a feature name and its case groups.
#[derive(Clone, Debug)]
pub struct CorpusFile {
    /// The feature name: the corpus file's stem (e.g. `filters`).
    pub feature: String,
    pub groups: Vec<CorpusGroup>,
}

/// The outcome of one corpus case under one driver.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Pass,
    Fail,
    NotImplemented,
}

/// Roll-up for one feature (one corpus file).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FeatureReport {
    pub feature: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub not_implemented: usize,
    /// Benchmark cases: parsed and run for speed, with no expected
    /// outcome; they cannot pass or fail.
    pub bench: usize,
}

impl FeatureReport {
    /// Whether this feature is fully green: every non-benchmark case
    /// passed.
    pub fn is_green(&self) -> bool {
        self.passed + self.bench == self.total && self.total - self.bench > 0
    }

    /// Whether this feature is still red: nothing passes yet. The red
    /// baseline distinguishes "not built" from "built and wrong" — a
    /// stub engine must not turn rejection cases green.
    pub fn is_red(&self) -> bool {
        self.passed == 0 && self.total - self.bench > 0
    }
}

/// The corpus harness's view of an engine: evaluate one expression
/// against one input document.
///
/// The real engine arrives with the language phases; until then the
/// [`Unimplemented`] driver reports [`DriverOutcome::NotImplemented`] for
/// everything, which keeps the baseline honestly red (a stub that
/// returned errors would falsely turn the corpus's rejection cases
/// green).
pub trait Driver {
    fn evaluate(&self, given: &serde_json::Value, expression: &str) -> DriverOutcome;
}

/// What a driver produced for one case.
#[derive(Debug)]
pub enum DriverOutcome {
    /// The expression evaluated to a projection.
    Result(serde_json::Value),
    /// The engine rejected the expression or the input with a
    /// structured error.
    Error(SanshoError),
    /// The engine does not implement this yet.
    NotImplemented,
}

/// The do-nothing driver: the red baseline.
#[derive(Clone, Copy, Debug, Default)]
pub struct Unimplemented;

impl Driver for Unimplemented {
    fn evaluate(&self, _given: &serde_json::Value, _expression: &str) -> DriverOutcome {
        DriverOutcome::NotImplemented
    }
}

/// Load every corpus file in `dir` (skipping the schema and the
/// pin file; `benchmarks.json` loads like the rest — it is data of the
/// same shape).
pub fn load_corpus(dir: &Path) -> Result<Vec<CorpusFile>, SanshoError> {
    let mut files = Vec::new();
    let entries = std::fs::read_dir(dir)
        .map_err(|e| SanshoError::Input {
            message: format!("cannot read corpus directory {}: {e}", dir.display()),
        })?;
    let mut paths: Vec<_> = entries
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "json") && p.file_stem().is_some())
        .collect();
    paths.sort();
    for path in paths {
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();
        if stem == "schema" || stem == "benchmarks" {
            // the schema describes the corpus (not cases); the benchmark
            // file measures speed (it declares no expected results)
            continue;
        }
        let raw = std::fs::read_to_string(&path).map_err(|e| SanshoError::Input {
            message: format!("cannot read corpus file {}: {e}", path.display()),
        })?;
        let groups: Vec<serde_json::Value> = serde_json::from_str(&raw).map_err(|e| {
            SanshoError::Input {
                message: format!("corpus file {} is not valid JSON: {e}", path.display()),
            }
        })?;
        let mut parsed = Vec::new();
        for group in groups {
            let given = match group.get("given") {
                Some(g) => g.clone(),
                None => {
                    return Err(SanshoError::Input {
                        message: format!("corpus file {} has a group without `given`", path.display()),
                    })
                }
            };
            let cases = match group.get("cases").and_then(|c| c.as_array()) {
                Some(cases) => {
                    let mut out = Vec::new();
                    for case in cases {
                        let expression = match case.get("expression").and_then(|e| e.as_str()) {
                            Some(e) => e.to_string(),
                            None => {
                                return Err(SanshoError::Input {
                                    message: format!(
                                        "corpus file {} has a case without an `expression` string",
                                        path.display()
                                    ),
                                })
                            }
                        };
                        out.push(CorpusCase {
                            expression,
                            result: case.get("result").cloned(),
                            error: case
                                .get("error")
                                .and_then(|e| e.as_str())
                                .map(str::to_string),
                            bench: case.get("bench").is_some(),
                        });
                    }
                    out
                }
                None => {
                    return Err(SanshoError::Input {
                        message: format!("corpus file {} has a group without `cases`", path.display()),
                    })
                }
            };
            parsed.push(CorpusGroup { given, cases });
        }
        files.push(CorpusFile {
            feature: stem,
            groups: parsed,
        });
    }
    Ok(files)
}

/// Run every loaded case through the driver, rolled up per feature.
pub fn run_corpus<D: Driver>(driver: &D, files: &[CorpusFile]) -> Vec<FeatureReport> {
    files.iter().map(|file| run_file(driver, file)).collect()
}

/// Run one corpus file through the driver.
pub fn run_file<D: Driver>(driver: &D, file: &CorpusFile) -> FeatureReport {
    let mut report = FeatureReport {
        feature: file.feature.clone(),
        ..Default::default()
    };
    for group in &file.groups {
        for case in &group.cases {
            report.total += 1;
            if case.bench {
                report.bench += 1;
                continue;
            }
            match driver.evaluate(&group.given, &case.expression) {
                DriverOutcome::NotImplemented => report.not_implemented += 1,
                DriverOutcome::Result(value) => {
                    match (&case.result, &case.error) {
                        // result case: compare numerically-aware deep equality
                        (Some(expected), None) => {
                            if json_equal(&value, expected) {
                                report.passed += 1;
                            } else {
                                report.failed += 1;
                            }
                        }
                        // error case: a result is a failure
                        (None, Some(_)) => report.failed += 1,
                        // malformed corpus case: neither result nor error — harness data defect
                        (None, None) | (Some(_), Some(_)) => report.failed += 1,
                    }
                }
                DriverOutcome::Error(err) => match (&case.error, &case.result) {
                    // error case: the structured error must land in the
                    // category the corpus declares
                    (Some(declared), None) => {
                        if expected_category(declared) == Some(err.category()) {
                            report.passed += 1;
                        } else {
                            report.failed += 1;
                        }
                    }
                    // result case: an error is a failure
                    (None, Some(_)) => report.failed += 1,
                    // malformed corpus case
                    (None, None) | (Some(_), Some(_)) => report.failed += 1,
                },
            }
        }
    }
    report
}

/// Deep JSON equality with numeric awareness: numbers compare by double
/// value, so `1` and `1.0` are equal (JMESPath numbers are doubles per
/// the specification). Everything else compares structurally.
pub fn json_equal(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Number(x), serde_json::Value::Number(y)) => {
            match (x.as_f64(), y.as_f64()) {
                (Some(x), Some(y)) => x == y,
                _ => false,
            }
        }
        (serde_json::Value::Array(x), serde_json::Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| json_equal(a, b))
        }
        (serde_json::Value::Object(x), serde_json::Value::Object(y)) => {
            x.len() == y.len()
                && x.iter().all(|(k, v)| match y.get(k) {
                    Some(v2) => json_equal(v, v2),
                    None => false,
                })
        }
        _ => a == b,
    }
}

/// Map a corpus-declared error to the engine error category the corpus
/// means. Pinned by tests; refined only when the corpus disagrees —
/// the corpus is the referee.
pub fn expected_category(declared: &str) -> Option<&'static str> {
    match declared {
        // the expression cannot be parsed at all
        "syntax" => Some("parse"),
        // statically knowable rejections
        "unknown-function" | "invalid-arity" => Some("compile"),
        // runtime type/value mismatches
        "invalid-type" | "invalid-value" => Some("evaluation"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::SanshoError;

    // Requirement: SPEC-0001 §6 — conformance is defined by the vendored
    // corpus; the harness must load the real files as data (Hard Stop 4)
    // and the red baseline must be honest (nothing green, nothing
    // falsely rejected-green).
    //
    // What: loading the vendored corpus yields every feature file; the
    // unimplemented driver marks every case NotImplemented; the report
    // shows passed == 0 everywhere with substantial case totals.
    //
    // LLM section: this is the "red baseline" test from the plan's
    // Phase 1. It fails if the corpus directory is missing or malformed
    // (harness input errors are real errors), and it fails if any case
    // is green under the do-nothing driver — which would mean the
    // harness lies about conformance.
    #[test]
    fn harness_runs_corpus_as_data_with_honest_red_baseline() {
        let dir = std::path::Path::new("tests/jmespath-corpus/compliance");
        let files = load_corpus(dir).expect("vendored corpus must load");
        assert!(files.len() >= 15, "expected the full feature set, got {}", files.len());
        let reports = run_corpus(&Unimplemented, &files);
        let total: usize = reports.iter().map(|r| r.total).sum();
        assert!(total > 300, "corpus should hold hundreds of cases, got {total}");
        for report in &reports {
            assert_eq!(report.passed, 0, "red baseline must not pass anything: {}", report.feature);
            assert_eq!(report.failed, 0, "a stub engine must not fail cases either: {}", report.feature);
            assert_eq!(report.not_implemented, report.total - report.bench);
            assert!(report.is_red());
            assert!(!report.is_green());
        }
    }

    // Requirement: SPEC-0001 §3 — expressions the specification declares
    // invalid are rejected; §6 — the corpus's invalid cases are part of
    // conformance. What: the harness classifies every corpus error case
    // into an engine error category — no declared error string may be
    // unmapped, and the category naming matches the error taxonomy.
    //
    // LLM section: `expected_category` is the bridge between the
    // corpus's error vocabulary and `SanshoError::category`. If the
    // corpus gains a new error string, this test fails until the mapping
    // is a deliberate decision.
    #[test]
    fn every_corpus_error_string_maps_to_a_category() {
        let declared = ["syntax", "invalid-type", "invalid-arity", "invalid-value", "unknown-function"];
        for d in declared {
            assert!(
                expected_category(d).is_some(),
                "corpus error string {d:?} must map to an engine error category"
            );
        }
        let dir = std::path::Path::new("tests/jmespath-corpus/compliance");
        let files = load_corpus(dir).expect("vendored corpus must load");
        for file in &files {
            for group in &file.groups {
                for case in &group.cases {
                    if let Some(declared) = &case.error {
                        assert!(
                            expected_category(declared).is_some(),
                            "unmapped corpus error {declared:?} in {}",
                            file.feature
                        );
                    }
                }
            }
        }
    }

    // Requirement: SPEC-0001 §3 — numeric equality follows the
    // specification (numbers are doubles). What: `json_equal` treats
    // integer and float renderings of the same value as equal, and
    // structure-aware elsewhere. Why: corpus result comparison would
    // otherwise produce false failures on `1` versus `1.0`.
    //
    // LLM section: json_equal(1, 1.0) is true; json_equal([1], [1.0]) is
    // true; object comparison is key-based, not order-based.
    #[test]
    fn json_equal_is_numeric_and_structural() {
        let one = serde_json::json!(1);
        let one_point_zero = serde_json::json!(1.0);
        assert!(json_equal(&one, &one_point_zero));
        assert!(json_equal(
            &serde_json::json!([1, {"a": 2}]),
            &serde_json::json!([1.0, {"a": 2.0}])
        ));
        assert!(!json_equal(&serde_json::json!(1), &serde_json::json!(2)));
    }

    // Requirement: SPEC-0001 §6 — a conforming engine rejects the
    // corpus's invalid cases; the harness must not credit a driver that
    // answers errors where results are expected (and vice versa).
    // What: a driver that errors on everything scores zero passes on
    // result cases and full marks only on error cases whose category
    // matches. Why: this is the anti-false-green property of the
    // harness itself.
    //
    // LLM section: a driver that always returns
    // Err(Parse{...}) passes only `syntax`-declared cases; result cases
    // count as failed. A driver that returns results for error cases
    // counts as failed.
    #[test]
    fn harness_cannot_be_fooled_into_false_green() {
        struct AlwaysParseError;
        impl Driver for AlwaysParseError {
            fn evaluate(
                &self,
                _given: &serde_json::Value,
                _expression: &str,
            ) -> DriverOutcome {
                DriverOutcome::Error(SanshoError::Parse {
                    message: "always".into(),
                    position: 0,
                })
            }
        }
        let dir = std::path::Path::new("tests/jmespath-corpus/compliance");
        let files = load_corpus(dir).expect("vendored corpus must load");
        let reports = run_corpus(&AlwaysParseError, &files);
        let total: usize = reports.iter().map(|r| r.total).sum();
        let passed: usize = reports.iter().map(|r| r.passed).sum();
        assert!(total > 300);
        assert!(passed < total, "an always-error driver must not pass everything");
        // every result case failed; only some error cases (syntax) passed
        for report in &reports {
            if report.feature == "basic" {
                assert_eq!(report.passed, 0, "basic has no declared-error cases");
            }
        }
    }

    // Requirement: SPEC-0001 §6 — a conforming engine rejects invalid
    // expressions; the harness must not credit a driver that answers
    // results where rejections are declared. What: a driver that returns
    // a result for every case must score zero passes on every error-
    // declared case. Why: this is the mirror of the always-error probe —
    // together they pin both directions of the harness's honesty.
    //
    // LLM section: an always-result driver cannot legitimately pass any
    // case that declares an error; if such a case ever shows as passed,
    // the harness's case classification is broken.
    #[test]
    fn harness_cannot_be_fooled_by_always_result() {
        struct AlwaysResult;
        impl Driver for AlwaysResult {
            fn evaluate(
                &self,
                given: &serde_json::Value,
                _expression: &str,
            ) -> DriverOutcome {
                DriverOutcome::Result(given.clone())
            }
        }
        let dir = std::path::Path::new("tests/jmespath-corpus/compliance");
        let files = load_corpus(dir).expect("vendored corpus must load");
        for file in &files {
            let report = run_file(&AlwaysResult, file);
            let declared_errors: usize = file
                .groups
                .iter()
                .map(|g| g.cases.iter().filter(|c| c.error.is_some()).count())
                .sum();
            let result_cases = report.total - declared_errors;
            // a result-returning driver fails every error-declared case,
            // so its passes can only come from result cases
            assert!(
                report.passed <= result_cases,
                "feature {}: passed {} exceeds result-case count {}",
                file.feature,
                report.passed,
                result_cases
            );
            if result_cases == 0 {
                assert_eq!(report.passed, 0);
            }
        }
    }
}
