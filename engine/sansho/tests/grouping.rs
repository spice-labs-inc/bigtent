//! Tests for grouping semantics: parenthesised expressions must keep their
//! identity both in evaluation and in cache keys.
//!
//! REQUIREMENT (PR #171 review, findings 2 and 3):
//! - (2) A parenthesised expression is a VALUE the postfixes project on, not
//!   sugar for splicing. `(foo[*].bar)[0]` is JMESPath for "index the
//!   RESULT of foo[*].bar" and must be `"x"`; `(foo[*]).baz` projects a
//!   field off an array value and must be `null`. The parser spliced the
//!   postfixes into the grouped chain instead, so `(foo[*].bar)[0]`
//!   behaved as `foo[*].bar[0]` (`[]`) and `(foo[*]).baz` as `foo[*].baz`
//!   (`[1,2]`).
//! - (3) The cache's canonical form must distinguish expressions that
//!   PARSE differently. `!(a == b)` and `!a == b` are different trees
//!   (`Not(Compare)` vs `Compare(Not, …)`) with different meanings, as are
//!   `a && (b || c)` and `a && b || c`; a canonical form that renders both
//!   identically hands the second compile the first one's program.
//!
//! THEORY OF THE TEST: grouping has a SEMANTIC effect only where precedence
//! would otherwise bind differently — so each test pins one precedence
//! boundary through the public surface (parse → compile → evaluate), plus
//! the cache-scope test that proves distinct trees produce distinct
//! programs through `ProgramCache`.

use sansho::cache::ProgramCache;
use sansho::error::SanshoError;
use sansho::parser::parse;
use serde_json::json;

/// Evaluate an expression against a document, as JSON out.
fn run(expression: &str, document: serde_json::Value) -> Result<serde_json::Value, SanshoError> {
    let parsed = parse(expression)?;
    let program = sansho::compile(&parsed, None)?;
    sansho::materialize(&sansho::evaluate(&program, document)?)
}

/// The review's first case: the index applies to the grouped projection's
/// result, not inside it. JMESPath: `"x"`.
#[test]
fn an_index_after_a_group_indexes_the_group_result() {
    let document = json!({"foo": [{"bar": "x"}, {"bar": "y"}]});
    let value = run("(foo[*].bar)[0]", document).expect("evaluates");
    assert_eq!(value, json!("x"), "(foo[*].bar)[0] is the first element of the result");
}

/// The review's second case: a field after a group reads off the array VALUE
/// — a field projection on an array is null in JMESPath.
#[test]
fn a_field_after_a_group_reads_off_the_array_value() {
    let document = json!({"foo": [{"baz": 1}, {"baz": 2}]});
    let value = run("(foo[*]).baz", document).expect("evaluates");
    assert_eq!(value, serde_json::Value::Null, "a field on an array value is null");
}

/// Finding 3, semantic half: the two trees differ in meaning, and a cache
/// that keys them together must return the RIGHT program for each. With
/// a=false, b=true, c=true: `a && (b || c)` is false, `a && b || c` is true.
#[test]
fn the_cache_distinguishes_and_or_grouping() {
    let cache = ProgramCache::new(16);
    let document = json!({"a": false, "b": true, "c": true});

    let grouped = cache
        .compile_cached("a && (b || c)")
        .expect("the grouped form compiles");
    let flat = cache
        .compile_cached("a && b || c")
        .expect("the flat form compiles");

    let grouped_value = sansho::materialize(&sansho::evaluate(&grouped, document.clone()).unwrap())
        .expect("the grouped program evaluates");
    let flat_value = sansho::materialize(&sansho::evaluate(&flat, document).unwrap())
        .expect("the flat program evaluates");

    assert_eq!(grouped_value, json!(false), "a && (b || c) with a=false is false");
    assert_eq!(flat_value, json!(true), "a && b || c with a=false is true");
}

/// Finding 3, negation half: `!(a == 'x')` negates the comparison;
/// `!a == 'x'` (this engine's precedence: `!` binds tighter than `==`)
/// compares the negated field. Distinct trees, distinct keys — and the
/// document that DISTINGUISHES them is a null, where negating null yields
/// true but comparing true against a string yields false.
#[test]
fn the_cache_distinguishes_not_comparison_grouping() {
    let cache = ProgramCache::new(16);
    let document = json!({"a": serde_json::Value::Null});

    let grouped = cache
        .compile_cached("!(a == 'x')")
        .expect("the grouped negation compiles");
    let flat = cache
        .compile_cached("!a == 'x'")
        .expect("the flat negation compiles");

    let grouped_value =
        sansho::materialize(&sansho::evaluate(&grouped, document.clone()).unwrap())
            .expect("the grouped program evaluates");
    let flat_value = sansho::materialize(&sansho::evaluate(&flat, document).unwrap())
        .expect("the flat program evaluates");

    assert_eq!(grouped_value, json!(true), "!(null == 'x') is true");
    assert_eq!(flat_value, json!(false), "!null == 'x' is (true == 'x'), false");
}
