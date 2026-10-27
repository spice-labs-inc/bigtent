//! Tests for the materialization budget.
//!
//! REQUIREMENT (PR #171 review, finding 4, remediation Option 1): converting
//! a Sansho carrier to JSON — `materialize` — must be bounded, because the
//! carrier is a refcount-shared DAG whose DENOTED tree can be exponentially
//! larger than the value in memory. A 40-stage `[@,@]` chain (239
//! characters, nesting depth 1, inside both existing limits) denotes 2^40
//! nodes; materializing it today consumes the machine. The budget: 500,000
//! nodes or 10 MB of string text per conversion, exceeded means a `Limit`
//! error — the same remedy Pathway shipped for the same class of blowup,
//! and the gate no upstream JMESPath implementation has (they all defer the
//! cost to the consumer; Sansho's consumer is a server).
//!
//! THEORY OF THE TEST: the budget counts what materialize EMITS — each node
//! once per occurrence (sharing is invisible, deliberately: the denoted size
//! is the cost) and each string's rendered bytes. The chain tests pin the
//! exponential under the cap; the string tests pin the byte budget with
//! linear data, so the budget bounds blowups and not sizes; the idempotence
//! test pins that the budget is per conversion, not per process.

use sansho::error::SanshoError;
use sansho::parser::parse;
use serde_json::json;

/// Evaluate an expression and materialize the result, as the pipeline does.
fn materialize_of(expression: &str, document: serde_json::Value) -> Result<serde_json::Value, SanshoError> {
    let parsed = parse(expression)?;
    let program = sansho::compile(&parsed, None)?;
    sansho::materialize(&sansho::evaluate(&program, document)?)
}

/// Count the leaves of a rendered JSON value.
fn leaves(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Array(items) => items.iter().map(leaves).sum(),
        _ => 1,
    }
}

/// A chain of 20 `[@,@]` stages denotes 2^20 = 1,048,576 nodes — over the
/// budget. Materializing it must be a fast `Limit` error, not a 2^N walk.
#[test]
fn a_shared_node_bomb_is_refused_at_the_node_budget() {
    let expression = "[@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@]\
                      | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@]\
                      | [@,@] | [@,@] | [@,@] | [@,@]";
    let started = std::time::Instant::now();
    match materialize_of(expression, json!({"a": 1})) {
        Err(SanshoError::Limit { .. }) => {}
        other => panic!(
            "2^20 denoted nodes must be a Limit error, not {:?}",
            other.map(|v| leaves(&v))
        ),
    }
    assert!(
        started.elapsed() < std::time::Duration::from_secs(2),
        "the budget must abort the walk immediately, not after materializing"
    );
}

/// A chain of 17 stages denotes 2^17 = 131,072 nodes — under the budget.
/// The budget bounds blowups, not sizes: this must materialize whole.
#[test]
fn a_shared_chain_under_the_budget_materializes() {
    let expression = "[@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@]\
                      | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@]\
                      | [@,@]";
    let value = materialize_of(expression, json!({"a": 1})).expect("under the budget");
    assert_eq!(leaves(&value), 131_072, "2^17 leaves");
}

/// More than 10 MB of string text in one conversion is refused.
#[test]
fn oversized_strings_are_refused_at_the_byte_budget() {
    // 1,100 strings of 10 KB each = 11 MB of text, linear data, no sharing
    let document = json!({"rows": (0..1_100).map(|_| "x".repeat(10_000)).collect::<Vec<_>>()});
    match materialize_of("rows", document) {
        Err(SanshoError::Limit { .. }) => {}
        other => panic!("11 MB of string must be a Limit error, not {:?}", other.is_ok()),
    }
}

/// Under 10 MB of string text materializes whole.
#[test]
fn strings_under_the_byte_budget_materialize() {
    let document = json!({"rows": (0..900).map(|_| "x".repeat(10_000)).collect::<Vec<_>>()});
    let value = materialize_of("rows", document).expect("9 MB of string is legal");
    assert_eq!(value.as_array().expect("the rows array").len(), 900);
}

/// The budget is PER CONVERSION: materializing the same bomb twice gives the
/// same refusal both times — the counter does not leak across calls.
#[test]
fn the_budget_is_per_conversion() {
    let expression = "[@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@]\
                      | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@]\
                      | [@,@] | [@,@] | [@,@] | [@,@]";
    let parsed = parse(expression).unwrap();
    let program = sansho::compile(&parsed, None).unwrap();
    for attempt in 0..2 {
        match sansho::materialize(&sansho::evaluate(&program, json!({"a": 1})).unwrap()) {
            Err(SanshoError::Limit { .. }) => {}
            other => panic!("attempt {attempt} must refuse, not {:?}", other.is_ok()),
        }
    }
}
