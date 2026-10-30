//! Tests for the comparison and string-rendering budgets.
//!
//! REQUIREMENT (PR #171 review, finding 4, remediation follow-up): the
//! evaluator walks the denoted tree in three places `materialize` cannot
//! see — deep equality (`trait_equal`), and string rendering (`string_for`,
//! the engine of `to_string`). Each must be bounded:
//! - Equality is depth-limited at **16**: two shared DAGs equal in denoted
//!   content but distinct in memory (the output of two separately evaluated
//!   chains) force a full structural walk — 2^N node pairs, which at N=20 is
//!   ~1M comparisons and at N=30 is a worker stalled for minutes. The
//!   pointer fast path (same `Rc` ⇒ equal, always) short-circuits the
//!   common self-comparison for free.
//! - Rendering is node-budgeted at the SAME cap as materialization
//!   (`MAX_MATERIALIZED_NODES`, 500,000): `to_string` of a bomb chain builds
//!   one enormous string inside the evaluator — memory and CPU — and the
//!   result is a single "small" string node that would sail through every
//!   other gate.
//!
//! THEORY OF THE TEST: the depth limit pins equality's worst case at 16
//! levels (real corpus data nests a few dozen at most; a hostile chain
//! exceeds it immediately); the budget test pins rendering to the same
//! bound as materialization; the shallow test proves legal comparisons and
//! renders still succeed.

use sansho::error::SanshoError;
use sansho::parser::parse;
use serde_json::json;

/// Evaluate an expression, as the pipeline does.
fn evaluate_of(expression: &str, document: serde_json::Value) -> Result<serde_json::Value, SanshoError> {
    let parsed = parse(expression)?;
    let program = sansho::compile(&parsed, None)?;
    sansho::materialize(&sansho::evaluate(&program, document)?)
}

/// A 20-stage chain, twice, compared for equality: equal in denoted content,
/// distinct in memory — a full 2^20 walk without a depth limit. Must be a
/// `Limit` error.
#[test]
fn equality_of_deep_chains_is_refused_at_the_depth_limit() {
    let one = "[@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@]\
               | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@]\
               | [@,@] | [@,@] | [@,@] | [@,@]";
    let expression = format!("({one}) == ({one})");
    match evaluate_of(&expression, json!({"a": 1})) {
        Err(SanshoError::Limit { .. }) => {}
        other => panic!(
            "equality at depth 20 must be a Limit error, not {:?}",
            other.ok()
        ),
    }
}

/// A 15-stage chain sits under the limit: legal comparison, equal result.
#[test]
fn equality_of_shallow_chains_still_succeeds() {
    let one = "[@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@]\
               | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@]";
    let expression = format!("({one}) == ({one})");
    let value = evaluate_of(&expression, json!({"a": 1})).expect("depth 15 is legal");
    assert_eq!(value, json!(true), "two equal chains are equal");
}

/// `to_string` of a bomb chain must be refused at the node budget — the
/// same 500,000-node maximum materialization uses.
#[test]
fn to_string_of_a_bomb_chain_is_refused_at_the_node_budget() {
    let one = "[@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@]\
               | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@] | [@,@]\
               | [@,@] | [@,@] | [@,@] | [@,@]";
    let expression = format!("to_string({one})");
    match evaluate_of(&expression, json!({"a": 1})) {
        Err(SanshoError::Limit { .. }) => {}
        other => panic!(
            "rendering 2^20 nodes must be a Limit error, not {:?}",
            other.ok().map(|v| v.as_str().map(str::len))
        ),
    }
}

/// `to_string` of a legal value still renders.
#[test]
fn to_string_of_a_legal_value_still_works() {
    let value = evaluate_of("to_string(rows)", json!({"rows": [1, 2, 3]})).expect("legal");
    assert_eq!(value, json!("[1,2,3]"));
}
