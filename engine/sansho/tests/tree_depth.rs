//! THE TREE-DEPTH BOUND: a flat operator chain is not parser nesting.
//!
//! REQUIREMENT (PR #171 review 4, comment 90, and its 11 siblings): the parser's
//! depth counter bounds PARSING, but `a|b|c|…` is one level of parser nesting and N
//! levels of TREE. Every recursive consumer of the tree — compilation, evaluation,
//! canonical rendering, the compiler's visitors, the item-gate builders — then
//! recurses N times and a 4,000-operator query overflows the stack and aborts the
//! process. The guard belongs on the TREE, so that one bound protects every
//! consumer instead of each consumer growing its own.
//!
//! THEORY OF THE TEST: each operator family is built flat and must be a `Limit`
//! error. A single consumer's guard would not satisfy this: the test parses only, so
//! it passes only if the bound is applied where the tree is created. The control at
//! the maximum permitted depth must still parse, so the bound is not a blanket
//! refusal.
use sansho::error::SanshoError;
use sansho::parser::parse;

/// A flat chain of `operators` pipes: `x|x|…`.
fn pipes(operators: usize) -> String {
    vec!["x"; operators + 1].join("|")
}

/// A flat chain of `or`s, `and`s, and comparisons.
fn ors(operators: usize) -> String { vec!["x"; operators + 1].join("||") }
fn ands(operators: usize) -> String { vec!["x"; operators + 1].join("&&") }
fn compares(operators: usize) -> String {
    (0..=operators).map(|i| format!("x == {i}")).collect::<Vec<_>>().join(" && ")
}
/// A flat chain of negations (already recursion, but through one node type).
fn nots(operators: usize) -> String { format!("{}x", "!".repeat(operators)) }

/// Every flat operator family is refused, not aborted.
#[test]
fn a_flat_operator_chain_is_refused_at_the_tree_depth_limit() {
    for (what, expression) in [
        ("pipe", pipes(4_000)),
        ("or", ors(4_000)),
        ("and", ands(4_000)),
        ("compare", compares(4_000)),
        ("not", nots(4_000)),
    ] {
        match parse(&expression) {
            Err(SanshoError::Limit { .. }) => {}
            other => panic!(
                "4,000 flat `{what}` operators must be a Limit error, not {:?} — the \
                 tree-depth bound is missing or not applied where the tree is built",
                other.map(|_| "Ok(expression)")
            ),
        }
    }
}

/// The control: a chain at the permitted depth still parses.
#[test]
fn a_chain_within_the_tree_depth_limit_still_parses() {
    assert!(parse(&pipes(30)).is_ok(), "30 pipes are within the bound");
    assert!(parse(&ors(30)).is_ok(), "30 `||` are within the bound");
}
