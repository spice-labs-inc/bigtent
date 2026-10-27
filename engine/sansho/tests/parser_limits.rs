//! Tests for the parser's nesting-depth limit.
//!
//! REQUIREMENT (PR #171 review, finding 1): the parser must reject hostile
//! nesting with a `Limit` error instead of overflowing the stack. The
//! original guard counted only `[` and `{`; the recursive-descent combinators
//! for parenthesised groups `( … )` and prefix negation `! …` recurse with no
//! bound, so an expression of a few thousand such characters — far under the
//! 16,384-character length limit — aborts the whole server process.
//!
//! THEORY OF THE TEST: the limit is a one-time, iterative, pre-parse scan;
//! these tests assert it covers EVERY character that begins a recursive
//! descent, not merely the two it was written for. Each test builds an
//! expression whose nesting is deep only in ONE character class, so a failure
//! localizes the unguarded character precisely.

use sansho::error::SanshoError;
use sansho::parser::parse;

/// `(` begins a recursive group parse. Two thousand nested groups overflow a
/// tokio worker's stack (2 MB); the test asserts a `Limit` error instead.
#[test]
fn nested_parentheses_are_rejected_at_the_depth_limit() {
    let expression = format!("{}x{}", "(".repeat(2_000), ")".repeat(2_000));
    match parse(&expression) {
        Err(SanshoError::Limit { .. }) => {}
        other => panic!(
            "2000 nested parentheses must be a Limit error, not {other:?} — \\
             the recursion guard does not count `(`"
        ),
    }
}

/// `!` begins a recursive negation parse. Four thousand of them — the shape
/// the review reported — must be a `Limit` error, not an overflow.
#[test]
fn nested_negations_are_rejected_at_the_depth_limit() {
    let expression = format!("!{}x", "!".repeat(4_000));
    match parse(&expression) {
        Err(SanshoError::Limit { .. }) => {}
        other => panic!(
            "4000 leading `!` must be a Limit error, not {other:?} — \\
             the recursion guard does not count `!`"
        ),
    }
}

/// The limit is not a blanket ban on nesting: expression AT the limit parse
/// fine. This pins the guard at the boundary so a future "fix" cannot quietly
/// become "reject everything with brackets".
#[test]
fn nesting_at_the_limit_still_parses() {
    let deep = "[".repeat(64) + "x" + &"]".repeat(64);
    parse(&deep).expect("64 nested brackets are at the limit and legal");
    let over = "[".repeat(65) + "x" + &"]".repeat(65);
    match parse(&over) {
        Err(SanshoError::Limit { .. }) => {}
        other => panic!("65 nested brackets must be a Limit error, not {other:?}"),
    }
}
