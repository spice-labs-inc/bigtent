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

/// FINDING (second review, #4/#55): the guard's hand-written lexer and the
/// parser's disagree about an ESCAPED QUOTE inside a raw string.
///
/// `'\''` is a raw string holding one quote: the parser reads `\'` as content
/// and the following `'` as the end. The guard's `Inside::Raw` arm ignores the
/// backslash, so it reads the `\'` as the END and the next `'` as a NEW raw
/// string — flipping its state. It then treats every following bracket as
/// string content and stops counting, so the depth limit is off exactly when
/// the input is hostile.
///
/// THEORY: after a legal raw string, deep nesting must still be refused.
#[test]
fn an_escaped_quote_in_a_raw_string_does_not_disable_the_guard() {
    let expression = format!("{} | x{}", "'\\''", "(".repeat(2_000)) + &")".repeat(2_000);
    match parse(&expression) {
        Err(SanshoError::Limit { .. }) => {}
        other => panic!(
            "2000 nested groups after a raw string must be a Limit error, not {other:?} — \
             the guard's raw-string arm disagrees with the parser's"
        ),
    }
}

/// The same invariant for all three string forms, each carrying an ESCAPED
/// delimiter: the guard must still see the deep nesting that follows.
#[test]
fn escaped_delimiters_do_not_disable_the_guard() {
    let deep = format!("{}@{}", "(".repeat(2_000), ")".repeat(2_000));
    let cases = [
        // a quoted string with an escaped quote
        format!("\"a\\\"b\" | {deep}"),
        // a raw string with an escaped quote
        format!("'a\\'b' | {deep}"),
        // a backtick literal with an escaped quote (valid JSON text)
        format!("`\"a\\\"b\"` | {deep}"),
    ];
    for expression in cases {
        match parse(&expression) {
            Err(SanshoError::Limit { .. }) => {}
            other => panic!(
                "deep nesting after an escaped delimiter must be refused, not {other:?}"
            ),
        }
    }
}
