//! The parsed form of a JMESPath expression, and its canonical
//! serialization.
//!
//! The canonical form is a deterministic re-serialization of the syntax
//! tree: no insignificant whitespace, fixed operator spellings, minimal
//! quoting for identifiers, and literals re-serialized from their parsed
//! JSON values. It is the cache key's input, so it must
//! be a fixed point under re-parsing and must not collide across
//! expressions of different meaning — both are tested here.

use serde_json::Value as J;

/// A comparison operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    /// The canonical spelling.
    pub fn as_str(&self) -> &'static str {
        match self {
            CmpOp::Eq => "==",
            CmpOp::Ne => "!=",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        }
    }
}

/// A slice selector's bounds: `[start:stop:step]`, each optional.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SliceSpec {
    pub start: Option<i64>,
    pub stop: Option<i64>,
    pub step: Option<i64>,
}

/// A function-call argument: a value expression, or an expression
/// reference (`&expression`), which is only valid as a function argument.
#[derive(Clone, Debug)]
pub enum FnArg {
    Value(Expr),
    Ref(Expr),
}

/// One link in a postfix chain: the operators that attach to a primary
/// with no separator (`a.b`, `a[0]`, `a[*]`, `a[?x]`, `a[]`, `a[0:2]`,
/// `a[0, 1]`, `a{x: b}`).
#[derive(Clone, Debug)]
pub enum Postfix {
    Field(String),
    /// `.*` (dot form) or `[*]` (bracket form, arrays only).
    Wildcard {
        bracket: bool,
    },
    Flatten,
    Index(i64),
    Slice(SliceSpec),
    Filter(Box<Expr>),
    Function(String, Vec<FnArg>),
    MultiList(Vec<Expr>),
    MultiHash(Vec<(String, Expr)>),
}

/// The head of an expression: everything that is not a postfix chain.
#[derive(Clone, Debug)]
pub enum Primary {
    /// A parenthesised expression: a VALUE the postfixes project on, not
    /// sugar for splicing. `(foo[*].bar)[0]` indexes the grouped result;
    /// the group must survive parsing, evaluation, AND the cache's
    /// canonical form, or the three disagree.
    Grouped(Box<Expr>),
    Current,
    Literal(J),
    RawString(String),
    Field(String),
    Function(String, Vec<FnArg>),
    MultiList(Vec<Expr>),
    MultiHash(Vec<(String, Expr)>),
}

/// A full JMESPath expression.
///
/// Precedence, loosest to tightest: pipe (`|`), logical or (`||`),
/// logical and (`&&`), comparators, not (`!`), then the postfix chain
/// off its primary.
#[derive(Clone, Debug)]
pub enum Expr {
    Pipe(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    And(Box<Expr>, Box<Expr>),
    Compare(CmpOp, Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    Chain(Primary, Vec<Postfix>),
}

/// Render an identifier canonically: unquoted when it matches the
/// unquoted-identifier grammar, quoted (as a JSON string) otherwise.
pub fn canonical_identifier(name: &str) -> String {
    let unquoted = !name.is_empty()
        && name
            .chars()
            .next()
            .map(|c| c.is_ascii_alphabetic() || c == '_')
            .unwrap_or(false)
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if unquoted {
        name.to_string()
    } else {
        // JSON string escaping (not Rust Debug escaping): the canonical
        // form must re-parse through the expression grammar, whose quoted
        // identifiers accept JSON escapes
        serde_json::to_string(name).unwrap_or_else(|_| format!("\"{name}\""))
    }
}

/// Render a literal canonically: backtick-quoted re-serialization of the
/// parsed JSON value, with embedded backticks escaped.
pub fn canonical_literal(value: &J) -> String {
    let json = serde_json::to_string(value).unwrap_or_else(|_| "null".into());
    format!("`{}`", json.replace('`', "\\`"))
}

/// The canonical form of an expression.
// Precedence levels, LOW binding first. A child rendered by a parent of a
// HIGHER level than itself gets parentheses, so the canonical form preserves
// the parse — two expressions that parse differently never share a key.
const PREC_PIPE: u8 = 1;
const PREC_OR: u8 = 2;
const PREC_AND: u8 = 3;
const PREC_COMPARE: u8 = 4;
const PREC_NOT: u8 = 5;
const PREC_PRIMARY: u8 = 6;

/// Render `expr` as a child of a parent at level `parent`, parenthesizing it
/// when its own binding is looser than the parent's context requires.
fn render_child(expr: &Expr, parent: u8, out: &mut String) {
    let needs_parens = expr_precedence(expr) < parent;
    if needs_parens {
        out.push('(');
        render(expr, out);
        out.push(')');
    } else {
        render(expr, out);
    }
}

/// The binding level of an expression's TOP operator. A chain or bare primary
/// binds tightest and never needs parentheses.
fn expr_precedence(expr: &Expr) -> u8 {
    match expr {
        Expr::Pipe(_, _) => PREC_PIPE,
        Expr::Or(_, _) => PREC_OR,
        Expr::And(_, _) => PREC_AND,
        Expr::Compare(_, _, _) => PREC_COMPARE,
        Expr::Not(_) => PREC_NOT,
        Expr::Chain(_, _) => PREC_PRIMARY,
    }
}

pub fn canonical(expr: &Expr) -> String {
    let mut out = String::new();
    render(expr, &mut out);
    out
}

fn render(expr: &Expr, out: &mut String) {
    match expr {
        Expr::Pipe(left, right) => {
            render_child(left, PREC_PIPE, out);
            out.push_str(" | ");
            render_child(right, PREC_PIPE, out);
        }
        Expr::Or(left, right) => {
            render_child(left, PREC_OR, out);
            out.push_str(" || ");
            render_child(right, PREC_OR, out);
        }
        Expr::And(left, right) => {
            render_child(left, PREC_AND, out);
            out.push_str(" && ");
            render_child(right, PREC_AND, out);
        }
        Expr::Compare(op, left, right) => {
            render_child(left, PREC_COMPARE, out);
            out.push(' ');
            out.push_str(op.as_str());
            out.push(' ');
            render_child(right, PREC_COMPARE, out);
        }
        Expr::Not(inner) => {
            out.push('!');
            render_child(inner.as_ref(), PREC_NOT, out);
        }
        Expr::Chain(primary, postfixes) => {
            render_primary(primary, out);
            for post in postfixes {
                render_postfix(post, out);
            }
        }
    }
}

fn render_primary(primary: &Primary, out: &mut String) {
    match primary {
        Primary::Grouped(inner) => {
            out.push('(');
            render(inner, out);
            out.push(')');
        }
        Primary::Current => out.push('@'),
        Primary::Literal(value) => out.push_str(&canonical_literal(value)),
        Primary::RawString(s) => {
            out.push('\'');
            // the raw string's quotes are escaped back on the way out
            out.push_str(&s.replace('\'', "\\\'"));
            out.push('\'');
        }
        Primary::Field(name) => out.push_str(&canonical_identifier(name)),
        Primary::Function(name, args) => {
            out.push_str(&canonical_identifier(name));
            out.push('(');
            for (i, arg) in args.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                match arg {
                    FnArg::Value(e) => render(e, out),
                    FnArg::Ref(e) => {
                        out.push('&');
                        render(e, out);
                    }
                }
            }
            out.push(')');
        }
        Primary::MultiList(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                render(item, out);
            }
            out.push(']');
        }
        Primary::MultiHash(entries) => {
            out.push('{');
            for (i, (k, v)) in entries.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&canonical_identifier(k));
                out.push_str(": ");
                render(v, out);
            }
            out.push('}');
        }
    }
}

fn render_postfix(post: &Postfix, out: &mut String) {
    match post {
        Postfix::Field(name) => {
            out.push('.');
            out.push_str(&canonical_identifier(name));
        }
        Postfix::Wildcard { bracket } => {
            if *bracket {
                out.push_str("[*]");
            } else {
                out.push_str(".*");
            }
        }
        Postfix::Flatten => out.push_str("[]"),
        Postfix::Index(i) => out.push_str(&format!("[{i}]")),
        Postfix::Slice(spec) => {
            out.push('[');
            if let Some(s) = spec.start {
                out.push_str(&s.to_string());
            }
            out.push(':');
            if let Some(s) = spec.stop {
                out.push_str(&s.to_string());
            }
            if let Some(s) = spec.step {
                out.push(':');
                out.push_str(&s.to_string());
            }
            out.push(']');
        }
        Postfix::Filter(pred) => {
            out.push_str("[?");
            render(pred, out);
            out.push(']');
        }
        Postfix::Function(name, args) => {
            out.push('.');
            render_primary(&Primary::Function(name.clone(), args.clone()), out);
        }
        Postfix::MultiList(items) => {
            // the postfix form always carries the dot: foo.[a, b]
            out.push('.');
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                render(item, out);
            }
            out.push(']');
        }
        Postfix::MultiHash(entries) => {
            out.push('.');
            out.push('{');
            for (i, (k, v)) in entries.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(&canonical_identifier(k));
                out.push_str(": ");
                render(v, out);
            }
            out.push('}');
        }
    }
}

/// Whether an expression can produce a string.
///
/// A REASONABLE EFFORT, not inference. It answers `true` whenever a string is
/// among the shapes the expression can produce, and `false` only when it is
/// certain that no string can come out — so a `false` is a proof of absence
/// rather than a guess, and a caller may refuse on it.
///
/// Why this shape: a downstream stage may take only strings, and a `false` lets
/// that be refused before anything runs. But an expression that can produce
/// several shapes is legal as long as ONE of them is a string, because at
/// runtime the non-strings are simply not forwarded — so this errs towards
/// `true` everywhere the outcome is uncertain. A field access answers `true`
/// even though it is usually a number, because nothing here knows the document.
pub fn can_be_a_string(expression: &Expr) -> bool {
    match expression {
        // a pipe's result is its right-hand side
        Expr::Pipe(_, right) => can_be_a_string(right),
        // `||` and `&&` yield one of their operands
        Expr::Or(left, right) | Expr::And(left, right) => {
            can_be_a_string(left) || can_be_a_string(right)
        }
        // a comparison and a `!` always yield a boolean
        Expr::Compare(..) | Expr::Not(_) => false,
        Expr::Chain(primary, postfixes) => {
            // a postfix that reshapes the result decides; otherwise the primary
            // and every postfix have to be considered
            postfixes
                .iter()
                .rev()
                .find_map(postfix_decides)
                .unwrap_or_else(|| can_be_a_string_primary(primary, postfixes))
        }
    }
}

/// Whether an emit entry can produce a string ROW.
///
/// [`can_be_a_string`] answers about the expression's VALUE. A row is not always
/// the value: with `unnest` off the row is what the expression produced, and with
/// it on a row is an ELEMENT of that value when the value is an array — and the
/// value itself when it is not.
///
/// That difference decides real queries. `[identifier, 'literal']` with `unnest`
/// emits two string rows, so an edge taking identifiers must accept it; the same
/// expression without `unnest` emits one array row, which is not an identifier.
/// And `{id: identifier}` with `unnest` still emits one object row, because there
/// is no array to unnest — so `unnest` is not a licence to answer true.
pub fn can_emit_a_string(expression: &Expr, unnest: bool) -> bool {
    if can_be_a_string(expression) {
        // the value itself can be a row
        return true;
    }
    if !unnest {
        // the row IS the value, and the value cannot be a string
        return false;
    }
    // `unnest` can only help when the value can be an array. If it certainly
    // cannot be one, there is nothing to unnest and the row is the value.
    !certainly_not_an_array(expression)
}

/// Whether an expression's value certainly cannot be an array.
///
/// The mirror of [`can_be_a_string`] and read the same way: `true` is a proof
/// that no array can come out, so a caller may rely on it.
fn certainly_not_an_array(expression: &Expr) -> bool {
    match expression {
        Expr::Pipe(_, right) => certainly_not_an_array(right),
        Expr::Or(left, right) | Expr::And(left, right) => {
            certainly_not_an_array(left) && certainly_not_an_array(right)
        }
        Expr::Compare(..) | Expr::Not(_) => true,
        Expr::Chain(primary, postfixes) => match postfixes.iter().rev().find_map(array_decides) {
            Some(not_an_array) => not_an_array,
            None => match primary {
                // the current node can be an array
                Primary::Current => false,
                // a group is whatever the grouped expression yields — ask it
                Primary::Grouped(inner) => certainly_not_an_array(inner),
                Primary::RawString(_) => true,
                Primary::Literal(value) => !value.is_array(),
                Primary::Field(_) | Primary::Function(..) => false,
                Primary::MultiList(_) => false,
                Primary::MultiHash(_) => true,
            },
        },
    }
}

/// A postfix that settles whether the value is an array.
fn array_decides(postfix: &Postfix) -> Option<bool> {
    match postfix {
        // always an array
        Postfix::Flatten | Postfix::Filter(_) | Postfix::MultiList(_) | Postfix::Wildcard { .. } => {
            Some(false)
        }
        // always an object
        Postfix::MultiHash(_) => Some(true),
        // depend on what they are applied to
        Postfix::Field(_) | Postfix::Index(_) | Postfix::Slice(_) | Postfix::Function(..) => None,
    }
}

/// A postfix whose result type it settles on its own, whatever came before it.
fn postfix_decides(postfix: &Postfix) -> Option<bool> {
    match postfix {
        // a flatten always yields an array
        Postfix::Flatten => Some(false),
        // a filter always yields an array
        Postfix::Filter(_) => Some(false),
        // a multiselect on either side yields an object
        Postfix::MultiHash(_) => Some(false),
        // a multiselect list yields an array
        Postfix::MultiList(_) => Some(false),
        // the rest depend on what they are applied to
        Postfix::Field(_)
        | Postfix::Wildcard { .. }
        | Postfix::Index(_)
        | Postfix::Slice(_)
        | Postfix::Function(..) => None,
    }
}

/// The primary, with the postfixes that do not decide for themselves.
fn can_be_a_string_primary(primary: &Primary, postfixes: &[Postfix]) -> bool {
    match primary {
        // the current node can be anything
        Primary::Current => true,
        // a group is a string only when the grouped expression is
        Primary::Grouped(inner) => can_be_a_string(inner),
        // a raw string is a string
        Primary::RawString(_) => true,
        // a literal is a string only when it IS one
        Primary::Literal(value) => value.is_string(),
        // a field, a function call and the rest are unknown or non-string
        Primary::Field(_) => true,
        Primary::Function(..) => true,
        Primary::MultiList(_) => false,
        Primary::MultiHash(_) => false,
    }
    .then_some(true)
    .or_else(|| {
        // a postfix chain ending in a function call can yield a string whatever
        // the primary was, because the function decides its own result type
        (postfixes
            .last()
            .map(|postfix| matches!(postfix, Postfix::Function(..)))
            .unwrap_or(false))
        .then_some(true)
    })
    .unwrap_or(false)
}

#[cfg(test)]
mod can_be_a_string_tests {
    use super::{can_be_a_string, can_emit_a_string};
    use crate::parser::parse;

    /// Requirement: a `false` from the analysis is a proof that no string can
    /// come out, never a guess.
    ///
    /// What: expressions whose result shape is fixed — a comparison, a `!`, a
    /// multiselect hash, a multiselect list, a literal number, a flatten, a
    /// filter.
    ///
    /// Why: a downstream stage may take only strings, and refusing an edge on
    /// this answer is only safe if `false` is certain. If any of these could
    /// yield a string, a legal query would be refused at compile time and the
    /// author would have no way to see why.
    ///
    /// LLM section: the assertion is on the SHAPE, which is what the function
    /// answers, not on a rendered result.
    #[test]
    fn a_false_is_a_proof_of_absence() {
        for expression in [
            "a == b",
            "!a",
            "{id: identifier}",
            "[identifier]",
            "`1`",
            "`true`",
            "names[]",
            "names[?@ == `1`]",
        ] {
            let parsed = parse(expression).expect("parses");
            assert!(
                !can_be_a_string(&parsed),
                "`{expression}` cannot produce a string"
            );
        }
    }

    /// Requirement: `unnest` changes what a row is, and the analysis has to
    /// follow it.
    ///
    /// What: a multiselect list, with and without `unnest`; a multiselect hash
    /// with `unnest`; a comparison with `unnest`; a filter with `unnest`.
    ///
    /// Why: without `unnest` the row is the expression's value, so a list of
    /// identifiers is one array row — not an identifier. With `unnest` each
    /// ELEMENT becomes a row, and `[identifier, 'literal']` therefore emits two
    /// string rows, which an edge taking identifiers must accept. An analysis
    /// that ignored the flag would refuse that edge and call a legal query
    /// illegal.
    ///
    /// LLM section: the multiselect list with `unnest` is the case this exists
    /// for — its entries are the rows, so the entries are what to ask about. A
    /// filter with `unnest` answers true because its elements are unknown, which
    /// is the conservative answer the value-level check also gives.
    #[test]
    fn unnest_decides_what_a_row_is() {
        let list = parse("[identifier, 'literal']").expect("parses");
        assert!(
            !can_emit_a_string(&list, false),
            "without unnest the row is the array itself"
        );
        assert!(
            can_emit_a_string(&list, true),
            "with unnest each entry is a row, and both are strings"
        );

        let hash = parse("{id: identifier}").expect("parses");
        assert!(
            !can_emit_a_string(&hash, true),
            "a hash is one object row whatever the flag says"
        );

        let compare = parse("a == b").expect("parses");
        assert!(
            !can_emit_a_string(&compare, true),
            "a boolean is not an array, so unnest has nothing to unnest"
        );

        let filter = parse("names[?@ == `1`]").expect("parses");
        assert!(
            can_emit_a_string(&filter, true),
            "the elements of a filtered array are unknown, so a string is possible"
        );
    }

    /// Requirement: an expression that CAN produce a string answers true, even
    /// when it can produce other shapes too.
    ///
    /// What: a raw string, a string literal, the current node, a field, a
    /// function, a pipe whose right-hand side is a string, and a multiselect
    /// whose entries include one.
    ///
    /// Why: the runtime policy is to forward strings and drop the rest, so an
    /// expression that yields several shapes is legal as long as one of them is a
    /// string. Refusing those would break queries that are perfectly well served
    /// by the drop.
    ///
    /// LLM section: `body.file_size` is the case that makes this necessary — it
    /// is a number in every document we have, and the analysis cannot know that,
    /// so it answers true and the walk receives nothing.
    #[test]
    fn a_possible_string_is_enough() {
        for expression in [
            "'literal'",
            "`\"quoted\"`",
            "@",
            "identifier",
            "body.file_size",
            "length(@)",
            "identifier | @",
            "a || b",
            "a && 'fallback'",
        ] {
            let parsed = parse(expression).expect("parses");
            assert!(
                can_be_a_string(&parsed),
                "`{expression}` can produce a string"
            );
        }
    }
}
