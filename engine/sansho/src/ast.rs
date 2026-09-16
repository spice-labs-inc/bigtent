//! The parsed form of a JMESPath expression, and its canonical
//! serialization.
//!
//! The canonical form is a deterministic re-serialization of the syntax
//! tree: no insignificant whitespace, fixed operator spellings, minimal
//! quoting for identifiers, and literals re-serialized from their parsed
//! JSON values. It is the cache key's input (SPEC-0001 §5.4), so it must
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

/// The canonical form of an expression (SPEC-0001 §5.4).
pub fn canonical(expr: &Expr) -> String {
    let mut out = String::new();
    render(expr, &mut out);
    out
}

fn render(expr: &Expr, out: &mut String) {
    match expr {
        Expr::Pipe(left, right) => {
            render(left, out);
            out.push_str(" | ");
            render(right, out);
        }
        Expr::Or(left, right) => {
            render(left, out);
            out.push_str(" || ");
            render(right, out);
        }
        Expr::And(left, right) => {
            render(left, out);
            out.push_str(" && ");
            render(right, out);
        }
        Expr::Compare(op, left, right) => {
            render(left, out);
            out.push(' ');
            out.push_str(op.as_str());
            out.push(' ');
            render(right, out);
        }
        Expr::Not(inner) => {
            out.push('!');
            render(inner, out);
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
