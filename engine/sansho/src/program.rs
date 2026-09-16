//! The walk program: the compiled form of a JMESPath expression
//! (SPEC-0001 §5).
//!
//! A spine is a linear instruction list — one edge per selector, in
//! source order. The evaluator threads a FLOW through the edges: plain
//! values navigate; projections carry collections, and every edge after
//! a projection applies per element (results push as flows; null results
//! drop). The flatten edge alone dissolves arrays into their parent
//! projection — the corpus is explicit that wildcard-under-wildcard stays
//! nested and `[]` is what flattens. A later phase adds path-trie
//! hash-consing (the edges stay addressable per position so the cursor
//! backend can share decoded bytes).

use crate::ast::{CmpOp, Expr, FnArg, Postfix, Primary, SliceSpec};
use crate::error::SanshoError;
use serde_json::Value as J;

/// The compiled program.
#[derive(Clone, Debug)]
pub enum Program {
    /// A navigation spine: the linear edge list.
    Spine(Spine),
    /// A literal JSON value: `` `...` ``, raw strings included.
    Literal(J),
    /// A multi-select list: `[expr, expr, ...]`.
    MultiList(Vec<Program>),
    /// A multi-select hash: `{key: expr, ...}`.
    MultiHash(Vec<(String, Program)>),
    /// `left | right`.
    Pipe(Box<Program>, Box<Program>),
    /// `left || right`.
    Or(Box<Program>, Box<Program>),
    /// `left && right`.
    And(Box<Program>, Box<Program>),
    /// `!inner`.
    Not(Box<Program>),
    /// `left op right`.
    Compare(CmpOp, Box<Program>, Box<Program>),
    /// A function call; arguments are value programs or expression
    /// references.
    Function(String, Vec<FnArgP>),
}

/// A function argument: value or expression reference.
#[derive(Clone, Debug)]
pub enum FnArgP {
    Value(Program),
    Ref(Box<Program>),
}

/// A navigation spine: the selectors in source order.
#[derive(Clone, Debug)]
pub struct Spine {
    pub edges: Vec<Edge>,
}

/// One selector step.
#[derive(Clone, Debug)]
pub enum Edge {
    /// The head program's value (a leading field, function call, or
    /// literal) or a value-construct spliced mid-chain.
    Compute(Box<Program>),
    /// `.key` — object member.
    Key(String),
    /// `[index]` — array element; negatives count from the end.
    Index(i64),
    /// `[start:stop:step]` — a projection over the slice (arrays), or
    /// the sliced string (strings).
    Slice(SliceSpec),
    /// `.*` — a projection over object values (objects only).
    WildcardDot,
    /// `[*]` — a projection over array elements (arrays only).
    WildcardBracket,
    /// `[?predicate]` — a projection over the array elements that
    /// satisfy the predicate.
    Filter(Box<Program>),
    /// `[]` — the flatten operator: dissolves the prior projection's
    /// collected arrays one level.
    Flatten,
}

// ---------- the built-in function table ----------

/// Arity bounds `(minimum, maximum)`; `None` maximum = variadic.
/// Names and arities are from the JMESPath specification's built-in
/// function list; arity is checked at compile time, so unknown functions
/// and wrong arities are compile errors.
pub fn function_arity(name: &str) -> Option<(usize, Option<usize>)> {
    match name {
        "abs" | "avg" | "ceil" | "floor" | "length" | "max" | "mean" | "min" | "reverse"
        | "sort" | "sum" | "to_array" | "to_number" | "to_string" | "keys" | "values"
        | "type" => Some((1, Some(1))),
        "contains" | "ends_with" | "join" | "map" | "max_by" | "min_by" | "sort_by"
        | "starts_with" => Some((2, Some(2))),
        "merge" | "not_null" => Some((1, None)),
        _ => None,
    }
}

/// Whether a known function's evaluation is implemented in the current
/// language phase (the corpus harness scores such cases as
/// not-implemented rather than failed).
pub fn function_implemented(name: &str) -> bool {
    matches!(
        name,
        "length" | "starts_with" | "ends_with" | "contains" | "type" | "keys" | "values"
            | "abs" | "avg" | "ceil" | "floor" | "join" | "map" | "max" | "max_by"
            | "mean" | "min" | "min_by" | "merge" | "not_null" | "reverse" | "sort"
            | "sort_by" | "sum" | "to_array" | "to_number" | "to_string"
    )
}

/// The functions whose first argument is an expression reference
/// (applied per element).
pub fn function_takes_expression(name: &str) -> bool {
    matches!(name, "map" | "max_by" | "min_by" | "sort_by")
}

// ---------- compilation ----------

/// Lower a parsed expression into a walk program.
pub fn compile(expr: &Expr) -> Result<Program, SanshoError> {
    match expr {
        Expr::Pipe(left, right) => Ok(Program::Pipe(
            Box::new(compile(left)?),
            Box::new(compile(right)?),
        )),
        Expr::Or(left, right) => Ok(Program::Or(
            Box::new(compile(left)?),
            Box::new(compile(right)?),
        )),
        Expr::And(left, right) => Ok(Program::And(
            Box::new(compile(left)?),
            Box::new(compile(right)?),
        )),
        Expr::Not(inner) => Ok(Program::Not(Box::new(compile(inner)?))),
        Expr::Compare(op, left, right) => Ok(Program::Compare(
            *op,
            Box::new(compile(left)?),
            Box::new(compile(right)?),
        )),
        Expr::Chain(primary, postfixes) => {
            let mut edges = Vec::with_capacity(postfixes.len() + 1);
            // the head: a field is the natural first edge; everything
            // else evaluates first and hands its value down
            match primary {
                Primary::Current => {}
                Primary::Field(name) => edges.push(Edge::Key(name.clone())),
                Primary::Literal(value) => {
                    edges.push(Edge::Compute(Box::new(Program::Literal(value.clone()))))
                }
                Primary::RawString(s) => edges.push(Edge::Compute(Box::new(Program::Literal(
                    J::String(s.clone()),
                )))),
                Primary::Function(name, args) => {
                    edges.push(Edge::Compute(Box::new(compile_function(name, args)?)));
                }
                Primary::MultiList(items) => {
                    let slots = items
                        .iter()
                        .map(compile)
                        .collect::<Result<Vec<_>, SanshoError>>()?;
                    edges.push(Edge::Compute(Box::new(Program::MultiList(slots))));
                }
                Primary::MultiHash(entries) => {
                    let slots = entries
                        .iter()
                        .map(|(k, v)| compile(v).map(|p| (k.clone(), p)))
                        .collect::<Result<Vec<_>, SanshoError>>()?;
                    edges.push(Edge::Compute(Box::new(Program::MultiHash(slots))));
                }
            }
            for post in postfixes {
                match post {
                    Postfix::Field(name) => edges.push(Edge::Key(name.clone())),
                    Postfix::Index(i) => edges.push(Edge::Index(*i)),
                    Postfix::Slice(spec) => edges.push(Edge::Slice(*spec)),
                    Postfix::Wildcard { bracket } => {
                        if *bracket {
                            edges.push(Edge::WildcardBracket);
                        } else {
                            edges.push(Edge::WildcardDot);
                        }
                    }
                    Postfix::Flatten => edges.push(Edge::Flatten),
                    Postfix::Filter(pred) => {
                        edges.push(Edge::Filter(Box::new(compile(pred)?)))
                    }
                    Postfix::MultiList(items) => {
                        let slots = items
                            .iter()
                            .map(compile)
                            .collect::<Result<Vec<_>, SanshoError>>()?;
                        edges.push(Edge::Compute(Box::new(Program::MultiList(slots))));
                    }
                    Postfix::MultiHash(entries) => {
                        let slots = entries
                            .iter()
                            .map(|(k, v)| compile(v).map(|p| (k.clone(), p)))
                            .collect::<Result<Vec<_>, SanshoError>>()?;
                        edges.push(Edge::Compute(Box::new(Program::MultiHash(slots))));
                    }
                    Postfix::Function(name, args) => {
                        edges.push(Edge::Compute(Box::new(compile_function(name, args)?)));
                    }
                }
            }
            Ok(Program::Spine(Spine { edges }))
        }
    }
}

fn compile_function(name: &str, args: &[FnArg]) -> Result<Program, SanshoError> {
    let program_args = args
        .iter()
        .map(|arg| match arg {
            FnArg::Value(e) => compile(e).map(FnArgP::Value),
            FnArg::Ref(e) => compile(e).map(|p| FnArgP::Ref(Box::new(p))),
        })
        .collect::<Result<Vec<_>, SanshoError>>()?;
    check_arity(name, &program_args)?;
    Ok(Program::Function(name.to_string(), program_args))
}

fn check_arity(name: &str, args: &[FnArgP]) -> Result<(), SanshoError> {
    match function_arity(name) {
        None => Err(SanshoError::Compile {
            message: format!("unknown function: {name}"),
        }),
        Some((min, max)) => {
            let len = args.len();
            let too_few = len < min;
            let too_many = max.map(|m| len > m).unwrap_or(false);
            if too_few || too_many {
                Err(SanshoError::Compile {
                    message: format!(
                        "function {name} called with {len} argument(s), expected {}",
                        match max {
                            Some(m) if m == min => format!("{min}"),
                            _ => format!("at least {min}"),
                        }
                    ),
                })
            } else {
                Ok(())
            }
        }
    }
}
