//! The walk program: the compiled form of a JMESPath expression
//! The compiled form.
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
use std::sync::Arc;

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
    /// Each selector with the expression bytes it came from: the span is what lets an
    /// evaluation failure name its place instead of formatting a sentence.
    pub edges: Vec<(Edge, crate::error::Span)>,
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

/// THE JMESPath FUNCTION TABLE — the one place the function set is written.
///
/// A function's arity, whether its evaluation is implemented, and whether it
/// takes an expression reference are three FACTS ABOUT ONE FUNCTION. They used to
/// live in three separate matches (`function_arity`, `function_implemented`,
/// `function_takes_expression`), so adding a function to one and forgetting
/// another silently produced a compile-time "unknown function" or a run-time
/// mis-evaluation. One row per function, and the three public questions below
/// are three views of it.
///
/// The names and arities are the JMESPath specification's built-in list; arity is
/// checked at compile time, so an unknown function or a wrong arity is a compile
/// error rather than a run-time surprise.
///
/// Columns: name, minimum arity, maximum arity (`None` = variadic), implemented,
/// takes an expression reference (applied per element).
const FUNCTIONS: &[(&str, usize, Option<usize>, bool, bool)] = &[
    ("abs", 1, Some(1), true, false),
    ("avg", 1, Some(1), true, false),
    ("ceil", 1, Some(1), true, false),
    ("contains", 2, Some(2), true, false),
    ("ends_with", 2, Some(2), true, false),
    ("floor", 1, Some(1), true, false),
    ("join", 2, Some(2), true, false),
    ("keys", 1, Some(1), true, false),
    ("length", 1, Some(1), true, false),
    ("map", 2, Some(2), true, true),
    ("max", 1, Some(1), true, false),
    ("max_by", 2, Some(2), true, true),
    ("merge", 1, None, true, false),
    ("min", 1, Some(1), true, false),
    ("min_by", 2, Some(2), true, true),
    ("not_null", 1, None, true, false),
    ("reverse", 1, Some(1), true, false),
    ("sort", 1, Some(1), true, false),
    ("sort_by", 2, Some(2), true, true),
    ("starts_with", 2, Some(2), true, false),
    ("sum", 1, Some(1), true, false),
    ("to_array", 1, Some(1), true, false),
    ("to_number", 1, Some(1), true, false),
    ("to_string", 1, Some(1), true, false),
    ("type", 1, Some(1), true, false),
    ("values", 1, Some(1), true, false),
];

/// The row for a name, if it is a JMESPath function.
fn function_row(name: &str) -> Option<&'static (&'static str, usize, Option<usize>, bool, bool)> {
    FUNCTIONS.iter().find(|(known, ..)| *known == name)
}

/// Arity bounds `(minimum, maximum)`; `None` maximum = variadic.
pub fn function_arity(name: &str) -> Option<(usize, Option<usize>)> {
    function_row(name).map(|&(_, minimum, maximum, _, _)| (minimum, maximum))
}

/// Whether a known function's evaluation is implemented in the current
/// language phase (the corpus harness scores such cases as
/// not-implemented rather than failed).
pub fn function_implemented(name: &str) -> bool {
    function_row(name).is_some_and(|&(_, _, _, implemented, _)| implemented)
}

/// The functions whose first argument is an expression reference
/// (applied per element).
pub fn function_takes_expression(name: &str) -> bool {
    function_row(name).is_some_and(|&(_, _, _, _, takes)| takes)
}


// ---------- compilation ----------

/// Lower a parsed expression into a walk program.
/// Compile a parsed expression into its program: the lowered tree the
/// evaluator walks.
///
/// The optional cache is the caller's — instantiated, passed, and managed
/// entirely by them — and is keyed by the canonical form of the
/// expression, so repeated expressions compile once. `None` compiles
/// fresh. Sub-compilation (the expression's own nested programs) is
/// uncached: only whole expressions are cached.
pub fn compile(
    expr: &Expr,
    cache: Option<&crate::cache::ProgramCache>,
) -> Result<Arc<Program>, SanshoError> {
    // THE SAME TREE BOUND, at the other entry point: `parse` refuses a tree that is
    // too deep, and `compile` is public and accepts a caller-built `Expr`, so the
    // bound is stated where the recursion actually starts as well. One guard for
    // every consumer (compilation, evaluation, rendering, the visitors) instead of
    // one per consumer.
    if crate::ast::deeper_than(expr, crate::parser::MAX_TREE_DEPTH_FOR_CALLERS) {
        return Err(SanshoError::limit(
            Some(crate::error::Span::new(0, 0)),
            crate::error::LimitFault::TreeDepth {
                allowed: crate::parser::MAX_TREE_DEPTH_FOR_CALLERS,
                observed: crate::ast::max_depth(expr),
            },
        ));
    }
    if let Some(cache) = cache {
        if let Some(program) = cache.get(expr)? {
            return Ok(program);
        }
        let program = Arc::new(compile_program(expr)?);
        cache.put(expr, program.clone())?;
        Ok(program)
    } else {
        Ok(Arc::new(compile_program(expr)?))
    }
}

fn compile_program(expr: &Expr) -> Result<Program, SanshoError> {
    match expr {
        Expr::Pipe(left, right) => Ok(Program::Pipe(
            Box::new(compile_program(left)?),
            Box::new(compile_program(right)?),
        )),
        Expr::Or(left, right) => Ok(Program::Or(
            Box::new(compile_program(left)?),
            Box::new(compile_program(right)?),
        )),
        Expr::And(left, right) => Ok(Program::And(
            Box::new(compile_program(left)?),
            Box::new(compile_program(right)?),
        )),
        Expr::Not(inner) => Ok(Program::Not(Box::new(compile_program(inner)?))),
        Expr::Compare(op, left, right) => Ok(Program::Compare(
            *op,
            Box::new(compile_program(left)?),
            Box::new(compile_program(right)?),
        )),
        Expr::Chain(primary, postfixes) => {
            let mut edges = Vec::with_capacity(postfixes.len() + 1);
            // the head: a field is the natural first edge; everything
            // else evaluates first and hands its value down
            // THE HEAD'S OWN SPAN, which the parser recorded: a compile failure in a
            // function call in head position points at the function's name.
            let head_span = primary.span;
            match &primary.node {
                Primary::Grouped(inner) => {
                    // the group computes a VALUE; the postfix edges that
                    // follow project on it, which is the whole semantics of
                    // grouping
                    let program = compile_program(inner)?;
                    edges.push((Edge::Compute(Box::new(program)), head_span));
                }
                Primary::Current => {}
                Primary::Field(name) => edges.push((Edge::Key(name.clone()), head_span)),
                Primary::Literal(value) => edges.push((
                    Edge::Compute(Box::new(Program::Literal(value.clone()))),
                    head_span,
                )),
                Primary::RawString(s) => edges.push((
                    Edge::Compute(Box::new(Program::Literal(J::String(s.clone())))),
                    head_span,
                )),
                Primary::Function(name, args) => {
                    edges.push((Edge::Compute(Box::new(compile_function(name, args, Some(head_span))?)), head_span));
                }
                Primary::MultiList(items) => {
                    let slots = items
                        .iter()
                        .map(compile_program)
                        .collect::<Result<Vec<_>, SanshoError>>()?;
                    edges.push((Edge::Compute(Box::new(Program::MultiList(slots))), head_span));
                }
                Primary::MultiHash(entries) => {
                    let slots = entries
                        .iter()
                        .map(|(k, v)| compile_program(v).map(|p| (k.clone(), p)))
                        .collect::<Result<Vec<_>, SanshoError>>()?;
                    edges.push((Edge::Compute(Box::new(Program::MultiHash(slots))), head_span));
                }
            }
            for spanned in postfixes {
                let post = &spanned.node;
                let at = spanned.span;
                match post {
                    Postfix::Field(name) => edges.push((Edge::Key(name.clone()), at)),
                    Postfix::Index(i) => edges.push((Edge::Index(*i), at)),
                    Postfix::Slice(spec) => edges.push((Edge::Slice(*spec), at)),
                    Postfix::Wildcard { bracket } => {
                        if *bracket {
                            edges.push((Edge::WildcardBracket, at));
                        } else {
                            edges.push((Edge::WildcardDot, at));
                        }
                    }
                    Postfix::Flatten => edges.push((Edge::Flatten, at)),
                    Postfix::Filter(pred) => {
                        edges.push((Edge::Filter(Box::new(compile_program(pred)?)), at))
                    }
                    Postfix::MultiList(items) => {
                        let slots = items
                            .iter()
                            .map(compile_program)
                            .collect::<Result<Vec<_>, SanshoError>>()?;
                        edges.push((Edge::Compute(Box::new(Program::MultiList(slots))), head_span));
                    }
                    Postfix::MultiHash(entries) => {
                        let slots = entries
                            .iter()
                            .map(|(k, v)| compile_program(v).map(|p| (k.clone(), p)))
                            .collect::<Result<Vec<_>, SanshoError>>()?;
                        edges.push((Edge::Compute(Box::new(Program::MultiHash(slots))), head_span));
                    }
                    Postfix::Function(name, args) => {
                        edges.push((Edge::Compute(Box::new(compile_function(name, args, Some(head_span))?)), head_span));
                    }
                }
            }
            Ok(Program::Spine(Spine { edges }))
        }
    }
}

fn compile_function(
    name: &str,
    args: &[FnArg],
    span: Option<crate::error::Span>,
) -> Result<Program, SanshoError> {
    // THE NAME'S OWN BYTES, not the whole call: a compile failure about a function
    // points at its name, which is what a reader needs to see underlined.
    let name_span = span.map(|span| crate::error::Span::of(span.start, name));
    let program_args = args
        .iter()
        .map(|arg| match arg {
            FnArg::Value(e) => compile_program(e).map(FnArgP::Value),
            FnArg::Ref(e) => compile_program(e).map(|p| FnArgP::Ref(Box::new(p))),
        })
        .collect::<Result<Vec<_>, SanshoError>>()?;
    check_arity(name, &program_args, name_span)?;
    Ok(Program::Function(name.to_string(), program_args))
}

fn check_arity(
    name: &str,
    args: &[FnArgP],
    span: Option<crate::error::Span>,
) -> Result<(), SanshoError> {
    match function_arity(name) {
        // THE REASON IS THE VARIANT: an unknown function is not a sentence about one
        None => Err(SanshoError::compile(
            span,
            None,
            crate::error::CompileFault::UnknownFunction {
                name: name.to_string(),
            },
        )),
        Some((min, max)) => {
            let len = args.len();
            let too_few = len < min;
            let too_many = max.map(|m| len > m).unwrap_or(false);
            if too_few || too_many {
                Err(SanshoError::compile(
                    span,
                    None,
                    crate::error::CompileFault::WrongArity {
                        name: name.to_string(),
                        minimum: min,
                        maximum: max,
                        found: len,
                    },
                ))
            } else {
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod function_table {
    //! THE FUNCTION TABLE MUST NOT DISAGREE WITH ITSELF.
    //!
    //! REQUIREMENT (class-A audit): the arity, implementation and
    //! expression-reference questions were answered by three separate matches over
    //! the same function names, so the set existed three times and nothing
    //! compared the copies. A name in one and not another is a compile-time
    //! "unknown function" for a function the specification has, or a run-time
    //! mis-evaluation.
    //!
    //! THEORY OF THE TEST: enumerate the table rather than list names — a row is
    //! unique and well-formed, the three public questions answer consistently for
    //! every row, and a name that is not a row answers nothing through any of
    //! them. Because the views are derived from the table, these assertions fail
    //! the moment a row is added half-way.
    use super::{function_arity, function_implemented, function_row, function_takes_expression, FUNCTIONS};

    /// Every row is unique and its arity is well-formed.
    #[test]
    fn the_table_has_no_duplicate_or_malformed_row() {
        for (position, (name, minimum, maximum, _, _)) in FUNCTIONS.iter().enumerate() {
            assert!(
                FUNCTIONS[..position].iter().all(|(earlier, ..)| earlier != name),
                "`{name}` appears twice: the first row would shadow the second"
            );
            assert!(*minimum >= 1, "`{name}` takes at least one argument");
            if let Some(maximum) = maximum {
                assert!(
                    maximum >= minimum,
                    "`{name}` has an empty arity range: {minimum}..={maximum}"
                );
            }
        }
    }

    /// The three questions answer from the same row for every name.
    #[test]
    fn the_three_questions_agree_for_every_row() {
        for (name, minimum, maximum, implemented, takes) in FUNCTIONS {
            assert_eq!(function_arity(name), Some((*minimum, *maximum)), "arity of `{name}`");
            assert_eq!(function_implemented(name), *implemented, "implemented `{name}`");
            assert_eq!(function_takes_expression(name), *takes, "takes-expression `{name}`");
            assert!(
                function_row(name).is_some(),
                "`{name}` is a row, so it must be findable"
            );
        }
        // and a name that is not a function is nobody's answer: `mean` is the
        // typo this table exists to refuse
        for unknown in ["mean", "nope", ""] {
            assert_eq!(function_arity(unknown), None, "`{unknown}` is not a function");
            assert!(!function_implemented(unknown), "`{unknown}` is not implemented");
            assert!(!function_takes_expression(unknown), "`{unknown}` takes no expression");
        }
    }
}
