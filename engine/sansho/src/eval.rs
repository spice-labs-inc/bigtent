//! The evaluator: runs a compiled walk program against a tree view
//! (SPEC-0001 §5). GENERIC over the backend: the same evaluator runs on
//! the materialized backend (in-memory JSON) and the cursor backend
//! (raw CBOR bytes, selectively materialized) — the corpus's
//! backend-agreement requirement is what the differential tests enforce.
//!
//! The evaluator threads a FLOW through the program's edges:
//!
//! - a backend node navigates lazily (key, index) or projects
//!   (wildcards, slice, filter, flatten) — nothing materializes until a
//!   value is consumed;
//! - every edge after a projection applies per element; the mapping
//!   RECURSES into nested projections preserving shape (a nested
//!   projection is a VALUE: an array — the corpus is explicit that
//!   wildcard-under-wildcard stays nested);
//! - an element whose result is null drops out of the projection;
//! - the flatten edge alone dissolves arrays one level into the parent
//!   projection;
//! - navigation misses evaluate to null (the specification's null
//!   semantics: no omission, no defaulting);
//! - value-level constructs (functions, comparators, logic, literals)
//!   materialize their inputs through the backend's memoized decode —
//!   the fusion point that keeps every byte decoded at most once.

use crate::ast::CmpOp;
use crate::corpus::{Driver, DriverOutcome};
use crate::error::SanshoError;
use crate::materialized::MaterializedNode;
use crate::program::{compile as to_program, Edge, FnArgP, Program, Spine};
use crate::view::Node;
use serde_json::Value as J;

/// Why evaluation stopped: either a structured error, or a construct the
/// current language phase does not implement (scored by the corpus
/// harness as not-implemented, never as a failure).
#[derive(Debug)]
pub enum Stop {
    Error(SanshoError),
    NotImplemented,
}

impl From<SanshoError> for Stop {
    fn from(e: SanshoError) -> Self {
        Stop::Error(e)
    }
}

/// What evaluation flows along.
pub(crate) enum Flow<N> {
    /// A backend position (lazy; materialization is the backend's
    /// memoized decode).
    One(N),
    /// A computed scalar or aggregate (functions, comparators, literals,
    /// multi-selects).
    Value(J),
    /// A projection's collection.
    Proj(Vec<Flow<N>>),
}

impl<'d, N: Node<'d>> Flow<N> {
    pub(crate) fn as_value(&self) -> Result<J, SanshoError> {
        match self {
            Flow::One(node) => node.materialize(),
            Flow::Value(value) => Ok(value.clone()),
            Flow::Proj(items) => {
                let mut array = Vec::with_capacity(items.len());
                for item in items {
                    array.push(item.as_value()?);
                }
                Ok(J::Array(array))
            }
        }
    }
}

/// The corpus-facing driver: the real engine, through the materialized
/// backend. Unimplemented constructs surface as not-implemented outcomes
/// so the corpus report distinguishes "not built yet" from "built and
/// wrong".
pub struct RealEngine;

impl Driver for RealEngine {
    fn evaluate(&self, given: &serde_json::Value, expression: &str) -> DriverOutcome {
        match crate::parser::parse(expression) {
            Err(e) => DriverOutcome::Error(e),
            Ok(parsed) => match to_program(&parsed) {
                Err(e) => DriverOutcome::Error(e),
                Ok(program) => match eval_program(&program, given) {
                    Ok(value) => DriverOutcome::Result(value),
                    Err(Stop::Error(e)) => DriverOutcome::Error(e),
                    Err(Stop::NotImplemented) => DriverOutcome::NotImplemented,
                },
            },
        }
    }
}

/// Evaluate a program against an in-memory JSON document.
pub fn evaluate_json(program: &Program, document: &J) -> Result<J, SanshoError> {
    eval_program(program, document).map_err(|stop| match stop {
        Stop::Error(e) => e,
        Stop::NotImplemented => SanshoError::Compile {
            message: "expression uses constructs not yet implemented".to_string(),
        },
    })
}

/// Evaluate a program against one CBOR document (the byte-slice input of
/// SPEC-0001 §2): the cursor backend, with every position decoded at
/// most once per evaluation.
pub fn evaluate_cbor(program: &Program, document: &[u8]) -> Result<J, SanshoError> {
    evaluate_cbor_with_stats(program, document).0
}

/// The instrumented form: the result plus the number of DISTINCT
/// positions decoded (the byte-once property's observable).
pub fn evaluate_cbor_with_stats(
    program: &Program,
    document: &[u8],
) -> (Result<J, SanshoError>, usize) {
    let root = crate::cursor::CursorNode::root(document);
    let result = match eval(program, &root) {
        Ok(flow) => flow.as_value(),
        Err(Stop::Error(e)) => Err(e),
        Err(Stop::NotImplemented) => Err(SanshoError::Compile {
            message: "expression uses constructs not yet implemented".to_string(),
        }),
    };
    (result, root.decode_count())
}

/// The phase-visible form of the CBOR entry: the not-implemented
/// constructs surface as [`Stop::NotImplemented`] (the corpus harness
/// scores those as not-built-yet, never as failures). Interim during the
/// phased construction.
pub fn evaluate_cbor_stopped(program: &Program, document: &[u8]) -> Result<J, Stop> {
    let root = crate::cursor::CursorNode::root(document);
    eval(program, &root).and_then(|flow| flow.as_value().map_err(Stop::from))
}

/// The corpus harness's materialized entry: the outcome-visible form.
pub(crate) fn eval_program(program: &Program, document: &J) -> Result<J, Stop> {
    let root = MaterializedNode::root(document);
    match eval(program, &root) {
        Ok(flow) => flow.as_value().map_err(Stop::from),
        Err(stop) => Err(stop),
    }
}

fn eval<'d, N: Node<'d>>(program: &Program, node: &N) -> Result<Flow<N>, Stop> {
    match program {
        Program::Literal(value) => Ok(Flow::Value(value.clone())),
        Program::Spine(spine) => eval_spine(Flow::One(node.clone()), spine),
        Program::MultiList(slots) => {
            if node.is_null() {
                return Ok(Flow::Value(J::Null));
            }
            let mut items = Vec::with_capacity(slots.len());
            for slot in slots {
                items.push(eval(slot, node)?.as_value()?);
            }
            Ok(Flow::Value(J::Array(items)))
        }
        Program::MultiHash(entries) => {
            if node.is_null() {
                return Ok(Flow::Value(J::Null));
            }
            // Collected into the in-memory map (key-ordered); result
            // equality is key-based, so this is order-insensitive to the
            // corpus and deterministic run to run.
            let mut object = serde_json::Map::new();
            for (key, slot) in entries {
                let value = eval(slot, node)?.as_value()?;
                object.insert(key.clone(), value);
            }
            Ok(Flow::Value(J::Object(object)))
        }
        Program::Pipe(left, right) => {
            // the pipe materializes its left side and the right side
            // evaluates against that value through the materialized
            // backend (a computed value has no backend position); the
            // result is a computed value
            let left_value = eval(left, node)?.as_value()?;
            let right_flow = eval_value_flow(right, &left_value)?;
            let value = right_flow.as_value().map_err(Stop::from)?;
            Ok(Flow::Value(value))
        }
        Program::Or(left, right) => {
            let left_value = eval(left, node)?.as_value()?;
            if truthy(&left_value) {
                Ok(Flow::Value(left_value))
            } else {
                eval(right, node)
            }
        }
        Program::And(left, right) => {
            let left_value = eval(left, node)?.as_value()?;
            if truthy(&left_value) {
                eval(right, node)
            } else {
                Ok(Flow::Value(left_value))
            }
        }
        Program::Not(inner) => {
            let value = eval(inner, node)?.as_value()?;
            Ok(Flow::Value(J::Bool(!truthy(&value))))
        }
        Program::Compare(op, left, right) => {
            let left_value = eval(left, node)?.as_value()?;
            let right_value = eval(right, node)?.as_value()?;
            compare(*op, &left_value, &right_value).map(Flow::Value)
        }
        Program::Function(name, args) => eval_function(name, args, node),
    }
}

/// Evaluate against a MATERIALIZED value (the pipe's right side; the
/// corpus harness's entry). This is the materialized backend's
/// instantiation of the evaluator.
fn eval_value_flow<'a>(
    program: &Program,
    document: &'a J,
) -> Result<Flow<MaterializedNode<'a>>, Stop> {
    let root = MaterializedNode::root(document);
    eval(program, &root)
}

fn eval_spine<'d, N: Node<'d>>(input: Flow<N>, spine: &Spine) -> Result<Flow<N>, Stop> {
    let mut flow = input;
    for edge in &spine.edges {
        flow = eval_edge(flow, edge)?;
    }
    Ok(flow)
}

fn eval_edge<'d, N: Node<'d>>(input: Flow<N>, edge: &Edge) -> Result<Flow<N>, Stop> {
    // the flatten operator consumes the whole flow (it is the great
    // flattener); every other edge after a projection applies per
    // element — the mapping recurses into nested projections preserving
    // shape, and null element-results drop
    if let Edge::Flatten = edge {
        return flatten_flow(input);
    }
    match input {
        Flow::One(node) => eval_edge_on_node(node, edge),
        Flow::Value(value) => eval_edge_on_json(value, edge),
        Flow::Proj(items) => {
            let mut flows = Vec::new();
            for item in items {
                match eval_edge(item, edge)? {
                    // null element-results drop (materialization is
                    // memoized, so the drop-decision is not wasted work)
                    Flow::One(node) => match node.materialize() {
                        Ok(J::Null) => {}
                        Ok(value) => flows.push(Flow::Value(value)),
                        Err(e) => return Err(Stop::Error(e)),
                    },
                    Flow::Value(J::Null) => {}
                    flow => flows.push(flow),
                }
            }
            Ok(Flow::Proj(flows))
        }
    }
}

fn eval_edge_on_json<'d, N: Node<'d>>(value: J, edge: &Edge) -> Result<Flow<N>, Stop> {
    match edge {
        Edge::Compute(program) => {
            let flow = eval_value_flow(program, &value)?;
            let computed = flow.as_value().map_err(Stop::from)?;
            Ok(Flow::Value(computed))
        }
        Edge::Key(name) => Ok(Flow::Value(
            value.get(name).cloned().unwrap_or(J::Null),
        )),
        Edge::Index(index) => {
            let resolved = match &value {
                J::Array(items) => {
                    let len = items.len() as i64;
                    let effective = if *index < 0 { len + index } else { *index };
                    if effective >= 0 && effective < len {
                        items.get(effective as usize).cloned()
                    } else {
                        None
                    }
                }
                _ => None,
            };
            Ok(Flow::Value(resolved.unwrap_or(J::Null)))
        }
        Edge::Slice(spec) => match &value {
            J::Array(items) => {
                let selected = slice_indices(items.len() as i64, spec)?;
                Ok(Flow::Value(J::Array(
                    selected
                        .into_iter()
                        .filter_map(|i| items.get(i as usize).cloned())
                        .collect(),
                )))
            }
            J::String(text) => {
                let chars: Vec<char> = text.chars().collect();
                let selected = slice_indices(chars.len() as i64, spec)?;
                Ok(Flow::Value(J::String(
                    selected
                        .into_iter()
                        .filter_map(|i| chars.get(i as usize))
                        .collect(),
                )))
            }
            _ => Ok(Flow::Value(J::Null)),
        },
        Edge::WildcardDot => match &value {
            J::Object(map) => Ok(Flow::Proj(
                map.values().cloned().map(Flow::Value).collect(),
            )),
            _ => Ok(Flow::Value(J::Null)),
        },
        Edge::WildcardBracket => match &value {
            J::Array(items) => {
                Ok(Flow::Proj(items.iter().cloned().map(Flow::Value).collect()))
            }
            _ => Ok(Flow::Value(J::Null)),
        },
        Edge::Filter(predicate) => match &value {
            J::Array(items) => {
                let mut flows = Vec::new();
                for item in items {
                    let keep = match eval_value_flow(predicate, item) {
                        Ok(flow) => truthy(&flow.as_value().map_err(Stop::from)?),
                        Err(Stop::Error(e)) => return Err(Stop::Error(e)),
                        Err(Stop::NotImplemented) => return Err(Stop::NotImplemented),
                    };
                    if keep {
                        flows.push(Flow::Value(item.clone()));
                    }
                }
                Ok(Flow::Proj(flows))
            }
            _ => Ok(Flow::Value(J::Null)),
        },
        Edge::Flatten => unreachable!("the flatten flow is consumed by eval_edge"),
    }
}

fn eval_edge_on_node<'d, N: Node<'d>>(node: N, edge: &Edge) -> Result<Flow<N>, Stop> {
    match edge {
        // a value-construct spliced into the chain: the multi-selects,
        // function calls, and literal heads
        Edge::Compute(program) => eval(program, &node),
        Edge::Key(name) => match node.get_key(name) {
            Some(child) => Ok(Flow::One(child)),
            None => Ok(Flow::Value(J::Null)),
        },
        Edge::Index(index) => {
            let resolved = match node.kind() {
                crate::view::Kind::Array => {
                    let len = node.container_len().unwrap_or(0) as i64;
                    let effective = if *index < 0 { len + index } else { *index };
                    if effective >= 0 && effective < len {
                        node.get_index(effective as usize)
                    } else {
                        None
                    }
                }
                _ => None,
            };
            Ok(match resolved {
                Some(child) => Flow::One(child),
                None => Flow::Value(J::Null),
            })
        }
        // the dot form reads object values only
        Edge::WildcardDot => match node.kind() {
            crate::view::Kind::Object => {
                Ok(Flow::Proj(node.entries().into_iter().map(|(_, v)| Flow::One(v)).collect()))
            }
            _ => Ok(Flow::Value(J::Null)),
        },
        // the bracket form reads array elements only
        Edge::WildcardBracket => match node.kind() {
            crate::view::Kind::Array => {
                Ok(Flow::Proj(node.elements().into_iter().map(Flow::One).collect()))
            }
            _ => Ok(Flow::Value(J::Null)),
        },
        Edge::Filter(predicate) => match node.kind() {
            crate::view::Kind::Array => {
                let mut flows = Vec::new();
                for element in node.elements() {
                    let keep = truthy(&eval(predicate, &element)?.as_value()?);
                    if keep {
                        flows.push(Flow::One(element));
                    }
                }
                Ok(Flow::Proj(flows))
            }
            _ => Ok(Flow::Value(J::Null)),
        },
        // the slice is a projection over the selected range (arrays), or
        // the sliced string (strings)
        Edge::Slice(spec) => match node.kind() {
            crate::view::Kind::Array => {
                let len = node.container_len().unwrap_or(0) as i64;
                let selected = slice_indices(len, spec)?;
                let mut flows = Vec::with_capacity(selected.len());
                for index in selected {
                    if let Some(element) = node.get_index(index as usize) {
                        flows.push(Flow::One(element));
                    }
                }
                Ok(Flow::Proj(flows))
            }
            crate::view::Kind::String => {
                let text = node
                    .as_str()
                    .map(|s| s.into_owned())
                    .unwrap_or_default();
                let chars: Vec<char> = text.chars().collect();
                let selected = slice_indices(chars.len() as i64, spec)?;
                let sliced: String = selected
                    .into_iter()
                    .filter_map(|i| chars.get(i as usize))
                    .collect();
                Ok(Flow::Value(J::String(sliced)))
            }
            _ => Ok(Flow::Value(J::Null)),
        },
        Edge::Flatten => unreachable!("the flatten flow is consumed by eval_edge"),
    }
}

/// The `[]` operator: dissolve array-valued results one level into the
/// projection; scalars pass through within an existing projection; a
/// plain non-array input flattens to null.
fn flatten_flow<'d, N: Node<'d>>(input: Flow<N>) -> Result<Flow<N>, Stop> {
    let mut flows = Vec::new();
    match input {
        Flow::Proj(items) => {
            for item in items {
                flatten_into(item, &mut flows);
            }
        }
        Flow::One(node) => match node.kind() {
            crate::view::Kind::Array => {
                for element in node.elements() {
                    match element.materialize() {
                        Ok(J::Null) => {}
                        Ok(J::Array(inner)) => {
                            flows.extend(inner.into_iter().map(|v| Flow::Value(v)))
                        }
                        Ok(plain) => flows.push(Flow::Value(plain)),
                        Err(e) => return Err(Stop::Error(e)),
                    }
                }
            }
            // the flatten of a plain non-array is null
            _ => return Ok(Flow::Value(J::Null)),
        },
        Flow::Value(J::Array(elements)) => {
            for element in elements {
                match element {
                    J::Null => {}
                    J::Array(inner) => flows.extend(inner.into_iter().map(|v| Flow::Value(v))),
                    plain => flows.push(Flow::Value(plain)),
                }
            }
        }
        Flow::Value(_) => return Ok(Flow::Value(J::Null)),
    }
    Ok(Flow::Proj(flows))
}

fn flatten_into<'d, N: Node<'d>>(flow: Flow<N>, flows: &mut Vec<Flow<N>>) {
    match flow {
        Flow::Value(J::Null) => {}
        Flow::Value(J::Array(elements)) => {
            for element in elements {
                match element {
                    J::Null => {}
                    J::Array(inner) => flows.extend(inner.into_iter().map(|v| Flow::Value(v))),
                    plain => flows.push(Flow::Value(plain)),
                }
            }
        }
        Flow::Value(plain) => flows.push(Flow::Value(plain)),
        Flow::One(node) => match node.materialize() {
            Ok(J::Null) => {}
            Ok(J::Array(elements)) => {
                for element in elements {
                    match element {
                        J::Null => {}
                        J::Array(inner) => {
                            flows.extend(inner.into_iter().map(|v| Flow::Value(v)))
                        }
                        plain => flows.push(Flow::Value(plain)),
                    }
                }
            }
            Ok(_) => flows.push(Flow::Value(node.materialize().unwrap_or(J::Null))),
            Err(e) => flows.push(Flow::Value(J::Null)),
        },
        Flow::Proj(items) => {
            for item in items {
                flatten_into(item, flows);
            }
        }
    }
}

fn truthy(value: &J) -> bool {
    match value {
        J::Null | J::Bool(false) => false,
        J::String(s) => !s.is_empty(),
        J::Array(items) => !items.is_empty(),
        J::Object(map) => !map.is_empty(),
        // numbers are always true, zero included
        J::Number(_) => true,
        J::Bool(_) => true,
    }
}

fn compare(op: CmpOp, left: &J, right: &J) -> Result<J, Stop> {
    let result = match op {
        CmpOp::Eq => crate::corpus::json_equal(left, right),
        CmpOp::Ne => !crate::corpus::json_equal(left, right),
        CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => {
            // ordering across different kinds (or NaN) is null
            match (left, right) {
                (J::Number(_), J::Number(_)) | (J::String(_), J::String(_)) => {
                    order_compare(op, left, right)?
                }
                _ => return Ok(J::Null),
            }
        }
    };
    Ok(J::Bool(result))
}

fn order_compare(op: CmpOp, left: &J, right: &J) -> Result<bool, Stop> {
    let ordering = match (left, right) {
        (J::Number(a), J::Number(b)) => match (a.as_f64(), b.as_f64()) {
            (Some(a), Some(b)) => a.partial_cmp(&b),
            _ => None,
        },
        (J::String(a), J::String(b)) => Some(a.cmp(b)),
        _ => None,
    };
    use std::cmp::Ordering;
    Ok(match ordering {
        None => false,
        Some(Ordering::Less) => matches!(op, CmpOp::Lt | CmpOp::Le),
        Some(Ordering::Equal) => matches!(op, CmpOp::Le | CmpOp::Ge),
        Some(Ordering::Greater) => matches!(op, CmpOp::Gt | CmpOp::Ge),
    })
}

fn kind_name(value: &J) -> &'static str {
    match value {
        J::Null => "null",
        J::Bool(_) => "boolean",
        J::Number(_) => "number",
        J::String(_) => "string",
        J::Array(_) => "array",
        J::Object(_) => "object",
    }
}

fn eval_function<'d, N: Node<'d>>(
    name: &str,
    args: &[FnArgP],
    node: &N,
) -> Result<Flow<N>, Stop> {
    if !crate::program::function_implemented(name) {
        return Err(Stop::NotImplemented);
    }

    // the expression-referencing functions apply their first argument
    // (the ref) per element of their second (the array)
    if crate::program::function_takes_expression(name) {
        let (ref_program, array_arg) = match args {
            // map takes (&expr, array); the _by family takes (array, &expr)
            [FnArgP::Ref(program), FnArgP::Value(array_program)] if name == "map" => {
                (program, array_program)
            }
            [FnArgP::Value(array_program), FnArgP::Ref(program)] => (program, array_program),
            _ => {
                return Err(Stop::Error(SanshoError::Evaluation {
                    message: format!(
                        "function {name} requires (expression-reference, array) or (array, expression-reference)"
                    ),
                }))
            }
        };
        let array = eval(array_arg, node)?.as_value()?;
        let items = match array {
            J::Array(items) => items,
            other => return Err(type_error(name, kind_name(&other))),
        };
        return match name {
            "map" => {
                let mut mapped = Vec::with_capacity(items.len());
                for item in &items {
                    let flow = eval_value_flow(ref_program, item)?;
                    mapped.push(flow.as_value().map_err(Stop::from)?);
                }
                Ok(Flow::Value(J::Array(mapped)))
            }
            "max_by" | "min_by" => {
                let want_max = name == "max_by";
                let mut best_item: Option<&J> = None;
                let mut best_key: Option<J> = None;
                for item in &items {
                    let flow = eval_value_flow(ref_program, item)?;
                    let key = flow.as_value().map_err(Stop::from)?;
                    check_by_key(&key)?;
                    let take = match &best_key {
                        None => true,
                        Some(current) => {
                            let ordering = order_values(current, &key)?;
                            if want_max {
                                ordering == std::cmp::Ordering::Less
                            } else {
                                ordering == std::cmp::Ordering::Greater
                            }
                        }
                    };
                    if take {
                        best_item = Some(item);
                        best_key = Some(key);
                    }
                }
                Ok(Flow::Value(best_item.cloned().unwrap_or(J::Null)))
            }
            "sort_by" => {
                let mut keyed: Vec<(J, &J)> = Vec::with_capacity(items.len());
                for item in &items {
                    let flow = eval_value_flow(ref_program, item)?;
                    let key = flow.as_value().map_err(Stop::from)?;
                    check_by_key(&key)?;
                    keyed.push((key, item));
                }
                // the keys must be UNIFORMLY typed (the same contract as
                // sort): mixed numbers and strings is invalid-type
                let any_number = keyed.iter().any(|(k, _)| matches!(k, J::Number(_)));
                let any_string = keyed.iter().any(|(k, _)| matches!(k, J::String(_)));
                if any_number && any_string {
                    return Err(type_error(name, "mixed-type keys"));
                }
                keyed.sort_by(|a, b| {
                    order_values(&a.0, &b.0).unwrap_or(std::cmp::Ordering::Equal)
                });
                Ok(Flow::Value(J::Array(
                    keyed.into_iter().map(|(_, item)| item.clone()).collect(),
                )))
            }
            _ => unreachable!("function_takes_expression gates this match"),
        };
    }

    // the value-argument functions: every argument evaluates eagerly;
    // an expression reference here is an invalid type
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        match arg {
            FnArgP::Value(program) => {
                values.push(eval(program, node).and_then(|flow| Ok(flow.as_value()?))?)
            }
            FnArgP::Ref(_) => {
                return Err(Stop::Error(SanshoError::Evaluation {
                    message: format!("function {name} does not accept expression references"),
                }))
            }
        }
    }
    let values = values;
    match (name, values.as_slice()) {
        ("length", [one]) => match one {
            J::String(s) => Ok(Flow::Value(J::Number((s.chars().count() as u64).into()))),
            J::Array(items) => Ok(Flow::Value(J::Number((items.len() as u64).into()))),
            J::Object(map) => Ok(Flow::Value(J::Number((map.len() as u64).into()))),
            other => Err(type_error("length", kind_name(other))),
        },
        ("type", [one]) => Ok(Flow::Value(J::String(kind_name(one).to_string()))),
        ("keys", [one]) => match one {
            J::Object(map) => Ok(Flow::Value(J::Array(
                map.keys().map(|k| J::String(k.clone())).collect(),
            ))),
            other => Err(type_error("keys", kind_name(other))),
        },
        ("values", [one]) => match one {
            J::Object(map) => Ok(Flow::Value(J::Array(map.values().cloned().collect()))),
            other => Err(type_error("values", kind_name(other))),
        },
        ("starts_with", [subject, prefix]) => {
            string_predicate("starts_with", subject, prefix, |s, p| s.starts_with(p))
        }
        ("ends_with", [subject, suffix]) => {
            string_predicate("ends_with", subject, suffix, |s, p| s.ends_with(p))
        }
        ("contains", [subject, needle]) => match (subject, needle) {
            (J::String(s), J::String(n)) => {
                Ok(Flow::Value(J::Bool(s.contains(n.as_str()))))
            }
            (J::Array(items), needle) => Ok(Flow::Value(J::Bool(
                items.iter().any(|item| crate::corpus::json_equal(item, needle)),
            ))),
            (other, _) => Err(type_error("contains", kind_name(other))),
        },
        // numeric single-array functions
        ("abs", [one]) => numeric("abs", one, f64::abs),
        ("ceil", [one]) => numeric("ceil", one, f64::ceil),
        ("floor", [one]) => numeric("floor", one, f64::floor),
        ("avg" | "mean", [one]) => match one {
            J::Array(items) => {
                if items.is_empty() {
                    return Ok(Flow::Value(J::Null));
                }
                let mut sum = 0.0;
                for item in items {
                    sum += number_of(item, "avg")?;
                }
                Ok(Flow::Value(J::Number(
                    serde_json::Number::from_f64(sum / items.len() as f64)
                        .unwrap_or_else(|| serde_json::Number::from(0u64)),
                )))
            }
            other => Err(type_error("avg", kind_name(other))),
        },
        ("sum", [one]) => match one {
            J::Array(items) => {
                let mut sum = 0.0;
                for item in items {
                    sum += number_of(item, "sum")?;
                }
                Ok(Flow::Value(J::Number(
                    serde_json::Number::from_f64(sum)
                        .unwrap_or_else(|| serde_json::Number::from(0u64)),
                )))
            }
            other => Err(type_error("sum", kind_name(other))),
        },
        ("max" | "min", [one]) => {
            let want_max = name == "max";
            match one {
                J::Array(items) => {
                    if items.is_empty() {
                        return Ok(Flow::Value(J::Null));
                    }
                    let mut best: Option<&J> = None;
                    for item in items {
                        let take = match best {
                            None => true,
                            Some(current) => {
                                let ordering = order_values(current, item)?;
                                if want_max {
                                    ordering == std::cmp::Ordering::Less
                                } else {
                                    ordering == std::cmp::Ordering::Greater
                                }
                            }
                        };
                        if take {
                            best = Some(item);
                        }
                    }
                    Ok(Flow::Value(best.cloned().unwrap_or(J::Null)))
                }
                other => Err(type_error(name, kind_name(other))),
            }
        }
        ("sort", [one]) => match one {
            J::Array(items) => {
                let all_numbers = items.iter().all(|i| matches!(i, J::Number(_)));
                let all_strings = items.iter().all(|i| matches!(i, J::String(_)));
                if !all_numbers && !all_strings {
                    return Err(type_error("sort", "mixed-type array"));
                }
                let mut sorted = items.clone();
                sorted.sort_by(|a, b| {
                    order_values(a, b).unwrap_or(std::cmp::Ordering::Equal)
                });
                Ok(Flow::Value(J::Array(sorted)))
            }
            other => Err(type_error("sort", kind_name(other))),
        },
        ("reverse", [one]) => match one {
            J::Array(items) => {
                let mut reversed = items.clone();
                reversed.reverse();
                Ok(Flow::Value(J::Array(reversed)))
            }
            J::String(text) => Ok(Flow::Value(J::String(
                text.chars().rev().collect(),
            ))),
            other => Err(type_error("reverse", kind_name(other))),
        },
        // string/number conversions
        ("to_string", [one]) => Ok(Flow::Value(match one {
            J::String(s) => J::String(s.clone()),
            J::Null => J::String(String::new()),
            other => J::String(other.to_string()),
        })),
        ("to_number", [one]) => Ok(Flow::Value(match one {
            J::Number(n) => J::Number(n.clone()),
            J::String(s) => {
                let trimmed = s.trim();
                match trimmed.parse::<f64>() {
                    Ok(value) => serde_json::Number::from_f64(value)
                        .map(J::Number)
                        .unwrap_or(J::Null),
                    Err(_) => J::Null,
                }
            }
            _ => J::Null,
        })),
        ("to_array", [one]) => Ok(Flow::Value(match one {
            J::Array(_) => one.clone(),
            other => J::Array(vec![other.clone()]),
        })),
        // the variadic object merge: later arguments win
        ("merge", many) => {
            let mut object = serde_json::Map::new();
            for argument in many {
                match argument {
                    J::Object(map) => {
                        for (key, value) in map {
                            object.insert(key.clone(), value.clone());
                        }
                    }
                    other => return Err(type_error("merge", kind_name(other))),
                }
            }
            Ok(Flow::Value(J::Object(object)))
        }
        // the first non-null argument; all-null yields null
        ("not_null", many) => {
            for argument in many {
                if !matches!(argument, J::Null) {
                    return Ok(Flow::Value(argument.clone()));
                }
            }
            Ok(Flow::Value(J::Null))
        }
        // string join over an array of strings
        ("join", [separator, items]) => match (separator, items) {
            (J::String(sep), J::Array(items)) => {
                let mut parts = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        J::String(text) => parts.push(text.clone()),
                        other => return Err(type_error("join", kind_name(other))),
                    }
                }
                Ok(Flow::Value(J::String(parts.join(sep))))
            }
            (other, _) => Err(type_error("join", kind_name(other))),
        },
        _ => Err(Stop::NotImplemented),
    }
}

/// The `_by` functions' key contract: numbers or strings only; null
/// (a missing by-value), booleans, and containers are invalid-type (the
/// corpus's referee; mixed key types fail the ordering).
fn check_by_key(key: &J) -> Result<(), Stop> {
    match key {
        J::Number(_) | J::String(_) => Ok(()),
        other => Err(type_error("sort_by/max_by/min_by", kind_name(other))),
    }
}

fn numeric<'d, N: Node<'d>>(
    name: &str,
    value: &J,
    function: fn(f64) -> f64,
) -> Result<Flow<N>, Stop> {
    let input = number_of(value, name)?;
    let result = function(input);
    Ok(Flow::Value(
        serde_json::Number::from_f64(result)
            .map(J::Number)
            .unwrap_or(J::Null),
    ))
}

fn number_of(value: &J, name: &str) -> Result<f64, Stop> {
    value
        .as_f64()
        .filter(|v| !v.is_nan())
        .ok_or_else(|| type_error(name, kind_name(value)))
}

fn order_values(left: &J, right: &J) -> Result<std::cmp::Ordering, Stop> {
    match (left, right) {
        (J::Number(a), J::Number(b)) => match (a.as_f64(), b.as_f64()) {
            (Some(a), Some(b)) => a
                .partial_cmp(&b)
                .ok_or_else(|| Stop::Error(SanshoError::Evaluation {
                    message: "cannot order NaN".to_string(),
                })),
            _ => Err(Stop::Error(SanshoError::Evaluation {
                message: "cannot order numbers".to_string(),
            })),
        },
        (J::String(a), J::String(b)) => Ok(a.cmp(b)),
        _ => Err(Stop::Error(SanshoError::Evaluation {
            message: format!(
                "cannot order {} and {}",
                kind_name(left),
                kind_name(right)
            ),
        })),
    }
}

fn string_predicate<'d, N: Node<'d>>(
    name: &str,
    subject: &J,
    argument: &J,
    predicate: fn(&str, &str) -> bool,
) -> Result<Flow<N>, Stop> {
    match (subject, argument) {
        (J::String(s), J::String(p)) => Ok(Flow::Value(J::Bool(predicate(s, p)))),
        (other, _) => Err(type_error(name, kind_name(other))),
    }
}

fn type_error(function: &str, found: &str) -> Stop {
    Stop::Error(SanshoError::Evaluation {
        message: format!("function {function} received invalid type: {found}"),
    })
}

/// The specification's slice algorithm: clamp bounds per sign of step;
/// negative steps walk the array in reverse. Returns indices to take.
fn slice_indices(len: i64, spec: &crate::ast::SliceSpec) -> Result<Vec<i64>, Stop> {
    let step = spec.step.unwrap_or(1);
    if step == 0 {
        return Err(Stop::Error(SanshoError::Evaluation {
            message: "slice step cannot be zero".to_string(),
        }));
    }
    let clamp = |v: i64| if v < 0 { v + len } else { v };
    let mut indices = Vec::new();
    if step > 0 {
        let start = match spec.start {
            Some(v) => clamp(v).clamp(0, len),
            None => 0,
        };
        let stop = match spec.stop {
            Some(v) => clamp(v).clamp(0, len),
            None => len,
        };
        let mut i = start;
        while i < stop {
            indices.push(i);
            i += step;
        }
    } else {
        let start = match spec.start {
            Some(v) => clamp(v).clamp(-1, len - 1),
            None => len - 1,
        };
        let stop = match spec.stop {
            Some(v) => clamp(v).clamp(-1, len),
            None => -1,
        };
        let mut i = start;
        while i > stop {
            indices.push(i);
            i += step;
        }
    }
    Ok(indices)
}
