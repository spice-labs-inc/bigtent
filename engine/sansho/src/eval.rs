//! The evaluator: runs a compiled walk program against a document,
//! performing EVERY data access through the [`SanshoTrait`]. The trait is
//! the ONLY thing carried through program execution: the evaluator's
//! functions take a trait value (a `&(dyn SanshoTrait<'a> + 'a)`) and
//! return one (`Box<dyn SanshoTrait<'a> + 'a>`); navigation returns
//! trait-bounded values, so no other value carrier exists.
//!
//! Evaluation state is internal bookkeeping: the values being walked are
//! trait values (a member is a trait value, borrowed from the document
//! where the format stores it), and a projection is a collection of them
//! (`Vec<Box<dyn SanshoTrait<'a> + 'a>>` — itself a trait value). The
//! document's bytes are decoded only at the materialization boundary.
//!
//! - a value navigates lazily (key, index) or projects (wildcards,
//!   slice, filter, flatten) — the traversal itself never decodes;
//! - every edge after a projection applies per element: the REMAINING
//!   spine is evaluated per kept element, preserving shape (nested
//!   projections stay nested);
//! - an element whose result is null drops out of the projection (an
//!   empty projection stays: a wildcard on a scalar is a valid empty row);
//! - the flatten edge alone dissolves arrays one level into the parent
//!   projection;
//! - navigation misses evaluate to null (the specification's null
//!   semantics: no omission, no defaulting);
//! - the value-level constructs (functions, comparators, logic, literals)
//!   read their inputs through the trait's probes — a computed scalar is a
//!   new value, never a decode of the document.
//!
//! The public entry is generic over the trait: pass any Sansho source
//! (`&[u8]`, `&serde_json::Value`, `&serde_cbor::Value`, `&Item`, a
//! scalar) boxed as a trait value, and get the materialized result.

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::ast::CmpOp;

use crate::error::SanshoError;
use crate::program::{Edge, FnArgP, Program};
use crate::view::{Kind, SanshoCarrier, SanshoTrait};
use serde_json::Value as SerdeJson;

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

impl Stop {
    pub fn into_error(self) -> SanshoError {
        match self {
            Stop::Error(e) => e,
            Stop::NotImplemented => SanshoError::Compile {
                message: "expression uses constructs not yet implemented".to_string(),
            },
        }
    }
}
/// The evaluation's internal flow: a value at this level, or a grouping of
/// flows at this level.
///
/// This is bookkeeping for the evaluation — never part of the trait, never
/// returned by the public entry — and it holds only trait values, so nothing
/// is copied and no `serde_json` value appears inside an evaluation. The two
/// shapes are what the projection semantics turn on: a grouping takes an edge
/// per item and its items stay at their own level, while a value takes the
/// edge directly.
pub(crate) enum Flow<'a> {
    /// One value at this level.
    Value(SanshoCarrier<'a>),
    /// A grouping at this level: the per-item results.
    Items(Vec<Flow<'a>>),
}

/// The emit: a flow as a trait value. A grouping becomes a collection value —
/// a refcount around the vector of its items — so no data is copied.
pub(crate) fn flow_value<'a>(flow: Flow<'a>) -> SanshoCarrier<'a> {
    match flow {
        Flow::Value(value) => value,
        Flow::Items(items) => Rc::new(
            items
                .into_iter()
                .map(flow_value)
                .collect::<Vec<SanshoCarrier<'a>>>(),
        ),
    }
}

/// The program evaluation: the ONLY data carrier is the trait value being
/// walked; a spine's result may be a grouping.
pub(crate) fn eval<'a>(program: &Program, node: SanshoCarrier<'a>) -> Result<Flow<'a>, Stop> {
    match program {
        Program::Literal(value) => Ok(Flow::Value(Rc::new(value.clone()))),
        Program::Spine(edges) => eval_spine(node, &edges.edges),
        Program::MultiList(slots) => {
            if node.is_null() {
                return Ok(Flow::Value(null_value()));
            }
            let mut items: Vec<SanshoCarrier<'a>> = Vec::with_capacity(slots.len());
            for slot in slots {
                let found = eval(slot, node.clone())?;
                items.push(flow_value(found));
            }
            Ok(Flow::Value(Rc::new(items)))
        }
        Program::MultiHash(entries) => {
            if node.is_null() {
                return Ok(Flow::Value(null_value()));
            }
            let mut object = HashMap::<String, SanshoCarrier<'a>>::new();
            for (key, slot) in entries {
                let value = eval(slot, node.clone())?;
                object.insert(key.clone(), flow_value(value));
            }
            Ok(Flow::Value(Rc::new(object) as SanshoCarrier<'a>))
        }
        Program::Pipe(left, right) => {
            let left_value = flow_value(eval(left, node)?);
            eval(right, left_value)
        }
        Program::Function(name, args) => eval_function(name, args, node).map(Flow::Value),
        Program::Compare(op, left, right) => {
            let left_value = flow_value(eval(left, node.clone())?);
            let right_value = flow_value(eval(right, node)?);
            compare(*op, &left_value, &right_value).map(Flow::Value)
        }
        Program::And(left, right) => {
            let left_value = flow_value(eval(left, node.clone())?);
            if truthy(&left_value) {
                eval(right, node)
            } else {
                Ok(Flow::Value(left_value))
            }
        }
        Program::Or(left, right) => {
            let left_value = flow_value(eval(left, node.clone())?);
            if truthy(&left_value) {
                Ok(Flow::Value(left_value))
            } else {
                eval(right, node)
            }
        }
        Program::Not(inner) => {
            let value = !truthy(&flow_value(eval(inner, node)?));
            Ok(Flow::Value(Rc::new(value)))
        }
    }
}

/// Walk a spine over a trait value. The FIRST edge applies to the value; a
/// projecting edge collects its elements into a grouping and the REMAINING
/// edges then apply at that collection's level; otherwise the chain walks the
/// single position.
fn eval_spine<'a>(node: SanshoCarrier<'a>, edges: &[Edge]) -> Result<Flow<'a>, Stop> {
    match edges {
        [] => Ok(Flow::Value(node)),
        [edge, rest @ ..] => eval_edge(node, edge, rest),
    }
}

/// Apply ONE edge to a trait value, then the remaining edges.
fn eval_edge<'a>(node: SanshoCarrier<'a>, edge: &Edge, rest: &[Edge]) -> Result<Flow<'a>, Stop> {
    match edge {
        Edge::Compute(program) => {
            // a value-construct spliced into the chain: the program evaluates
            // over the current value and the result continues as a VALUE (a
            // computed value has no backing position)
            let flow = eval(program, node)?;
            continue_flow(flow, rest)
        }
        Edge::Key(name) => match node.kind() {
            // the specification's null semantics: a miss on a value that is
            // not an object is null, never an error
            Kind::Object => match node.get_key(name).map_err(Stop::from)? {
                Some(child) => continue_single(child, rest),
                None => Ok(Flow::Value(null_value())),
            },
            _ => Ok(Flow::Value(null_value())),
        },
        Edge::Index(index) => match node.kind() {
            // a miss on a value that is not an array is null
            Kind::Array => {
                let len = node.container_len().unwrap_or(0) as i64;
                let effective = if *index < 0 { len + index } else { *index };
                let resolved = if effective >= 0 && effective < len {
                    node.get_index(effective as usize).map_err(Stop::from)?
                } else {
                    None
                };
                match resolved {
                    Some(child) => continue_single(child, rest),
                    None => Ok(Flow::Value(null_value())),
                }
            }
            _ => Ok(Flow::Value(null_value())),
        },
        // the dot form reads object values only
        Edge::WildcardDot => match node.kind() {
            Kind::Object => {
                let mut elements: Vec<SanshoCarrier<'a>> = Vec::new();
                for (_, member) in node.as_object().map_err(Stop::from)? {
                    elements.push(member);
                }
                project(elements, rest)
            }
            _ => Ok(Flow::Value(null_value())),
        },
        // the bracket form reads array elements only
        Edge::WildcardBracket => match node.kind() {
            Kind::Array => project(node.as_elements().map_err(Stop::from)?, rest),
            _ => Ok(Flow::Value(null_value())),
        },
        Edge::Filter(predicate) => match node.kind() {
            Kind::Array => project(filter_elements(node, predicate)?, rest),
            _ => Ok(Flow::Value(null_value())),
        },
        // the slice is a projection over the selected range (arrays), or the
        // sliced string (strings)
        Edge::Slice(spec) => match node.kind() {
            Kind::Array => {
                let len = node.container_len().unwrap_or(0) as i64;
                let selected = slice_indices(len, spec)?;
                let mut elements: Vec<SanshoCarrier<'a>> = Vec::with_capacity(selected.len());
                for index in selected {
                    if let Some(element) = node.get_index(index as usize).map_err(Stop::from)? {
                        elements.push(element);
                    }
                }
                project(elements, rest)
            }
            Kind::String => {
                let text = node.as_str().map(|s| s.to_string()).unwrap_or_default();
                let chars: Vec<char> = text.chars().collect();
                let selected = slice_indices(chars.len() as i64, spec)?;
                let sliced: String = selected
                    .into_iter()
                    .filter_map(|i| chars.get(i as usize))
                    .collect();
                Ok(Flow::Value(Rc::new(sliced)))
            }
            _ => Ok(Flow::Value(null_value())),
        },
        Edge::Flatten => {
            // the flatten of the current value: dissolve array-valued results
            // one level
            let flow = flatten_value(node)?;
            continue_flow(flow, rest)
        }
    }
}

/// Collect the elements into a grouping at their own level, then apply the
/// REMAINING edges at that collection's level — the two-step split that keeps
/// a projection from adding a level per edge.
fn project<'a>(elements: Vec<SanshoCarrier<'a>>, rest: &[Edge]) -> Result<Flow<'a>, Stop> {
    let flow = Flow::Items(elements.into_iter().map(Flow::Value).collect());
    continue_flow(flow, rest)
}

/// Continue the spine on a SINGLE member position (a key/index hit): the
/// remaining edges walk the member; an empty remainder is the member itself.
fn continue_single<'a>(child: SanshoCarrier<'a>, rest: &[Edge]) -> Result<Flow<'a>, Stop> {
    match rest {
        [] => Ok(Flow::Value(child)),
        [edge, more @ ..] => eval_edge(child, edge, more),
    }
}

/// Continue the spine on a FLOW: the remaining edges apply; an empty remainder
/// passes the flow through.
fn continue_flow<'a>(flow: Flow<'a>, rest: &[Edge]) -> Result<Flow<'a>, Stop> {
    match rest {
        [] => Ok(flow),
        [edge, more @ ..] => eval_flow(flow, edge, more),
    }
}

/// Apply an edge to a flow. A VALUE takes the edge directly. A GROUPING takes
/// it per item, at each item's own depth (a nested grouping keeps its shape),
/// and the results stay at this level — so the level count does not grow with
/// the number of projecting edges. A null value item drops; an empty grouping
/// item stays (a wildcard on a scalar inside a projection is a valid empty
/// row). The flatten dissolves array-valued items one level INTO this
/// collection.
fn eval_flow<'a>(flow: Flow<'a>, edge: &Edge, rest: &[Edge]) -> Result<Flow<'a>, Stop> {
    match edge {
        Edge::Flatten => {
            // a computed flatten: dissolve arrays one level into this collection
            let mut flows = Vec::new();
            flatten_into(flow, &mut flows)?;
            continue_flow(Flow::Items(flows), rest)
        }
        _ => match flow {
            Flow::Value(value) => eval_edge(value, edge, rest),
            Flow::Items(items) => {
                let mut results = Vec::new();
                for item in items {
                    collect_projected(item, edge, &mut results)?;
                }
                continue_flow(Flow::Items(results), rest)
            }
        },
    }
}

/// Apply the edge to ONE flow item, collecting the result at the collection's
/// level: a value item's edge result joins the collection (a null result
/// drops); a nested grouping keeps its shape — the edge applies to each of its
/// items and the results stay one item.
fn collect_projected<'a>(
    item: Flow<'a>,
    edge: &Edge,
    results: &mut Vec<Flow<'a>>,
) -> Result<(), Stop> {
    match item {
        Flow::Value(value) => match eval_edge(value, edge, &[])? {
            Flow::Value(result) if result.is_null() => Ok(()),
            flow => {
                results.push(flow);
                Ok(())
            }
        },
        Flow::Items(items) => {
            let mut nested = Vec::new();
            for item in items {
                collect_projected(item, edge, &mut nested)?;
            }
            results.push(Flow::Items(nested));
            Ok(())
        }
    }
}

/// The `[]` operator on a single value: dissolve array-valued elements ONE
/// level into the collection; null elements drop; a plain element passes
/// through; a plain non-array flattens to null.
fn flatten_value<'a>(node: SanshoCarrier<'a>) -> Result<Flow<'a>, Stop> {
    let mut flows = Vec::new();
    match node.kind() {
        Kind::Array => {
            for element in node.as_elements().map_err(Stop::from)? {
                if element.is_null() {
                    continue;
                }
                if element.kind() == Kind::Array {
                    for inner in element.as_elements().map_err(Stop::from)? {
                        flows.push(Flow::Value(inner));
                    }
                } else {
                    flows.push(Flow::Value(element));
                }
            }
        }
        _ => return Ok(Flow::Value(null_value())),
    }
    Ok(Flow::Items(flows))
}

/// The flatten of a flow: array-valued items dissolve ONE level into the
/// collection, and a grouping dissolves its items so the collection stays flat
/// (the double-bracket to flat rule).
fn flatten_into<'a>(flow: Flow<'a>, flows: &mut Vec<Flow<'a>>) -> Result<(), Stop> {
    match flow {
        Flow::Value(value) => {
            if value.is_null() {
                return Ok(());
            }
            if value.kind() == Kind::Array {
                for element in value.as_elements().map_err(Stop::from)? {
                    if element.is_null() {
                        continue;
                    }
                    if element.kind() == Kind::Array {
                        for inner in element.as_elements().map_err(Stop::from)? {
                            flows.push(Flow::Value(inner));
                        }
                    } else {
                        flows.push(Flow::Value(element));
                    }
                }
            } else {
                flows.push(Flow::Value(value));
            }
        }
        Flow::Items(items) => {
            for item in items {
                flatten_into(item, flows)?;
            }
        }
    }
    Ok(())
}

/// The filter's kept elements. The hoisted leaf-predicate fast path
/// compares the borrowed text directly — the relaxed contract: an
/// element whose text the format cannot validate, and a byte string,
/// fall through to the general path and are dropped, never errored.
fn filter_elements<'a>(
    node: SanshoCarrier<'a>,
    predicate: &Program,
) -> Result<Vec<SanshoCarrier<'a>>, Stop> {
    let leaf = leaf_predicate_shape(predicate);
    let mut kept: Vec<SanshoCarrier<'a>> = Vec::new();
    for element in node.as_elements().map_err(Stop::from)? {
        let keep = match &leaf {
            Some((name, prefix)) if element.kind() == Kind::String => match element.as_str() {
                Ok(text) => match name.as_str() {
                    "starts_with" => text.starts_with(prefix.as_str()),
                    "ends_with" => text.ends_with(prefix.as_str()),
                    _ => text.contains(prefix.as_str()),
                },
                Err(_) => truthy(&flow_value(eval(predicate, element.clone())?)),
            },
            _ => truthy(&flow_value(eval(predicate, element.clone())?)),
        };
        if keep {
            kept.push(element);
        }
    }
    Ok(kept)
}

/// The null value: a navigation miss, or an edge that does not apply.
fn null_value<'a>() -> SanshoCarrier<'a> {
    Rc::new(SerdeJson::Null)
}

/// The specification's truthiness: false for null and false, an empty
/// string, an empty array, an empty object; true for numbers (zero
/// included) and true.
pub fn truthy<'a>(value: &(dyn SanshoTrait<'a> + 'a)) -> bool {
    match value.kind() {
        Kind::Null => false,
        Kind::Bool => value.as_bool().unwrap_or(false),
        Kind::String => value.as_str().map(|s| !s.is_empty()).unwrap_or(false),
        Kind::Array | Kind::Object => value.container_len().map(|len| len > 0).unwrap_or(false),
        Kind::Number => true,
        Kind::Bytes => true,
    }
}

/// Value equality over the trait: the same kind and the same content,
/// recursively for containers. Numbers compare as doubles (the
/// specification's reduction); objects compare by key and member.
fn trait_equal<'a>(left: &SanshoCarrier<'a>, right: &SanshoCarrier<'a>) -> Result<bool, Stop> {
    if left.kind() != right.kind() {
        return Ok(false);
    }
    match left.kind() {
        Kind::Null => Ok(true),
        Kind::Bool => Ok(left.as_bool() == right.as_bool()),
        Kind::Number => match (left.as_f64(), right.as_f64()) {
            (Ok(a), Ok(b)) => Ok(a == b),
            _ => Ok(false),
        },
        Kind::String => Ok(left.as_str() == right.as_str()),
        Kind::Array => {
            let a = left.as_elements()?;
            let Ok(b) = right.as_elements() else {
                return Ok(false);
            };
            Ok(a.len() == b.len() && {
                for (va, vb) in a.iter().zip(&b) {
                    if !trait_equal(va, vb)? {
                        return Ok(false);
                    }
                }
                true
            })
        }
        Kind::Object => {
            // a JSON object is an UNORDERED collection of name/value pairs, so
            // the comparison cannot depend on either side's member order: the
            // sources each iterate in their own order (a sorted map, a
            // document's own order, a CBOR map's order), and a positional
            // comparison calls two equal objects different whenever those
            // orders differ

            let a = left.as_object()?;
            let Ok(b) = right.as_object() else {
                return Ok(false);
            };
            Ok(a.len() == b.len() && {
                for (k, v) in a {
                    let Some(vb) = b.get(k) else {
                        return Ok(false);
                    };
                    if !trait_equal(&v, vb)? {
                        return Ok(false);
                    }
                }
                true
            })
        }
        Kind::Bytes => {
            let Ok(a) = left.as_bytes() else {
                return Ok(false);
            };
            let Ok(b) = right.as_bytes() else {
                return Ok(false);
            };

            Ok(a == b)
        }
    }
}

pub fn compare<'a>(
    op: CmpOp,
    left: &SanshoCarrier<'a>,
    right: &SanshoCarrier<'a>,
) -> Result<SanshoCarrier<'a>, Stop> {
    let result = match op {
        CmpOp::Eq => trait_equal(left, right)?,
        CmpOp::Ne => !trait_equal(left, right)?,
        CmpOp::Lt | CmpOp::Le | CmpOp::Gt | CmpOp::Ge => {
            // ordering across different kinds (or NaN) is null
            match (left.kind(), right.kind()) {
                (Kind::Number, Kind::Number) | (Kind::String, Kind::String) => {
                    order_compare(op, left, right)?
                }
                _ => return Ok(null_value()),
            }
        }
    };
    Ok(Rc::new(result))
}

pub fn order_compare<'a>(
    op: CmpOp,
    left: &SanshoCarrier<'a>,
    right: &SanshoCarrier<'a>,
) -> Result<bool, Stop> {
    let ordering = match (left.kind(), right.kind()) {
        (Kind::Number, Kind::Number) => match (left.as_f64(), right.as_f64()) {
            (Ok(a), Ok(b)) => a.partial_cmp(&b),
            _ => None,
        },
        (Kind::String, Kind::String) => match (left.as_str(), right.as_str()) {
            (Ok(a), Ok(b)) => Some(a.cmp(b)),
            _ => None,
        },
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

/// The kind's name as the language spells it (`type(@)` and the
/// invalid-type errors).
pub fn kind_name<'a>(value: SanshoCarrier<'a>) -> &'static str {
    match value.kind() {
        Kind::Null => "null",
        Kind::Bool => "boolean",
        Kind::Number => "number",
        Kind::String => "string",
        Kind::Array => "array",
        Kind::Object => "object",
        Kind::Bytes => "bytes",
    }
}

fn eval_function<'a>(
    name: &str,
    args: &[FnArgP],
    node: SanshoCarrier<'a>,
) -> Result<SanshoCarrier<'a>, Stop> {
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
                }));
            }
        };
        let array = flow_value(eval(array_arg, node.clone())?);
        if array.kind() != Kind::Array {
            return Err(type_error(name, kind_name(array)));
        }
        let items = array.as_elements()?;
        return match name {
            "map" => {
                let mut mapped = Vec::with_capacity(items.len());
                for item in items {
                    mapped.push(flow_value(eval(ref_program, item)?));
                }
                Ok(Rc::new(mapped))
            }
            "max_by" | "min_by" => {
                let want_max = name == "max_by";
                let mut best: Option<(SanshoCarrier<'a>, SanshoCarrier<'a>)> = None;
                for item in items {
                    let key = flow_value(eval(ref_program, item.clone())?);
                    check_by_key(key.clone())?;
                    let take = match &best {
                        None => true,
                        Some((_, current)) => {
                            let ordering = order_values(current.clone(), key.clone())?;
                            if want_max {
                                ordering == std::cmp::Ordering::Less
                            } else {
                                ordering == std::cmp::Ordering::Greater
                            }
                        }
                    };
                    if take {
                        best = Some((item, key));
                    }
                }
                Ok(best.map(|(item, _)| item).unwrap_or_else(|| null_value()))
            }
            "sort_by" => {
                let mut keyed: Vec<(SanshoCarrier<'a>, SanshoCarrier<'a>)> =
                    Vec::with_capacity(items.len());
                for item in items {
                    let key = flow_value(eval(ref_program, item.clone())?);
                    check_by_key(key.clone())?;
                    keyed.push((key, item));
                }
                // the keys must be UNIFORMLY typed (the same contract as
                // sort): mixed numbers and strings is invalid-type
                let any_number = keyed.iter().any(|(k, _)| k.kind() == Kind::Number);
                let any_string = keyed.iter().any(|(k, _)| k.kind() == Kind::String);
                if any_number && any_string {
                    return Err(type_error(name, "mixed-type keys"));
                }
                keyed.sort_by(|a, b| {
                    order_values(a.0.clone(), b.0.clone()).unwrap_or(std::cmp::Ordering::Equal)
                });
                Ok(Rc::new(
                    keyed.into_iter().map(|(_, item)| item).collect::<Vec<_>>(),
                ))
            }
            _ => unreachable!("function_takes_expression gates this match"),
        };
    }

    // the length-over-container fast path: an array or object node's
    // element count is a HEADER read — the whole subtree must NOT
    // materialize to count its elements.
    if name == "length"
        && let [FnArgP::Value(single)] = args
    {
        let value = flow_value(eval(single, node.clone())?);
        // a string's length is its character count, not a container's
        // member count: only the containers take the header fast path
        if matches!(value.kind(), Kind::Array | Kind::Object) {
            return value
                .container_len()
                .map_err(Stop::from)
                .map(|v| Rc::new(v as i64) as SanshoCarrier<'a>);
        }
    }

    // the value-argument functions: every argument evaluates eagerly; an
    // expression reference here is an invalid type
    let mut values: Vec<SanshoCarrier<'a>> = Vec::with_capacity(args.len());
    for arg in args {
        match arg {
            FnArgP::Value(program) => values.push(flow_value(eval(program, node.clone())?)),
            FnArgP::Ref(_) => {
                return Err(Stop::Error(SanshoError::Evaluation {
                    message: format!("function {name} does not accept expression references"),
                }));
            }
        }
    }

    // the argument-passing functions hand an argument through unchanged:
    // no copy of the document's data
    if name == "to_array" && values.len() == 1 {
        let one = values.into_iter().next().expect("one argument");
        return Ok(match one.kind() {
            Kind::Array => one,
            _ => Rc::new(vec![one]),
        });
    }
    if name == "not_null" {
        for value in values {
            if !value.is_null() {
                return Ok(value);
            }
        }
        return Ok(null_value());
    }
    if name == "merge" {
        let mut merged = HashMap::<String, SanshoCarrier<'a>>::new();
        for value in values.iter() {
            if value.kind() != Kind::Object {
                return Err(type_error("merge", kind_name(value.clone())));
            }
            let entries = value.as_object()?;
            for (key, member) in entries {
                // later arguments win
                merged.insert(key.to_string(), member.clone());
            }
        }
        return Ok(Rc::new(merged));
    }

    match (name, (values.first().cloned(), values.get(1))) {
        ("length", (Some(one), _)) => match one.kind() {
            Kind::String => Ok(Rc::new(string_length(one.clone()) as u64)),
            Kind::Array | Kind::Object => one
                .container_len()
                .map_err(Stop::from)
                .map(|v| Rc::new(v as u64) as SanshoCarrier<'a>),
            _ => Err(type_error("length", kind_name(one.clone()))),
        },
        ("type", (Some(one), _)) => Ok(Rc::new(kind_name(one.clone()).to_string())),
        ("keys", (Some(one), _)) => match one.kind() {
            Kind::Object => {
                // the key text borrows the document: no key string is built

                let keys: HashSet<String> =
                    one.as_object()?.keys().map(|key| key.to_string()).collect();
                Ok(Rc::new(keys))
            }
            _ => Err(type_error("keys", kind_name(one.clone()))),
        },
        ("values", (Some(one), _)) => match one.kind() {
            Kind::Object => {
                let members: Vec<SanshoCarrier<'a>> =
                    one.as_object()?.values().cloned().collect();
                Ok(Rc::new(members))
            }
            _ => Err(type_error("values", kind_name(one.clone()))),
        },
        ("starts_with", (Some(subject), Some(prefix))) => {
            string_predicate("starts_with", subject.clone(), prefix.clone(), |s, p| {
                s.starts_with(p)
            })
        }
        ("ends_with", (Some(subject), Some(suffix))) => {
            string_predicate("ends_with", subject.clone(), suffix.clone(), |s, p| {
                s.ends_with(p)
            })
        }
        ("contains", (Some(subject), Some(needle))) => match subject.kind() {
            Kind::String => match (subject.as_str(), needle.as_str()) {
                (Ok(s), Ok(n)) => Ok(Rc::new(s.contains(n)) as SanshoCarrier<'a>),
                _ => Err(type_error("contains", kind_name(subject.clone()))),
            },
            Kind::Array => {
                let found = {
                    let mut f = false;
                    for v in subject.as_elements()? {
                        if trait_equal(&v, needle)? {
                            f = true;
                            break;
                        }
                    }
                    f
                };
                Ok(Rc::new(found) as SanshoCarrier<'a>)
            }
            _ => Err(type_error("contains", kind_name(subject.clone()))),
        },
        // numeric single-array functions
        ("abs", (Some(one), None)) => numeric("abs", one.clone(), f64::abs),
        ("ceil", (Some(one), None)) => numeric("ceil", one.clone(), f64::ceil),
        ("floor", (Some(one), None)) => numeric("floor", one.clone(), f64::floor),
        ("avg" | "mean", (Some(one), None)) => match one.kind() {
            Kind::Array => {
                let items = one.as_elements()?;
                if items.is_empty() {
                    return Ok(null_value());
                }
                let mut sum = 0.0;
                for item in &items {
                    sum += number_of(item.clone(), "avg")?;
                }
                Ok(number_value(sum / items.len() as f64))
            }
            _ => Err(type_error("avg", kind_name(Rc::new(one)))),
        },
        ("sum", (Some(one), None)) => match one.kind() {
            Kind::Array => {
                let mut sum = 0.0;
                for item in one.as_elements()? {
                    sum += number_of(item, "sum")?;
                }
                Ok(number_value(sum))
            }
            _ => Err(type_error("sum", kind_name(one.clone()))),
        },
        ("max" | "min", (Some(one), None)) => {
            let want_max = name == "max";
            match one.kind() {
                Kind::Array => {
                    let items = one.as_elements()?;

                    if items.is_empty() {
                        return Ok(null_value());
                    }
                    let mut best: Option<SanshoCarrier<'a>> = None;
                    for item in items {
                        let take = match &best {
                            None => true,
                            Some(current) => {
                                let ordering = order_values(current.clone(), item.clone())?;
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
                    Ok(best.unwrap_or_else(|| null_value()))
                }
                _ => Err(type_error(name, kind_name(one.clone()))),
            }
        }
        ("sort", (Some(one), None)) => match one.kind() {
            Kind::Array => {
                let mut items = one.as_elements()?;
                let all_numbers = items.iter().all(|item| item.kind() == Kind::Number);
                let all_strings = items.iter().all(|item| item.kind() == Kind::String);
                if !all_numbers && !all_strings {
                    return Err(type_error("sort", "mixed-type array"));
                }
                items.sort_by(|a, b| {
                    order_values(a.clone(), b.clone()).unwrap_or(std::cmp::Ordering::Equal)
                });
                Ok(Rc::new(items))
            }
            _ => Err(type_error("sort", kind_name(one.clone()))),
        },
        ("reverse", (Some(one), None)) => match one.kind() {
            Kind::Array => {
                let mut items = one.as_elements()?;
                items.reverse();
                Ok(Rc::new(items))
            }
            Kind::String => match one.as_str() {
                Ok(text) => Ok(Rc::new(text.chars().rev().collect::<String>())),
                _ => Err(type_error("reverse", kind_name(one.clone()))),
            },
            _ => Err(type_error("reverse", kind_name(one.clone()))),
        },
        // string/number conversions
        ("to_string", (Some(one), None)) => {
            Ok(Rc::new(string_for(&one, false)?) as SanshoCarrier<'a>)
        }
        ("to_number", (Some(one), None)) => match one.kind() {
            Kind::Number => Ok(number_value(one.as_f64().unwrap_or(0.0))),
            Kind::String => Ok(match one.as_str() {
                Ok(s) => match s.trim().parse::<f64>().ok() {
                    Some(value) => number_value(value),
                    _ => null_value(),
                },
                _ => null_value(),
            }),
            _ => Ok(null_value()),
        },
        // string join over an array of strings
        ("join", (Some(separator), Some(items))) => match separator.as_str() {
            Ok(sep) if items.kind() == Kind::Array => {
                let mut parts = Vec::with_capacity(items.container_len().unwrap_or(0));

                let elements = items.as_elements()?;
                for item in elements {
                    match item.as_str() {
                        Ok(text) => parts.push(text.to_string()),
                        _ => return Err(type_error("join", kind_name(Rc::new(item)))),
                    }
                }
                Ok(Rc::new(parts.join(sep.as_ref())))
            }
            _ => Err(type_error("join", kind_name(separator.clone()))),
        },
        _ => Err(Stop::NotImplemented),
    }
}

/// The `_by` functions' key contract: numbers or strings only; null
/// (a missing by-value), booleans, and containers are invalid-type (the
/// corpus's referee; mixed key types fail the ordering).
fn check_by_key<'a>(key: SanshoCarrier<'a>) -> Result<(), Stop> {
    match key.kind() {
        Kind::Number | Kind::String => Ok(()),
        _ => Err(type_error("sort_by/max_by/min_by", kind_name(key))),
    }
}

/// A computed number as a value.
fn number_value<'a>(value: f64) -> SanshoCarrier<'a> {
    Rc::new(value)
}

/// Escape a text for embedding in a JSON string.
///
/// JSON forbids three things raw inside a string: the quotation
/// mark, the reverse solidus, and the whole control range
/// (U+0000 through U+001F). The five control characters with a
/// short form take it; the rest take `\u00XX`. Everything above
/// the control range, including all non-ASCII text, is legal raw
/// and is passed through unchanged.
///
/// The surrounding quotes are the CALLER's job, so that one
/// function serves both a member value and a member key.
fn escape_string(the_str: &str) -> String {
    let mut escaped = String::with_capacity(the_str.len());
    for c in the_str.chars() {
        match c {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\u{8}' => escaped.push_str("\\b"),
            '\u{c}' => escaped.push_str("\\f"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                escaped.push_str(&format!("\\u{:04x}", c as u32));
            }
            _ => escaped.push(c),
        }
    }
    escaped
}
/// The `to_string` rendering: a value as its JSON text.
///
/// A string converts to itself — unquoted at the top level, where the
/// specification returns a string argument as it is, and quoted with its
/// escapes as a member of a container, where it is part of that container's
/// JSON text. Null renders as `null`, a boolean as `true` or `false`, a
/// number as its JSON number text, and an array or object as its members
/// rendered recursively and compactly. A non-finite number has no JSON text
/// and renders as `null`; a byte string has no JSON representation at all and
/// renders as a placeholder.
fn string_for<'b>(one: &SanshoCarrier<'b>, quote_string: bool) -> Result<String, Stop> {
    match one.kind() {
        Kind::String => Ok({
            let the_str = one.as_str()?;
            if quote_string {
                format!("\"{}\"", escape_string(the_str))
            } else {
                the_str.to_string()
            }
        }),
        Kind::Null => Ok("null".to_string()),
        Kind::Number => Ok(one.as_sansho_number().map(|n| match n {
            crate::view::SanshoNumber::I64(v) => v.to_string(),
            crate::view::SanshoNumber::U64(v) => v.to_string(),
            crate::view::SanshoNumber::F64(v) if v.is_finite() => format!("{v:?}"),
            crate::view::SanshoNumber::F64(_) => "null".to_string(),
        })?),
        Kind::Bool => Ok(one.as_bool().map(|b| b.to_string())?),
        Kind::Array => Ok({
            let arr = one.as_elements()?;

            let mut out = vec![];
            for v in arr {
                out.push(string_for(&v, true)?);
            }

            format!("[{}]", out.join(","))
        }),

        Kind::Object => Ok({
            let arr = one.as_object()?;

            let mut out = vec![];
            for (k, v) in arr {
                out.push(format!(
                    "\"{}\":{}",
                    escape_string(k),
                    string_for(&v, true)?
                ));
            }

            format!("{{{}}}", out.join(","))
        }),

        _ => Ok("\"Binary_data\"".to_string()),
    }
}

/// A string's character count, for `length`.
fn string_length<'a>(value: SanshoCarrier<'a>) -> usize {
    value.as_str().map(|s| s.chars().count()).unwrap_or(0)
}

/// The numeric coercion for the numeric functions: a number (never NaN),
/// or an invalid-type error naming the function.
fn numeric<'a>(
    name: &str,
    value: SanshoCarrier<'a>,
    function: fn(f64) -> f64,
) -> Result<SanshoCarrier<'a>, Stop> {
    let input = number_of(value, name)?;
    Ok(number_value(function(input)))
}

fn number_of<'a>(value: SanshoCarrier<'a>, _name: &str) -> Result<f64, Stop> {
    value.as_f64().map_err(Stop::from)
}

fn order_values<'a>(
    left: SanshoCarrier<'a>,
    right: SanshoCarrier<'a>,
) -> Result<std::cmp::Ordering, Stop> {
    match (left.kind(), right.kind()) {
        (Kind::Number, Kind::Number) => match (left.as_f64(), right.as_f64()) {
            (Ok(a), Ok(b)) => a.partial_cmp(&b).ok_or_else(|| {
                Stop::Error(SanshoError::Evaluation {
                    message: "cannot order NaN".to_string(),
                })
            }),
            _ => Err(Stop::Error(SanshoError::Evaluation {
                message: "cannot order numbers".to_string(),
            })),
        },
        (Kind::String, Kind::String) => match (left.as_str(), right.as_str()) {
            (Ok(a), Ok(b)) => Ok(a.cmp(b)),
            _ => Err(Stop::Error(SanshoError::Evaluation {
                message: "cannot order strings".to_string(),
            })),
        },
        _ => Err(Stop::Error(SanshoError::Evaluation {
            message: format!("cannot order {} and {}", kind_name(left), kind_name(right)),
        })),
    }
}

fn string_predicate<'a>(
    name: &str,
    subject: SanshoCarrier<'a>,
    argument: SanshoCarrier<'a>,
    predicate: fn(&str, &str) -> bool,
) -> Result<SanshoCarrier<'a>, Stop> {
    match (subject.as_str(), argument.as_str()) {
        (Ok(s), Ok(p)) => Ok(Rc::new(predicate(s, p))),
        _ => Err(type_error(name, kind_name(subject.clone()))),
    }
}

/// The compiled leaf-predicate shape `[?starts_with(@, 'prefix')]`
/// (verified against the compiler's output): the predicate is
/// `Spine[Compute(Function(name, [Value(Spine{[]}), Value(Spine{edges:[Compute(Literal(prefix))]})]))]`.
fn leaf_predicate_shape(predicate: &Program) -> Option<(String, String)> {
    let Program::Spine(pred_spine) = predicate else {
        return None;
    };
    if pred_spine.edges.len() != 1 {
        return None;
    }
    let Edge::Compute(inner) = &pred_spine.edges[0] else {
        return None;
    };
    let Program::Function(name, args) = &**inner else {
        return None;
    };
    if !matches!(name.as_str(), "starts_with" | "ends_with" | "contains") {
        return None;
    }
    let [
        FnArgP::Value(Program::Spine(subject)),
        FnArgP::Value(Program::Spine(prefix_spine)),
    ] = args.as_slice()
    else {
        return None;
    };
    // the subject must be the empty spine (`@` — the current node)
    if !subject.edges.is_empty() {
        return None;
    }
    // the prefix must be Spine[Compute(Literal(string))]
    if prefix_spine.edges.len() != 1 {
        return None;
    }
    let Edge::Compute(prefix_inner) = &prefix_spine.edges[0] else {
        return None;
    };
    let Program::Literal(prefix) = &**prefix_inner else {
        return None;
    };
    // the prefix is the literal's own text: a string literal must be
    // unwrapped, not rendered as JSON (which would carry its quotes)
    let text = match prefix {
        serde_json::Value::String(text) => text.clone(),
        _ => return None,
    };
    Some((name.clone(), text))
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

#[cfg(test)]
#[path = "eval_tests.rs"]
mod tests;
