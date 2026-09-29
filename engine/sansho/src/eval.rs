//! The evaluator: runs a compiled walk program against a document,
//! performing EVERY data access through the [`SanshoTrait`] (SPEC-0001
//! §5). The trait is the thing carried through program execution — the
//! evaluator's functions take `&impl SanshoTrait<'a>` and navigation
//! returns trait-bounded values; no other value carrier exists.
//!
//! Evaluation state is internal bookkeeping: the FLOW holds only owned
//! values (computed scalars and the materialized row boundary — the
//! emit); references into the document live only as trait-bounded
//! values being walked, per element, inside the evaluation. A
//! single-position spine materializes at its end (the row boundary);
//! projection output elements materialize per element at their spine's
//! end.
//!
//! - a value navigates lazily (key, index) or projects (wildcards,
//!   slice, filter, flatten) — nothing materializes until the spine
//!   end;
//! - every edge after a projection applies per element: the REMAINING
//!   spine is evaluated per kept element, preserving shape (nested
//!   projections stay nested);
//! - an element whose result is null drops out of the projection;
//! - the flatten edge alone dissolves arrays one level into the parent
//!   projection;
//! - navigation misses evaluate to null (the specification's null
//!   semantics: no omission, no defaulting);
//! - value-level constructs (functions, comparators, logic, literals)
//!   materialize their inputs through the value's boundary decode —
//!   the fusion point that keeps every byte decoded at most once.
//!
//! The public entry is [`lookup`], generic over the trait: pass any
//! Sansho source (`&[u8]`, `&serde_json::Value`, `&serde_cbor::Value`,
//! `&Item`, a scalar), and get the reference-shaped result.

use crate::ast::CmpOp;
use crate::corpus::{Driver, DriverOutcome};
use crate::error::SanshoError;
use crate::program::{Edge, FnArgP, Program, compile as to_program};
use crate::view::{Kind, SanshoTrait};
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

impl Stop {
    pub(crate) fn into_error(self) -> SanshoError {
        match self {
            Stop::Error(e) => e,
            Stop::NotImplemented => SanshoError::Compile {
                message: "expression uses constructs not yet implemented".to_string(),
            },
        }
    }
}

/// The evaluation's internal result shape: owned values only — the
/// reference-shaped values live as trait-bounded values being walked,
/// never stored here (internal bookkeeping; not part of the public
/// surface).
#[derive(Debug)]
pub(crate) enum Flow {
    /// A computed scalar or aggregate (functions, comparators,
    /// literals, multi-selects) or a materialized row (the emit
    /// boundary).
    Value(J),
    /// A projection's collection.
    Proj(Vec<Flow>),
}

impl Flow {
    /// Materialize the flow (the emit boundary: rows of owned JSON).
    pub fn as_value(&self) -> Result<J, SanshoError> {
        match self {
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

/// The corpus-facing driver: the real engine, through the JSON source.
/// Unimplemented constructs surface as not-implemented outcomes so the
/// corpus report distinguishes "not built yet" from "built and wrong".
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

/// The reference-shaped result of [`lookup`]: the rows produced at the
/// spine ends (the emit boundary). Materialize with [`Self::as_value`].
pub struct ZeroCopySanshoResult<'d> {
    flow: Flow,
    _lifetime: std::marker::PhantomData<&'d ()>,
}

impl<'d> ZeroCopySanshoResult<'d> {
    /// Materialize the result (the emit boundary: rows of owned JSON).
    pub fn as_value(&self) -> Result<J, SanshoError> {
        self.flow.as_value()
    }
}

/// The generic lookup entry — the source-trait surface: evaluate a
/// compiled program against ANY [`SanshoTrait`] source — a `&[u8]`
/// CBOR document, a `serde_json::Value`, a `serde_cbor::Value`, an
/// `Item` (in the embedding crate), or a scalar — passing the raw
/// type. The trait is the only data-access surface an evaluation
/// touches; the source value ITSELF is the trait value the evaluator
/// walks.
pub fn lookup<'a, T: SanshoTrait<'a> + ?Sized>(
    data: &'a T,
    program: &Program,
) -> Result<ZeroCopySanshoResult<'a>, SanshoError> {
    let flow = eval(program, data).map_err(|stop| stop.into_error())?;
    Ok(ZeroCopySanshoResult {
        flow,
        _lifetime: std::marker::PhantomData,
    })
}

/// The materialized convenience form of [`lookup`]: the result
/// collapsed at the emit boundary (the row boundary for a walk's
/// emit).
pub fn lookup_value<'a, T: SanshoTrait<'a> + ?Sized>(
    data: &'a T,
    program: &Program,
) -> Result<J, SanshoError> {
    lookup(data, program).and_then(|result| result.as_value())
}

/// Evaluate a program against an in-memory JSON document. The
/// document is a BORROW — the evaluation routes through the borrowed
/// `&J` trait surface, whose members are references into the tree (no
/// per-member clones); the owned value form stays available to callers
/// passing an owned value directly to [`lookup_value`].
pub fn evaluate_json(program: &Program, document: &J) -> Result<J, SanshoError> {
    lookup_value(&document, program)
}

/// Evaluate a program against one CBOR document (the byte-slice input of
/// SPEC-0001 §2): the byte source, with every position decoded at
/// most once per evaluation.
pub fn evaluate_cbor(program: &Program, document: &[u8]) -> Result<J, SanshoError> {
    lookup_value(&document, program)
}

/// The instrumented form: the result plus the number of DISTINCT
/// positions decoded (the byte-once property's observable). The decode
/// memo is internal bookkeeping; the count is read from the root
/// position the evaluator walked.
pub fn evaluate_cbor_with_stats(
    program: &Program,
    document: &[u8],
) -> (Result<J, SanshoError>, usize) {
    let root = crate::source::CborNode::root(document);
    let result = lookup_value(&document, program);
    (result, root.decode_count())
}

/// The phase-visible form of the CBOR entry: the not-implemented
/// constructs surface as [`Stop::NotImplemented`] (the corpus harness
/// scores those as not-built-yet, never as failures). Interim during the
/// phased construction.
pub fn evaluate_cbor_stopped(program: &Program, document: &[u8]) -> Result<J, Stop> {
    eval(program, &document).and_then(|flow| flow.as_value().map_err(Stop::from))
}

/// The corpus harness's JSON entry: the outcome-visible form.
pub(crate) fn eval_program(program: &Program, document: &J) -> Result<J, Stop> {
    eval(program, document).and_then(|flow| flow.as_value().map_err(Stop::from))
}

/// The program evaluation: the ONLY data carrier is the trait value
/// being walked.

fn eval<'a, T: SanshoTrait<'a> + ?Sized>(program: &Program, node: &T) -> Result<Flow, Stop> {
    match program {
        Program::Literal(value) => Ok(Flow::Value(value.clone())),
        Program::Spine(spine) => eval_spine(node, &spine.edges),
        Program::MultiList(slots) => {
            if node.is_null() {
                return Ok(Flow::Value(J::Null));
            }
            let mut items = Vec::with_capacity(slots.len());
            for slot in slots {
                let value = eval(slot, node)?.as_value()?;
                items.push(value);
            }
            Ok(Flow::Value(J::Array(items)))
        }
        Program::MultiHash(entries) => {
            if node.is_null() {
                return Ok(Flow::Value(J::Null));
            }
            let mut object = serde_json::Map::new();
            for (key, slot) in entries {
                let value = eval(slot, node)?.as_value()?;
                object.insert(key.clone(), value);
            }
            Ok(Flow::Value(J::Object(object)))
        }
        Program::Pipe(left, right) => {
            let left_value = eval(left, node)?.as_value()?;
            eval(right, &left_value)
        }
        Program::Function(name, args) => eval_function(name, args, node),
        Program::Compare(op, left, right) => {
            let left_value = eval(left, node)?.as_value()?;
            let right_value = eval(right, node)?.as_value()?;
            compare(*op, &left_value, &right_value).map(Flow::Value)
        }
        Program::And(left, right) => {
            let left_value = eval(left, node)?.as_value()?;
            if truthy(&left_value) {
                eval(right, node)
            } else {
                Ok(Flow::Value(left_value))
            }
        }
        Program::Or(left, right) => {
            let left_value = eval(left, node)?.as_value()?;
            if truthy(&left_value) {
                Ok(Flow::Value(left_value))
            } else {
                eval(right, node)
            }
        }
        Program::Not(inner) => {
            let value = eval(inner, node)?.as_value()?;
            Ok(Flow::Value(J::Bool(!truthy(&value))))
        }
    }
}

/// Walk a spine over a trait value. The FIRST edge applies to the
/// value; if it produces a projection, the REMAINING edges apply PER
/// KEPT ELEMENT (the projection semantics); otherwise the chain walks
/// the single position, materializing at the spine end (the row
/// boundary).
fn eval_spine<'a, T: SanshoTrait<'a> + ?Sized>(node: &T, edges: &[Edge]) -> Result<Flow, Stop> {
    match edges {
        [] => node.materialize().map(Flow::Value).map_err(Stop::from),
        [edge, rest @ ..] => eval_edge(node, edge, rest),
    }
}

/// Apply ONE edge to a trait value, then the remaining edges: for a
/// projection-producing edge, per element; for a position-producing
/// edge, on the member.
fn eval_edge<'a, T: SanshoTrait<'a> + ?Sized>(
    node: &T,
    edge: &Edge,
    rest: &[Edge],
) -> Result<Flow, Stop> {
    match edge {
        Edge::Compute(program) => {
            // a value-construct spliced into the chain: the program
            // evaluates over the current value; if the spine continues,
            // the result continues as a VALUE (a computed value has no
            // backing position)
            let flow = eval(program, node)?;
            continue_flow(flow, rest)
        }
        Edge::Key(name) => match node.get_key(name) {
            Some(child) => continue_single(&child, rest),
            None => Ok(Flow::Value(J::Null)),
        },
        Edge::Index(index) => {
            let resolved = match node.kind() {
                Kind::Array => {
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
            match resolved {
                Some(child) => continue_single(&child, rest),
                None => Ok(Flow::Value(J::Null)),
            }
        }
        // the dot form reads object values only
        Edge::WildcardDot => match node.kind() {
            Kind::Object => {
                let members: Vec<_> = node.entries().into_iter().map(|(_, v)| v).collect();
                let flow = eval_projection(members, &[])?;
                continue_flow(flow, rest)
            }
            _ => Ok(Flow::Value(J::Null)),
        },
        // the bracket form reads array elements only
        Edge::WildcardBracket => match node.kind() {
            Kind::Array => {
                let elements = node.elements();
                let flow = eval_projection(elements, &[])?;
                continue_flow(flow, rest)
            }
            _ => Ok(Flow::Value(J::Null)),
        },
        Edge::Filter(predicate) => match node.kind() {
            Kind::Array => {
                let mut kept = Vec::new();
                // the hoisted leaf-predicate fast path (Phase 2): the
                // dominant filter shape `[?starts_with(@, 'prefix')]`
                // is evaluated per element WITHOUT the program-eval
                // recursion — a borrowed leaf read + a boolean. Non-
                // string elements and as_str-None elements fall
                // through to the general path (semantics preserved).
                let leaf = leaf_predicate_shape(predicate);
                for element in node.elements() {
                    let keep = match &leaf {
                        Some((name, prefix)) if element.kind() == Kind::String => {
                            // the RAW-BYTES fast path (owner decision
                            // 2026-09-23): the string-predicate
                            // comparison runs on the unvalidated text
                            // bytes — no UTF-8 validation per element.
                            // An invalid-UTF-8 element in a filter
                            // predicate is DROPPED (not errored — the
                            // relaxed contract, pinned in the escape
                            // matrix). Byte strings (major 2) return
                            // None here and keep the base64url general
                            // path.
                            match element.raw_text_bytes() {
                                Some(bytes) => match name.as_str() {
                                    "starts_with" => bytes.starts_with(prefix.as_bytes()),
                                    "ends_with" => bytes.ends_with(prefix.as_bytes()),
                                    _ => bytes
                                        .windows(prefix.len())
                                        .any(|w| w == prefix.as_bytes()),
                                },
                                None => truthy(
                                    &eval(predicate, &element)?
                                        .as_value()
                                        .map_err(Stop::from)?,
                                ),
                            }
                        }
                        _ => truthy(
                            &eval(predicate, &element)?
                                .as_value()
                                .map_err(Stop::from)?,
                        ),
                    };
                    if keep {
                        kept.push(element);
                    }
                }
                let flow = eval_projection(kept, &[])?;
                continue_flow(flow, rest)
            }
            _ => Ok(Flow::Value(J::Null)),
        },
        // the slice is a projection over the selected range (arrays), or
        // the sliced string (strings)
        Edge::Slice(spec) => match node.kind() {
            Kind::Array => {
                let len = node.container_len().unwrap_or(0) as i64;
                let selected = slice_indices(len, spec)?;
                let mut elements = Vec::with_capacity(selected.len());
                for index in selected {
                    if let Some(element) = node.get_index(index as usize) {
                        elements.push(element);
                    }
                }
                let flow = eval_projection(elements, &[])?;
                continue_flow(flow, rest)
            }
            Kind::String => {
                let text = node.as_str().map(|s| s.into_owned()).unwrap_or_default();
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
        Edge::Flatten => {
            // the flatten of the current value: dissolve array-valued
            // results one level
            let flow = flatten_value(node).map_err(Stop::from)?;
            continue_flow(flow, rest)
        }
    }
}

/// Continue the spine on a SINGLE member position (a key/index hit):
/// the remaining edges walk the member; an empty remainder materializes
/// the member at the row boundary.
fn continue_single<'a, T: SanshoTrait<'a> + ?Sized>(child: &T, rest: &[Edge]) -> Result<Flow, Stop> {
    match rest {
        [] => child.materialize().map(Flow::Value).map_err(Stop::from),
        [edge, more @ ..] => eval_edge(child, edge, more),
    }
}

/// Continue the spine on a FLOW (a computed value or a flatten): the
/// remaining edges apply; an empty remainder passes the flow through.
fn continue_flow(flow: Flow, rest: &[Edge]) -> Result<Flow, Stop> {
    match rest {
        [] => Ok(flow),
        [edge, more @ ..] => eval_flow(flow, edge, more),
    }
}

/// Apply an edge to an owned-value flow (computed values have no
/// backing position): the edge applies PER ELEMENT through the JSON
/// value's own trait surface (`J` implements [`SanshoTrait`]) — a
/// projection continues per element, and the nested-projection shape
/// is preserved.
/// Apply ONE edge to a collection (the projection semantics): the
/// edge applies PER ITEM, at its own depth — a value item yields its
/// edge result, a nested projection item yields the edge applied to
/// each of ITS items (the per-item nesting is preserved: a
/// `[*]`/`.*`-chain stays nested per source item, `foo[*].bar[*].kind`
/// → `[[kinds],[kinds]]`). A null value result drops out of the
/// collection; an empty projection item stays (a wildcard-on-scalar
/// yields a valid empty row, `foo.*.*.*` → `[[],[],[]]`). The FLATTEN
/// edge is handled at the collection level: it dissolves array-valued
/// items one level INTO the collection (the `...[][]` → flat rule, and
/// the reason a flatten after a projection flattens the whole row
/// shape).
fn eval_flow(flow: Flow, edge: &Edge, rest: &[Edge]) -> Result<Flow, Stop> {
    match edge {
        Edge::Flatten => {
            // a computed flatten: dissolve arrays one level
            let mut flows = Vec::new();
            flatten_into(flow, &mut flows)?;
            continue_flow(Flow::Proj(flows), rest)
        }
        _ => match flow {
            Flow::Value(value) => eval_edge(&value, edge, rest),
            Flow::Proj(items) => {
                let mut results = Vec::new();
                for item in items {
                    collect_projected(item, edge, &mut results)?;
                }
                continue_flow(Flow::Proj(results), rest)
            }
        },
    }
}

/// Apply the edge to one flow item, collecting the results at the
/// collection's level: a value item's edge result joins the collection
/// (a null result drops); a nested projection item keeps its shape —
/// the edge applies to each of its items and the results stay one item
/// (the per-source-item nesting).
fn collect_projected(item: Flow, edge: &Edge, results: &mut Vec<Flow>) -> Result<(), Stop> {
    match item {
        Flow::Value(value) => match eval_edge(&value, edge, &[])? {
            Flow::Value(J::Null) => Ok(()),
            flow => {
                results.push(flow);
                Ok(())
            }
        },
        Flow::Proj(items) => {
            let mut nested = Vec::new();
            for item in items {
                collect_projected(item, edge, &mut nested)?;
            }
            results.push(Flow::Proj(nested));
            Ok(())
        }
    }
}

/// The projection continuation — the collection semantics over
/// per-item GROUPS. Each projecting edge produces, for every source
/// value, the group of its results (an object's members, an array's
/// elements, a filter's kept elements, a key hit); the next edge
/// applies per group member, preserving the projection's per-item
/// nesting (`foo[*].bar[*].kind` → `[[kinds],[kinds]]`). The flatten
/// edge dissolves arrays one level INTO the collection and collapses
/// the grouping (`...[][]` → flat).
fn eval_projection<'a, I: IntoIterator<Item = impl SanshoTrait<'a>>>(
    elements: I,
    edges: &[Edge],
) -> Result<Flow, Stop> {
    match edges {
        // the collected end: each element materializes at the row
        // boundary; null elements drop
        [] => {
            let mut flows = Vec::new();
            for element in elements {
                match element.materialize().map_err(Stop::from)? {
                    J::Null => {}
                    value => flows.push(Flow::Value(value)),
                }
            }
            Ok(Flow::Proj(flows))
        }
        [Edge::Flatten, rest @ ..] => {
            // a leading/early flatten dissolves the elements into the
            // collection and collapses the grouping
            let mut dissolved = Vec::new();
            for element in elements {
                match element.materialize().map_err(Stop::from)? {
                    J::Null => {}
                    J::Array(items) => dissolved.extend(items),
                    value => dissolved.push(value),
                }
            }
            finish_projection(vec![dissolved], rest, true)
        }
        [edge, rest @ ..] => {
            let groups = apply_edge_batch(elements, edge)?;
            finish_projection(groups, rest, false)
        }
    }
}

/// The first edge, applied once per element (the elements enter as one
/// group); the per-element results become the initial groups.
fn apply_edge_batch<'a, I: IntoIterator<Item = impl SanshoTrait<'a>>>(
    elements: I,
    edge: &Edge,
) -> Result<Vec<Vec<J>>, Stop> {
    let mut groups = Vec::new();
    for element in elements {
        groups.push(apply_single_projection_edge(&element, edge)?);
    }
    Ok(groups)
}

/// One projecting edge on ONE value: the group of its results.
fn apply_single_projection_edge<'a, T: SanshoTrait<'a> + ?Sized>(
    element: &T,
    edge: &Edge,
) -> Result<Vec<J>, Stop> {
    match edge {
        // the dissolve: array values one level, scalars pass through
        // (this grouping collapses at the finish level)
        Edge::Flatten => {
            let mut group = Vec::new();
            match element.materialize().map_err(Stop::from)? {
                J::Null => {}
                J::Array(items) => group.extend(items),
                value => group.push(value),
            }
            Ok(group)
        }
        Edge::Key(name) => Ok(match element.get_key(name) {
            Some(child) => vec![child.materialize().map_err(Stop::from)?],
            None => Vec::new(),
        }),
        Edge::Index(index) => {
            let resolved = match element.kind() {
                Kind::Array => {
                    let len = element.container_len().unwrap_or(0) as i64;
                    let effective = if *index < 0 { len + index } else { *index };
                    if effective >= 0 && effective < len {
                        element.get_index(effective as usize)
                    } else {
                        None
                    }
                }
                _ => None,
            };
            Ok(match resolved {
                Some(child) => vec![child.materialize().map_err(Stop::from)?],
                None => Vec::new(),
            })
        }
        Edge::WildcardDot => Ok(match element.kind() {
            Kind::Object => element
                .entries()
                .into_iter()
                .map(|(_, v)| v.materialize().map_err(Stop::from))
                .collect::<Result<Vec<_>, _>>()?,
            _ => Vec::new(),
        }),
        Edge::WildcardBracket => Ok(match element.kind() {
            Kind::Array => element
                .elements()
                .into_iter()
                .map(|v| v.materialize().map_err(Stop::from))
                .collect::<Result<Vec<_>, _>>()?,
            _ => Vec::new(),
        }),
        Edge::Filter(predicate) => {
            let mut group = Vec::new();
            if element.kind() == Kind::Array {
                // the hoisted leaf-predicate fast path (the raw-bytes
                // comparison, no program recursion) where it applies
                let leaf = leaf_predicate_shape(predicate);
                for member in element.elements() {
                    let keep = match &leaf {
                        Some((name, prefix)) if member.kind() == Kind::String => {
                            match member.raw_text_bytes() {
                                Some(bytes) => match name.as_str() {
                                    "starts_with" => bytes.starts_with(prefix.as_bytes()),
                                    "ends_with" => bytes.ends_with(prefix.as_bytes()),
                                    _ => bytes.windows(prefix.len()).any(|w| w == prefix.as_bytes()),
                                },
                                None => truthy(
                                    &eval(predicate, &member)?.as_value().map_err(Stop::from)?,
                                ),
                            }
                        }
                        _ => truthy(
                            &eval(predicate, &member)?.as_value().map_err(Stop::from)?,
                        ),
                    };
                    if keep {
                        group.push(member.materialize().map_err(Stop::from)?);
                    }
                }
            }
            Ok(group)
        }
        Edge::Slice(spec) => Ok(match element.kind() {
            Kind::Array => {
                let len = element.container_len().unwrap_or(0) as i64;
                let selected = slice_indices(len, spec)?;
                let mut group = Vec::new();
                for index in selected {
                    if let Some(child) = element.get_index(index as usize) {
                        group.push(child.materialize().map_err(Stop::from)?);
                    }
                }
                group
            }
            _ => Vec::new(),
        }),
        Edge::Compute(program) => {
            Ok(vec![eval(program, element)?.as_value().map_err(Stop::from)?])
        }
    }
}

/// Continue the groups along the remaining edges; the flatten
/// dissolves arrays one level into the COLLECTION and collapses the
/// grouping; every other edge applies per group member, the per-member
/// results becoming the next groups; the empty tail emits the groups
/// as the projection's arrays.
fn finish_projection(groups: Vec<Vec<J>>, edges: &[Edge], collapsed: bool) -> Result<Flow, Stop> {
    match edges {
        // the projection's end: an uncollapsed collection emits each
        // group as its array (the per-item nesting); a collapsed
        // collection (after a flatten) emits the values flat
        [] => {
            if collapsed {
                let mut flows = Vec::new();
                for group in groups {
                    for value in group {
                        if !value.is_null() {
                            flows.push(Flow::Value(value));
                        }
                    }
                }
                Ok(Flow::Proj(flows))
            } else {
                let flows = groups
                    .into_iter()
                    .map(|g| Flow::Value(J::Array(g)))
                    .collect();
                Ok(Flow::Proj(flows))
            }
        }
        [Edge::Flatten, rest @ ..] => {
            // the dissolve: arrays one level into the collection, the
            // grouping collapses
            let mut dissolved = Vec::new();
            for group in groups {
                for value in group {
                    match value {
                        J::Null => {}
                        J::Array(items) => dissolved.extend(items),
                        value => dissolved.push(value),
                    }
                }
            }
            finish_projection(vec![dissolved], rest, true)
        }
        [edge, rest @ ..] => {
            // each source group survives the edge: its values' results
            // aggregate back into the group, preserving the per-item
            // nesting (a reservation's instances stay one group)
            let mut next = Vec::new();
            for group in groups {
                let mut aggregated = Vec::new();
                for value in group {
                    if value.is_null() {
                        continue;
                    }
                    aggregated.extend(apply_single_projection_edge(&value, edge)?);
                }
                next.push(aggregated);
            }
            finish_projection(next, rest, collapsed)
        }
    }
}

/// The `[]` operator on a single value: dissolve array-valued results
/// one level into the projection; a plain non-array flattens to null.
fn flatten_value<'a, T: SanshoTrait<'a> + ?Sized>(node: &T) -> Result<Flow, SanshoError> {
    let mut flows = Vec::new();
    match node.kind() {
        Kind::Array => {
            for element in node.elements() {
                match element.materialize()? {
                    J::Null => {}
                    J::Array(inner) => flows.extend(inner.into_iter().map(Flow::Value)),
                    plain => flows.push(Flow::Value(plain)),
                }
            }
        }
        // the flatten of a plain non-array is null
        _ => return Ok(Flow::Value(J::Null)),
    }
    Ok(Flow::Proj(flows))
}

fn flatten_into(flow: Flow, flows: &mut Vec<Flow>) -> Result<(), Stop> {
    match flow {
        Flow::Value(J::Null) => {}
        Flow::Value(J::Array(elements)) => {
            for element in elements {
                match element {
                    J::Null => {}
                    J::Array(inner) => flows.extend(inner.into_iter().map(Flow::Value)),
                    plain => flows.push(Flow::Value(plain)),
                }
            }
        }
        Flow::Value(plain) => flows.push(Flow::Value(plain)),
        Flow::Proj(items) => {
            for item in items {
                flatten_into(item, flows)?;
            }
        }
    };
    Ok(())
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

fn eval_function<'a, T: SanshoTrait<'a> + ?Sized>(name: &str, args: &[FnArgP], node: &T) -> Result<Flow, Stop> {
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
        let array = eval(array_arg, node)?.as_value()?;
        let items = match array {
            J::Array(items) => items,
            other => return Err(type_error(name, kind_name(&other))),
        };
        return match name {
            "map" => {
                let mut mapped = Vec::with_capacity(items.len());
                for item in &items {
                    let flow = eval(ref_program, item)?;
                    mapped.push(flow.as_value().map_err(Stop::from)?);
                }
                Ok(Flow::Value(J::Array(mapped)))
            }
            "max_by" | "min_by" => {
                let want_max = name == "max_by";
                let mut best_item: Option<&J> = None;
                let mut best_key: Option<J> = None;
                for item in &items {
                    let flow = eval(ref_program, item)?;
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
                    let flow = eval(ref_program, item)?;
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
                keyed.sort_by(|a, b| order_values(&a.0, &b.0).unwrap_or(std::cmp::Ordering::Equal));
                Ok(Flow::Value(J::Array(
                    keyed.into_iter().map(|(_, item)| item.clone()).collect(),
                )))
            }
            _ => unreachable!("function_takes_expression gates this match"),
        };
    }

    // the length-over-container fast path: an array or object node's
    // element count is a HEADER read — the whole subtree must NOT
    // materialize to count its elements. Returns the count directly
    // (bypassing the general `length` arm, which expects the container
    // value).
    if name == "length" {
        if let [FnArgP::Value(single)] = args {
            if let Flow::Value(value) = eval(single, node)? {
                if let Some(len) = match &value {
                    J::Array(items) => Some(items.len()),
                    J::Object(map) => Some(map.len()),
                    _ => None,
                } {
                    return Ok(Flow::Value(J::Number((len as u64).into())));
                }
            }
        }
    }

    // the value-argument functions: every argument evaluates eagerly;
    // an expression reference here is an invalid type
    let mut values = Vec::with_capacity(args.len());
    for arg in args {
        match arg {
            FnArgP::Value(program) => {
                let flow = eval(program, node)?;
                let value = match flow {
                    Flow::Value(v) => v,
                    Flow::Proj(items) => J::Array(
                        items
                            .iter()
                            .map(|f| f.as_value())
                            .collect::<Result<_, SanshoError>>()?,
                    ),
                };
                values.push(value)
            }
            FnArgP::Ref(_) => {
                return Err(Stop::Error(SanshoError::Evaluation {
                    message: format!("function {name} does not accept expression references"),
                }));
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
            (J::String(s), J::String(n)) => Ok(Flow::Value(J::Bool(s.contains(n.as_str())))),
            (J::Array(items), needle) => Ok(Flow::Value(J::Bool(
                items
                    .iter()
                    .any(|item| crate::corpus::json_equal(item, needle)),
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
                sorted.sort_by(|a, b| order_values(a, b).unwrap_or(std::cmp::Ordering::Equal));
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
            J::String(text) => Ok(Flow::Value(J::String(text.chars().rev().collect()))),
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

fn numeric(name: &str, value: &J, function: fn(f64) -> f64) -> Result<Flow, Stop> {
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
            (Some(a), Some(b)) => a.partial_cmp(&b).ok_or_else(|| {
                Stop::Error(SanshoError::Evaluation {
                    message: "cannot order NaN".to_string(),
                })
            }),
            _ => Err(Stop::Error(SanshoError::Evaluation {
                message: "cannot order numbers".to_string(),
            })),
        },
        (J::String(a), J::String(b)) => Ok(a.cmp(b)),
        _ => Err(Stop::Error(SanshoError::Evaluation {
            message: format!("cannot order {} and {}", kind_name(left), kind_name(right)),
        })),
    }
}

fn string_predicate(
    name: &str,
    subject: &J,
    argument: &J,
    predicate: fn(&str, &str) -> bool,
) -> Result<Flow, Stop> {
    match (subject, argument) {
        (J::String(s), J::String(p)) => Ok(Flow::Value(J::Bool(predicate(s, p)))),
        (other, _) => Err(type_error(name, kind_name(other))),
    }
}

/// The compiled leaf-predicate shape `[?starts_with(@, 'prefix')]`
/// (verified against the compiler's output): the predicate is
/// `Spine[Compute(Function(name, [Value(Spine{[]}), Value(Spine{edges:[Compute(Literal(prefix))]})]))]`.
fn leaf_predicate_shape(predicate: &Program) -> Option<(String, String)> {
    let Program::Spine(pred_spine) = predicate else { return None };
    if pred_spine.edges.len() != 1 { return None }
    let Edge::Compute(inner) = &pred_spine.edges[0] else { return None };
    let Program::Function(name, args) = &**inner else { return None };
    if !matches!(name.as_str(), "starts_with" | "ends_with" | "contains") { return None }
    let [FnArgP::Value(Program::Spine(subject)), FnArgP::Value(Program::Spine(prefix_spine))] = args.as_slice() else { return None };
    // the subject must be the empty spine (`@` — the current node)
    if !subject.edges.is_empty() { return None }
    // the prefix must be Spine[Compute(Literal(string))]
    if prefix_spine.edges.len() != 1 { return None }
    let Edge::Compute(prefix_inner) = &prefix_spine.edges[0] else { return None };
    let Program::Literal(J::String(prefix)) = &**prefix_inner else { return None };
    Some((name.clone(), prefix.clone()))
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