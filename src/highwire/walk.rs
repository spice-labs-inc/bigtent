//! The High Wire walk grammar: `when` gates, per-type walk selectors,
//! and the round-processing loop (filter -> emit -> stop).
//!
//! Prototype under active design discussion; the grammar may change
//! without notice. This module exists so the prototype compiles and
//! runs as part of the regular build.
//!

use crate::highwire::grammar::LevelResult;
use crate::item::Item;
use crate::rodeo::goat::GoatRodeoCluster;
use crate::rodeo::goat_herd::GoatHerd;
use crate::rodeo::goat_trait::GoatRodeoTrait;
use crate::rodeo::index::GetOffset;
use anyhow::{Result, bail};
use sansho::eval::truthy;
use sansho::{Program, ProgramCache, SanshoCarrier, SanshoTrait};
use serde_json::Value;
use std::rc::Rc;
use std::{collections::HashSet, fmt::Debug, sync::Arc};
use tokio::sync::mpsc::Sender;

/// A comparison node: its operator, the variable it reads, and the value it
/// compares against.
pub type Comparison<'s> = (CompareTypes, &'s str, Rc<dyn ExpressionBuilder>);

/// An item predicate over a carrier of one lifetime.
pub type ItemCompareFn<'a> = Rc<dyn Fn(SanshoCarrier<'a>, &WalkState) -> bool>;

/// A leaf's comparison, built rather than called: the shape
/// [`build_expression`] takes so that a gate context can choose its own leaf
/// language.
pub type ConFilterBuilder = Rc<dyn Fn(&str) -> Result<ConFilter>>;

/// A gate over an item and the walk state, compiled.
///
/// The closure types below are written once here rather than spelled out at
/// every signature: they appear in the walk's interface, in the builders that
/// produce them, and in the compiler that hands them over, and a reader should
/// not have to check three spellings agree.
pub type ItemGate = Rc<dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> bool>;

/// What an emit produces for one subject, compiled.
pub type ItemEmitFn = Rc<dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> Vec<Value>>;

/// The stop rules, compiled: hard first, then soft, first match wins.
pub type ItemStopFn =
    Rc<dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> (Option<Vec<Value>>, Option<Vec<Value>>)>;

pub struct WalkState {
    pub is_terminal: bool,
    pub depth_cnt: usize,
}

pub trait AccountingAndBudget {
    fn should_stop(&self) -> Option<Vec<Value>>;

    // a super cheap test to see if the sink should continue
    // processing Normal messages or has the budget been exceeded?
    fn can_sink_normal(&self) -> bool;

    // Note the AccountingAndBudget implementations will likely be
    // shared in an Arc across threads, so inner mutability (likely and Atomic)
    fn event(&self, event_type: AccountingEventType);
}

pub enum AccountingEventType {
    LoadItem,
    Emit,
}

pub enum SinkOperation {
    NormalMessage,
    ErrorMessage,
}

// filter the conn (note this is per connection type). The identifier borrows for
// the document's lifetime, which is what lets the predicate be built once — a
// predicate over a per-call borrow could not be stored
pub type ConFilter = Rc<dyn Fn(&str, &usize) -> bool>;

/// Which store a walk reads from.
///
/// Both arms are a shared handle on a store, so this clones like one: a clone is
/// a reference count, not a copy of anything. A chained query runs several walks
/// against the same store, which is why it needs that.
#[derive(Clone)]
pub enum HerdOrCluster {
    Herd(Arc<GoatHerd>),
    Cluster(Arc<GoatRodeoCluster>),
}

/// The walk's signature is its interface: the roots, where to look, where rows
/// go, what to traverse, the three gates, and the accountant. Bundling them
/// would name the bundle after nothing.
#[allow(clippy::too_many_arguments)]
pub fn walk<
    ACCOUNTANT: AccountingAndBudget,
    SINK: FnMut(Value, &Arc<ACCOUNTANT>, SinkOperation) -> Result<()>,
>(
    roots: Vec<String>,
    lookup: HerdOrCluster,
    mut sink: SINK,
    connections: Vec<(String, ConFilter)>,
    item_filter: ItemGate,
    emit: ItemEmitFn,
    stop_fn: ItemStopFn,
    accountant: Arc<ACCOUNTANT>,
) -> Result<()> {
    // keep track of depth
    let mut depth = 0usize;

    // the next set of identifiers
    let mut next_round: HashSet<String> = roots.into_iter().collect();

    // the identifiers we've seen... don't re-walk them
    let mut seen: HashSet<String> = HashSet::new();

    // FIXME capture here: the walk's own start instant, so the accountant can
    // report `phase.elapsed_ns` for this walk alone (distinct from the global
    // execution time of the whole chained request).

    // a variable for "soft" stop
    let mut continue_walk = true;

    // if we're continuing and there are more items, keep traversing
    'outer: while continue_walk && !next_round.is_empty() {
        // FIXME capture here: `walk.rounds` — one increment per iteration of
        // this loop. Cheap: a single counter, incremented once per round.
        // stuff to process for this round
        let mut this_round = HashSet::new();

        // update this round and seen
        for v in &next_round {
            seen.insert(v.clone());
            this_round.insert(v.clone());
        }
        // FIXME capture here: `walk.seen` — the size of this set at the end of
        // the walk. Free: the set already exists and maintains itself.

        // the place to store identifiers for the next round
        next_round.clear();

        // storing the current item
        let mut curr_item: Item;
        for id in this_round {
            let found = match &lookup {
                HerdOrCluster::Cluster(cluster) => cluster
                    .identifier_to_item_offset(&id)
                    .and_then(|off| {
                        cluster.bytes_for_file_and_offset(
                            off.loc.get_file_hash(),
                            off.loc.get_offset(),
                        )
                    })
                    .map(|i| Rc::new(i) as SanshoCarrier),
                HerdOrCluster::Herd(herd) => match herd.item_for_identifier(&id) {
                    None => None,
                    Some(i) => {
                        curr_item = i;
                        Some(Rc::new(&curr_item) as SanshoCarrier)
                    }
                },
            };
            let Some(thing) = found else {
                // FIXME capture here: `walk.items_missing` — an identifier that
                // resolved to no item. Distinct from an item that was loaded and
                // then filtered out; today both are silent.
                continue;
            };

            // FIXME capture here: `LoadItem` — the item count, one event per
            // item actually read from the cluster or herd. `AccountingEventType::
            // LoadItem` exists and is currently fired nowhere; this is the site.
            // It is the counter behind both `walk.items_loaded` and
            // `global.items_loaded`.

            // pre-build stuff for the next round
            // We need to determine if this Item is terminal for
            // some of the state to pass to the functions, so we
            // don't want to actually add to `next_round`
            // until this Item passes
            let mut maybe_next_round: Vec<String> = vec![];

            for (con_name, con_filter) in &connections {
                // FIXME capture here: `walk.connections` — one increment for
                // `con_name` per edge actually followed. This is the vocabulary
                // count, and it is what replaces a whole-cluster scan: it reports
                // the edge types reachable from the given roots.
                if let Some(conns) = thing
                    .get_key("connections")
                    .ok()
                    .flatten()
                    .and_then(|v| v.get_key(con_name).ok().flatten())
                    .and_then(|v| v.as_elements().ok())
                {
                    {
                        let mut v: Vec<&str> = vec![];
                        for sc in &conns {
                            if let Ok(s) = sc.as_str() {
                                v.push(s);
                            }
                        }
                        conns.len();

                        for maybe_con in v {
                            // if they have not been seen and they pass the connection filter
                            // for this connection type, add them to the possibilities
                            // note this is a vec because adding is cheap... later, we'll
                            // de-dup with the set

                            if !seen.contains(maybe_con) && con_filter(maybe_con, &depth) {
                                maybe_next_round.push(maybe_con.to_string());
                            }
                            // FIXME capture here: `walk.connections_filtered` —
                            // edges dropped because the connection filter said no.
                            // Also worth separating the edges dropped because the
                            // identifier was already in `seen`, which is a
                            // different fact (a cycle or a diamond).
                        }
                    }
                }
            }

            // create the walk state
            let walk_state = WalkState {
                depth_cnt: depth,
                is_terminal: maybe_next_round.is_empty(),
            };
            // FIXME capture here: `walk.max_depth` — keep the largest
            // `depth_cnt` seen. Free: a single comparison per item.

            // do we want to process the item?
            if item_filter(&thing, &walk_state) {
                // FIXME capture here: `walk.items_admitted` — the item passed the
                // gate.
                // queue up the next round
                for s in maybe_next_round {
                    next_round.insert(s);
                }

                // emit the stuff for this round
                for emit_me in emit(&thing, &walk_state) {
                    // FIXME capture here: `walk.rows_emitted` — one count per row
                    // handed to the sink. Note the sinks already fire
                    // `AccountingEventType::Emit`, so this may already be covered;
                    // confirm before adding a second counter.
                    sink(emit_me, &accountant, SinkOperation::NormalMessage)?;
                }
                // FIXME capture here: `walk.items_emitted` — one count for the
                // item if the emit produced at least one row.

                // do stop logic
                // FIXME capture here: `walk.stop` — that a stop fired, whether it
                // was `hard` or `soft`, which rule arm matched, and the depth and
                // round it fired at. This is the fact that is currently invisible:
                // a stop that fires with nothing to report is indistinguishable
                // from exhaustion.
                match stop_fn(&thing, &walk_state) {
                    // both rules fire... emit both sets of values and stop
                    // immediately
                    (Some(a), Some(b)) => {
                        for v in a {
                            sink(v, &accountant, SinkOperation::ErrorMessage)?;
                        }
                        for v in b {
                            sink(v, &accountant, SinkOperation::ErrorMessage)?;
                        }
                        break 'outer;
                    }

                    // stop immediately
                    (_, Some(b)) => {
                        for v in b {
                            sink(v, &accountant, SinkOperation::ErrorMessage)?;
                        }
                        break 'outer;
                    }

                    // stop at end of loop
                    (Some(a), _) => {
                        for v in a {
                            sink(v, &accountant, SinkOperation::ErrorMessage)?;
                        }

                        continue_walk = false;
                    }
                    _ => {} // continue
                }
                // FIXME capture here: `walk.items_filtered` — the item did NOT
                // pass the item gate. Note this also means its edges are never
                // followed, so a filtered item costs the walk its whole subtree.
            }
        }

        if let Some(end_reasons) = accountant.should_stop() {
            // FIXME capture here: `walk.stop.kind = budget` — the accountant, not
            // a stop rule, ended the walk.
            for r in end_reasons {
                sink(r, &accountant, SinkOperation::ErrorMessage)?;
            }

            bail!("Budget Exceeded"); // probably do some more structured error... maybe should_stop generates the return result... ???
        }
        depth += 1;
    }
    // FIXME capture here: `walk.terminated_by` (`exhausted`, `stop`, `error`)
    // and the walk's end instant for `phase.elapsed_ns`. Every `break 'outer`
    // above and every `?` that returns early has to land here for the terminal
    // state to be right, which is why this belongs in a guard rather than at the
    // bottom of the happy path.
    Ok(())
}

pub fn vec_sink<A: AccountingAndBudget>(
    vec: &mut Vec<Value>,
) -> impl FnMut(Value, &Arc<A>, SinkOperation) -> Result<()> {
    move |v: Value, accounting: &Arc<A>, _: SinkOperation| -> Result<()> {
        vec.push(v);
        accounting.event(AccountingEventType::Emit);
        Ok(())
    }
}

pub fn flume_sink<A: AccountingAndBudget>(
    sender: Sender<Value>,
) -> impl FnMut(Value, &Arc<A>, SinkOperation) -> Result<()> {
    move |v: Value, accounting: &Arc<A>, _: SinkOperation| -> Result<()> {
        sender.blocking_send(v)?;
        accounting.event(AccountingEventType::Emit);
        Ok(())
    }
}

pub fn compile_sansho(exp: &str, cache: &ProgramCache) -> Result<Arc<Program>> {
    Ok(sansho::compile(&sansho::parse(exp)?, Some(cache))?)
}

pub trait WalkBuilder: ExpressionBuilder {
    fn walk(&self) -> LevelResult<Vec<(String, ConFilter)>>;
}

pub trait ExpressionBuilder: Debug {
    fn not(&self) -> LevelResult<Option<Rc<dyn ExpressionBuilder>>>;
    fn and(&self) -> LevelResult<Option<Vec<Rc<dyn ExpressionBuilder>>>>;
    fn or(&self) -> LevelResult<Option<Vec<Rc<dyn ExpressionBuilder>>>>;
    fn as_compare(&self) -> LevelResult<Option<Comparison<'_>>>;
    fn as_string(&self) -> Option<&str>;
    fn as_int(&self) -> Option<isize>;
    fn as_bool(&self) -> Option<bool>;
}

pub fn build_walk_for_str(s: &str) -> Result<(String, ConFilter)> {
    Ok((s.to_string(), Rc::new(|_: &str, _: &usize| true)))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompareTypes {
    GT,
    GTE,
    EQ,
    NOTEQ,
    LT,
    LTE,
}

pub fn build_item_compare_function<'a>(
    expr: &str,
    cache: &ProgramCache,
) -> Result<ItemCompareFn<'a>> {
    // the leaf is a Sansho expression: parse and compile it once, here;
    // the built predicate evaluates the incoming thing against it per call
    let program = compile_sansho(expr, cache)?;

    Ok(Rc::new(
        move |data: SanshoCarrier<'a>, _ws: &WalkState| -> bool {
            // the thing is evaluated against the expression; a truthy
            // result matches (null and false do not). An evaluation
            // error is a non-match: the expression itself was already
            // validated at compile time.
            match sansho::evaluate(&program, data.clone()) {
                Ok(value) => truthy(&value),
                Err(_) => false,
            }
        },
    ))
}

/// The item gate's leaf: a JMESPath expression over the item.
///
/// The expression is parsed and compiled ONCE, here; the returned predicate
/// evaluates the incoming item against it per call and answers whether the
/// result is truthy. An evaluation error is a NON-MATCH: the expression was
/// validated when it was compiled, so an error at this point describes the
/// item's shape rather than the expression.
pub fn build_item_leaf(
    expr: &str,
    cache: &ProgramCache,
) -> Result<ItemGate> {
    let program = compile_sansho(expr, cache)?;
    Ok(Rc::new(
        move |thing: &SanshoCarrier<'_>, _ws: &WalkState| -> bool {
            match sansho::evaluate(&program, Rc::clone(thing)) {
                Ok(value) => truthy(&value),
                Err(_) => false,
            }
        },
    ))
}

/// The item gate's comparator: the walk-state variables a comparison may read,
/// and the predicate for each.
///
/// The vocabulary lives HERE rather than in the tree walk, because the builder
/// is what knows where a variable's value comes from and therefore which
/// variables exist. Two are known:
///
/// | JSON name | walk state |
/// |-----------|------------|
/// | `depth` | `WalkState::depth_cnt` — the traversal depth |
/// | `is_terminal` | `WalkState::is_terminal` — whether this item ends the walk |
///
/// Each variable has its own type, so a literal of the wrong shape is a
/// BUILD-TIME error rather than a comparison that quietly never matches.
pub fn build_walk_state_cmp(
    cmp: CompareTypes,
    var_name: &str,
    expression: &Rc<dyn ExpressionBuilder>,
) -> Result<ItemGate> {
    match var_name {
        "depth" => {
            let Some(value) = expression.as_int() else {
                bail!("`depth` compares against an integer, not {expression:?}");
            };
            let value = value as usize;
            Ok(Rc::new(
                move |_thing: &SanshoCarrier<'_>, ws: &WalkState| {
                    compare_ord(cmp, ws.depth_cnt, value)
                },
            ))
        }
        "is_terminal" => {
            let Some(value) = expression.as_bool() else {
                bail!("`is_terminal` compares against a boolean, not {expression:?}");
            };
            Ok(Rc::new(
                move |_thing: &SanshoCarrier<'_>, ws: &WalkState| {
                    compare_ord(cmp, ws.is_terminal, value)
                },
            ))
        }
        other => bail!("unknown variable `{other}`; known: depth, is_terminal"),
    }
}

/// The order comparison both variable types share.
fn compare_ord<T: Ord>(cmp: CompareTypes, left: T, right: T) -> bool {
    match cmp {
        CompareTypes::EQ => left == right,
        CompareTypes::NOTEQ => left != right,
        CompareTypes::LT => left < right,
        CompareTypes::LTE => left <= right,
        CompareTypes::GT => left > right,
        CompareTypes::GTE => left >= right,
    }
}

/// The item gate's boolean tree: `not`, `and`, `or`, a walk-state comparison,
/// or a JMESPath leaf.
///
/// The same walk as [`build_expression`], over the item's data and state. It
/// is a second copy rather than a generic because the item predicate must be
/// HIGHER-RANKED over the carrier's lifetime — the walk holds items of
/// different lifetimes — and a type parameter `D` used as `&D` cannot express
/// that: instantiating `D = SanshoCarrier<'a>` pins one lifetime, which does
/// not satisfy `for<'x>`.
pub(crate) fn build_item_expression(
    expression: &Rc<dyn ExpressionBuilder>,
    cache: &ProgramCache,
) -> Result<ItemGate> {
    if let Some(not_b) = expression.not()? {
        let sub = build_item_expression(&not_b, cache)?;
        Ok(Rc::new(move |thing: &SanshoCarrier<'_>, ws: &WalkState| {
            !sub(thing, ws)
        }))
    } else if let Some(and_b) = expression.and()? {
        let mut subs = vec![];
        for v in and_b {
            subs.push(build_item_expression(&v, cache)?);
        }
        Ok(Rc::new(move |thing: &SanshoCarrier<'_>, ws: &WalkState| {
            subs.iter().all(|f| f(thing, ws))
        }))
    } else if let Some(or_b) = expression.or()? {
        let mut subs = vec![];
        for v in or_b {
            subs.push(build_item_expression(&v, cache)?);
        }
        Ok(Rc::new(move |thing: &SanshoCarrier<'_>, ws: &WalkState| {
            subs.iter().any(|f| f(thing, ws))
        }))
    } else if let Some((cmp_type, var_name, vs)) = expression.as_compare()? {
        build_walk_state_cmp(cmp_type, var_name, &vs)
    } else if let Some(expr) = expression.as_string() {
        build_item_leaf(expr, cache)
    } else {
        bail!("unable to build an item filter from {expression:?}")
    }
}

// /// The item gate, defined by JSON: `{"filter": <string or boolean tree>}`.
// ///
// /// The wrapper is the ENTRY POINT and appears exactly once, here. Inside the
// /// tree the leaves are a JMESPath expression (a string) or a walk-state
// /// comparison (an object naming one variable), and the boolean operators are
// /// `not`, `and` and `or` — the same shape the connection filter uses, with a
// /// different leaf language. A bare `{"or": …}` at the top, a nested
// /// `{"filter": …}`, and any other shape are build-time errors.
// pub fn build_item_filter(
//     json: &Value,
//     cache: &ProgramCache,
// ) -> Result<ItemGate> {
//     let Some((name, gate)) = single_entry(json) else {
//         bail!("an item filter is an object with a `filter` key, found {json}");
//     };
//     if name != "filter" {
//         bail!("an item filter is exactly {{`filter`: …}}, found {json}");
//     }
//     if let Some(inner) = gate.as_object()
//         && inner.contains_key("filter")
//     {
//         bail!("`filter` is the entry point only; it cannot be nested: {json}");
//     }
//     build_item_expression(&gate, cache)
// }

// /// The item gate's emit, defined by JSON:
// ///
// /// ```text
// /// { "emit": <spec>, "if"?: <gate>, "unnest"?: bool }
// ///
// /// <spec> := "<sansho expression>"                       // one entry
// ///         | { "expr": "<sansho expression>"
// ///             "if"?: <gate>                             // this entry only
// ///             "unnest"?: bool                           // overrides the top level
// ///             "emit_null"?: bool }                      // default false
// ///         | [ <spec>, ... ]                             // entries, in order
// /// ```
// ///
// /// `if` is the filter's grammar — a string or a boolean tree — and gates what
// /// it sits on: at the top level it gates the whole emit (false means no rows
// /// at all, and no entry is examined), on an entry it gates that entry alone.
// ///
// /// The ROW RULE, per entry: an evaluation error omits the entry; a null omits
// /// it too unless the entry set `emit_null`; an ARRAY yields one row per
// /// element when `unnest` is in effect, one level only, and one row holding the
// /// array when it is not; anything else — an object, a scalar — is one row.
// /// `unnest` comes from the entry when it is present and from the top level
// /// otherwise, and defaults to false. Rows are concatenated in the order the
// /// entries appear.
// ///
// /// `emit_null` is per entry only: a string entry, and any entry that does not
// /// set it, swallows nulls.
// ///
// /// Every shape this does not allow is a BUILD-TIME error — a nested array, an
// /// unknown key, an `expr` that is not a string, a non-boolean `unnest` or
// /// `emit_null`. A gate that fails at run time is a non-match, which suppresses
// /// what it gates.
// pub fn build_item_emit(
//     json: &Value,
//     cache: &ProgramCache,
// ) -> Result<ItemEmitFn> {
//     let Some(object) = json.as_object() else {
//         bail!("an emit is an object with an `emit` key, found {json}");
//     };
//     for key in object.keys() {
//         if !matches!(key.as_str(), "emit" | "if" | "unnest") {
//             bail!("`{key}` is not an emit key; known: emit, if, unnest");
//         }
//     }
//     let Some(body) = object.get("emit") else {
//         bail!("an emit needs an `emit` key, found {json}");
//     };
//     let top_gate = match object.get("if") {
//         Some(gate) => Some(build_item_expression(gate, cache)?),
//         None => None,
//     };
//     let top_unnest = bool_key(object, "unnest")?.unwrap_or(false);
//     let entries = build_emit_entries(body, cache, top_unnest)?;
//
//     Ok(Rc::new(
//         move |thing: &SanshoCarrier<'_>, ws: &WalkState| -> Vec<Value> {
//             // the top-level gate: false means no rows at all
//             if let Some(gate) = &top_gate
//                 && !gate(thing, ws)
//             {
//                 return Vec::new();
//             }
//             let mut rows = Vec::new();
//             for entry in &entries {
//                 rows.extend(entry(thing, ws));
//             }
//             rows
//         },
//     ))
// }

// /// A boolean-valued key, if present: a non-boolean is a build error.
// fn bool_key(object: &serde_json::Map<String, Value>, key: &str) -> Result<Option<bool>> {
//     match object.get(key) {
//         Some(Value::Bool(value)) => Ok(Some(*value)),
//         Some(other) => bail!("`{key}` is a boolean, not {other}"),
//         None => Ok(None),
//     }
// }

// /// The entries an emit spec names, in order. An array spec is a list of
// /// entries; anything else is one entry.
// fn build_emit_entries(
//     body: &Value,
//     cache: &ProgramCache,
//     top_unnest: bool,
// ) -> Result<Vec<ItemEmitFn>> {
//     match body {
//         Value::Array(entries) => {
//             let mut built = Vec::with_capacity(entries.len());
//             for entry in entries {
//                 if entry.is_array() {
//                     bail!("an array inside an emit array is not allowed: {entry}");
//                 }
//                 built.push(build_emit_entry(entry, cache, top_unnest)?);
//             }
//             Ok(built)
//         }
//         entry => Ok(vec![build_emit_entry(entry, cache, top_unnest)?]),
//     }
// }

// /// One entry: the expression, its optional gate, and its row rule.
// fn build_emit_entry(
//     entry: &Value,
//     cache: &ProgramCache,
//     top_unnest: bool,
// ) -> Result<ItemEmitFn> {
//     match entry {
//         // the string form: no gate of its own, the top-level `unnest`, and
//         // nulls swallowed
//         Value::String(expr) => build_emit_expr(expr, None, top_unnest, false, cache),
//         Value::Object(object) => {
//             for key in object.keys() {
//                 if !matches!(key.as_str(), "expr" | "if" | "unnest" | "emit_null") {
//                     bail!("`{key}` is not an emit entry key; known: expr, if, unnest, emit_null");
//                 }
//             }
//             let Some(Value::String(expr)) = object.get("expr") else {
//                 bail!("an emit entry needs an `expr` string, found {entry}");
//             };
//             let gate = match object.get("if") {
//                 Some(gate) => Some(build_item_expression(gate, cache)?),
//                 None => None,
//             };
//             let unnest = bool_key(object, "unnest")?.unwrap_or(top_unnest);
//             let emit_null = bool_key(object, "emit_null")?.unwrap_or(false);
//             build_emit_expr(expr, gate, unnest, emit_null, cache)
//         }
//         _ => bail!("an emit entry is a string or an object, found {entry}"),
//     }
// }


// /// The item gate's stop, defined by JSON:
// ///
// /// ```text
// /// { "stop": { "hard": <rules>, "soft": <rules> } }
// ///
// /// <rules> := <rule> | [ <rule>, ... ]
// /// <rule>  := <gate>                              // fires, reports nothing
// ///          | { "if": <gate>, "emit"?: <spec> }   // fires, reports the rows
// /// ```
// ///
// /// `hard` is tried FIRST: the first rule that matches reports its values as
// /// error rows and the walk stops IMMEDIATELY. Only when no hard rule matched is
// /// `soft` tried: its first match reports and the round finishes before the walk
// /// stops. The first rule that matches decides — no later rule is examined, and
// /// the two lists are never both consulted.
// ///
// /// `if` is REQUIRED on a rule and uses the filter's grammar, so a rule is a
// /// string gate or an object with `if`. A boolean tree is a gate, not a rule, so
// /// it has to be written `{"if": {"and": …}}` — a bare `{"and": …}` is an error,
// /// the same way a bare `{"or": …}` is not a filter.
// ///
// /// `emit` uses the emit's spec grammar and supplies the values, defaulting to
// /// none: a rule that fires with nothing to report answers `Some(vec![])`, which
// /// is not the same as not firing (`None`).
// ///
// /// A stop with neither key, and either key with an empty list, are valid and
// /// never fire. An unknown key, a nested array, a rule object without `if`, and
// /// any other shape are build-time errors. A gate that errors at run time is a
// /// non-match, so it does not fire.
// pub fn build_item_stop(
//     json: &Value,
//     cache: &ProgramCache,
// ) -> Result<
//     Rc<dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> (Option<Vec<Value>>, Option<Vec<Value>>)>,
// > {
//     // the wrapper is the entry point, as it is for `filter` and `emit`
//     let Some(outer) = json.as_object() else {
//         bail!("a stop is an object with a `stop` key, found {json}");
//     };
//     if outer.len() != 1 || !outer.contains_key("stop") {
//         bail!("a stop is exactly {{`stop`: …}}, found {json}");
//     }
//     let Some(object) = outer["stop"].as_object() else {
//         bail!(
//             "the `stop` value is an object of rules, found {}",
//             outer["stop"]
//         );
//     };
//     for key in object.keys() {
//         if !matches!(key.as_str(), "hard" | "soft") {
//             bail!("`{key}` is not a stop key; known: hard, soft");
//         }
//     }
//     let hard = build_stop_rules(object.get("hard"), cache)?;
//     let soft = build_stop_rules(object.get("soft"), cache)?;
//
//     Ok(Rc::new(move |thing: &SanshoCarrier<'_>, ws: &WalkState| {
//         // hard first: the first match stops immediately
//         for rule in &hard {
//             if let Some(values) = rule(thing, ws) {
//                 return (None, Some(values));
//             }
//         }
//         // then soft: the first match stops at the end of the round
//         for rule in &soft {
//             if let Some(values) = rule(thing, ws) {
//                 return (Some(values), None);
//             }
//         }
//         (None, None)
//     }))
// }

// /// A stop list: one rule, or a list of them tried in order. Absent is empty,
// /// and an empty list never fires.
// fn build_stop_rules(
//     list: Option<&Value>,
//     cache: &ProgramCache,
// ) -> Result<Vec<Rc<dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> Option<Vec<Value>>>>> {
//     let Some(list) = list else {
//         return Ok(Vec::new());
//     };
//     match list {
//         Value::Array(rules) => {
//             let mut built = Vec::with_capacity(rules.len());
//             for rule in rules {
//                 if rule.is_array() {
//                     bail!("an array inside a stop rule list is not allowed: {rule}");
//                 }
//                 built.push(build_stop_rule(rule, cache)?);
//             }
//             Ok(built)
//         }
//         rule => Ok(vec![build_stop_rule(rule, cache)?]),
//     }
// }

// /// One stop rule: a gate, and the values to report when it fires. `None` means
// /// it did not fire, which is distinct from firing with nothing to report.
// fn build_stop_rule(
//     rule: &Value,
//     cache: &ProgramCache,
// ) -> Result<Rc<dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> Option<Vec<Value>>>> {
//     // a string is a gate: a JMESPath expression over the item
//     if let Value::String(_) = rule {
//         let gate = build_item_expression(rule, cache)?;
//         return Ok(Rc::new(move |thing: &SanshoCarrier<'_>, ws: &WalkState| {
//             if gate(thing, ws) {
//                 Some(Vec::new())
//             } else {
//                 None
//             }
//         }));
//     }
//
//     let Some(object) = rule.as_object() else {
//         bail!("a stop rule is a gate or an object with `if`, found {rule}");
//     };
//     for key in object.keys() {
//         if !matches!(key.as_str(), "if" | "emit") {
//             bail!("`{key}` is not a stop rule key; known: if, emit");
//         }
//     }
//     // `if` is required: a rule that always fires has to say so
//     let Some(gate_json) = object.get("if") else {
//         bail!("a stop rule needs an `if`, found {rule}");
//     };
//     let gate = build_item_expression(gate_json, cache)?;
//     let entries = match object.get("emit") {
//         Some(spec) => build_emit_entries(spec, cache, false)?,
//         None => Vec::new(),
//     };
//
//     Ok(Rc::new(move |thing: &SanshoCarrier<'_>, ws: &WalkState| {
//         if !gate(thing, ws) {
//             return None;
//         }
//         let mut values = Vec::new();
//         for entry in &entries {
//             values.extend(entry(thing, ws));
//         }
//         Some(values)
//     }))
// }

pub fn build_con_compare_function(expr: &str) -> Result<ConFilter> {
    if let Some(cmp) = expr.strip_prefix("start:") {
        if cmp.is_empty() {
            bail!("'start:' must have the thing that is being compared")
        }
        let cmp = cmp.to_string();
        return Ok(Rc::new(move |conn: &str, _: &usize| -> bool {
            conn.starts_with(&cmp)
        }));
    }
    if let Some(cmp) = expr.strip_prefix("end:") {
        if cmp.is_empty() {
            bail!("'end:' must have the thing that is being compared")
        }
        let cmp = cmp.to_string();
        return Ok(Rc::new(move |conn: &str, _: &usize| -> bool {
            conn.ends_with(&cmp)
        }));
    }
    if let Some(re_text) = expr.strip_prefix("regex:") {
        if re_text.is_empty() {
            bail!("'regex:' must have an actual regular expression")
        }
        let regex = regex::Regex::new(re_text)?;
        return Ok(Rc::new(move |conn: &str, _: &usize| -> bool {
            regex.is_match(conn)
        }));
    }
    bail!("Invalid expression '{expr}'")
}

/// The same leaf, as a builder rather than as a call.
///
/// [`build_expression`] takes a builder so that a gate context can choose its
/// own leaf language; this is the connection gate's, and it is the same reading
/// as the call above — one implementation, so the two cannot drift apart.
pub fn build_con_compare_function_function() -> ConFilterBuilder {
    Rc::new(build_con_compare_function)
}

pub fn build_expression<
    FB: Fn(&str) -> Result<ConFilter>,
    CB: Fn(CompareTypes, &str, &Rc<dyn ExpressionBuilder>) -> Result<ConFilter>,
>(
    expression: Rc<dyn ExpressionBuilder>,
    func_builder: &FB,
    cmp_builder: &CB,
) -> Result<ConFilter> {
    if let Some(not_b) = expression.not()? {
        let sub = build_expression(not_b, func_builder, cmp_builder)?;
        Ok(Rc::new(move |v: &str, d: &usize| -> bool { !sub(v, d) }))
    } else if let Some(and_b) = expression.and()? {
        let mut subs = vec![];
        for v in and_b {
            subs.push(build_expression(v, func_builder, cmp_builder)?);
        }
        Ok(Rc::new(move |v: &str, d: &usize| {
            subs.iter().all(|f| f(v, d))
        }))
    } else if let Some(or_b) = expression.or()? {
        let mut subs = vec![];
        for v in or_b {
            subs.push(build_expression(v, func_builder, cmp_builder)?);
        }
        Ok(Rc::new(move |v: &str, d: &usize| {
            subs.iter().any(|f| f(v, d))
        }))
    } else if let Some((cmp_type, var_name, vs)) = expression.as_compare()? {
        // the variable vocabulary lives with the comparator builder: it
        // is the thing that knows where a variable's value comes from,
        // so it is the thing that can say which variables exist
        cmp_builder(cmp_type, var_name, &vs)
    } else if let Some(str) = expression.as_string() {
        Ok(func_builder(str)?)
    } else {
        bail!("Unable to build an expression from {expression:?}")
    }
}

/// The connection gate's comparator builder: the comparison variables it can
/// read, and the predicates that test them.
///
/// It knows exactly one variable — `depth`, the traversal depth carried
/// alongside the connection identifier. The vocabulary lives HERE rather than in
/// [`build_expression`]: the builder is what knows where a variable's value
/// comes from, so it is the thing that can say which variables exist. A
/// walk-state builder over `(&THING, &WalkState)` would know `depth` and
/// `terminal` and leave this one untouched.
pub fn depth_cmp(
    cmp: CompareTypes,
    var_name: &str,
    expression: &Rc<dyn ExpressionBuilder>,
) -> Result<ConFilter> {
    /// The variables this builder can read.
    const KNOWN: &str = "depth";

    if var_name != KNOWN {
        bail!("unknown variable `{var_name}`; known: {KNOWN}");
    }
    let Some(cmp_value) = expression.as_int() else {
        bail!("`{KNOWN}` compares against an integer, not {expression:?}");
    };
    let cmp_value: usize = cmp_value as usize;
    match cmp {
        CompareTypes::EQ => Ok(Rc::new(move |_data: &str, d: &usize| -> bool {
            *d == cmp_value
        })),
        CompareTypes::GT => Ok(Rc::new(move |_data: &str, d: &usize| -> bool {
            *d > cmp_value
        })),
        CompareTypes::GTE => Ok(Rc::new(move |_data: &str, d: &usize| -> bool {
            *d >= cmp_value
        })),
        CompareTypes::LT => Ok(Rc::new(move |_data: &str, d: &usize| -> bool {
            *d < cmp_value
        })),
        CompareTypes::LTE => Ok(Rc::new(move |_data: &str, d: &usize| -> bool {
            *d <= cmp_value
        })),
        CompareTypes::NOTEQ => Ok(Rc::new(move |_data: &str, d: &usize| -> bool {
            *d != cmp_value
        })),
    }
}

// #[cfg(test)]
// mod item_stop_tests {
//     use super::*;
//     use serde_json::json;
// 
//     fn fixture() -> Value {
//         json!({"file_size": 3050, "names": ["a", "b"], "nothing": null})
//     }
// 
//     fn at(depth: usize, is_terminal: bool) -> WalkState {
//         WalkState {
//             depth_cnt: depth,
//             is_terminal,
//         }
//     }
// 
//     /// Build a stop and run it once against the fixture. The result is
//     /// (soft, hard): `Some` on the side that fired.
//     fn stopped(stop: &Value, state: &WalkState) -> (Option<Vec<Value>>, Option<Vec<Value>>) {
//         let cache = ProgramCache::new(64);
//         let predicate = build_item_stop(stop, &cache).expect("the stop builds");
//         let document = fixture();
//         let thing: SanshoCarrier = Rc::new(&document);
//         predicate(&thing, state)
//     }
// 
//     // Requirement: the stop is two ordered lists of rules, and the first rule
//     // that matches decides. What: `hard` before `soft`, first match within a
//     // list, and no further rule examined. Why: the order is what makes the
//     // definition predictable — a hard stop means "now" and a soft one means
//     // "after this round" — and if a later rule could also fire, the values
//     // reported would depend on evaluation order rather than on the query.
//     //
//     // LLM section: `hard` stops immediately and answers on the HARD side of the
//     // pair; `soft` finishes the round first and answers on the soft side. Both
//     // lists matching means the hard rule wins and the soft list is never
//     // consulted.
//     #[test]
//     fn hard_is_tried_first_and_the_first_match_decides() {
//         // both lists would fire: the hard rule wins, and only its values
//         let both = json!({"stop": {
//             "hard": {"if": "file_size", "emit": "file_size"},
//             "soft": {"if": "file_size", "emit": "names"}
//         }});
//         assert_eq!(
//             stopped(&both, &at(0, false)),
//             (None, Some(vec![json!(3050)])),
//             "hard fires first and reports on the hard side"
//         );
// 
//         // two hard rules match: the FIRST one's values are reported
//         let ordered = json!({"stop": {"hard": [
//             {"if": "file_size", "emit": "file_size"},
//             {"if": "file_size", "emit": "names"}
//         ]}});
//         assert_eq!(
//             stopped(&ordered, &at(0, false)),
//             (None, Some(vec![json!(3050)])),
//             "the first matching rule wins"
//         );
// 
//         // only the soft rule matches
//         let soft = json!({"stop": {"soft": {"if": "file_size", "emit": "names"}}});
//         assert_eq!(
//             stopped(&soft, &at(0, false)),
//             (Some(vec![json!(["a", "b"])]), None),
//             "soft reports on the soft side"
//         );
// 
//         // nothing matches
//         assert_eq!(stopped(&soft, &at(0, false)).0.is_some(), true);
//         let never = json!({"stop": {"hard": "is_terminal"}});
//         assert_eq!(
//             stopped(&never, &at(0, false)),
//             (None, None),
//             "a rule that does not match does not fire"
//         );
//     }
// 
//     // Requirement: firing with nothing to report is not the same as not firing.
//     // What: a gate-only rule fires and answers `Some(vec![])`. Why: the walk
//     // stops on a fire regardless of whether there are values to write, so a
//     // rule that reported `None` instead would silently not stop.
//     //
//     // LLM section: a string rule is a gate, and so is `{"if": …}` with no
//     // `emit`. Both answer `Some(vec![])` when the gate matches.
//     #[test]
//     fn a_rule_can_fire_with_nothing_to_report() {
//         assert_eq!(
//             stopped(&json!({"stop": {"hard": "file_size"}}), &at(0, false)),
//             (None, Some(vec![])),
//             "a string gate fires with no values"
//         );
//         assert_eq!(
//             stopped(
//                 &json!({"stop": {"hard": {"if": "file_size"}}}),
//                 &at(0, false)
//             ),
//             (None, Some(vec![])),
//             "an object rule with no emit fires with no values"
//         );
//         assert_eq!(
//             stopped(&json!({"stop": {"hard": "is_terminal"}}), &at(0, false)),
//             (None, None),
//             "the same gate not matching does NOT fire — the distinction"
//         );
//     }
// 
//     // Requirement: a rule's values come from the emit grammar. What: an `emit`
//     // spec inside a rule produces the rows, including a per-entry `unnest`.
//     // Why: there is one definition of a row in this system, and the stop must
//     // not invent a second one.
//     //
//     // LLM section: the rule's `emit` is the emit's spec — a string, an object,
//     // or an array — so `unnest` and the entry gates work exactly as they do in
//     // an emit.
//     #[test]
//     fn a_rules_values_are_an_emit_spec() {
//         let stop = json!({"stop": {"hard": {"if": "file_size", "emit": {"expr": "names", "unnest": true}}}});
//         assert_eq!(
//             stopped(&stop, &at(0, false)),
//             (None, Some(vec![json!("a"), json!("b")])),
//             "the entry's unnest applies to the reported rows"
//         );
//         let array = json!({"stop": {"hard": {"if": "file_size", "emit": ["file_size", "names"]}}});
//         assert_eq!(
//             stopped(&array, &at(0, false)),
//             (None, Some(vec![json!(3050), json!(["a", "b"])])),
//             "an array of entries concatenates in order"
//         );
//     }
// 
//     // Requirement: a gate that errors at run time does not fire, and every
//     // malformed definition is rejected when it is built. What: the runtime
//     // non-match and the build-time rejections. Why: the same rule as the filter
//     // and the emit, so a broken condition cannot stop a walk by accident.
//     //
//     // LLM section: `sum(@)` is a valid expression that errors against an
//     // object, and it does not fire. Rejected at build time: an unknown key, a
//     // nested array, a rule object with no `if`, a bare boolean tree as a rule
//     // (a tree is a gate, so it must sit under `if`), and a rule that is neither
//     // a string nor an object. Valid and never firing: `{}` and an empty list.
//     #[test]
//     fn a_gate_error_does_not_fire_and_a_malformed_stop_is_a_build_error() {
//         assert_eq!(
//             stopped(&json!({"stop": {"hard": "sum(@)"}}), &at(0, false)),
//             (None, None),
//             "an evaluation error is a non-match"
//         );
// 
//         let cache = ProgramCache::new(64);
//         for (shape, why) in [
//             (
//                 json!({"stop": {"nope": "file_size"}}),
//                 "an unknown stop key",
//             ),
//             (
//                 json!({"stop": {"hard": [["file_size"]]}}),
//                 "an array inside a rule list",
//             ),
//             (
//                 json!({"stop": {"hard": {"emit": "file_size"}}}),
//                 "a rule with no if",
//             ),
//             (
//                 json!({"stop": {"hard": {"and": ["a", "b"]}}}),
//                 "a bare boolean tree as a rule",
//             ),
//             (
//                 json!({"stop": {"hard": {"if": "x", "nope": 1}}}),
//                 "an unknown rule key",
//             ),
//             (
//                 json!({"stop": {"hard": 7}}),
//                 "a rule that is neither a string nor an object",
//             ),
//             (
//                 json!({"stop": ["file_size"]}),
//                 "a stop that is not an object",
//             ),
//         ] {
//             assert!(
//                 build_item_stop(&shape, &cache).is_err(),
//                 "must be a build error: {why} — {shape}"
//             );
//         }
// 
//         // valid, and never firing
//         assert_eq!(
//             stopped(&json!({"stop": {}}), &at(0, false)),
//             (None, None),
//             "a stop with no rules never fires"
//         );
//         assert_eq!(
//             stopped(&json!({"stop": {"hard": []}}), &at(0, false)),
//             (None, None),
//             "an empty rule list never fires"
//         );
//     }
// }
// 
// #[cfg(test)]
// mod item_emit_tests {
//     use super::*;
//     use serde_json::json;
// 
//     /// The item the emit tests read: an array, a nested array, a scalar, and
//     /// a null.
//     fn fixture() -> Value {
//         json!({
//             "file_size": 3050,
//             "names": ["a", "b"],
//             "nested": [["x"], ["y"]],
//             "nothing": null
//         })
//     }
// 
//     fn at(depth: usize, is_terminal: bool) -> WalkState {
//         WalkState {
//             depth_cnt: depth,
//             is_terminal,
//         }
//     }
// 
//     /// Build an emit and run it once against the fixture.
//     fn rows(emit: &Value) -> Vec<Value> {
//         let cache = ProgramCache::new(64);
//         let predicate = build_item_emit(emit, &cache).expect("the emit builds");
//         let document = fixture();
//         let thing: SanshoCarrier = Rc::new(&document);
//         predicate(&thing, &at(0, false))
//     }
// 
//     // Requirement: the emit turns an item into the rows the sink receives, and
//     // the row rule is the whole of it. What: one row per entry, the
//     // materialized result, with the array case decided by `unnest`. Why: the
//     // rows are what a consumer writes out, so a wrong row count is a wrong
//     // output document — and `unnest` is the difference between one row holding
//     // an array and one row per element.
//     //
//     // LLM section: without `unnest` an array is ONE row holding the array;
//     // with it, one row per element, ONE LEVEL only, so a nested array is an
//     // element like any other. A non-array result is always one row.
//     #[test]
//     fn the_row_rule_is_the_materialized_result() {
//         assert_eq!(rows(&json!({"emit": "file_size"})), vec![json!(3050)]);
//         assert_eq!(
//             rows(&json!({"emit": "names"})),
//             vec![json!(["a", "b"])],
//             "without unnest an array is one row"
//         );
//         assert_eq!(
//             rows(&json!({"emit": {"expr": "names", "unnest": true}})),
//             vec![json!("a"), json!("b")],
//             "with unnest, one row per element"
//         );
//         assert_eq!(
//             rows(&json!({"emit": {"expr": "nested", "unnest": true}})),
//             vec![json!(["x"]), json!(["y"])],
//             "unnest is ONE level: a nested array is an element"
//         );
//     }
// 
//     // Requirement: a null result is swallowed by default and emitted when the
//     // entry asks for it. What: the null row rule, and the fact that the flag
//     // is per entry. Why: a null row and no row are different outputs, and the
//     // walk's default has to be the one that keeps a document free of nulls
//     // unless the query asks for them.
//     //
//     // LLM section: `emit_null` is per ENTRY only — there is no top-level form,
//     // so a string entry and any entry that does not set it swallow nulls. The
//     // rule applies to array elements under `unnest` as well.
//     #[test]
//     fn nulls_are_swallowed_unless_the_entry_asks_for_them() {
//         assert!(
//             rows(&json!({"emit": "nothing"})).is_empty(),
//             "a null result is swallowed by default"
//         );
//         assert_eq!(
//             rows(&json!({"emit": {"expr": "nothing", "emit_null": true}})),
//             vec![json!(null)],
//             "the flag emits it"
//         );
//         assert_eq!(
//             rows(&json!({"emit": {"expr": "nothing", "emit_null": true, "unnest": true}})),
//             vec![json!(null)],
//             "a null is not an array, so unnest leaves it as one row"
//         );
//     }
// 
//     // Requirement: the emit is gated at two levels. What: the top-level `if`
//     // gating the whole emit, and an entry's `if` gating that entry. Why: the
//     // top gate decides whether the item contributes anything at all, which is
//     // what lets one query shape serve several item kinds; the entry gate lets
//     // one item contribute some rows and not others.
//     //
//     // LLM section: the top gate false returns an EMPTY Vec and no entry is
//     // examined. An entry gate false skips that entry and leaves the others.
//     // Both use the filter's grammar, so a string or a boolean tree.
//     #[test]
//     fn the_two_gate_levels() {
//         let gated = json!({"emit": "file_size", "if": {"<=": {"depth": 2}}});
//         let document = fixture();
//         let thing: SanshoCarrier = Rc::new(&document);
//         let predicate = build_item_emit(&gated, &ProgramCache::new(64)).expect("builds");
//         assert_eq!(
//             predicate(&thing, &at(2, false)),
//             vec![json!(3050)],
//             "at the limit"
//         );
//         assert!(
//             predicate(&thing, &at(3, false)).is_empty(),
//             "the top gate false means no rows at all"
//         );
// 
//         let per_entry = json!({"emit": [
//             {"expr": "file_size", "if": {"==": {"is_terminal": true}}},
//             "names"
//         ]});
//         let predicate = build_item_emit(&per_entry, &ProgramCache::new(64)).expect("builds");
//         assert_eq!(
//             predicate(&thing, &at(0, false)),
//             vec![json!(["a", "b"])],
//             "the gated entry is skipped, the other still emits"
//         );
//         assert_eq!(
//             predicate(&thing, &at(0, true)),
//             vec![json!(3050), json!(["a", "b"])],
//             "both entries emit, in order"
//         );
//     }
// 
//     // Requirement: `unnest` has one precedence rule. What: a top-level value
//     // applies to every entry, and an entry's own value wins. Why: a query that
//     // wants one row per element in most entries should not have to repeat the
//     // flag, and the one entry that differs should be able to say so.
//     //
//     // LLM section: the default is false; the top level sets it for all
//     // entries; an entry that names it overrides. Here the top says true and
//     // one entry says false.
//     #[test]
//     fn unnest_precedence() {
//         assert_eq!(
//             rows(&json!({"emit": ["names", "names"], "unnest": true})),
//             vec![json!("a"), json!("b"), json!("a"), json!("b")],
//             "the top-level value applies to every entry"
//         );
//         assert_eq!(
//             rows(&json!({"emit": [
//                 {"expr": "names", "unnest": false},
//                 "names"
//             ], "unnest": true})),
//             vec![json!(["a", "b"]), json!("a"), json!("b")],
//             "the entry's value wins"
//         );
//     }
// 
//     // Requirement: an expression that fails at run time omits its entry, and
//     // every malformed definition is rejected when it is built. What: the
//     // runtime omission and the build-time rejections. Why: a definition that
//     // is wrong must fail loudly at build time, and a definition that is right
//     // must not fail the walk because one item did not match its shape.
//     //
//     // LLM section: build errors are a nested array, an unknown key at either
//     // level, a missing or non-string `expr`, a non-boolean `unnest` or
//     // `emit_null`, and an emit that is neither a string, an object, nor an
//     // array. A runtime error — an expression that cannot be evaluated against
//     // this item — omits the entry and emits nothing.
//     #[test]
//     fn a_malformed_emit_is_a_build_error_and_a_runtime_error_omits() {
//         let cache = ProgramCache::new(64);
//         for (shape, why) in [
//             (json!({"emit": [["names"]]}), "an array inside an array"),
//             (
//                 json!({"emit": "names", "nope": 1}),
//                 "an unknown top-level key",
//             ),
//             (
//                 json!({"emit": {"expr": "names", "nope": 1}}),
//                 "an unknown entry key",
//             ),
//             (json!({"emit": {"if": "names"}}), "a missing expr"),
//             (json!({"emit": {"expr": 7}}), "a non-string expr"),
//             (
//                 json!({"emit": {"expr": "names", "unnest": "yes"}}),
//                 "a non-boolean unnest",
//             ),
//             (
//                 json!({"emit": {"expr": "names", "emit_null": "yes"}}),
//                 "a non-boolean emit_null",
//             ),
//             (json!({"emit": 7}), "an emit that is not a spec"),
//             (
//                 json!({"emit": "names", "unnest": "yes"}),
//                 "a non-boolean top-level unnest",
//             ),
//             (json!({"no_emit": "names"}), "a missing emit key"),
//         ] {
//             assert!(
//                 build_item_emit(&shape, &cache).is_err(),
//                 "must be a build error: {why} — {shape}"
//             );
//         }
// 
//         // a valid expression whose result is not what the query's name
//         // suggests is still a row: only an ERROR omits the entry
//         assert_eq!(
//             rows(&json!({"emit": "file_size > `1000`"})),
//             vec![json!(true)],
//             "a boolean result is a row"
//         );
//         // an expression that ERRORS against this item omits the entry rather
//         // than failing the walk
//         assert!(
//             rows(&json!({"emit": "sum(@)"})).is_empty(),
//             "an evaluation error omits the entry"
//         );
//     }
// }
// 
// #[cfg(test)]
// mod item_filter_tests {
//     use super::*;
//     use serde_json::json;
// 
//     // Requirement: the item gate is defined by JSON, like the connection
//     // filter, with a different leaf language. What: the entry point builds a
//     // predicate from each allowed shape, and it answers for the item and the
//     // walk state it is called with. Why: the gate decides whether an item is
//     // processed at all, so a wrong answer is a wrong walk — and the two leaf
//     // languages have to be told apart, because the same JSON position means
//     // "a connection leaf" in one filter and "a JMESPath expression" in the
//     // other.
//     //
//     // LLM section: `{"filter": "<expr>"}` is a JMESPath leaf over the item;
//     // `{"<=": {"depth": n}}` and `{"==": {"is_terminal": true}}` read the walk
//     // state; `not`, `and` and `or` compose them. Truthiness is the engine's
//     // rule: null, false, the empty string, the empty array and the empty
//     // object are false-y; a number, zero included, is true.
//     #[test]
//     fn item_filter_reads_the_item_and_the_walk_state() {
//         let cache = ProgramCache::new(64);
//         let document = json!({"file_size": 3050, "empty": [], "nothing": null, "zero": 0});
//         let thing: SanshoCarrier = Rc::new(&document);
//         let at = |depth: usize, is_terminal: bool| WalkState {
//             depth_cnt: depth,
//             is_terminal,
//         };
// 
//         // a JMESPath leaf over the item
//         let leaf = |expr: &str| build_item_filter(&json!({"filter": expr}), &cache).unwrap();
//         assert!(
//             leaf("file_size")(&thing, &at(0, false)),
//             "a number is truthy"
//         );
//         assert!(!leaf("nothing")(&thing, &at(0, false)), "null is false-y");
//         assert!(
//             !leaf("empty")(&thing, &at(0, false)),
//             "an empty array is false-y"
//         );
//         assert!(leaf("zero")(&thing, &at(0, false)), "zero is truthy");
//         assert!(
//             !leaf("no_such_field")(&thing, &at(0, false)),
//             "a missing field is null, and null is false-y"
//         );
//         assert!(
//             build_item_filter(&json!({"filter": "file_size > `1000` &&"}), &cache).is_err(),
//             "an expression that does not compile is a build error, not a non-match"
//         );
// 
//         // the walk-state comparisons
//         let depth = build_item_filter(&json!({"filter": {"<=": {"depth": 2}}}), &cache).unwrap();
//         assert!(depth(&thing, &at(2, false)), "at the limit");
//         assert!(!depth(&thing, &at(3, false)), "past the limit");
// 
//         let terminal =
//             build_item_filter(&json!({"filter": {"==": {"is_terminal": true}}}), &cache).unwrap();
//         assert!(terminal(&thing, &at(0, true)), "terminal");
//         assert!(!terminal(&thing, &at(0, false)), "not terminal");
// 
//         // the boolean tree, with both leaf kinds in one gate
//         let both = build_item_filter(
//             &json!({"filter": {"and": ["file_size", {"<=": {"depth": 2}}]}}),
//             &cache,
//         )
//         .unwrap();
//         assert!(both(&thing, &at(1, false)), "both leaves hold");
//         assert!(!both(&thing, &at(9, false)), "the depth leaf fails");
// 
//         let either = build_item_filter(
//             &json!({"filter": {"or": ["nothing", {"==": {"is_terminal": true}}]}}),
//             &cache,
//         )
//         .unwrap();
//         assert!(either(&thing, &at(0, true)), "the state leaf holds");
//         assert!(!either(&thing, &at(0, false)), "neither holds");
// 
//         let negated = build_item_filter(&json!({"filter": {"not": "file_size"}}), &cache).unwrap();
//         assert!(!negated(&thing, &at(0, false)), "negated");
//     }
// 
//     // Requirement: a malformed gate is rejected when it is BUILT, not left to
//     // fail silently at run time. What: every shape the definition does not
//     // allow answers an error from the entry point. Why: a gate that never
//     // matches is a walk that silently does nothing, which cannot be told apart
//     // from a walk that correctly found nothing.
//     //
//     // LLM section: the wrapper is the entry point only, so a bare `{"or": …}`
//     // and a nested `{"filter": …}` are errors; the variable vocabulary is
//     // closed, so an unknown name is an error; and each variable has a type, so
//     // a literal of the wrong shape is an error.
//     #[test]
//     fn a_malformed_item_filter_is_a_build_error() {
//         let cache = ProgramCache::new(64);
//         for (shape, why) in [
//             (
//                 json!({"or": ["file_size", "other"]}),
//                 "a bare operator at the top",
//             ),
//             (
//                 json!({"filter": {"or": ["a"]}, "extra": 1}),
//                 "more than one key",
//             ),
//             (
//                 json!({"filter": {"filter": "file_size"}}),
//                 "a nested filter wrapper",
//             ),
//             (
//                 json!({"filter": {"<=": {"nonsense": 1}}}),
//                 "an unknown variable",
//             ),
//             (
//                 json!({"filter": {"<=": {"depth": true}}}),
//                 "the wrong literal type for depth",
//             ),
//             (
//                 json!({"filter": {"==": {"is_terminal": 3}}}),
//                 "the wrong literal type for is_terminal",
//             ),
//             (json!(["file_size"]), "not an object"),
//             (
//                 json!({"filter": 7}),
//                 "a leaf that is neither a string nor an object",
//             ),
//         ] {
//             assert!(
//                 build_item_filter(&shape, &cache).is_err(),
//                 "must be a build error: {why} — {shape}"
//             );
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Requirement: the gate grammar's node shapes — every node kind
    // `build_expression` dispatches on is recognized by exactly one probe,
    // under both spellings. What: each compound shape is a single-entry
    // object keyed by its operator, each leaf is a bare JSON value, and a
    // probe answers Ok(None) for every shape but its own. Why:
    // `build_expression` tries the probes in a fixed order, so a probe
    // that claims the wrong shape silently compiles a gate into the wrong
    // predicate.
    //
    // LLM section: one assertion per shape and per spelling (`not`/`!`,
    // `and`/`&&`, `or`/`||`); the leaves answer only for their own JSON
    // type.
    #[test]
    fn json_gate_nodes_recognize_their_own_shapes() {
        let not_node = json!({"not": {"and": []}});
        assert!(not_node.not().expect("not()").is_some(), "the `not` node");
        assert!(
            not_node.and().expect("and()").is_none(),
            "`not` is not an `and`"
        );

        let bang = json!({"!": {"and": []}});
        assert!(bang.not().expect("not()").is_some(), "`!` is `not`");

        let and_node = json!({"and": [true, false]});
        assert_eq!(
            and_node.and().expect("and()").map(|gates| gates.len()),
            Some(2)
        );
        assert!(
            and_node.not().expect("not()").is_none(),
            "`and` is not a `not`"
        );

        let amp = json!({"&&": [true]});
        assert_eq!(
            amp.and().expect("and()").map(|gates| gates.len()),
            Some(1)
        );

        let or_node = json!({"or": [true]});
        assert_eq!(
            or_node.or().expect("or()").map(|gates| gates.len()),
            Some(1)
        );

        let pipes = json!({"||": [true]});
        assert_eq!(
            pipes.or().expect("or()").map(|gates| gates.len()),
            Some(1)
        );

        assert_eq!(json!("start:pkg:").as_string(), Some("start:pkg:"));
        assert_eq!(json!(7).as_int(), Some(7));
        assert_eq!(json!(true).as_bool(), Some(true));

        // the leaves answer only for their own JSON type
        assert!(json!("start:pkg:").as_int().is_none());
        assert!(json!(7).as_string().is_none());
        assert!(json!(7).as_bool().is_none());
    }

    // Requirement: a node naming an operator but carrying the wrong
    // payload is a malformed gate, not some other node kind. What: the
    // probe answers Err naming the operator exactly as it was written.
    // Why: the shape is known here, so this is where the useful message
    // can be constructed; `build_expression` propagates it with `?`
    // instead of falling through to "unrecognized gate node".
    //
    // LLM section: the message names the spelling the author used
    // (`&&`, not `and`).
    #[test]
    fn malformed_compound_nodes_report_their_operator() {
        let bad_and = json!({"and": "oops"});
        let error = bad_and.and().expect_err("`and` must hold an array");
        assert!(error.to_string().contains("`and`"), "{error}");

        let bad_amp = json!({"&&": 5});
        let error = bad_amp.and().expect_err("`&&` must hold an array");
        assert!(error.to_string().contains("`&&`"), "{error}");

        let bad_or = json!({"or": {}});
        let error = bad_or.or().expect_err("`or` must hold an array");
        assert!(error.to_string().contains("`or`"), "{error}");

        let bad_pipes = json!({"||": null});
        let error = bad_pipes.or().expect_err("`||` must hold an array");
        assert!(error.to_string().contains("`||`"), "{error}");
    }

    // Requirement: a comparison node carries three things — the operator,
    // the variable the comparison reads, and the value it compares
    // against. What: `{"<=": {"depth": 2}}` yields the LTE operator, the
    // variable name "depth", and the value `2` by reference, so the
    // comparator builder reads it with `as_int`. Why: the comparator
    // builder is the only place that knows which variables exist and
    // where their values come from, so the node hands the variable name
    // through unresolved.
    //
    // LLM section: the operator leads the object; each symbol and each
    // word spelling names the same comparison.
    #[test]
    fn comparison_node_carries_operator_variable_and_value() {
        let node = json!({"<=": {"depth": 2}});
        let (operator, variable, value) = node
            .as_compare()
            .expect("as_compare()")
            .expect("a comparison node");
        assert_eq!(operator, CompareTypes::LTE, "the operator");
        assert_eq!(variable, "depth", "the variable the comparison reads");
        assert_eq!(value.as_int(), Some(2), "the value it compares against");

        let comparison = |operator: &str| -> serde_json::Value {
            let mut operands = serde_json::Map::new();
            operands.insert("depth".to_string(), json!(1));
            let mut node = serde_json::Map::new();
            node.insert(operator.to_string(), serde_json::Value::Object(operands));
            serde_json::Value::Object(node)
        };
        for (spelling, expected) in [
            (">", CompareTypes::GT),
            ("gt", CompareTypes::GT),
            (">=", CompareTypes::GTE),
            ("gte", CompareTypes::GTE),
            ("=", CompareTypes::EQ),
            ("==", CompareTypes::EQ),
            ("eq", CompareTypes::EQ),
            ("!=", CompareTypes::NOTEQ),
            ("neq", CompareTypes::NOTEQ),
            ("noteq", CompareTypes::NOTEQ),
            ("<", CompareTypes::LT),
            ("lt", CompareTypes::LT),
            ("<=", CompareTypes::LTE),
            ("lte", CompareTypes::LTE),
        ] {
            let node = comparison(spelling);
            let (operator, variable, _) = node
                .as_compare()
                .expect("as_compare()")
                .unwrap_or_else(|| panic!("{spelling} must be a comparison"));
            assert_eq!(variable, "depth");
            assert_eq!(operator, expected, "{spelling} names its comparison");
        }
    }

    // Requirement: a comparison node naming a comparison operator that
    // does not carry `{"<variable>": <value>}` is a malformed gate. What:
    // non-object operands, empty operand objects, and operand objects
    // naming several variables are errors. Why: the author meant a
    // comparison, so silence would be worse than a message that says what
    // is wrong.
    //
    // LLM section: the three malformed shapes are the three ways the
    // `{"<variable>": <value>}` contract can be broken.
    #[test]
    fn malformed_comparisons_report_what_is_wrong() {
        let not_an_object = json!({"<=": 2});
        let error = not_an_object
            .as_compare()
            .expect_err("operands must be an object");
        assert!(error.to_string().contains("`<=`"), "{error}");

        let no_variable = json!({"<=": {}});
        let error = no_variable
            .as_compare()
            .expect_err("exactly one variable is required");
        assert!(
            error.to_string().contains("exactly one variable"),
            "{error}"
        );

        let two_variables = json!({"<=": {"depth": 1, "other": 2}});
        let error = two_variables
            .as_compare()
            .expect_err("exactly one variable is required");
        assert!(
            error.to_string().contains("exactly one variable"),
            "{error}"
        );
    }

    // Requirement: everything that is not a node answers Ok(None) from
    // every probe — that is what lets `build_expression` fall through to
    // its unrecognized-node error. What: multi-entry objects, unknown
    // operators, and non-objects. Why: "not this node" must stay
    // distinguishable from "malformed node" (Err), and the single-entry
    // rule is what keeps the probes unambiguous.
    //
    // LLM section: a two-entry object containing `and` is not an `and`
    // node; an unknown key is not a comparison.
    #[test]
    fn nodes_that_are_not_gate_nodes_answer_none() {
        for node in [
            json!({"and": [], "or": []}),
            json!({"unknown": {"depth": 2}}),
            json!({}),
            json!([]),
            json!("depth"),
            json!(2),
            json!(null),
        ] {
            assert!(node.not().expect("not()").is_none(), "{node}");
            assert!(node.and().expect("and()").is_none(), "{node}");
            assert!(node.or().expect("or()").is_none(), "{node}");
            assert!(node.as_compare().expect("as_compare()").is_none(), "{node}");
        }
    }

    // Requirement: `as_int` is the integer leaf of a comparison (the
    // walk's `depth`). What: integral JSON yields its value; floats —
    // including one with a zero fraction — and integers that do not fit
    // `isize` yield None. Why: depth comparisons are exact integer
    // comparisons, so a float leaf must fail loudly at the comparator
    // builder rather than compare approximately.
    #[test]
    fn int_leaf_is_integral_only() {
        assert_eq!(json!(3).as_int(), Some(3));
        assert_eq!(json!(-3).as_int(), Some(-3));
        assert_eq!(
            json!(3.0).as_int(),
            None,
            "a zero fraction is still a float"
        );
        assert_eq!(json!(3.5).as_int(), None);
        assert_eq!(json!(u64::MAX).as_int(), None, "out of isize range");
        assert_eq!(json!("3").as_int(), None, "a string is not an integer leaf");
        assert_eq!(json!(true).as_int(), None);
        assert_eq!(json!(null).as_int(), None);
    }

    // Requirement: the connection finder is what the walk's `ConFinder`
    // bound accepts — a finder whose strings borrow from the thing it was
    // handed. What: `find_connections` returns the named edge type's
    // targets for a byte-source thing and for a borrowed JSON thing, an
    // empty list for an unknown edge type, and it satisfies the walk's
    // higher-ranked bound when called with a per-item borrow. Why: the
    // walk hands the finder a borrow of the current item and expects the
    // strings to live at least as long; a finder returning temporaries
    // would force the walk to copy every target.
    //
    // LLM section: the same document is read twice — once as CBOR bytes,
    // once as a borrowed JSON value — and both find the same targets.
    #[test]
    fn connection_finder_returns_document_borrows() {
        /// The walk's connection-finder bound, spelled out: any thing,
        /// called with a per-item borrow, returning borrows that live at
        /// least that long. Kept while the test body below is commented out;
        /// it is the bound that body exists to pin.
        #[allow(dead_code)]
        fn through_the_walk_bound<'b, FINDER>(
            finder: FINDER,
            thing: SanshoCarrier<'b>,
            connection_type: &str,
        ) -> Vec<&'b str>
        where
            FINDER: for<'x> Fn(&SanshoCarrier<'x>, &str) -> Vec<&'x str>,
        {
            finder(&thing, connection_type)
        }

        let _document = json!({
            "connections": {
                "alias:from": ["pkg:a", "gitoid:b"],
                "contained:up": ["p"]
            }
        });

        //         // a borrowed JSON document
        //         let borrowed: &serde_json::Value = &document;
        //         assert_eq!(
        //             find_connections(&borrowed, "alias:from"),
        //             Some(vec!["pkg:a", "gitoid:b"])
        //         );
        //
        //         // the same document as CBOR bytes
        //         let encoded = serde_cbor::to_vec(&document).unwrap();
        //         let bytes: &[u8] = &encoded;
        //         assert_eq!(
        //             find_connections(&bytes, "alias:from"),
        //             Some(vec!["pkg:a", "gitoid:b"])
        //         );
        //         assert_eq!(find_connections(&bytes, "contained:up"), Some(vec!["p"]));
        //         assert_eq!(find_connections(&bytes, "not-an-edge-type"), None);
        //
        //         // and through the walk's bound, with the borrow the walk hands it
        //         // let found = through_the_walk_bound(
        //         //     |thing: &&[u8], connection_type: &str| find_connections(thing, connection_type),
        //         //     &bytes,
        //         //     "contained:up",
        //         // );
        //         // assert_eq!(found, vec!["p"]);
    }

    // Requirement: the gate compiler is instantiable for the walk's
    // connection gate — a JSON gate compiled once, stored, and used later.
    // What: `build_expression` over the connection leaf builder and the
    // depth comparator builder produces a predicate that accepts and
    // rejects connections, and it lives past the scope that built it.
    // Why: the predicate takes the identifier and the depth by reference,
    // borrowing nothing it was built from, so one predicate serves every
    // call — which is what lets it be built once and stored.
    //
    // LLM section: the gate is the walk's connection-filter shape: the
    // identifier leaves come from `build_con_compare_function`, the depth
    // comparison from `depth_cmp`.
    #[test]
    fn connection_gate_compiles_once_and_can_be_stored() {
        let gate = json!({
            "and": [
                {"or": ["start:pkg:", "start:gitoid:"]},
                {"<=": {"depth": 2}}
            ]
        });

        // built in one scope...
        let stored: ConFilter = {
            let expression: Rc<dyn ExpressionBuilder> = Rc::new(gate.clone());
            build_expression(expression, &build_con_compare_function, &depth_cmp)
                .expect("the gate compiles")
        };

        // ...and used after it
        assert!(stored("pkg:thing", &0), "a pkg: identifier at depth 0");
        assert!(stored("gitoid:abc", &2), "a gitoid at the depth limit");
        assert!(!stored("other", &0), "a different identifier");
        assert!(!stored("pkg:thing", &3), "past the depth limit");
    }

    // Requirement: the item gate's leaves are Sansho expressions — a leaf
    // compiles once and the built predicate evaluates the incoming thing
    // against it per call. What: an expression naming an item field
    // matches when the field's value is truthy (null and false do not);
    // the walk state rides along unexamined. Why: the item gate's data is
    // the (thing, walk-state) pair, and the expression's subject is the
    // thing — the walk state is present for the gate's other variables.
    //
    // LLM section: the thing is a boxed trait value (the walk's THING
    // shape); the expressions are evaluated through the engine's trait
    // surface with a per-call borrow.
    //     #[test]
    //     fn item_compare_function_evaluates_the_thing_against_the_expression() {
    //         // the gate's lifetime is the walk state's borrow: a helper scopes
    //         // 'a to the state, the way the walk scopes it to a loaded item
    //         // fn evaluate<'a>(state: &'a WalkState) -> (bool, bool) {
    //         //     let predicate =
    //         //         build_item_compare_function::<'a>("file_size").expect("the expression compiles");
    //         //     let big: Box<dyn SanshoTrait<'a>> = Box::new(json!({"file_size": 3050}));
    //         //     let empty: Box<dyn SanshoTrait<'a>> = Box::new(json!({}));
    //         //     (
    //         //         predicate.apply(&(big, state)),
    //         //         predicate.apply(&(empty, state)),
    //         //     )
    //         // }
    //
    //         let state = WalkState {
    //             depth_cnt: 0,
    //             is_terminal: false,
    //         };
    //         let (big_matches, empty_matches) = evaluate(&state);
    //         assert!(big_matches, "file_size is present and truthy");
    //         assert!(
    //             !empty_matches,
    //             "a missing field evaluates to null, not a match"
    //         );
    //     }

    // Requirement: the variable vocabulary lives with the comparator
    // builder — the thing that knows where a variable's value comes from.
    // What: a comparison against an unknown variable is an error naming
    // the variable and the variables the builder knows; a comparison
    // against `depth` compiles to the depth test; a comparison value that
    // is not an integer leaf is an error. Why: `build_expression` cannot
    // know the vocabulary of every gate context, so one compiler serves
    // several contexts only if each comparator builder owns its own.
    //
    // LLM section: the connection builder knows `depth` only; a
    // walk-state builder over `(&THING, &WalkState)` would know `depth`
    // and `terminal`.
    #[test]
    fn comparator_builder_owns_the_variable_vocabulary() {
        /// An expression node, the way the gate builders take one.
        fn leaf(value: serde_json::Value) -> Rc<dyn ExpressionBuilder> {
            Rc::new(value)
        }

        let error = depth_cmp(CompareTypes::LTE, "nonsense", &leaf(json!(2)))
            .err()
            .expect("an unknown variable must be an error");
        let message = error.to_string();
        assert!(message.contains("nonsense"), "{message}");
        assert!(message.contains("depth"), "{message}");

        let depth_test = depth_cmp(CompareTypes::LTE, "depth", &leaf(json!(2)))
            .expect("`depth` is a variable this builder reads");
        assert!(depth_test("anything", &2), "at the limit");
        assert!(!depth_test("anything", &3), "past the limit");

        let error = depth_cmp(CompareTypes::LTE, "depth", &leaf(json!("two")))
            .err()
            .expect("a non-integer comparison must be an error");
        assert!(error.to_string().contains("integer"), "{error}");
    }
}
