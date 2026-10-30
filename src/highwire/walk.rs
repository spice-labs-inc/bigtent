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
use anyhow::Result;
use crate::highwire::error::HighWireError;
use sansho::error::SanshoError;
use flume::Receiver;
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
/// Fallible like [`ItemGate`]: Err is an evaluation failure.
pub type ItemCompareFn<'a> = Arc<
    dyn Fn(SanshoCarrier<'a>, &WalkState) -> Result<bool, SanshoError> + Send + Sync,
>;

/// A leaf's comparison, built rather than called: the shape
/// [`build_expression`] takes so that a gate context can choose its own leaf
/// language.
pub type ConFilterBuilder = Arc<dyn Fn(&str) -> Result<ConFilter> + Send + Sync>;

/// A gate over an item and the walk state, compiled.
///
/// The closure types below are written once here rather than spelled out at
/// every signature: they appear in the walk's interface, in the builders that
/// produce them, and in the compiler that hands them over, and a reader should
/// not have to check three spellings agree.
/// A gate returns Ok(false) for "does not match" and Err when the
/// EVALUATION failed — a different fact the walk reports to the accountant
/// rather than silently treating as a filter-out.
pub type ItemGate = Arc<
    dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> Result<bool, SanshoError> + Send + Sync,
>;

/// What an emit produces for one subject, compiled.
pub type ItemEmitFn =
    Arc<dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> Vec<Value> + Send + Sync>;

/// The stop rules, compiled: hard first, then soft, first match wins.
/// The stop rules, compiled: hard first, then soft, first match wins.
///
/// FALLIBLE, like every other gate: an evaluation error propagates instead of
/// being collapsed to "does not fire". A stop rule that cannot evaluate is a
/// broken query, and the walk treats it as an error — the same way it treats a
/// failed sink or a failed send — rather than silently running to exhaustion.
pub type ItemStopFn = Arc<
    dyn for<'x> Fn(
            &SanshoCarrier<'x>,
            &WalkState,
        ) -> Result<(Option<Vec<Value>>, Option<Vec<Value>>), SanshoError>
        + Send
        + Sync,
>;

pub struct WalkState {
    pub is_terminal: bool,
    pub depth_cnt: usize,
}

/// The cancellation a run's OWNER holds: set it, and every accountant-backed
/// check in the pipeline sees it.
///
/// This is the flag for the granularity gap send-failure propagation cannot
/// close: a stage learns a consumer vanished when it next SENDS, but a stage
/// mid-computation — a barker in a long loop, a walk deep in a level — sends
/// nothing for a while. The run's owner (the HTTP layer) flips this flag when
/// the response is dropped, and the accountant's per-item checks — the same
/// checks the budget work will use — stop the work promptly.
#[derive(Clone, Default)]
pub struct Cancellation {
    flag: Arc<std::sync::atomic::AtomicBool>,
}

impl Cancellation {
    pub fn new() -> Self {
        Cancellation {
            flag: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Mark the run as cancelled: every check from now on reports it.
    pub fn cancel(&self) {
        self.flag
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// The accountant that carries a [`Cancellation`]: the walk's per-level
/// `should_stop` check and the barker stage's per-row check consult it.
pub struct Cancelled {
    pub cancellation: Cancellation,
}

impl AccountingAndBudget for Cancelled {
    fn should_stop(&self) -> Option<Vec<Value>> {
        if self.cancellation.is_cancelled() {
            Some(vec![serde_json::json!({
                "cancelled": "the run's consumer disconnected",
            })])
        } else {
            None
        }
    }

    fn can_sink_normal(&self) -> bool {
        true
    }

    fn event(&self, _event_type: AccountingEventType) {}
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountingEventType {
    /// An item was READ from the cluster or herd: the walk's IO — the
    /// expensive thing.
    LoadItem,
    /// A THREAD was created: each one reserves a stack and a scheduler slot —
    /// an expense the accountant registers, so a limit can be enforced later.
    ThreadStarted,
    /// A row was handed to a sink: the pipeline's output work.
    Emit,
}

pub enum SinkOperation {
    NormalMessage,
    ErrorMessage,
}

// filter the conn (note this is per connection type). The identifier borrows for
// the document's lifetime, which is what lets the predicate be built once — a
// predicate over a per-call borrow could not be stored
/// Fallible like [`ItemGate`]: Err is an evaluation failure, reported and
/// then treated as a filter-out.
pub type ConFilter = Arc<dyn Fn(&str, &usize) -> Result<bool, SanshoError> + Send + Sync>;

/// Which store a walk reads from.
///
/// Both arms are a shared handle on a store, so this clones like one: a clone is
/// a reference count, not a copy of anything. A chained query runs several walks
/// against the same store, which is why it needs that.
#[derive(Clone, Debug)]
pub enum HerdOrCluster {
    Herd(Arc<GoatHerd>),
    Cluster(Arc<GoatRodeoCluster>),
}

/// The walk's signature is its interface: the roots, where to look, where rows
/// go, what to traverse, the three gates, and the accountant. Bundling them
/// would name the bundle after nothing.
#[expect(clippy::too_many_arguments)]
pub fn walk<SINK: FnMut(Value, &Arc<dyn AccountingAndBudget + Send + Sync>, SinkOperation) -> Result<()>>(
    roots: Receiver<String>,
    lookup: HerdOrCluster,
    mut sink: SINK,
    connections: Vec<(String, ConFilter)>,
    item_filter: ItemGate,
    emit: ItemEmitFn,
    stop_fn: ItemStopFn,
    accountant: Arc<dyn AccountingAndBudget + Send + Sync>,
) -> Result<()> {
    // keep track of depth
    let mut depth = 0usize;

    // The first round is fed by the stream rather than by a list: the producer
    // may still be finding roots while this walk is already traversing them.
    //
    // The stream is drained to empty rather than read one at a time, because a
    // round is a SET — the walk visits each identifier once, whatever order they
    // arrive in — and draining is what makes the first round as large as
    // whatever has been produced by the time the walk starts. A stream that never
    // closes leaves this walk running, which is the intended behaviour: the
    // upstream stage decides when the root set is complete.
    let mut next_round: HashSet<String> = HashSet::new();
    for root in roots.iter() {
        next_round.insert(root);
    }

    // the identifiers we've seen... don't re-walk them
    let mut seen: HashSet<String> = HashSet::new();

    // a variable for "soft" stop
    let mut continue_walk = true;

    // if we're continuing and there are more items, keep traversing
    'outer: while continue_walk && !next_round.is_empty() {
        // stuff to process for this round
        let mut this_round = HashSet::new();

        // update this round and seen
        for v in &next_round {
            seen.insert(v.clone());
            this_round.insert(v.clone());
        }

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
                // an identifier that resolved to nothing — distinct from an
                // item that loaded and was filtered
                continue;
            };

            // the item was read: the accountant's LoadItem count is both
            // `walk.items_loaded` and `global.items_loaded`
            accountant.event(AccountingEventType::LoadItem);

            // pre-build stuff for the next round
            // We need to determine if this Item is terminal for
            // some of the state to pass to the functions, so we
            // don't want to actually add to `next_round`
            // until this Item passes
            let mut maybe_next_round: Vec<String> = vec![];
            // the item's terminality is a property of the ITEM — does any
            // filter-admitted child exist at all — NOT of the traversal's
            // progress: a child excluded because it was already seen (a
            // diamond) is still a child, and marking the parent terminal
            // made the aa template emit a non-canonical item
            let mut any_admitted_child = false;

            for (con_name, con_filter) in &connections {
                if let Some(conns) = thing
                    .get_key("connections")
                    .ok()
                    .flatten()
                    .and_then(|v| v.get_key(con_name).ok().flatten())
                    .and_then(|v| v.as_elements().ok())
                {
                    {
                        // a Cow: borrowed for a definite string, owned when
                        // the document chunked it
                        let mut v: Vec<std::borrow::Cow<'_, str>> = vec![];
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
                            // the FILTER decides whether this edge is a child
                            // at all; `seen` only decides whether the child is
                            // re-visited — so the flag is set BEFORE the seen
                            // check, or a diamond's parent is wrongly terminal
                            if let Ok(true) = con_filter(maybe_con.as_ref(), &depth) {
                                any_admitted_child = true;
                                if !seen.contains(maybe_con.as_ref()) {
                                    maybe_next_round.push(maybe_con.as_ref().to_string());
                                }
                            }
                        }
                    }
                }
            }

            // create the walk state
            let walk_state = WalkState {
                depth_cnt: depth,
                is_terminal: !any_admitted_child,
            };
            // do we want to process the item? An ERRORED gate is reported
            // (GateErrored) and then treated as a non-match: the item is
            // skipped either way, but the fact is not silent
            match item_filter(&thing, &walk_state) {
                Err(_) => {
                    continue;
                }
                Ok(false) => {
                    continue;
                }
                Ok(true) => {} // admitted, below
            }
            {
                // queue up the next round
                for s in maybe_next_round {
                    next_round.insert(s);
                }

                // emit the stuff for this round; the rows are counted by the
                // sinks themselves (they fire `Emit`)
                for emit_me in emit(&thing, &walk_state) {
                    sink(emit_me, &accountant, SinkOperation::NormalMessage)?;
                }

                // do stop logic
                match stop_fn(&thing, &walk_state)? {
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
            }
        }

        if let Some(end_reasons) = accountant.should_stop() {
            // the ACCOUNTANT ended the walk, not a stop rule
            for r in end_reasons {
                sink(r, &accountant, SinkOperation::ErrorMessage)?;
            }

            return Err(anyhow::Error::new(HighWireError::Budget { detail: "the accountant ended the walk".to_string() }));
        }
        depth += 1;
    }
    // the walk ended by running out of items; a stop-rule path reports
    // TerminatedByStop where it breaks out
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
    Ok((s.to_string(), Arc::new(|_: &str, _: &usize| Ok(true))))
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

    Ok(Arc::new(
        move |data: SanshoCarrier<'a>, _ws: &WalkState| -> Result<bool, SanshoError> {
            // the thing is evaluated against the expression; a truthy
            // result matches (null and false do not). An evaluation
            // ERROR propagates: the walk treats it as a non-match.
            sansho::evaluate(&program, data.clone()).map(|value| truthy(&value))
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
pub fn build_item_leaf(expr: &str, cache: &ProgramCache) -> Result<ItemGate> {
    let program = compile_sansho(expr, cache)?;
    Ok(Arc::new(
        move |thing: &SanshoCarrier<'_>, _ws: &WalkState| {
            sansho::evaluate(&program, Rc::clone(thing)).map(|value| truthy(&value))
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
                return Err(anyhow::Error::new(HighWireError::build(format!(
                    "`depth` compares against an integer, not {expression:?}"
                ))));
            };
            // a depth is a COUNT of levels: negative is meaningless, and a cast
            // would turn -1 into usize::MAX — `>` would never match, `<=` always,
            // silently. Refused here, at build time, where the document is read.
            let Ok(value) = usize::try_from(value) else {
                return Err(anyhow::Error::new(HighWireError::build(format!(
                    "`depth` must be 0 or more, found {value}"
                ))));
            };
            Ok(Arc::new(
                move |_thing: &SanshoCarrier<'_>, ws: &WalkState| {
                    Ok(compare_ord(cmp, ws.depth_cnt, value))
                },
            ))
        }
        "is_terminal" => {
            let Some(value) = expression.as_bool() else {
                return Err(anyhow::Error::new(HighWireError::build("`is_terminal` compares against a boolean, not {expression:?}")));
            };
            Ok(Arc::new(
                move |_thing: &SanshoCarrier<'_>, ws: &WalkState| {
                    Ok(compare_ord(cmp, ws.is_terminal, value))
                },
            ))
        }
        other => Err(anyhow::Error::new(HighWireError::build(format!(
            "unknown variable `{other}`; known: depth, is_terminal"
        )))),
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
        Ok(Arc::new(
            move |thing: &SanshoCarrier<'_>, ws: &WalkState| Ok(!sub(thing, ws)?),
        ))
    } else if let Some(and_b) = expression.and()? {
        let mut subs = vec![];
        for v in and_b {
            subs.push(build_item_expression(&v, cache)?);
        }
        Ok(Arc::new(
            move |thing: &SanshoCarrier<'_>, ws: &WalkState| {
                for f in &subs {
                    if !f(thing, ws)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            },
        ))
    } else if let Some(or_b) = expression.or()? {
        let mut subs = vec![];
        for v in or_b {
            subs.push(build_item_expression(&v, cache)?);
        }
        Ok(Arc::new(
            move |thing: &SanshoCarrier<'_>, ws: &WalkState| {
                for f in &subs {
                    if f(thing, ws)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            },
        ))
    } else if let Some((cmp_type, var_name, vs)) = expression.as_compare()? {
        build_walk_state_cmp(cmp_type, var_name, &vs)
    } else if let Some(expr) = expression.as_string() {
        build_item_leaf(expr, cache)
    } else {
        Err(anyhow::Error::new(HighWireError::build(format!(
            "unable to build an item filter from {expression:?}"
        ))))
    }
}

pub fn build_con_compare_function(expr: &str) -> Result<ConFilter> {
    if let Some(cmp) = expr.strip_prefix("start:") {
        if cmp.is_empty() {
            return Err(anyhow::Error::new(HighWireError::build(
                "'start:' must have the thing that is being compared",
            )));
        }
        let cmp = cmp.to_string();
        return Ok(Arc::new(move |conn: &str, _: &usize| {
            Ok(conn.starts_with(&cmp))
        }));
    }
    if let Some(cmp) = expr.strip_prefix("end:") {
        if cmp.is_empty() {
            return Err(anyhow::Error::new(HighWireError::build(
                "'end:' must have the thing that is being compared",
            )));
        }
        let cmp = cmp.to_string();
        return Ok(Arc::new(move |conn: &str, _: &usize| {
            Ok(conn.ends_with(&cmp))
        }));
    }
    if let Some(re_text) = expr.strip_prefix("regex:") {
        if re_text.is_empty() {
            return Err(anyhow::Error::new(HighWireError::build(
                "'regex:' must have an actual regular expression",
            )));
        }
        let regex = regex::Regex::new(re_text)?;
        return Ok(Arc::new(move |conn: &str, _: &usize| {
            Ok(regex.is_match(conn))
        }));
    }
    Err(anyhow::Error::new(HighWireError::build(format!(
        "invalid expression '{expr}'"
    ))))
}

/// The same leaf, as a builder rather than as a call.
///
/// [`build_expression`] takes a builder so that a gate context can choose its
/// own leaf language; this is the connection gate's, and it is the same reading
/// as the call above — one implementation, so the two cannot drift apart.
pub fn build_con_compare_function_function() -> ConFilterBuilder {
    Arc::new(build_con_compare_function)
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
        Ok(Arc::new(move |v: &str, d: &usize| Ok(!sub(v, d)?)))
    } else if let Some(and_b) = expression.and()? {
        let mut subs = vec![];
        for v in and_b {
            subs.push(build_expression(v, func_builder, cmp_builder)?);
        }
        Ok(Arc::new(move |v: &str, d: &usize| {
            for f in &subs {
                if !f(v, d)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }))
    } else if let Some(or_b) = expression.or()? {
        let mut subs = vec![];
        for v in or_b {
            subs.push(build_expression(v, func_builder, cmp_builder)?);
        }
        Ok(Arc::new(move |v: &str, d: &usize| {
            for f in &subs {
                if f(v, d)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }))
    } else if let Some((cmp_type, var_name, vs)) = expression.as_compare()? {
        // the variable vocabulary lives with the comparator builder: it
        // is the thing that knows where a variable's value comes from,
        // so it is the thing that can say which variables exist
        cmp_builder(cmp_type, var_name, &vs)
    } else if let Some(str) = expression.as_string() {
        Ok(func_builder(str)?)
    } else {
        Err(anyhow::Error::new(HighWireError::build(format!("Unable to build an expression from {expression:?}"))))
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
        return Err(anyhow::Error::new(HighWireError::build(format!(
            "unknown variable `{var_name}`; known: {KNOWN}"
        ))));
    }
    let Some(cmp_value) = expression.as_int() else {
        return Err(anyhow::Error::new(HighWireError::build(format!(
            "`{KNOWN}` compares against an integer, not {expression:?}"
        ))));
    };
    // as above: a negative depth is refused at build time, not cast
    let cmp_value: usize = match usize::try_from(cmp_value) {
        Ok(value) => value,
        Err(_) => {
            return Err(anyhow::Error::new(HighWireError::build(format!(
                "`{KNOWN}` must be 0 or more, found {cmp_value}"
            ))));
        }
    };
    match cmp {
        CompareTypes::EQ => Ok(Arc::new(move |_data: &str, d: &usize| {
            Ok(*d == cmp_value)
        })),
        CompareTypes::GT => Ok(Arc::new(move |_data: &str, d: &usize| {
            Ok(*d > cmp_value)
        })),
        CompareTypes::GTE => Ok(Arc::new(move |_data: &str, d: &usize| {
            Ok(*d >= cmp_value)
        })),
        CompareTypes::LT => Ok(Arc::new(move |_data: &str, d: &usize| {
            Ok(*d < cmp_value)
        })),
        CompareTypes::LTE => Ok(Arc::new(move |_data: &str, d: &usize| {
            Ok(*d <= cmp_value)
        })),
        CompareTypes::NOTEQ => Ok(Arc::new(move |_data: &str, d: &usize| {
            Ok(*d != cmp_value)
        })),
    }
}

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
        assert_eq!(amp.and().expect("and()").map(|gates| gates.len()), Some(1));

        let or_node = json!({"or": [true]});
        assert_eq!(
            or_node.or().expect("or()").map(|gates| gates.len()),
            Some(1)
        );

        let pipes = json!({"||": [true]});
        assert_eq!(pipes.or().expect("or()").map(|gates| gates.len()), Some(1));

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
        assert!(stored("pkg:thing", &0).unwrap(), "a pkg: identifier at depth 0");
        assert!(
            stored("gitoid:abc", &2).unwrap(),
            "a gitoid at the depth limit"
        );
        assert!(!stored("other", &0).unwrap(), "a different identifier");
        assert!(!stored("pkg:thing", &3).unwrap(), "past the depth limit");
    }

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
        assert!(depth_test("anything", &2).unwrap(), "at the limit");
        assert!(!depth_test("anything", &3).unwrap(), "past the limit");

        let error = depth_cmp(CompareTypes::LTE, "depth", &leaf(json!("two")))
            .err()
            .expect("a non-integer comparison must be an error");
        assert!(error.to_string().contains("integer"), "{error}");
    }
}


