//! The compiler: a query, turned into the thing that runs it.
//!
//! [`compile`] takes an `Rc<dyn Query>` — the grammar's levels, whatever data
//! structure they were read out of — and returns a function that runs the whole
//! chain. **Everything is decided here.** What comes out the other side holds no
//! syntax at all: the levels are gone, the phase graph is ordered, the data flow
//! between phases is wired, and every expression is a compiled program behind a
//! closure. A run walks and nothing else.
//!
//! # What compiling means
//!
//! | level | syntax | compiled to |
//! |---|---|---|
//! | `WalkConnection` | a gate expression | a predicate over an edge's name and depth |
//! | `ItemFilter` | a gate expression | a predicate over an item and the walk state |
//! | `ItemEmit` / `EmitEntry` | sansho expressions | the rows an item produces |
//! | `ItemStop` / `StopRule` | gates and reports | the rules that end the walk |
//! | `RootPredicate` | a sansho expression | the program that turns a row into identifiers |
//!
//! Nothing is compiled per item and nothing is compiled per run: every
//! expression is parsed and compiled exactly once, here. The returned function
//! may be called as often as you like.
//!
//! # Failures say where
//!
//! A compilation error is only useful if it says WHERE, so every failure is a
//! [`CompileError`] carrying the phase and the level it was reading. "a root
//! draws on `jars`, and no phase is named that" is half an answer; "in phase
//! `classes`, reading its root: a root draws on `jars`, and no phase is named
//! that" is the answer.
//!
//! # The phase graph is ordered here, not discovered at run time
//!
//! A phase whose root is `Predicates` draws on the rows of the phases it names,
//! so those phases must run first. That is a property of the query, which means
//! the compiler settles it: phases are topologically ordered, a reference to a
//! phase that does not exist is a compile error, and so is a cycle.
//!
//! # Rows are not kept
//!
//! A phase's rows feed the phases that draw on it and are then gone. The
//! compiler knows, per phase, exactly which phases read its rows and which
//! program each of them applies — so as each row is emitted it is turned into
//! identifiers for the phases waiting on it, and the row itself is dropped. No
//! phase's rows are held; no phase's rows are held for a phase that never reads
//! them; a phase nobody draws on retains nothing at all.
//!
//! `tee` decides whether a phase's rows also reach the caller's sink. A phase
//! without `tee` is invisible in the output — its rows exist only to become the
//! identifiers of what comes after it.
//!
//! # Not here
//!
//! `emit_at_end` is not compiled. It reads the statistics a walk gathers, and no
//! statistics type exists yet. A query that carries one is REFUSED rather than
//! compiled without it: dropping it silently would be the compiler reporting
//! success for something it did not do.

use crate::highwire::grammar::{
    EmitEntry, GrammarError, ItemEmit, ItemStop, LevelResult, Query, Root, RootKind, Step, StopRule,
    Walk, WalkConnection,
};
use crate::highwire::walk::{
    AccountingAndBudget, ConFilter, ExpressionBuilder, HerdOrCluster, SinkOperation, WalkState,
    build_con_compare_function, build_expression, build_item_expression, compile_sansho, depth_cmp,
    walk,
};
use anyhow::Result;
use sansho::{Program, ProgramCache, SanshoCarrier, evaluate, materialize};
use serde_json::Value;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

/// A gate over an item and the walk state, compiled.
pub type Gate = Rc<dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> bool>;

/// What an emit produces for one subject, compiled.
pub type EmitFn = Rc<dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> Vec<Value>>;

/// The stop rules, compiled: hard first, then soft, first match wins.
pub type StopFn = Rc<
    dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> (Option<Vec<Value>>, Option<Vec<Value>>),
>;

/// The result of compiling.
/// Where a read is happening: the route taken to reach it.
///
/// The compiler descends the tree, so it is the compiler — not the reader — that
/// knows how a node was reached. Each read carries the route so far, and a
/// failure that comes back up from a reader gets those steps put in front of it.
///
/// The route starts at the query and its second step is the phase, so a failure
/// reads from the outside in:
///
/// ```text
/// the query -> phase `classes` -> its emit -> entry 1 -> the gate: …
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
struct Location {
    path: Vec<Step>,
}

impl Location {
    /// The query itself, before any phase is read.
    fn query() -> Self {
        Location {
            path: vec![Step::Query],
        }
    }

    /// A route beginning at a phase.
    fn of(phase: &Step) -> Self {
        Location {
            path: vec![Step::Query, phase.clone()],
        }
    }

    /// One step further in.
    fn step(&self, step: Step) -> Self {
        let mut path = self.path.clone();
        path.push(step);
        Location { path }
    }

    /// A failure found here.
    fn error(&self, detail: impl Into<String>) -> GrammarError {
        GrammarError {
            path: self.path.clone(),
            detail: detail.into(),
        }
    }

    /// A failure found further in, located here.
    fn locate(&self, error: GrammarError) -> GrammarError {
        let mut path = self.path.clone();
        path.extend(error.path);
        GrammarError {
            path,
            detail: error.detail,
        }
    }
}

/// A level reader's failure, located. The reader's own route is kept and this
/// location's steps are put in front of it.
fn located<T>(at: &Location, result: LevelResult<T>) -> LevelResult<T> {
    result.map_err(|error| at.locate(error))
}

/// A builder's failure, located.
///
/// A builder works on syntax that has already been read, so it has no route of
/// its own; this location supplies one.
fn located_build<T>(at: &Location, result: anyhow::Result<T>) -> LevelResult<T> {
    result.map_err(|error| at.error(error.to_string()))
}

/// Where a phase starts, compiled.
///
/// `Predicates` carries each producing phase's POSITION, resolved here, so the
/// planner never looks a name up again and cannot look one up wrongly. By the
/// time anything runs the programs live in the plan, and this variant is a note
/// that the phase's identifiers arrive from upstream.
enum CompiledRoot {
    /// Identifiers named outright.
    Enumerated(Vec<String>),

    /// Identifiers derived from the rows of the phases at these positions.
    Predicates(Vec<(usize, Arc<Program>)>),
}

/// One phase, compiled: no syntax survives.
struct CompiledPhase {
    name: Option<String>,
    roots: CompiledRoot,
    connections: Vec<(String, ConFilter)>,
    filter: Gate,
    emit: EmitFn,
    stop: StopFn,
    tee: bool,
    emit_at_end: Option<EmitFn>,
}

/// Whether a gate compiled for a level may read the walk state.
///
/// A gate is evaluated against a subject and the walk state. Which subjects have
/// a walk state is a fact about the level, not about the gate, so it is settled
/// where the level is compiled rather than checked when the gate runs.
#[derive(Clone, Copy, PartialEq)]
enum Variables {
    /// The subject is an item part-way through a walk, so `depth` and
    /// `is_terminal` mean something and a gate may compare them.
    Allowed,

    /// The subject is not an item part-way through a walk, so there is no walk
    /// state for a gate to read. A gate that asks for one is a mistake, and it
    /// is refused where it is written rather than answered with a value that
    /// means nothing.
    Refused,
}

/// The phase graph, settled at compile time.
struct Plan {
    /// The phases in the order they must run.
    order: Vec<usize>,

    /// Per phase, the phases that read its rows and the program each applies.
    /// Indexed by the producing phase's position.
    consumers: Vec<Vec<(usize, Arc<Program>)>>,
}

/// Compile a query into the function that runs it.
///
/// The returned function takes the accountant, the sink, and the lookup — the
/// three things a run needs that are not part of the query. It may be called
/// more than once: everything expensive has already happened here.
pub fn compile<ACCOUNTANT, SINK>(
    query: Rc<dyn Query>,
    cache: &ProgramCache,
) -> LevelResult<impl FnMut(Arc<ACCOUNTANT>, SINK, HerdOrCluster) -> Result<()>>
where
    ACCOUNTANT: AccountingAndBudget,
    SINK: FnMut(Value, &Arc<ACCOUNTANT>, SinkOperation) -> Result<()>,
{
    let phases = located(&Location::query(), query.phases())?;

    // First pass: every phase's name. A root is resolved against these, so they
    // must exist before any root is compiled — a reference to a phase that does
    // not exist is a compile error, never something met at run time.
    let mut by_name: HashMap<String, usize> = HashMap::new();
    for (position, phase) in phases.iter().enumerate() {
        let name = located(
            &Location::query().step(Step::Phase {
                name: None,
                position,
            }),
            phase.name(),
        )?;
        let Some(name) = name else {
            continue;
        };
        if by_name.insert(name.clone(), position).is_some() {
            return Err(Location {
                path: vec![
                    Step::Query,
                    Step::Phase {
                        name: Some(name),
                        position,
                    },
                ],
            }
            .error("two phases carry this name; a name identifies one phase"));
        }
    }

    // Second pass: compile every level, refusing what cannot be valid.
    let mut compiled = Vec::with_capacity(phases.len());
    for (position, phase) in phases.iter().enumerate() {
        compiled.push(compile_phase(phase, position, &by_name, cache)?);
    }

    // Third pass: order the phases and wire the rows between them.
    let plan = plan_phases(&compiled)?;

    Ok(
        move |accountant: Arc<ACCOUNTANT>, mut sink: SINK, lookup: HerdOrCluster| -> Result<()> {
            run(&compiled, &plan, accountant, &mut sink, lookup)
        },
    )
}

/// Compile one phase.
fn compile_phase(
    phase: &Rc<dyn Walk>,
    position: usize,
    by_name: &HashMap<String, usize>,
    cache: &ProgramCache,
) -> LevelResult<CompiledPhase> {
    let name = located(
        &Location::query().step(Step::Phase {
            name: None,
            position,
        }),
        phase.name(),
    )?;
    let who = Step::Phase { name: name.clone(), position };
    let here = || Location::of(&who);

    let emit_at_end = located(&here().step(Step::EmitAtEnd), phase.emit_at_end())?;

    let root = located(&here().step(Step::Root), phase.root())?;
    let connections = located(&here().step(Step::Connections), phase.connections())?;
    let filter = located(&here().step(Step::Filter), phase.filter())?;
    let emit = located(&here().step(Step::Emit), phase.emit())?;
    let stop = located(&here().step(Step::Stop), phase.stop())?;

    Ok(CompiledPhase {
        name,
        roots: compile_root(&root, &who, by_name, cache)?,
        connections: compile_connections(&connections, &who)?,
        filter: match filter {
            Some(filter) => {
                let gate = located(&here().step(Step::Filter), filter.gate())?;
                located_build(&here().step(Step::Filter), build_item_expression(&gate, cache))?
            }
            None => Rc::new(|_: &SanshoCarrier<'_>, _: &WalkState| true),
        },
        emit: compile_emit(&emit, &who, Step::Emit, cache, Variables::Allowed)?,
        stop: compile_stop(stop, &who, cache)?,
        tee: located(&here().step(Step::Tee), phase.tee())?,
        emit_at_end: match emit_at_end {
            // the subject of an end-of-walk emit is the statistics a walk
            // gathers, not an item, so its gates have no walk state to read
            Some(emit) => Some(compile_emit(
                &emit,
                &who,
                Step::EmitAtEnd,
                cache,
                Variables::Refused,
            )?),
            None => None,
        },
    })
}

/// Refuse a gate that reads the walk state.
///
/// This is a validation pass over the gate tree rather than a second compiler:
/// what the gate MEANS is the same wherever it is compiled, and the only thing
/// that differs is whether a variable has an answer. Asking the tree whether it
/// asks for one, and then compiling it as usual, keeps one compiler for gates
/// and puts the restriction where it belongs — with the level that has no walk
/// state to offer.
fn refuse_variables(expression: &Rc<dyn ExpressionBuilder>, at: &Location) -> LevelResult<()> {
    if let Some(operand) = located(at, expression.not())? {
        return refuse_variables(&operand, at);
    }
    if let Some(operands) = located(at, expression.and())? {
        for operand in &operands {
            refuse_variables(operand, at)?;
        }
        return Ok(());
    }
    if let Some(operands) = located(at, expression.or())? {
        for operand in &operands {
            refuse_variables(operand, at)?;
        }
        return Ok(());
    }
    if located(at, expression.as_compare())?.is_some() {
        return Err(at.error(
            "a gate here runs when the walk is over, where there is no walk state to read; it \
             may not compare a variable",
        ));
    }
    Ok(())
}

/// Compile a phase's root.
///
/// The kind is asked for rather than guessed: a root is exactly one of the two,
/// and reading the wrong accessor on the wrong kind is the bug the selector
/// exists to prevent.
fn compile_root(
    root: &Rc<dyn Root>,
    who: &Step,
    by_name: &HashMap<String, usize>,
    cache: &ProgramCache,
) -> LevelResult<CompiledRoot> {
    let here = || Location::of(who);

    match located(&here().step(Step::Root), root.kind())? {
        RootKind::Enumerated => {
            let identifiers = located(&here().step(Step::Root), root.identifiers())?;
            if identifiers.is_empty() {
                return Err(here().step(Step::Root).error(
                    "a root that names its identifiers names none; a walk with no roots is not a \
                     walk",
                ));
            }
            Ok(CompiledRoot::Enumerated(identifiers))
        }

        RootKind::Predicates => {
            let predicates = located(&here().step(Step::Root), root.predicates())?;
            if predicates.is_empty() {
                return Err(here().step(Step::Root).error(
                    "a root that derives its identifiers names no predicate to derive them from, \
                     so the walk would start from nothing",
                ));
            }
            let mut compiled = Vec::with_capacity(predicates.len());
            for predicate in predicates {
                let source = located(&here().step(Step::Root), predicate.source())?;
                let Some(&producer) = by_name.get(source.as_str()) else {
                    return Err(here().step(Step::Root).error(format!(
                        "a root draws on `{source}`, and no phase is named that"
                    )));
                };
                let expression = located(&here().step(Step::Root), predicate.expression())?;
                let text = located(&here().step(Step::Root), expression.source())?;
                let program = located_build(&here().step(Step::Root), compile_sansho(text, cache))?;
                compiled.push((producer, program));
            }
            Ok(CompiledRoot::Predicates(compiled))
        }
    }
}

/// Compile a phase's connections.
///
/// A connection with no gate admits every edge of its type — that is what the
/// bare form means — so the absence compiles to a predicate that always answers
/// true rather than being left for the walk to interpret.
fn compile_connections(
    connections: &[Rc<dyn WalkConnection>],
    who: &Step,
) -> LevelResult<Vec<(String, ConFilter)>> {
    let here = || Location::of(who);
    let mut compiled = Vec::with_capacity(connections.len());
    for connection in connections {
        let connection_type = located(&here().step(Step::Connections), connection.connection_type())?;
        let gate: ConFilter = match located(&here().step(Step::Connections), connection.gate())? {
            Some(expression) => located_build(
                &here().step(Step::Connections),
                build_expression(expression, &build_con_compare_function, &depth_cmp),
            )?,
            None => Rc::new(|_: &str, _: &usize| true),
        };
        compiled.push((connection_type, gate));
    }
    Ok(compiled)
}

/// Compile an emit level into the closure the walk calls.
///
/// The level is built up from its entries and then consumed here: what survives
/// is the closure, and the entries are gone with it.
fn compile_emit(
    emit: &Rc<dyn ItemEmit>,
    who: &Step,
    level: Step,
    cache: &ProgramCache,
    variables: Variables,
) -> LevelResult<EmitFn> {
    let here = || Location::of(who);
    let gate = match located(&here().step(level.clone()), emit.gate())? {
        Some(expression) => Some(compile_gate(
            &expression,
            &here().step(level.clone()).step(Step::EmitGate),
            cache,
            variables,
        )?),
        None => None,
    };
    let top_unnest = located(&here().step(level.clone()), emit.unnest())?;
    let mut entries = Vec::new();
    for (index, entry) in located(&here().step(level.clone()), emit.entries())?
        .into_iter()
        .enumerate()
    {
        entries.push(compile_entry(
            &entry,
            &here().step(level.clone()).step(Step::Entry(index)),
            cache,
            top_unnest,
            variables,
        )?);
    }

    let rows = CompiledEmit { gate, entries };
    Ok(Rc::new(move |thing: &SanshoCarrier<'_>, ws: &WalkState| {
        rows.rows(thing, ws)
    }))
}

/// One emit entry, compiled: the program, its gate, and its row rules.
struct CompiledEntry {
    program: Arc<Program>,
    gate: Option<Gate>,
    unnest: bool,
    emit_null: bool,
}

/// An emit level, before it is turned into a closure.
struct CompiledEmit {
    gate: Option<Gate>,
    entries: Vec<CompiledEntry>,
}

impl CompiledEmit {
    /// The rows this emit produces for one subject.
    ///
    /// The level's gate is checked first: false means no row at all and no entry
    /// is examined. Otherwise the entries' rows are concatenated in order.
    ///
    /// The row rule per entry, in the order it applies: a gate that answers
    /// false omits the entry; an evaluation error omits it; a null omits it
    /// unless the entry asked for nulls; an array yields one row per element
    /// when `unnest` is in effect, ONE LEVEL only, and one row holding the array
    /// when it is not.
    fn rows(&self, thing: &SanshoCarrier<'_>, ws: &WalkState) -> Vec<Value> {
        if let Some(gate) = &self.gate
            && !gate(thing, ws)
        {
            return Vec::new();
        }
        let mut rows = Vec::new();
        for entry in &self.entries {
            if let Some(gate) = &entry.gate
                && !gate(thing, ws)
            {
                continue;
            }
            let Ok(value) = evaluate(&entry.program, Rc::clone(thing)) else {
                continue;
            };
            if entry.unnest && let Ok(elements) = value.as_elements() {
                for element in elements {
                    let Ok(row) = materialize(&element) else {
                        continue;
                    };
                    if row.is_null() && !entry.emit_null {
                        continue;
                    }
                    rows.push(row);
                }
                continue;
            }
            match materialize(&value) {
                Ok(row) if row.is_null() && !entry.emit_null => {}
                Ok(row) => rows.push(row),
                Err(_) => {}
            }
        }
        rows
    }
}

/// Compile one emit entry.
///
/// `unnest` falls back to the emit's answer and `emit_null` does not: a null is
/// a row only when the entry itself asked for it, so a level default for it
/// would be a way to emit nulls by accident.
fn compile_entry(
    entry: &Rc<dyn EmitEntry>,
    at: &Location,
    cache: &ProgramCache,
    top_unnest: bool,
    variables: Variables,
) -> LevelResult<CompiledEntry> {
    let text = located(&at.step(Step::Expression), entry.expression())?;
    Ok(CompiledEntry {
        program: located_build(&at.step(Step::Expression), compile_sansho(text, cache))?,
        gate: match located(at, entry.gate())? {
            Some(expression) => Some(compile_gate(
                &expression,
                &at.step(Step::Gate),
                cache,
                variables,
            )?),
            None => None,
        },
        unnest: located(at, entry.unnest())?.unwrap_or(top_unnest),
        emit_null: located(at, entry.emit_null())?.unwrap_or(false),
    })
}

/// Compile a gate, refusing the walk state first when the level has none.
fn compile_gate(
    expression: &Rc<dyn ExpressionBuilder>,
    at: &Location,
    cache: &ProgramCache,
    variables: Variables,
) -> LevelResult<Gate> {
    if variables == Variables::Refused {
        refuse_variables(expression, at)?;
    }
    located_build(at, build_item_expression(expression, cache))
}

/// Compile a stop level into the closure the walk calls.
///
/// Hard rules are tried first and a match ends the walk at once — the second
/// element of the pair. Only when no hard rule matched are the soft rules tried,
/// and a match ends the walk at the end of the round — the first element. The
/// first match decides and nothing after it is evaluated. A phase with no stop
/// compiles to two empty lists, which never fire.
fn compile_stop(
    stop: Option<Rc<dyn ItemStop>>,
    who: &Step,
    cache: &ProgramCache,
) -> LevelResult<StopFn> {
    let here = || Location::of(who);
    let (hard, soft) = match stop {
        None => (Vec::new(), Vec::new()),
        Some(stop) => {
            let mut hard = Vec::new();
            for rule in located(&here().step(Step::Stop), stop.hard())? {
                hard.push(compile_rule(&rule, who, cache)?);
            }
            let mut soft = Vec::new();
            for rule in located(&here().step(Step::Stop), stop.soft())? {
                soft.push(compile_rule(&rule, who, cache)?);
            }
            (hard, soft)
        }
    };

    Ok(Rc::new(
        move |thing: &SanshoCarrier<'_>, ws: &WalkState| -> (
            Option<Vec<Value>>,
            Option<Vec<Value>>,
        ) {
            for rule in &hard {
                if let Some(values) = rule.fires(thing, ws) {
                    return (None, Some(values));
                }
            }
            for rule in &soft {
                if let Some(values) = rule.fires(thing, ws) {
                    return (Some(values), None);
                }
            }
            (None, None)
        },
    ))
}

/// One stop rule, compiled.
struct CompiledRule {
    gate: Gate,
    report: Option<CompiledEmit>,
}

impl CompiledRule {
    /// The rows this rule reports, or `None` when it does not fire.
    ///
    /// Firing with nothing to report is `Some(vec![])`, which is NOT the same as
    /// not firing: a caller can tell a stop from exhaustion by whether a rule
    /// answered at all.
    fn fires(&self, thing: &SanshoCarrier<'_>, ws: &WalkState) -> Option<Vec<Value>> {
        if !(self.gate)(thing, ws) {
            return None;
        }
        Some(match &self.report {
            Some(report) => report.rows(thing, ws),
            None => Vec::new(),
        })
    }
}

/// Compile one stop rule.
fn compile_rule(
    rule: &Rc<dyn StopRule>,
    who: &Step,
    cache: &ProgramCache,
) -> LevelResult<CompiledRule> {
    let here = || Location::of(who);
    let gate = located(&here().step(Step::Stop), rule.gate())?;
    Ok(CompiledRule {
        gate: located_build(&here().step(Step::Stop), build_item_expression(&gate, cache))?,
        report: match located(&here().step(Step::Stop), rule.report())? {
            Some(emit) => Some(compile_emit_level(&emit, who, cache)?),
            None => None,
        },
    })
}

/// The emit LEVEL behind a stop rule's report.
///
/// A rule's report is an emit, but it is not the walk's emit: it is evaluated
/// once when the rule fires rather than for every item. It keeps the entries and
/// drops the gate, because the rule's own gate already decided.
fn compile_emit_level(
    emit: &Rc<dyn ItemEmit>,
    who: &Step,
    cache: &ProgramCache,
) -> LevelResult<CompiledEmit> {
    let here = || Location::of(who);
    let top_unnest = located(&here().step(Step::Stop), emit.unnest())?;
    let mut entries = Vec::new();
    for (index, entry) in located(&here().step(Step::Stop), emit.entries())?
        .into_iter()
        .enumerate()
    {
        // a rule's report fires ON an item, so its gates may read the walk state
        entries.push(compile_entry(
            &entry,
            &Location::of(who).step(Step::Stop).step(Step::Entry(index)),
            cache,
            top_unnest,
            Variables::Allowed,
        )?);
    }
    Ok(CompiledEmit {
        gate: None,
        entries,
    })
}

/// Order the phases and wire the rows between them.
///
/// A phase that draws on another must run after it, so the graph is sorted
/// topologically and a cycle is a build error: it is a property of the query,
/// knowable before anything runs, and meeting it at run time would mean meeting
/// it with a half-finished walk.
///
/// Every producing position here was resolved when the roots were compiled, so
/// there is no name left to look up and none to get wrong.
fn plan_phases(phases: &[CompiledPhase]) -> LevelResult<Plan> {
    let mut consumers: Vec<Vec<(usize, Arc<Program>)>> = vec![Vec::new(); phases.len()];
    let mut dependencies: Vec<Vec<usize>> = vec![Vec::new(); phases.len()];
    for (consumer, phase) in phases.iter().enumerate() {
        let CompiledRoot::Predicates(predicates) = &phase.roots else {
            continue;
        };
        for (producer, program) in predicates {
            consumers[*producer].push((consumer, Arc::clone(program)));
            dependencies[consumer].push(*producer);
        }
    }

    let mut order = Vec::with_capacity(phases.len());
    let mut state = vec![Visit::Unvisited; phases.len()];
    for position in 0..phases.len() {
        visit(position, phases, &dependencies, &mut state, &mut order)?;
    }
    Ok(Plan { order, consumers })
}

/// How far a phase has been got through while ordering.
#[derive(Clone, Copy, PartialEq)]
enum Visit {
    Unvisited,
    InProgress,
    Done,
}

/// Depth-first ordering. A phase already in progress is one we came back to, and
/// that is a cycle.
fn visit(
    position: usize,
    phases: &[CompiledPhase],
    dependencies: &[Vec<usize>],
    state: &mut [Visit],
    order: &mut Vec<usize>,
) -> LevelResult<()> {
    match state[position] {
        Visit::Done => return Ok(()),
        Visit::InProgress => {
            return Err(Location {
                path: vec![Step::Query, phase_ref(phases, position)],
            }
            .error("the phases form a cycle through this phase"));
        }
        Visit::Unvisited => {}
    }
    state[position] = Visit::InProgress;
    for &dependency in &dependencies[position] {
        visit(dependency, phases, dependencies, state, order)?;
    }
    state[position] = Visit::Done;
    order.push(position);
    Ok(())
}

/// A compiled phase, named if it has a name.
fn phase_ref(phases: &[CompiledPhase], position: usize) -> Step {
    Step::Phase {
        name: phases[position].name.clone(),
        position,
    }
}

/// Run every phase, in the order the plan settled on.
fn run<ACCOUNTANT, SINK>(
    phases: &[CompiledPhase],
    plan: &Plan,
    accountant: Arc<ACCOUNTANT>,
    sink: &mut SINK,
    lookup: HerdOrCluster,
) -> Result<()>
where
    ACCOUNTANT: AccountingAndBudget,
    SINK: FnMut(Value, &Arc<ACCOUNTANT>, SinkOperation) -> Result<()>,
{
    // The identifiers each phase is waiting for. Nothing else is kept: a phase's
    // rows are turned into these as they are emitted and are then gone.
    let mut waiting: Vec<Vec<String>> = vec![Vec::new(); phases.len()];

    for &position in &plan.order {
        let phase = &phases[position];
        let roots = match &phase.roots {
            CompiledRoot::Enumerated(identifiers) => identifiers.clone(),
            CompiledRoot::Predicates(_) => std::mem::take(&mut waiting[position]),
        };

        // The LAST phase's rows are the run's output, and always reach the sink.
        // `tee` is for the phases in the middle: their rows are needed by what
        // comes after them, and are usually not wanted as output as well — which
        // is why they are not output by default.
        let tee = phase.tee || plan.order.last() == Some(&position);
        let consumers = &plan.consumers[position];
        let waiting = &mut waiting;
        let collecting = |row: Value, acc: &Arc<ACCOUNTANT>, op: SinkOperation| -> Result<()> {
            if !consumers.is_empty() {
                // one handle on the row, shared by every consumer; the row
                // itself is untouched and is the one the sink gets
                let shared: Rc<Value> = Rc::new(row.clone());
                for (consumer, program) in consumers {
                    let carrier: SanshoCarrier<'_> = shared.clone();
                    let Ok(value) = evaluate(program, carrier) else {
                        continue;
                    };
                    let into = &mut waiting[*consumer];
                    if let Ok(identifier) = value.as_str() {
                        into.push(identifier.to_string());
                        continue;
                    }
                    if let Ok(elements) = value.as_elements() {
                        for element in elements {
                            if let Ok(identifier) = element.as_str() {
                                into.push(identifier.to_string());
                            }
                        }
                    }
                }
            }
            if tee {
                sink(row, acc, op)?;
            }
            Ok(())
        };

        walk(
            roots,
            lookup.clone(),
            collecting,
            phase.connections.clone(),
            Rc::clone(&phase.filter),
            Rc::clone(&phase.emit),
            Rc::clone(&phase.stop),
            Arc::clone(&accountant),
        )?;

        if let Some(emit_at_end) = &phase.emit_at_end {
            // The statistics a walk gathers are not collected yet, so the
            // subject here is an empty one: every field an end-of-walk
            // expression asks for is absent. The state is the stub the level's
            // gates were compiled against — they may not read it, and this is
            // what one gets if one ever does.
            let subject: SanshoCarrier<'_> = Rc::new(Value::Object(serde_json::Map::new()));
            let state = WalkState {
                depth_cnt: 0,
                is_terminal: false,
            };
            for row in emit_at_end(&subject, &state) {
                sink(row, &accountant, SinkOperation::NormalMessage)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod compile_tests {
    use super::*;
    use crate::highwire::walk::{AccountingEventType, SinkOperation};
    use crate::item::Item;
    use crate::rodeo::goat_herd::GoatHerd;
    use crate::rodeo::member::member_synth;
    use crate::rodeo::robo_goat::RoboticGoat;
    use serde_json::json;

    /// The accountant a run uses here: it never stops the walk and counts
    /// nothing, because these tests are about what the walk does, not about
    /// bounding it.
    struct NoBound;

    impl AccountingAndBudget for NoBound {
        fn should_stop(&self) -> Option<Vec<Value>> {
            None
        }

        fn can_sink_normal(&self) -> bool {
            true
        }

        fn event(&self, _event_type: AccountingEventType) {}
    }

    /// An item with an identifier and nothing else.
    ///
    /// A walk that traverses no connections never looks at anything but the
    /// identifier, so an item with none of the other fields is a complete
    /// subject for these tests — and having nothing else means a test that
    /// passes did so because of the identifier, not because of a field it
    /// accidentally carried.
    fn item(identifier: &str) -> Item {
        Item {
            identifier: identifier.to_string(),
            connections: Default::default(),
            body_mime_type: None,
            body: None,
        }
    }

    /// A herd holding the named identifiers, so a query can be RUN and not just
    /// compiled.
    fn herd(identifiers: &[&str]) -> HerdOrCluster {
        let items: Vec<Item> = identifiers.iter().map(|id| item(id)).collect();
        let member = member_synth(RoboticGoat::new("test", items, Value::Null));
        HerdOrCluster::Herd(Arc::new(GoatHerd::new(vec![member])))
    }

    /// A cache sized for one query. Every expression in a query is compiled
    /// through it, once.
    fn cache() -> ProgramCache {
        ProgramCache::new(64)
    }

    /// The sink type the compile-only tests use.
    ///
    /// It is a FUNCTION POINTER rather than a closure because a closure's type
    /// cannot be written down, and a test that only compiles still has to name
    /// the sink type to ask for a compiled query at all.
    type Discard = fn(Value, &Arc<NoBound>, SinkOperation) -> Result<()>;

    /// Compile a document, the way a caller would: hand it over as a query, get
    /// back the function that runs it, or the reason it could not be compiled.
    fn compile_only(document: Value) -> LevelResult<()> {
        let query: Rc<dyn Query> = Rc::new(document);
        let cache = cache();
        compile::<NoBound, Discard>(query, &cache).map(|_run| ())
    }

    /// Compile a document and run it against a herd, collecting every row that
    /// reaches the sink.
    ///
    /// Only a phase with `tee` reaches the sink, so the rows collected are the
    /// rows the query asked to be seen. Compiling and running live in one
    /// function because the sink here is a closure, and a closure's type has to
    /// be inferred where it is passed.
    fn run_document(document: Value, lookup: HerdOrCluster) -> Result<Vec<Value>> {
        let query: Rc<dyn Query> = Rc::new(document);
        let cache = cache();
        let mut rows: Vec<Value> = Vec::new();
        // the compiled query and the sink that borrows `rows` both live in this
        // block, so both are gone before `rows` is handed back
        {
            let mut run = compile(query, &cache)?;
            let sink = |row: Value, _acc: &Arc<NoBound>, _op: SinkOperation| -> Result<()> {
                rows.push(row);
                Ok(())
            };
            run(Arc::new(NoBound), sink, lookup)?;
        }
        Ok(rows)
    }

    /// Requirement: a query is a document, and the smallest document that is one
    /// compiles.
    ///
    /// What: `{"roots": [...], "emit": "identifier"}` — enumerated roots, no
    /// connections, a bare emit — compiles and produces a runnable function.
    ///
    /// Why: every level of the grammar has an optional form, and the compiler
    /// must not demand more than the grammar does. If the smallest legal
    /// document needed a wrapper or a filter, the grammar would be lying about
    /// what is optional.
    ///
    /// LLM section: no `walk` key at all, which means no connections to follow;
    /// the walk visits its roots and stops.
    #[test]
    fn the_smallest_query_compiles() {
        let compiled = compile_only(json!({
            "roots": ["a", "b"],
            "emit": "identifier",
        }));
        assert!(
            compiled.is_ok(),
            "the smallest legal query compiles: {:?}",
            compiled.err()
        );
    }

    /// Requirement: a walk may traverse, gate, emit and stop, and a document
    /// using every one of those levels compiles.
    ///
    /// What: a walk list with a bare connection and a gated one, a filter that
    /// is a boolean tree, an emit with a top-level gate and two entries, and a
    /// stop with hard and soft rules.
    ///
    /// Why: the levels are read by different readers, and a level that only
    /// works alone is not a level. This is the document that exercises all of
    /// them at once, which is where a compiler's wiring shows.
    ///
    /// LLM section: the gate leaves are `start:`/`end:`/`regex:` against a
    /// connection's own name, and the filter's leaves are sansho expressions
    /// over the item. They are different leaf languages and the compiler must
    /// route each to its own reader.
    #[test]
    fn a_walk_with_every_level_compiles() {
        let compiled = compile_only(json!({
            "roots": ["a"],
            "walk": [
                "alias:to",
                {"contained:down": {"and": ["start:BOOT-INF", {"<=": {"depth": 2}}]}}
            ],
            "filter": {"and": ["contains(@, 'x')", {"==": {"is_terminal": true}}]},
            "emit": {
                "if": "length(@) > `0`",
                "unnest": false,
                "emit": [
                    "identifier",
                    {"expr": "@", "if": "`true`", "unnest": true, "emit_null": true}
                ]
            },
            "stop": {
                "hard": [{"if": {"==": {"depth": 3}}, "emit": ["identifier"]}],
                "soft": {"if": "`false`"}
            },
            "tee": true
        }));
        assert!(
            compiled.is_ok(),
            "a walk using every level compiles: {:?}",
            compiled.err()
        );
    }

    /// Requirement: phases chain, and a phase's roots may be derived from the
    /// rows of a phase named earlier.
    ///
    /// What: two phases, the second naming the first as the source of its roots.
    ///
    /// Why: the chain is the whole reason the query level exists. If the second
    /// phase's root could not be resolved to the first, the document would be a
    /// chain in name only.
    ///
    /// LLM section: `{"roots": {"first": "id"}}` is the predicate form — the
    /// expression `id` is applied to each row the phase `first` emitted.
    #[test]
    fn a_chain_of_two_phases_compiles() {
        let compiled = compile_only(json!({
            "phases": [
                {
                    "name": "first",
                    "roots": ["a"],
                    "emit": {"expr": "{id: identifier}", "unnest": false},
                    "tee": true
                },
                {
                    "name": "second",
                    "roots": {"first": "id"},
                    "emit": "identifier",
                    "tee": true
                }
            ]
        }));
        assert!(
            compiled.is_ok(),
            "a two-phase chain compiles: {:?}",
            compiled.err()
        );
    }

    /// Requirement: several phases may feed one, which is why a root names its
    /// sources rather than its source.
    ///
    /// What: a diamond — `left` and `right` both feed `join`, which draws its
    /// roots from both at once.
    ///
    /// Why: a single-source reference could not express this, and the union is
    /// the case that forces phases to be named. If the compiler only accepted
    /// one source, the diamond would compile into a query that quietly ignored
    /// half its input.
    ///
    /// LLM section: `{"roots": {"left": "id", "right": "id"}}` — two entries,
    /// each with its OWN expression, because two phases' rows need not have the
    /// same shape.
    #[test]
    fn a_diamond_of_phases_compiles() {
        let compiled = compile_only(json!({
            "phases": [
                {"name": "left", "roots": ["a"], "emit": {"expr": "{id: identifier}"}},
                {"name": "right", "roots": ["b"], "emit": {"expr": "{id: identifier}"}},
                {
                    "name": "join",
                    "roots": {"left": "id", "right": "id"},
                    "emit": "identifier",
                    "tee": true
                }
            ]
        }));
        assert!(
            compiled.is_ok(),
            "a diamond compiles: {:?}",
            compiled.err()
        );
    }

    /// Requirement: a query that compiles can be run, and it emits the rows the
    /// walk reached.
    ///
    /// What: the smallest query, run against a herd holding `a` and `b`, emits
    /// both identifiers.
    ///
    /// Why: compiling is only half of it. A compiled function that produces no
    /// rows, or the wrong ones, has compiled nothing worth having — and this is
    /// the only test that proves the wiring from a JSON document through the
    /// grammar, the compiler and the walk to a row.
    ///
    /// LLM section: the roots are visited, `emit` is `identifier`, and the phase
    /// is teed, so every visited item produces one row.
    #[test]
    fn the_smallest_query_runs_and_emits_its_roots() -> Result<()> {
        let rows = run_document(
            json!({
                "roots": ["a", "b"],
                "emit": "identifier",
                "tee": true
            }),
            herd(&["a", "b"]),
        )?;
        let mut emitted: Vec<&str> = rows.iter().filter_map(|row| row.as_str()).collect();
        emitted.sort_unstable();
        assert_eq!(
            emitted,
            vec!["a", "b"],
            "both roots are visited and each produces its identifier"
        );
        Ok(())
    }

    /// Requirement: the rows of one phase become the roots of the next.
    ///
    /// What: `first` emits `{"id": "a"}`-shaped rows for two items; `second`
    /// derives its roots from those rows through `id` and emits them.
    ///
    /// Why: this is the chain working end to end. The second phase's walk starts
    /// from identifiers that did not exist until the first phase ran, so a test
    /// that only compiles the chain would not show that the wiring carries
    /// anything.
    ///
    /// LLM section: `first` emits an object per item; `second`'s predicate reads
    /// the `id` field of each. Both are teed, so the rows are `a`, `b` from
    /// `first` and then `a`, `b` again from `second`.
    #[test]
    fn a_phase_draws_its_roots_from_the_phase_before_it() -> Result<()> {
        let rows = run_document(
            json!({
                "phases": [
                    {
                        "name": "first",
                        "roots": ["a", "b"],
                        "emit": {"expr": "{id: identifier}"},
                        "tee": true
                    },
                    {
                        "name": "second",
                        "roots": {"first": "id"},
                        "emit": "identifier",
                        "tee": true
                    }
                ]
            }),
            herd(&["a", "b"]),
        )?;
        assert_eq!(rows.len(), 4, "two rows from each phase: {rows:?}");

        // The roots of a walk are held in a SET, so the order two items are
        // visited in is not the order they were written. The rows are compared
        // as the sets they are, in phase order.
        let first: Vec<&Value> = rows.iter().take(2).collect();
        assert!(first.contains(&&json!({"id": "a"})), "first phase: {rows:?}");
        assert!(first.contains(&&json!({"id": "b"})), "first phase: {rows:?}");

        let second: Vec<&Value> = rows.iter().skip(2).collect();
        assert!(
            second.contains(&&json!("a")),
            "the second phase starts from the first's rows: {rows:?}"
        );
        assert!(
            second.contains(&&json!("b")),
            "the second phase starts from the first's rows: {rows:?}"
        );
        Ok(())
    }

    /// Requirement: a root that names no identifiers is refused, and the refusal
    /// says which phase and which level.
    ///
    /// What: `{"roots": []}` — an enumerated root with an empty list.
    ///
    /// Why: a walk with no roots is not a walk. It can only mean every item in
    /// the cluster, and a cluster may hold twenty billion of them. Refusing it
    /// at compile time is the difference between a mistake and an outage.
    ///
    /// LLM section: the phase is unnamed, so it is identified by POSITION — the
    /// only name it has.
    #[test]
    fn a_root_that_names_no_identifiers_is_refused_with_its_phase() {
        let error = compile_only(json!({"roots": [], "emit": "identifier"}))
            .expect_err(
            "an empty root list is refused");

        assert_eq!(
            error.path.get(1),
            Some(&Step::Phase {
                name: None,
                position: 0
            }),
            "the phase is named by position"
        );
        assert_eq!(error.path.last(), Some(&Step::Root));
        assert!(
            error.detail.contains("names none"),
            "the detail says what is wrong: {}",
            error.detail
        );
        assert!(
            error.to_string().contains("the phase at position 0"),
            "the rendered error says where: {error}"
        );
    }

    /// Requirement: a root drawing on a phase that does not exist is refused,
    /// and the refusal names the phase it could not find.
    ///
    /// What: `{"roots": {"ghost": "id"}}` — a predicate naming a phase no phase
    /// is called.
    ///
    /// Why: this is the failure that would otherwise surface as an empty walk at
    /// run time, which looks exactly like a walk that found nothing. It is a
    /// property of the document and must be caught while reading it.
    ///
    /// LLM section: the detail quotes the missing name, so the author can see
    /// which reference is wrong rather than being told a reference is wrong.
    #[test]
    fn a_root_drawing_on_a_missing_phase_names_the_phase_it_could_not_find() {
        let error = compile_only(json!({"roots": {"ghost": "id"}, "emit": "identifier"}))
            .expect_err(
            "a missing source phase is refused");

        assert_eq!(error.path.last(), Some(&Step::Root));
        assert!(
            error.detail.contains("ghost"),
            "the detail names the missing phase: {}",
            error.detail
        );
    }

    /// Requirement: two phases may not share a name.
    ///
    /// What: a chain where both phases are called `same`.
    ///
    /// Why: a name is how a phase is referred to, so two phases with one name
    /// make every reference to it ambiguous. The compiler could pick one, and
    /// whichever it picked would be wrong half the time.
    ///
    /// LLM section: the failure is the QUERY's, not a phase's, so the level is
    /// `Query` and the phase is the name that was duplicated.
    #[test]
    fn a_duplicate_phase_name_is_refused_with_the_name() {
        let error = compile_only(json!({
            "phases": [
                {"name": "same", "roots": ["a"], "emit": "identifier"},
                {"name": "same", "roots": ["b"], "emit": "identifier"}
            ]
        }))
        .expect_err(
        "a duplicate name is refused");

        assert_eq!(error.path.first(), Some(&Step::Query));
        assert_eq!(
            error.path.get(1),
            Some(&Step::Phase {
                name: Some("same".to_string()),
                position: 1
            })
        );
        assert!(error.detail.contains("one phase"), "{}", error.detail);
    }

    /// Requirement: the phase graph is acyclic.
    ///
    /// What: two phases, each deriving its roots from the other.
    ///
    /// Why: neither can run first, so there is no order in which the query
    /// means anything. Detecting it at compile time is the only place it can be
    /// detected at all — at run time the walk would simply never start.
    ///
    /// LLM section: the error names a phase that lies ON the cycle, so the
    /// author has somewhere to start looking rather than being told only that
    /// one exists.
    #[test]
    fn a_cycle_is_refused_and_names_a_phase_in_it() {
        let error = compile_only(json!({
            "phases": [
                {"name": "one", "roots": {"two": "id"}, "emit": "identifier"},
                {"name": "two", "roots": {"one": "id"}, "emit": "identifier"}
            ]
        }))
        .expect_err(
        "a cycle is refused");

        assert_eq!(error.path.first(), Some(&Step::Query));
        assert!(
            error.detail.contains("cycle"),
            "the detail says it is a cycle: {}",
            error.detail
        );
        match error.path.get(1) {
            Some(Step::Phase {
                name: Some(name), ..
            }) => assert!(
                name == "one" || name == "two",
                "the named phase is on the cycle, found `{name}`"
            ),
            other => panic!("a phase on the cycle is named, found {other:?}"),
        }
    }

    /// Requirement: a walk emits something, so a document without an emit is
    /// refused.
    ///
    /// What: a walk with roots and no `emit`.
    ///
    /// Why: a query that emits nothing has no output, which is a mistake rather
    /// than a choice — the author wanted rows and would get silence.
    ///
    /// LLM section: the level is `Emit`, and the phase is named by position
    /// because the document has no name for it.
    #[test]
    fn a_walk_with_no_emit_is_refused_with_its_phase_and_level() {
        let error = compile_only(json!({"roots": ["a"]}))
            .expect_err(
            "a walk without an emit is refused");

        assert_eq!(error.path.last(), Some(&Step::Emit));
        assert_eq!(
            error.path.get(1),
            Some(&Step::Phase {
                name: None,
                position: 0
            })
        );
        assert!(
            error.detail.contains("emit"),
            "the detail says what is missing: {}",
            error.detail
        );
    }

    /// Requirement: a malformed level is refused at the level it was read, not
    /// somewhere downstream.
    ///
    /// What: a filter whose `and` holds a number instead of a list of gates.
    ///
    /// Why: the reader that knows the shape is the reader that can describe the
    /// mistake. If the failure surfaced as a missing predicate later, the author
    /// would be told a gate is absent rather than told which gate is wrong.
    ///
    /// LLM section: `and` is a compound node whose payload must be an array; the
    /// error comes from the expression reader and is located at the filter
    /// level, which is the level the author wrote.
    #[test]
    fn a_malformed_gate_is_refused_at_the_level_it_was_read() {
        let error = compile_only(json!({
            "roots": ["a"],
            "filter": {"and": 7},
            "emit": "identifier"
        }))
        .expect_err(
        "a malformed gate is refused");

        assert_eq!(
            error.path.last(),
            Some(&Step::Filter),
            "the filter is where it was written"
        );
        assert!(
            error.detail.contains("array"),
            "the detail says what the shape should be: {}",
            error.detail
        );
    }

    /// Requirement: `emit_at_end` emits once, after the walk, and its rows reach
    /// the sink.
    ///
    /// What: a phase that emits its identifier and then emits a constant at the
    /// end.
    ///
    /// Why: the level exists to say something about a phase as a whole rather
    /// than about an item. A level that compiles but never runs would be a level
    /// the compiler accepted and dropped, and the caller could not tell. The
    /// ORDER is what makes it the end of the walk: its row follows the walk's.
    ///
    /// LLM section: the subject of an end-of-walk emit is the statistics a walk
    /// gathers. Those are not collected yet, so the subject is an empty one and
    /// this test uses an expression that reads nothing from it — a constant.
    #[test]
    fn emit_at_end_emits_after_the_walk() -> Result<()> {
        let rows = run_document(
            json!({
                "roots": ["a"],
                "emit": "identifier",
                "tee": true,
                "emit_at_end": {"emit": "`1`"}
            }),
            herd(&["a"]),
        )?;
        assert_eq!(
            rows.len(),
            2,
            "one row from the walk and one at the end: {rows:?}"
        );
        assert_eq!(rows[0], json!("a"), "the walk's row comes first");
        assert_eq!(rows[1], json!(1), "the end row comes after the walk");
        Ok(())
    }

    /// Requirement: a gate in an end-of-walk emit may not read the walk state.
    ///
    /// What: an `emit_at_end` whose entry's gate compares `depth`, nested inside
    /// an `and`.
    ///
    /// Why: there is no walk state when the walk is over. Answering the
    /// comparison with a stub would be the compiler inventing a fact about a
    /// walk that has finished, and the author would never learn that their gate
    /// meant nothing. Refusing it where it is written is the only place the
    /// mistake can be explained to them.
    ///
    /// LLM section: the refusal is a validation pass over the gate tree, so it
    /// finds a variable however deeply the gate nests it — which is what the
    /// nesting here is for.
    #[test]
    fn a_variable_in_an_emit_at_end_gate_is_refused() {
        let error = compile_only(json!({
            "roots": ["a"],
            "emit": "identifier",
            "emit_at_end": {
                "emit": {
                    "expr": "`1`",
                    "if": {"and": ["`true`", {"<=": {"depth": 2}}]}
                }
            }
        }))
        .expect_err(
        "a gate that reads the walk state is refused here");

        assert_eq!(
            error.path,
            vec![
                Step::Query,
                Step::Phase {
                    name: None,
                    position: 0
                },
                Step::EmitAtEnd,
                Step::Entry(0),
                Step::Gate
            ],
            "the failure says which phase, which level, which entry and which gate"
        );
        assert!(
            error.detail.contains("walk state"),
            "the detail says why: {}",
            error.detail
        );
    }

    /// Requirement: the same gate is fine in the walk's own emit, where the
    /// walk state exists.
    ///
    /// What: the document the previous test refused, with the gate on the
    /// walk's emit instead of on the end-of-walk emit.
    ///
    /// Why: the restriction belongs to the LEVEL, not to the gate. If it were
    /// applied to gates in general, the compiler would refuse the most ordinary
    /// query there is — one that gates an entry on how deep it is.
    ///
    /// LLM section: the two tests together pin that the refusal is scoped to
    /// `emit_at_end` and does not leak into the emit.
    #[test]
    fn the_same_gate_is_allowed_in_the_walks_own_emit() {
        let compiled = compile_only(json!({
            "roots": ["a"],
            "emit": {"expr": "identifier", "if": {"and": ["`true`", {"<=": {"depth": 2}}]}},
            "tee": true
        }));
        assert!(
            compiled.is_ok(),
            "a gate on the walk's emit may read the walk state: {:?}",
            compiled.err()
        );
    }

    /// Requirement: a failure says where it happened, in the words of the level
    /// that found it.
    ///
    /// What: the rendered form of a located error, for a named phase.
    ///
    /// Why: the whole point of the located error is that a message like "a root
    /// draws on `ghost`" is half an answer. This pins that the rendered form
    /// carries the phase, the level and the detail together, because that
    /// rendering is what a caller actually sees.
    ///
    /// LLM section: the phase is named in the document, so the rendering uses
    /// the name rather than the position.
    #[test]
    fn a_rendered_failure_carries_the_phase_the_level_and_the_detail() {
        let error = compile_only(json!({
            "phases": [{
                "name": "classes",
                "roots": {"ghost": "id"},
                "emit": "identifier"
            }]
        }))
        .expect_err(
        "a missing source phase is refused");

        let rendered = error.to_string();
        assert!(
            rendered.contains("phase `classes`"),
            "the rendering names the phase: {rendered}"
        );
        assert!(
            rendered.contains("its root"),
            "the rendering names the level: {rendered}"
        );
        assert!(
            rendered.contains("ghost"),
            "the rendering carries the detail: {rendered}"
        );
    }
}
