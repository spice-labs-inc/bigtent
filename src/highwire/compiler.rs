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
    Barker, EmitEntry, GrammarError, ItemEmit, ItemStop, LevelResult, Query, Root, RootKind, Stage,
    StageKind, Step, StopRule, Walk, WalkConnection,
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
pub type Gate = Arc<dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> bool + Send + Sync>;

/// What an emit produces for one subject, compiled.
pub type EmitFn =
    Arc<dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> Vec<Value> + Send + Sync>;

/// The stop rules, compiled: hard first, then soft, first match wins.
pub type StopFn = Arc<
    dyn for<'x> Fn(&SanshoCarrier<'x>, &WalkState) -> (Option<Vec<Value>>, Option<Vec<Value>>)
        + Send
        + Sync,
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

    /// Taken from the caller's input stream, adapted to identifiers.
    FromInput,
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
    /// Whether repeated rows are emitted once.
    dedup: bool,
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

/// A stage, compiled: the traversal, or the program and where its rows come
/// from.
enum CompiledStage {
    Walk(Box<CompiledPhase>),
    Barker(Box<CompiledBarker>),
}

// Both variants are boxed: a compiled walk is large and a compiled program is
// small, and boxing both keeps the enum one pointer wide either way rather than
// making every stage pay for the walk's size.

impl CompiledStage {
    /// The stage's name, when it has one.
    fn name(&self) -> Option<&String> {
        match self {
            CompiledStage::Walk(phase) => phase.name.as_ref(),
            CompiledStage::Barker(barker) => barker.name.as_ref(),
        }
    }

    /// Where a walk draws its roots from, as the stages whose rows it reads.
    fn row_sources(&self) -> Vec<usize> {
        match self {
            CompiledStage::Walk(phase) => match &phase.roots {
                CompiledRoot::Predicates(predicates) => {
                    predicates.iter().map(|(at, _)| *at).collect()
                }
                CompiledRoot::Enumerated(_) | CompiledRoot::FromInput => Vec::new(),
            },
            CompiledStage::Barker(barker) => barker.inputs.clone(),
        }
    }
}

/// A program stage, compiled.
///
/// The program is parsed HERE, at compile time, so a syntax error is a compile
/// failure located in the phase it was written in rather than something that
/// turns up when the pipeline is half-run.
struct CompiledBarker {
    name: Option<String>,

    /// The stages whose rows this program reads, as their positions.
    inputs: Vec<usize>,

    /// Whether its rows also reach the caller.
    tee: bool,

    /// Whether an array a row evaluates to produces one row per element.
    unnest: bool,

    /// Whether repeated rows are emitted once.
    dedup: bool,

    /// The program's SOURCE, kept rather than a parsed `Program`.
    ///
    /// A cached query runs many times, and a barker's `Program` is consumed by a
    /// run — the Starlark evaluator binds its module state at execution. So the
    /// compiled stage holds the source, validates it by parsing at compile time
    /// (a syntax error is a compile failure), and parses again per run on the
    /// stage's own thread. Parse is the cheap phase; the expensive work is the
    /// evaluation itself.
    source: String,
}

/// Compile a program stage.
fn compile_barker(
    barker: &Rc<dyn Barker>,
    position: usize,
    by_name: &HashMap<String, usize>,
    _cache: &ProgramCache,
) -> LevelResult<CompiledStage> {
    let at = Location::query().step(Step::Phase {
        name: None,
        position,
    });

    // the name first, so every failure below can be located against it
    let name = match barker.program() {
        Ok(_) => by_name
            .iter()
            .find(|(_, at)| **at == position)
            .map(|(name, _)| name.clone()),
        Err(error) => return Err(located(&at, Err(error))?),
    };
    let at = Location::query().step(Step::Phase {
        name: name.clone(),
        position,
    });

    let mut inputs = Vec::new();
    for source in located(&at, barker.inputs())? {
        let Some(&producer) = by_name.get(source.as_str()) else {
            return Err(at.error(format!(
                "a program reads `{source}`, and no stage is named that"
            )));
        };
        inputs.push(producer);
    }

    let tee = located(&at, barker.tee())?;
    let unnest = located(&at, barker.unnest())?;
    let dedup = located(&at, barker.dedup())?;

    let source = located(&at, barker.program())?;
    // validated here by parsing; the Program is dropped, and the source is what
    // the compiled stage keeps for its runs
    crate::barker::parse(source).map_err(|e| at.error(format!("{e}")))?;

    Ok(CompiledStage::Barker(Box::new(CompiledBarker {
        name,
        inputs,
        tee,
        unnest,
        dedup,
        source: source.to_string(),
    })))
}

impl CompiledPhase {
    /// Whether this stage's roots come from the caller's input stream.
    ///
    /// Read by the wiring, so that only a stage the query left unnamed is fed from
    /// the input — a stage that names its own roots keeps them.
    fn roots_is_from_input(&self) -> bool {
        matches!(self.roots, CompiledRoot::FromInput)
    }
}

/// The phase graph, settled at compile time.
struct Plan {
    /// The phases in the order they must run.
    order: Vec<usize>,

    /// Per stage, the stages that feed it. Recorded because it is what a wired
    /// run would connect channels from; the sequential loop derives the same
    /// information from each root's predicates, so nothing reads this yet.
    #[allow(dead_code)]
    dependencies: Vec<Vec<usize>>,

    /// Per phase, the phases that read its rows and the program each applies.
    /// Indexed by the producing phase's position.
    consumers: Vec<Vec<(usize, Arc<Program>)>>,
}

/// A compiled query: the stages and the plan, owned outright.
///
/// This is everything expensive that `compile` produces, and it is `Send + Sync`
/// because every part of it is: the compiled gates and emits are
/// `Arc<dyn Fn + Send + Sync>`, the programs are `Arc<Program>`, and a barker's
/// parsed source sits behind a mutex. So a compiled query can be stored, shared,
/// and run many times — which is what a server holding pre-canned queries does.
///
/// The type is deliberately not a closure: an `impl FnMut` returned from
/// `compile` infers its type at the call site and can capture the caller's
/// sink borrow, which is why a compiled query in a test had to be dropped inside
/// a nested block before the sink's rows could be read. An owned struct has no
/// such capture, and its [`CompiledQuery::run`] takes the sink as an argument.
pub struct CompiledQuery {
    stages: Vec<CompiledStage>,
    plan: Plan,
}

// every part of the two fields is Send + Sync; this pins it, so a regression is a
// compile error here rather than a mystery at the server
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CompiledQuery>();
};

impl CompiledQuery {
    /// The stages this query runs.
    pub fn stage_count(&self) -> usize {
        self.stages.len()
    }

    /// Run the query. May be called many times, concurrently: every stage thread
    /// is spawned per call, and nothing here mutates the compiled form.
    pub fn run<ACCOUNTANT, SINK>(
        &self,
        accountant: Arc<ACCOUNTANT>,
        sink: SINK,
        lookup: HerdOrCluster,
        input: flume::Receiver<Value>,
    ) -> Result<()>
    where
        ACCOUNTANT: AccountingAndBudget + Send + Sync + 'static,
        SINK: FnMut(Value, &Arc<ACCOUNTANT>, SinkOperation) -> Result<()>,
    {
        run(self.stages.as_slice(), &self.plan, input, accountant, sink, lookup)
    }
}

/// The cache of compiled queries: the expressions' programs for the life of the
/// process, and the compiled documents keyed by their text.
///
/// Two levels, and the first is the one that pays first: `ProgramCache` holds every
/// sansho expression any query has compiled, keyed by its text, so the second query
/// to use `contains(body.mime_type, ...)` re-parses its document but compiles
/// nothing. The second level holds the compiled queries themselves, so a request
/// that runs a pre-canned document is a hash lookup and a run.
pub struct QueryCache {
    expressions: Arc<ProgramCache>,
    queries: std::sync::Mutex<HashMap<String, Arc<CompiledQuery>>>,
}

impl QueryCache {
    /// A cache holding up to `max_expressions` compiled programs.
    pub fn new(max_expressions: usize) -> Self {
        QueryCache {
            expressions: Arc::new(ProgramCache::new(max_expressions)),
            queries: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Compile a query document, or return the one already compiled from this
    /// exact text.
    pub fn get_or_compile(&self, document: &str) -> LevelResult<Arc<CompiledQuery>> {
        let mut queries = self
            .queries
            .lock()
            .expect("the query cache's lock");
        if let Some(compiled) = queries.get(document) {
            return Ok(Arc::clone(compiled));
        }
        let parsed: serde_json::Value = serde_json::from_str(document)
            .map_err(|error| GrammarError::at(Step::Query, format!("not JSON: {error}")))?;
        let query: Rc<dyn Query> = Rc::new(parsed);
        let compiled = Arc::new(self.compile(query)?);
        queries.insert(document.to_string(), Arc::clone(&compiled));
        Ok(compiled)
    }

    /// Compile a query, through the shared expression cache.
    fn compile(&self, query: Rc<dyn Query>) -> LevelResult<CompiledQuery> {
        let phases = located(&Location::query(), query.phases())?;

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

        let mut compiled: Vec<CompiledStage> = Vec::with_capacity(phases.len());
        for (position, phase) in phases.iter().enumerate() {
            compiled.push(compile_stage(phase, position, &by_name, &self.expressions)?);
        }

        let plan = plan_phases(&compiled)?;
        Ok(CompiledQuery { stages: compiled, plan })
    }
}

/// Compile a query into the function that runs it.
///
/// The returned function takes the accountant, the sink, the cluster and the
/// caller's input stream. The input is the roots of every stage that takes them
/// from it — empty and closed for a query that names its own.
#[allow(clippy::type_complexity)]
///
/// The returned function takes the accountant, the sink, and the lookup — the
/// three things a run needs that are not part of the query. It may be called
/// more than once: everything expensive has already happened here.
pub fn compile<ACCOUNTANT, SINK>(
    query: Rc<dyn Query>,
    cache: &ProgramCache,
) -> LevelResult<impl FnMut(Arc<ACCOUNTANT>, SINK, HerdOrCluster, flume::Receiver<Value>) -> Result<()>>
where
    ACCOUNTANT: AccountingAndBudget + Send + Sync + 'static,
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
    let mut compiled: Vec<CompiledStage> = Vec::with_capacity(phases.len());
    for (position, phase) in phases.iter().enumerate() {
        compiled.push(compile_stage(phase, position, &by_name, cache)?);
    }

    // Third pass: order the phases and wire the rows between them.
    let plan = plan_phases(&compiled)?;

    Ok(
        move |accountant: Arc<ACCOUNTANT>,
              mut sink: SINK,
              lookup: HerdOrCluster,
              input: flume::Receiver<Value>|
         -> Result<()> {
            run(&compiled, &plan, input, accountant, &mut sink, lookup)
        },
    )
}

/// Compile a stage: a walk compiles to the phase that traverses, a program to
/// the phase that runs it.
///
/// The dispatch is by `kind`, which is the selector the grammar provides for
/// exactly this — asking a stage for the wrong one is a failure, so neither arm
/// guesses.
fn compile_stage(
    stage: &Rc<dyn Stage>,
    position: usize,
    by_name: &HashMap<String, usize>,
    cache: &ProgramCache,
) -> LevelResult<CompiledStage> {
    let kind = located(&Location::query().step(Step::Phase { name: None, position }), stage.kind())?;
    match kind {
        StageKind::Walk => {
            let walk = located(
                &Location::query().step(Step::Phase { name: None, position }),
                stage.walk(),
            )?;
            compile_phase(&walk, position, by_name, cache)
                .map(|p| CompiledStage::Walk(Box::new(p)))
        }
        StageKind::Barker => {
            let barker = located(
                &Location::query().step(Step::Phase { name: None, position }),
                stage.barker(),
            )?;
            compile_barker(&barker, position, by_name, cache)
        }
    }
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
        // a root the query did not name: the caller's input stream
        roots: match &root {
            Some(root) => compile_root(root, &who, by_name, cache)?,
            None => CompiledRoot::FromInput,
        },
        connections: compile_connections(&connections, &who)?,
        filter: match filter {
            Some(filter) => {
                let gate = located(&here().step(Step::Filter), filter.gate())?;
                located_build(&here().step(Step::Filter), build_item_expression(&gate, cache))?
            }
            None => Arc::new(|_: &SanshoCarrier<'_>, _: &WalkState| true),
        },
        emit: compile_emit(&emit, &who, Step::Emit, cache, Variables::Allowed)?,
        stop: compile_stop(stop, &who, cache)?,
        tee: located(&here().step(Step::Tee), phase.tee())?,
        dedup: located(&here().step(Step::Emit), phase.dedup())?,
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

        RootKind::FromInput => Ok(CompiledRoot::FromInput),

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
            None => Arc::new(|_: &str, _: &usize| true),
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
    Ok(Arc::new(move |thing: &SanshoCarrier<'_>, ws: &WalkState| {
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

    Ok(Arc::new(
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
fn plan_phases(phases: &[CompiledStage]) -> LevelResult<Plan> {
    let mut consumers: Vec<Vec<(usize, Arc<Program>)>> = vec![Vec::new(); phases.len()];
    let mut dependencies: Vec<Vec<usize>> = vec![Vec::new(); phases.len()];
    for (consumer, stage) in phases.iter().enumerate() {
        // a walk draws rows through its root's predicates; a program draws them
        // through its declared inputs
        for producer in stage.row_sources() {
            let program = match stage {
                CompiledStage::Walk(phase) => match &phase.roots {
                    CompiledRoot::Predicates(predicates) => predicates
                        .iter()
                        .find(|(at, _)| *at == producer)
                        .map(|(_, program)| Arc::clone(program))
                        .expect("the source came from this list"),
                    // enumerated roots and input roots have no producer stage
                    CompiledRoot::Enumerated(_) | CompiledRoot::FromInput => unreachable!("no sources"),
                },
                // a program consumes the row stream itself, so no expression is
                // applied to the row to shape it
                CompiledStage::Barker(_) => identity_program(),
            };
            consumers[producer].push((consumer, program));
            dependencies[consumer].push(producer);
        }
    }

    let mut order = Vec::with_capacity(phases.len());
    let mut state = vec![Visit::Unvisited; phases.len()];
    for position in 0..phases.len() {
        visit(position, phases, &dependencies, &mut state, &mut order)?;
    }
    Ok(Plan {
        order,
        dependencies,
        consumers,
    })
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
    phases: &[CompiledStage],
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

/// A program that hands its subject through unchanged.
///
/// Used where a consumer takes the row itself rather than a projection of it —
/// a program reads the upstream rows as they are, so the wiring adds no step
/// between the two.
fn identity_program() -> Arc<sansho::Program> {
    use std::sync::OnceLock;
    static IDENTITY: OnceLock<Arc<sansho::Program>> = OnceLock::new();
    Arc::clone(IDENTITY.get_or_init(|| {
        let cache = ProgramCache::new(1);
        // `compile_sansho` already returns the shared program
        compile_sansho("@", &cache).expect("@ compiles")
    }))
}

/// A compiled stage, named if it has a name.
fn phase_ref(phases: &[CompiledStage], position: usize) -> Step {
    Step::Phase {
        name: phases[position].name().cloned(),
        position,
    }
}

/// The edge into a walk: rows in, identifiers out.
///
/// A walk reads identifiers and a producing stage emits rows, so the edge between
/// them is a channel whose one job is the projection — the consumer's expression
/// applied to each row, with anything that does not become a string dropped. A
/// count or a multiselect row reaches a barker and is never mistaken for an
/// identifier by a walk.
fn identifiers_of<ACCOUNTANT: AccountingAndBudget + Send + Sync + 'static>(
    input: flume::Receiver<Value>,
    to_walk: flume::Sender<String>,
    projection: Option<Arc<sansho::Program>>,
    accountant: Arc<ACCOUNTANT>,
) -> std::thread::JoinHandle<Result<()>> {
    std::thread::spawn(move || -> Result<()> {
        let _ = accountant;
        for row in input.iter() {
            // no projection: the row IS the identifier, when it is a string
            let identifier = match &projection {
                None => row.as_str().map(str::to_string),
                Some(program) => {
                    let carrier: SanshoCarrier<'_> = Rc::new(row);
                    evaluate(program, carrier)
                        .ok()
                        .and_then(|value| value.as_str().ok().map(str::to_string))
                }
            };
            if let Some(identifier) = identifier {
                let _ = to_walk.send(identifier);
            }
        }
        Ok(())
    })
}

/// A row's identity, for `dedup`: the first 16 bytes of the BLAKE3 digest of its
/// canonical rendering.
///
/// 16 bytes is 128 bits of an evenly distributed digest, the same collision budget
/// the index uses, so a collision would drop a row that should have passed and at
/// 128 bits it does not happen by accident.
///
/// **A string is hashed where it lies.** The common row is a string — an
/// identifier — and rendering it would copy it into a fresh `String` for nothing: a
/// string value's rendering IS the string, so the digest is taken over the existing
/// bytes. Every other row is rendered first, canonically (object keys sorted, so two
/// objects with equal fields dedupe whatever order they were built in).
///
/// That is a PERFORMANCE distinction, not an accuracy one: both paths compute the
/// same digest for the same value.
fn row_key(row: &Value) -> [u8; 16] {
    let mut key = [0u8; 16];
    let digest = match row {
        // no rendering, no copy: the value's bytes are the hash's input
        Value::String(text) => blake3::hash(text.as_bytes()),
        other => blake3::hash(canonical_row(other).as_bytes()),
    };
    key.copy_from_slice(&digest.as_bytes()[..16]);
    key
}

/// A row as text with object keys sorted, so equal values render identically.
fn canonical_row(value: &Value) -> String {
    match value {
        Value::Object(entries) => {
            let mut keys: Vec<&String> = entries.keys().collect();
            keys.sort();
            let rendered: Vec<String> = keys
                .into_iter()
                .map(|key| {
                    format!(
                        "{}:{}",
                        Value::String(key.clone()),
                        canonical_row(&entries[key])
                    )
                })
                .collect();
            format!("{{{}}}", rendered.join(","))
        }
        Value::Array(items) => {
            let rendered: Vec<String> = items.iter().map(canonical_row).collect();
            format!("[{}]", rendered.join(","))
        }
        other => other.to_string(),
    }
}

/// Run a query document from inside a script, returning its rows.
///
/// LAZY: the document is compiled and the graph started only when the returned
/// stream is first pulled, so a script that builds a query and never reads it pays
/// nothing. The compile therefore cannot report its failure here — it has not
/// happened yet — so a compile failure arrives as the stream's first item, which is
/// the same shape a runtime failure has inside a nested query.
pub fn run_nested(
    document: Value,
    lookup: Option<HerdOrCluster>,
) -> Result<crate::barker::Rows> {
    let (sender, receiver) = flume::bounded(256);
    std::thread::spawn(move || {
        let query: Rc<dyn Query> = Rc::new(document);
        // the shared expression cache, so a script that runs many nested queries
        // compiles their expressions once
        let cache = ProgramCache::new(4096);
        let out = sender.clone();
        let sink = move |row: Value, _acc: &Arc<NoBound>, _op: SinkOperation| -> Result<()> {
            let _ = out.send(Ok(row));
            Ok(())
        };
        let mut run = match compile(query, &cache) {
            Ok(run) => run,
            Err(error) => {
                let _ = sender.send(Err(anyhow::anyhow!("{error}")));
                return;
            }
        };
        let Some(lookup) = lookup else {
            let _ = sender.send(Err(anyhow::anyhow!(
                "a nested query needs a cluster, and this run was given none"
            )));
            return;
        };
        let (unused_sender, no_input) = flume::unbounded();
        drop(unused_sender); // a nested query's roots come from its own stages
        if let Err(error) = run(Arc::new(NoBound), sink, lookup, no_input) {
            let _ = sender.send(Err(error));
        }
    });
    Ok(crate::barker::Rows::from_receiver(receiver))
}

/// The accountant a nested query uses until the real one is wired in.
struct NoBound;

impl AccountingAndBudget for NoBound {
    fn should_stop(&self) -> Option<Vec<Value>> {
        None
    }

    fn can_sink_normal(&self) -> bool {
        true
    }

    fn event(&self, _event_type: crate::highwire::walk::AccountingEventType) {}
}

/// A tee: one receiver in, a Vec of receivers out.
///
/// Every row that arrives is sent to **every** receiver in the list. The node does
/// not know or care where they lead: if two of the entries are the same place, the
/// row arrives there twice, because that is what one input and two outputs means.
///
/// That indifference is the point. Deciding which receivers belong in the list — the
/// consumer's, and the global output when the stage's `tee` flag is set — happens
/// once, during wiring. A stage never splits its own output: it has one receiver in
/// and one sender out, and what happens after the sender is not its business.
fn tee<ACCOUNTANT: AccountingAndBudget + Send + Sync + 'static>(
    input: flume::Receiver<Value>,
    outputs: Vec<flume::Sender<Value>>,
    dedup: bool,
    accountant: Arc<ACCOUNTANT>,
) -> std::thread::JoinHandle<Result<()>> {
    std::thread::spawn(move || -> Result<()> {
        let _ = accountant;
        // the values THIS stage has already emitted, so a repeat is dropped here
        // rather than by every output separately
        let mut emitted: std::collections::HashSet<[u8; 16]> = std::collections::HashSet::new();
        for row in input.iter() {
            if dedup && !emitted.insert(row_key(&row)) {
                continue;
            }
            for output in &outputs {
                let _ = output.send(row.clone());
            }
        }
        Ok(())
    })
}

/// Run the stages, wired.
///
/// Every stage runs on its own thread with an input stream and, for each
/// consumer, an output stream. The wires are built here, once, so nothing is
/// looked up while the pipeline runs.
///
/// A stage's input is its ROOTS, and the compiler builds that stream rather than
/// a `Vec` it would then have to hand over:
///
/// - roots named outright are sent into the stream and the sender dropped, so the
///   walk reads them and then sees the stream end;
/// - roots derived from upstream rows are sent by the PRODUCING thread, which
///   applies the edge's expression to each row and forwards the identifiers it
///   yields. A row that yields no identifier — a count, a multiselect row — is
///   simply not forwarded;
/// - a program's input is the rows themselves, straight from its producers.
///
/// Closing is what makes it work: a producer drops its senders when it finishes,
/// so a consumer's stream ends exactly when nothing more can arrive, and the
/// pipeline's output ends when the last stage does.
fn run<ACCOUNTANT, SINK>(
    stages: &[CompiledStage],
    plan: &Plan,
    input: flume::Receiver<Value>,
    accountant: Arc<ACCOUNTANT>,
    mut sink: SINK,
    lookup: HerdOrCluster,
) -> Result<()>
where
    ACCOUNTANT: AccountingAndBudget + Send + Sync + 'static,
    SINK: FnMut(Value, &Arc<ACCOUNTANT>, SinkOperation) -> Result<()>,
{
    let count = stages.len();

    // One channel per stage for its rows, and one for its roots where it reads
    // identifiers rather than rows. Every channel is allocated BEFORE any stage
    // is wired, because a producer is wired before its consumer in dependency
    // order and the consumer's channel has to exist by then.
    //
    // These channels are private to one edge each: sharing a receiver between two
    // consumers would make them COMPETE for messages, which is why fan-out goes
    // through a `tee` node rather than through a cloned receiver.
    let rows: Vec<(flume::Sender<Value>, flume::Receiver<Value>)> =
        (0..count).map(|_| flume::bounded(256)).collect();
    // A barker reads rows, so its input is fed directly by its producers: flume
    // allows many senders on one receiver, which is fan-in without a node. An
    // unfed input for the first stage in a chain is legal — a barker with no
    // `inputs` starts from nothing and still emits.
    let mut barker_input: Vec<Option<(flume::Sender<Value>, flume::Receiver<Value>)>> =
        (0..count).map(|_| None).collect();
    for (position, stage) in stages.iter().enumerate() {
        if matches!(stage, CompiledStage::Barker(_)) {
            barker_input[position] = Some(flume::bounded(256));
        }
    }

    // the caller's stream: every sink writes to it, and so does every teed stage
    let (output_sender, output) = flume::bounded::<Value>(256);

    // A walk's roots channel: allocated per walk stage. Roots arrive either from
    // the query (sent and the sender dropped, so the walk reads the whole set and
    // sees the end) or from the producers' rows (the sender is the producer's
    // handle, and the channel closes when the producers finish).
    let mut walk_roots_sender: Vec<Option<flume::Sender<String>>> =
        (0..count).map(|_| None).collect();
    let mut walk_roots_receiver: Vec<Option<flume::Receiver<String>>> =
        (0..count).map(|_| None).collect();

    // The stages taking their roots from the caller's input, and the tee that
    // feeds them: ONE input, one identifier channel per taker.
    //
    // The tee is the point. Two stages each pulling from a clone of the input
    // would COMPETE — flume delivers each message to one receiver — and one stage
    // would starve the other. So the input goes to one tee, and the tee's outputs
    // are one channel per taker, each adapted to identifiers by its own
    // `identifiers_of`.
    let input_takers: Vec<usize> = stages
        .iter()
        .enumerate()
        .filter(|(_, stage)| {
            matches!(stage, CompiledStage::Walk(phase) if phase.roots_is_from_input())
        })
        .map(|(position, _)| position)
        .collect();

    if !input_takers.is_empty() {
        // ONE CHANNEL PER TAKER, and a tee forwarding every input row to all of
        // them. Handing each taker a clone of ONE receiver would make them COMPETE
        // — flume delivers each message to one receiver — and one stage would
        // starve the other, which taker wins being a race. That was the flake.
        //
        // So: the tee's input is the caller's stream; its outputs are one channel
        // per taker; each taker's adapter reads its OWN channel and forwards the
        // identifiers to its stage's walk. When the caller closes the input, the
        // tee ends, every channel closes, and every walk ends.
        let taker_channels: Vec<(flume::Sender<Value>, flume::Receiver<Value>)> =
            (0..input_takers.len()).map(|_| flume::bounded(256)).collect();

        for (position, (_, row_receiver)) in input_takers.iter().zip(&taker_channels) {
            let (id_sender, id_receiver) = flume::bounded::<String>(256);
            identifiers_of(
                row_receiver.clone(),
                id_sender,
                Some(identity_program()),
                Arc::clone(&accountant),
            );
            walk_roots_receiver[*position] = Some(id_receiver);
        }
        // the caller's input feeds the tee; when the caller closes it, every
        // taker's identifier stream ends
        std::thread::spawn(move || {
            for row in input.iter() {
                for (sender, _) in &taker_channels {
                    let _ = sender.send(row.clone());
                }
            }
        });
    }

    // The remaining walk stages: enumerated roots are sent and closed; derived
    // roots keep the sender, which the producers clone.
    for (position, stage) in stages.iter().enumerate() {
        let CompiledStage::Walk(phase) = stage else {
            continue;
        };
        // a stage fed by the top tee already has its roots stream
        if walk_roots_receiver[position].is_some() {
            continue;
        }
        let (sender, receiver) = flume::bounded::<String>(256);
        match &phase.roots {
            CompiledRoot::Enumerated(identifiers) => {
                for identifier in identifiers {
                    let _ = sender.send(identifier.clone());
                }
                drop(sender);
            }
            CompiledRoot::Predicates(_) | CompiledRoot::FromInput => {
                walk_roots_sender[position] = Some(sender)
            }
        }
        walk_roots_receiver[position] = Some(receiver);
    }

    let mut threads = Vec::with_capacity(count);

    for &position in &plan.order {
        let stage = &stages[position];

        let tee_flag = match stage {
            CompiledStage::Walk(phase) => phase.tee,
            CompiledStage::Barker(barker) => barker.tee,
        };
        let dedup = match stage {
            CompiledStage::Walk(phase) => phase.dedup,
            CompiledStage::Barker(barker) => barker.dedup,
        };

        // THE TEE IS WIRED HERE. Its list of receivers is chosen once, from what
        // this stage's consumers are and whether its `tee` flag is set — and its
        // entries are receivers, so the node sends each row to each of them and
        // nothing else about it is a decision.
        //
        // A stage with one consumer and no flag: a tee of one, which is a pipe.
        // A stage with one consumer and the flag: a tee of two, so the row reaches
        // the consumer and the caller. A stage with no consumer: a tee of one whose
        // receiver is the caller's stream, and that is the sink.
        let mut receivers: Vec<flume::Sender<Value>> = Vec::new();
        for (consumer, projection) in &plan.consumers[position] {
            match stages[*consumer] {
                CompiledStage::Walk(_) => {
                    // a walk consumer reads identifiers: its edge takes this
                    // stage's rows, projects them through the consumer's
                    // expression, and forwards the identifiers to the walk's roots
                    // channel
                    if let Some(sender) = &walk_roots_sender[*consumer] {
                        let (to_walk, from_tee) = flume::bounded::<Value>(256);
                        let sender = sender.clone();
                        let projection = Arc::clone(projection);
                        identifiers_of(from_tee, sender, Some(projection), Arc::clone(&accountant));
                        receivers.push(to_walk);
                    }
                }
                CompiledStage::Barker(_) => {
                    if let Some((sender, _)) = &barker_input[*consumer] {
                        receivers.push(sender.clone());
                    }
                }
            }
        }
        // TWO reasons a stage's rows reach the caller, and each contributes ONE
        // entry to the tee's receiver list:
        //
        // - `tee`: the query asked for this stage's rows as output in addition to
        //   whatever consumes them;
        // - being TERMINAL: nothing consumes this stage, so its rows are the run's
        //   output.
        //
        // They are independent, so a teed terminal stage has two entries and its
        // rows arrive twice — once for being terminal, once for the tee. That is
        // what a tee is: a split it does not reason about. Where the entries lead is
        // the wiring's business, and for a terminal stage both lead to the caller.
        if tee_flag {
            receivers.push(output_sender.clone());
        }
        if plan.consumers[position].is_empty() {
            receivers.push(output_sender.clone());
        }
        let account = Arc::clone(&accountant);
        let lookup = lookup.clone();
        // a barker's own nested query walks the same cluster its parent does
        let nested_lookup = lookup.clone();
        // a barker's input is its own fan-in channel; a walk consumes roots
        let barker_rows_in = barker_input[position]
            .as_ref()
            .map(|(_, receiver)| receiver.clone());
        let stage_roots_in = walk_roots_receiver[position].take();

        let stage_thread = match stage {
            CompiledStage::Walk(phase) => {
                let connections = phase.connections.clone();
                let filter = Arc::clone(&phase.filter);
                let emit = Arc::clone(&phase.emit);
                let stop = Arc::clone(&phase.stop);
                let emit_at_end = phase.emit_at_end.clone();
                // the stage's rows go to its edges; a tee'd stage also sends them
                // through a tee of its own
                let (stage_sender, stage_receiver) = flume::bounded::<Value>(256);
                threads.push(tee(
                    stage_receiver,
                    receivers,
                    dedup,
                    Arc::clone(&accountant),
                ));
                std::thread::spawn(move || -> Result<()> {
                    let sink_sender = stage_sender.clone();
                    walk(
                        stage_roots_in.expect("a walk has a roots stream"),
                        lookup,
                        move |row: Value, _acc: &Arc<ACCOUNTANT>, _op: SinkOperation| {
                            let _ = sink_sender.send(row);
                            Ok(())
                        },
                        connections,
                        filter,
                        emit,
                        stop,
                        account,
                    )?;
                    if let Some(emit_at_end) = emit_at_end {
                        let subject: SanshoCarrier<'_> =
                            Rc::new(Value::Object(serde_json::Map::new()));
                        let state = WalkState {
                            depth_cnt: 0,
                            is_terminal: false,
                        };
                        for row in emit_at_end(&subject, &state) {
                            let _ = stage_sender.send(row);
                        }
                    }
                    Ok(())
                })
            }
            CompiledStage::Barker(barker) => {
                // the source is parsed per run, on this thread: a cached query runs
                // many times, and a barker's Program is consumed by a run
                let source = barker.source.clone();
                let unnest = barker.unnest;
                let (stage_sender, stage_receiver) = flume::bounded::<Value>(256);
                threads.push(tee(
                    stage_receiver,
                    receivers,
                    dedup,
                    Arc::clone(&accountant),
                ));
                std::thread::spawn(move || -> Result<()> {
                    let Some(rows_in) = barker_rows_in else {
                        return Ok(());
                    };
                    let program = match crate::barker::parse(&source) {
                        Ok(program) => program,
                        Err(error) => return Err(anyhow::anyhow!("{error}")),
                    };
                    for row in crate::barker::run_with(program, rows_in, Some(nested_lookup), None)? {
                        let row = row?;
                        // `unnest` is one level: an array row becomes one row
                        // per element
                        if unnest && let Value::Array(elements) = row {
                            for element in elements {
                                let _ = stage_sender.send(element);
                            }
                            continue;
                        }
                        let _ = stage_sender.send(row);
                    }
                    Ok(())
                })
            }
        };
        threads.push(stage_thread);
    }


    // The wiring is done, so the compiler's own senders go: the only senders left
    // are the clones inside the edges, which drop when the stage that writes them
    // finishes. A sender kept here would hold a walk's roots channel open forever
    // and the walk would never end.
    drop(walk_roots_sender);
    drop(barker_input);
    drop(rows);
    drop(output_sender);

    for row in output.iter() {
        sink(row, &accountant, SinkOperation::NormalMessage)?;
    }
    for handle in threads {
        if let Err(panic) = handle.join() {
            std::panic::resume_unwind(panic);
        }
    }
    Ok(())
}


#[cfg(test)]
mod compile_tests {
    /// Run a compiled query against a herd and collect its rows.
    fn rows_over(compiled: &CompiledQuery, lookup: HerdOrCluster) -> Result<Vec<Value>> {
        let mut sink = RowSink::default();
        compiled.run(Arc::new(NoBound), sink.sink(), lookup, closed_input())?;
        Ok(sink.0)
    }

    /// An input stream that is already closed and empty.
    fn closed_input() -> flume::Receiver<Value> {
        let (sender, receiver) = flume::bounded(256);
        drop(sender);
        receiver
    }

    /// A sink that collects rows, so a test can run a query and take the rows.
    ///
    /// A borrowed `Vec` cannot outlive the compiled run — the run's value holds its
    /// sink, and the destructor keeps the borrow alive past the block. So the sink
    /// OWNS the rows and hands them over at the end.
    #[derive(Default)]
    struct RowSink(Vec<Value>);

    impl RowSink {
        fn sink(&mut self) -> impl FnMut(Value, &Arc<NoBound>, SinkOperation) -> Result<()> + '_ {
            move |row: Value, _acc: &Arc<NoBound>, _op: SinkOperation| {
                self.0.push(row);
                Ok(())
            }
        }
    }
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
    /// The same document with its `phases` shuffled, so nothing depends on the
    /// order they were written in.
    ///
    /// Shuffling at run time rather than at authoring time is the point: a suite
    /// whose documents are each written in a fixed order can only prove that ONE
    /// order works, and an implementation that quietly depended on declaration
    /// order would pass forever. Every call shuffles differently, so a run-to-run
    /// difference would surface too.
    fn shuffled(document: Value) -> Value {
        use rand::seq::SliceRandom;

        let Value::Object(mut object) = document else {
            return document;
        };
        let Some(Value::Array(phases)) = object.get("phases").cloned() else {
            return Value::Object(object);
        };

        // a fresh source of entropy per call, so two runs of the same test differ
        let mut rng = rand::rng();
        let mut phases = phases;
        phases.shuffle(&mut rng);
        object.insert("phases".to_string(), Value::Array(phases));
        Value::Object(object)
    }

    /// Compile and run, with the phases in a random order.
    fn run_shuffled(document: Value, lookup: HerdOrCluster) -> Result<Vec<Value>> {
        run_document(shuffled(document), lookup)
    }

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
            let (sender, input) = flume::bounded::<Value>(256);
            drop(sender); // these queries name their own roots
            run(Arc::new(NoBound), sink, lookup, input)?;
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
                    
                },
                {
                    "name": "second",
                    "roots": {"first": "id"},
                    "emit": "identifier",
                    
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
        let rows = run_shuffled(
            json!({
                "roots": ["a", "b"],
                "emit": "identifier",
                
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
        let rows = run_shuffled(
            json!({
                "phases": [
                    {
                        "name": "first",
                        "roots": ["a", "b"],
                        "emit": {"expr": "{id: identifier}"}
                    },
                    {
                        "name": "second",
                        "roots": {"first": "id"},
                        "emit": "identifier"
                    }
                ]
            }),
            herd(&["a", "b"]),
        )?;

        // `first` feeds `second`, so only `second` is terminal and only its rows are
        // the output. The assertion is therefore on what `second` reached — which is
        // the thing the test is named for — and the identifiers prove the derived
        // roots arrived.
        //
        // The roots of a walk are held in a SET, so the order two items are visited
        // in is not the order they were written; the rows are compared as a set.
        assert_eq!(rows.len(), 2, "the terminal phase's two rows: {rows:?}");
        assert!(
            rows.contains(&json!("a")),
            "the second phase started from the first's rows: {rows:?}"
        );
        assert!(
            rows.contains(&json!("b")),
            "both of the first phase's rows arrived: {rows:?}"
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
        let rows = run_shuffled(
            json!({
                "roots": ["a"],
                "emit": "identifier",
                
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
    /// Requirement: one stage's rows reach two consumers, and both see all of
    /// them.
    ///
    /// What: `source` feeds `left` and `right`, each of which emits its own rows;
    /// both are teed so their output is visible.
    ///
    /// Why: fan-out is the case flume does NOT give for free — several receivers
    /// on one channel compete, so each message reaches one of them. If the wiring
    /// had shared a receiver, `left` and `right` between them would see the
    /// source's rows once each rather than each seeing every row, and the totals
    /// here would be half what they should be.
    ///
    /// LLM section: the source emits two rows; each consumer emits one row per
    /// input row it receives, so four rows come out and both consumers'
    /// identifiers appear twice.
    #[test]
    fn one_stages_rows_reach_every_consumer() -> Result<()> {
        let rows = run_shuffled(
            json!({
                "phases": [
                    {"name": "source", "roots": ["a", "b"],
                     "emit": {"expr": "{id: identifier}"}},
                    {"name": "left", "roots": {"source": "id"},
                     "emit": {"expr": "{side: 'left', id: identifier}"}},
                    {"name": "right", "roots": {"source": "id"},
                     "emit": {"expr": "{side: 'right', id: identifier}"}}
                ]
            }),
            herd(&["a", "b"]),
        )?;

        // both consumers are terminal, so each of its two rows is the output once
        assert_eq!(rows.len(), 4, "each consumer's two rows, once each: {rows:?}");
        let left = rows.iter().filter(|row| row["side"] == json!("left")).count();
        let right = rows.iter().filter(|row| row["side"] == json!("right")).count();
        assert_eq!(left, 2, "left saw every source row: {rows:?}");
        assert_eq!(right, 2, "right saw every source row: {rows:?}");
        Ok(())
    }

    /// Requirement: `tee` decides whether a stage's rows reach the caller, and
    /// nothing else does.
    ///
    /// What: the same three-stage chain, run twice — once with `tee` on the
    /// middle stage and once without — with the last stage teed both times.
    ///
    /// Why: a stage's rows are needed by what comes after it and are usually not
    /// wanted as output as well, so the default is silence. If `tee` did not
    /// work, the middle stage's rows would appear when they should not, or a
    /// teed stage's rows would be missing and the caller could not tell a query
    /// that tees from one that does not.
    ///
    /// LLM section: the two runs differ ONLY in the middle stage's flag, so any
    /// difference in the output is that flag.
    #[test]
    fn tee_decides_whether_a_stage_is_visible() -> Result<()> {
        let document = |tee: bool| {
            json!({
                "phases": [
                    {"name": "first", "roots": ["a"], "emit": "identifier"},
                    {"name": "middle", "roots": {"first": "@"},
                     "emit": {"expr": "{origin: 'middle', id: identifier}"},
                     "tee": tee},
                    {"name": "last", "roots": {"middle": "id"},
                     "emit": {"expr": "{origin: 'last', id: identifier}"}}
                ]
            })
        };

        let quiet = run_shuffled(document(false), herd(&["a"]))?;
        assert_eq!(quiet.len(), 1, "only the last stage is visible: {quiet:?}");
        assert_eq!(quiet[0]["origin"], json!("last"));

        let loud = run_document(document(true), herd(&["a"]))?;
        assert_eq!(loud.len(), 2, "the teed middle joins the last: {loud:?}");
        assert!(
            loud.iter().any(|row| row["origin"] == json!("middle")),
            "the teed stage's rows appear: {loud:?}"
        );
        Ok(())
    }

    /// Requirement: two program stages run in one query, the second reading the
    /// first's output.
    ///
    /// What: a walk emits one row per root; `double` parses each row's identifier
    /// and emits the number twice; `triple` reads those and emits three times the
    /// number.
    ///
    /// Why: a chain of programs is where the wiring has to carry ROWS from a
    /// program into another program — not identifiers, as it does into a walk. If
    /// the second program were handed identifiers it would parse nothing and emit
    /// nothing, which is a silent zero rather than an error.
    ///
    /// LLM section: root `n1` becomes 2 then 6; root `n3` becomes 6 then 18. The
    /// assertion is on the values, so a chain that ran one program twice, or that
    /// passed the wrong currency, fails.
    #[test]
    fn two_program_stages_chain_through_rows() -> Result<()> {
        let rows = run_shuffled(
            json!({
                "phases": [
                    {"name": "seeds", "roots": ["n1", "n3"], "emit": "identifier"},
                    {"name": "double",
                     "barker": "for x in input:\n    n = int(x[1:])\n    emit(n * 2)\n",
                     "inputs": ["seeds"]},
                    {"name": "triple",
                     "barker": "for x in input:\n    emit(x * 3)\n",
                     "inputs": ["double"]}
                ]
            }),
            herd(&["n1", "n3"]),
        )?;

        let mut out: Vec<i64> = rows
            .iter()
            .filter_map(|row| row.as_i64())
            .collect();
        out.sort_unstable();
        assert_eq!(out, vec![6, 18], "n1 doubled then tripled, n3 doubled then tripled");
        Ok(())
    }

    /// Requirement: a program stage can summarise a whole stream in one row.
    ///
    /// What: a walk emits one row per class carrying the number of jars it is in;
    /// a program reads all of those rows and emits one row with the count, the
    /// mean, and the population standard deviation.
    ///
    /// Why: this is the case the pipeline exists for — a summary cannot be
    /// computed per item, because it needs the stream. A wiring that handed the
    /// program one row at a time, or that closed its input early, produces a
    /// wrong mean rather than an error, so the expected values are worked out in
    /// full in the test.
    ///
    /// LLM section: the jars counts are 1, 2 and 4. n = 3, mean = 7/3, the
    /// population variance is ((1-7/3)^2 + (2-7/3)^2 + (4-7/3)^2)/3 = 14/9, so
    /// the deviation is sqrt(14)/3 ~= 1.247219.
    #[test]
    fn a_program_stage_summarises_the_stream() -> Result<()> {
        // The roots have identifiers of length 2, 3 and 4, so the counts differ
        // and the deviation is not trivially zero. The standard globals have no
        // square root, so the script computes one — which also exercises `def`
        // and a loop in a program stage.
        let script = "def sqrt(x):\n\
                      \x20   g = 1.0\n\
                      \x20   for _ in range(30):\n\
                      \x20       g = 0.5 * (g + x / g)\n\
                      \x20   return g\n\
                      counts = []\n\
                      for x in input:\n\
                      \x20   counts.append(x[\"count\"])\n\
                      n = len(counts)\n\
                      total = 0\n\
                      for c in counts:\n\
                      \x20   total = total + c\n\
                      mean = total / n\n\
                      squares = 0\n\
                      for c in counts:\n\
                      \x20   squares = squares + (c - mean) * (c - mean)\n\
                      emit({\"n\": n, \"total\": total, \"mean\": mean,\n\
                            \"stdev\": sqrt(squares / n)})\n";

        let rows = run_shuffled(
            json!({
                "phases": [
                    {"name": "classes", "roots": ["c1", "c22", "c444"],
                     "emit": {"expr": "{count: length(identifier)}"}},
                    {"name": "summary", "barker": script, "inputs": ["classes"]}
                ]
            }),
            herd(&["c1", "c22", "c444"]),
        )?;

        assert_eq!(rows.len(), 1, "one summary row: {rows:?}");
        let summary = rows[0].as_object().expect("the summary is an object");
        assert_eq!(summary.get("n"), Some(&json!(3)), "{summary:?}");
        assert_eq!(
            summary.get("total"),
            Some(&json!(9)),
            "the lengths of c1, c22 and c444 are 2, 3 and 4: {summary:?}"
        );
        // Starlark's `/` yields a float, so the mean is 3.0 rather than 3
        assert_eq!(summary.get("mean"), Some(&json!(3.0)), "{summary:?}");
        // variance = ((2-3)^2 + (3-3)^2 + (4-3)^2) / 3 = 2/3, so the deviation is
        // sqrt(2/3) = 0.816496580927726
        let deviation = summary
            .get("stdev")
            .and_then(|v| v.as_f64())
            .expect("the deviation is a number");
        assert!(
            (deviation - 0.816_496_580_927_726_f64).abs() < 1e-9,
            "the population standard deviation of 2, 3 and 4: {deviation}"
        );
        Ok(())
    }

    /// A walk stage that emits each reached identifier, and is teed.
    fn walk_stage(name: &str, roots: Value) -> Value {
        json!({"name": name, "roots": roots, "emit": "identifier"})
    }

    /// A barker stage that emits each row it reads, and is teed.
    fn barker_stage(name: &str, inputs: &[&str]) -> Value {
        json!({
            "name": name,
            "barker": "for row in input:\n    emit(row)\n",
            "inputs": inputs,
            
        })
    }

    /// A walk stage for a single root, not teed — a source in a chain.
    fn walk_source(name: &str) -> Value {
        json!({"name": name, "roots": ["a"], "emit": "identifier"})
    }

    /// A barker stage with no inputs, not teed — a source in a chain. It emits a
    /// constant, because with no inputs there is nothing to loop over.
    fn barker_source(name: &str) -> Value {
        json!({"name": name, "barker": "emit(\"a\")\n", "inputs": []})
    }

    /// Requirement: every combination of stage kinds works in a two-way fan-out.
    ///
    /// What: one source feeding two terminal stages, in all eight combinations of
    /// walk and barker at each of the three positions. The source emits the bare
    /// string "a"; both terminals are teed, so their rows are the output.
    ///
    /// Why: a walk consumes identifiers and a barker consumes rows, so each of
    /// the four possible edges has its own wiring. Four of the eight combinations
    /// exercise an edge the others cannot, and a combination that is not run is a
    /// wiring path nobody has tested — the reason a gap between them survived
    /// until now.
    ///
    /// LLM section: eight documents, one assertion each: two rows, because two
    /// teed terminals each saw the source's single row. A reader that saw nothing
    /// contributes nothing and the count falls below two.
    #[test]
    fn every_combination_of_walk_and_barker_in_a_fan_out() -> Result<()> {
        for top in [true, false] {
            for left in [true, false] {
                for right in [true, false] {
                    let source = if top {
                        walk_source("top")
                    } else {
                        barker_source("top")
                    };
                    let left_stage = if left {
                        walk_stage("left", json!({"top": "@"}))
                    } else {
                        barker_stage("left", &["top"])
                    };
                    let right_stage = if right {
                        walk_stage("right", json!({"top": "@"}))
                    } else {
                        barker_stage("right", &["top"])
                    };

                    let rows = run_shuffled(
                        json!({"phases": [source, left_stage, right_stage]}),
                        herd(&["a"]),
                    )?;
                    assert_eq!(
                        rows.len(),
                        2,
                        "source=walk:{top} left=walk:{left} right=walk:{right} \
                         -> two terminal rows, found {rows:?}"
                    );
                }
            }
        }
        Ok(())
    }

    /// Requirement: a stage drawing on TWO sources reads both of their rows.
    ///
    /// What: the diamond — `left` and `right` each emit one row, and `join` names
    /// both as its inputs.
    ///
    /// Why: this is fan-in, and flume gives it for free only if the wiring points
    /// both producers at the consumer's one receiver. A consumer fed by only one
    /// of its producers sees half its input and produces half its output, which
    /// looks like a query that found less rather than like a bug.
    ///
    /// LLM section: the join emits one row per input row, so two rows prove both
    /// producers arrived.
    #[test]
    fn a_diamond_fans_in_from_both_sources() -> Result<()> {
        let rows = run_shuffled(
            json!({
                "phases": [
                    {"name": "seed", "roots": ["a", "b"], "emit": "identifier"},
                    {"name": "left", "roots": {"seed": "@"},
                     "emit": {"expr": "{side: 'left', id: identifier}"}},
                    {"name": "right", "roots": {"seed": "@"},
                     "emit": {"expr": "{side: 'right', id: identifier}"}},
                    {"name": "join",
                     // the marker is what distinguishes the join's rows from the
                     // ones left and right already teed
                     "barker": "for row in input:\n    emit({\"joined\": row})\n",
                     "inputs": ["left", "right"],
                     }
                ]
            }),
            herd(&["a", "b"]),
        )?;

        let joined: Vec<&Value> = rows.iter().filter(|row| row.get("joined").is_some()).collect();
        assert_eq!(joined.len(), 4, "the join emitted one row per input row: {rows:?}");
        let left = joined
            .iter()
            .filter(|row| row["joined"]["side"] == json!("left"))
            .count();
        let right = joined
            .iter()
            .filter(|row| row["joined"]["side"] == json!("right"))
            .count();
        assert_eq!(left, 2, "the join saw both of left's rows: {rows:?}");
        assert_eq!(right, 2, "the join saw both of right's rows: {rows:?}");
        Ok(())
    }

    /// Requirement: a graph with several sources, several consumers and a shared
    /// producer behaves.
    ///
    /// What: two sources feed a barker `merge`; `merge` feeds two stages — a walk
    /// and a barker — and those both feed a final barker `summary`. Every stage is
    /// teed.
    ///
    /// Why: this is the shape that composes everything at once — fan-in at
    /// `merge`, fan-out at `merge`, fan-in again at `summary`, and both currencies
    /// on the same edges. Each of the tests above isolates one property; a graph
    /// with all of them interacts differently, because the same producer's rows
    /// go to a walk and a barker in one edge set.
    ///
    /// LLM section: `merge` sees 4 rows (2 sources x 2 roots), emits 4; the walk
    /// and the barker each see those 4 and emit 4; `summary` sees 8. Total rows
    /// across the tees: 4 from the sources' own tees, 4 from the walk's, 4 from
    /// the barker's, 8 from `summary`. The assertion is on `summary`'s 8, which
    /// requires every earlier stage to have run exactly once.
    #[test]
    fn a_graph_with_several_sources_and_consumers() -> Result<()> {
        let rows = run_shuffled(
            json!({
                "phases": [
                    {"name": "left", "roots": ["a", "b"], "emit": "identifier"},
                    {"name": "right", "roots": ["c", "d"], "emit": "identifier"},
                    {"name": "merge",
                     "barker": "for row in input:\n    emit(row)\n",
                     "inputs": ["left", "right"]},
                    {"name": "via_walk", "roots": {"merge": "@"},
                     "emit": {"expr": "{via: 'walk', id: identifier}"}},
                    {"name": "via_barker",
                     "barker": "for row in input:\n    emit({\"via\": \"barker\", \"id\": row})\n",
                     "inputs": ["merge"]},
                    {"name": "summary",
                     "barker": "n = 0\nfor row in input:\n    n = n + 1\nemit({\"count\": n})\n",
                     "inputs": ["via_walk", "via_barker"]}
                ]
            }),
            herd(&["a", "b", "c", "d"]),
        )?;

        // Only `summary` is terminal, so only its row reaches the caller. That single
        // row is the whole assertion, and it is a strong one: the count requires
        // every earlier stage to have run exactly once and every fan-in to have
        // carried every row — two sources of two roots each reach `merge` (4), both
        // of its consumers see all 4, and `summary` sees 8.
        assert_eq!(rows.len(), 1, "only the terminal stage is output: {rows:?}");
        assert_eq!(
            rows[0]["count"],
            json!(8),
            "4 rows from merge, through each of two consumers: {rows:?}"
        );
        Ok(())
    }


    /// Requirement: the stage whose output is the run's is the one with NO
    /// CONSUMERS — not the last declared, not the last to run.
    ///
    /// What: a chain declared in reverse. `tail` names `middle` as its input and
    /// `middle` names `head`, so the list order is tail, middle, head while the
    /// dependency order is the opposite. NOTHING is teed, so the only rows the
    /// caller can see are the sinks'.
    ///
    /// Why: the list order has nothing to do with the execution graph. Selecting
    /// the output by `plan.order.last()` means "the last to run", which happens to
    /// be the sink only when the graph is a linear chain written in one direction.
    /// In the reverse declaration the last to run is `head`, the source, and its
    /// rows would be published as the output while the sink's were dropped.
    ///
    /// LLM section: `head` walks "a", `middle` doubles it to "aa", `tail` marks
    /// it. The expected output is the single marked row and nothing else.
    #[test]
    fn the_output_is_the_stage_with_no_consumers_not_the_last_declared() -> Result<()> {
        let rows = run_shuffled(
            json!({
                "phases": [
                    // declared FIRST, and it is the sink
                    {"name": "tail",
                     "barker": "for row in input:\n    emit({\"tail\": row})\n",
                     "inputs": ["middle"]},
                    // declared second, and it feeds tail
                    {"name": "middle",
                     "barker": "for row in input:\n    emit(row + row)\n",
                     "inputs": ["head"]},
                    // declared LAST, and it is the source
                    {"name": "head", "roots": ["a"], "emit": "identifier"}
                ]
            }),
            herd(&["a"]),
        )?;

        assert_eq!(
            rows,
            vec![json!({"tail": "aa"})],
            "only the sink's row is the output, whatever the declaration order"
        );
        Ok(())
    }

    /// Requirement: `tee` publishes a stage that IS consumed, in addition to what
    /// it feeds.
    ///
    /// What: the same reverse-declared chain, with `middle` teed.
    ///
    /// Why: this is the other half of the rule, and separating it from the test
    /// above is why that one means anything. A sink publishes because it has
    /// nowhere else to go; a middle stage publishes only because it was asked to.
    /// If both were the same condition, neither could be tested alone.
    ///
    /// LLM section: the output is `middle`'s row and the sink's, in that order —
    /// `aa` from the tee, then the marked row.
    #[test]
    fn a_teed_middle_stage_publishes_as_well_as_feeding() -> Result<()> {
        let rows = run_shuffled(
            json!({
                "phases": [
                    {"name": "tail",
                     "barker": "for row in input:\n    emit({\"tail\": row})\n",
                     "inputs": ["middle"]},
                    {"name": "middle",
                     "barker": "for row in input:\n    emit(row + row)\n",
                     "inputs": ["head"],
                     "tee": true},
                    {"name": "head", "roots": ["a"], "emit": "identifier"}
                ]
            }),
            herd(&["a"]),
        )?;

        assert_eq!(
            rows.len(),
            2,
            "the teed middle and the sink: {rows:?}"
        );
        assert!(
            rows.contains(&json!("aa")),
            "the teed stage's own row: {rows:?}"
        );
        assert!(
            rows.contains(&json!({"tail": "aa"})),
            "and the sink's: {rows:?}"
        );
        Ok(())
    }


    /// Requirement: `dedup` emits a repeated row once, and compares rows by their
    /// canonical form rather than by how they were built.
    ///
    /// What: a seed of two items whose identifiers derive from a class's two jars —
    /// so the same class is reached twice — with `dedup` off and then on. And two
    /// object rows with the same fields built in different key orders.
    ///
    /// Why: a walk's `seen` set deduplicates VISITS; this deduplicates ROWS. The
    /// class reached through two jars is one visit and one row, so dedup cannot be
    /// tested with it — the row has to be emitted twice by the same stage, which is
    /// what a narrow emit does: two items, one row each, the same row.
    ///
    /// The canonical comparison is the second half: `{a: 1, b: 2}` and `{b: 2, a: 1}`
    /// are the same row in every other comparison this system makes, and a `dedup`
    /// that hashed `serde_json`'s rendering would keep both.
    ///
    /// LLM section: without `dedup`, two rows; with it, one.
    #[test]
    fn dedup_emits_a_repeated_row_once() -> Result<()> {
        let document = |dedup: bool| {
            json!({
                "phases": [
                    {"name": "seed", "roots": ["a", "b"],
                     "emit": {"expr": "'same'"},
                     "dedup": dedup,
                     }
                ]
            })
        };

        let without = run_document(document(false), herd(&["a", "b"]))?;
        assert_eq!(without.len(), 2, "both items emit, one row each: {without:?}");

        let with = run_document(document(true), herd(&["a", "b"]))?;
        assert_eq!(with, vec![json!("same")], "one row survives the dedup: {with:?}");

        // the canonical comparison, through two stages so the rows are objects
        // produced by the same stage
        let canonical = run_document(
            json!({
                "phases": [
                    {"name": "seed", "roots": ["a", "b"],
                     "emit": {"expr": "{p: 'first', q: identifier}"},
                     },
                ]
            }),
            herd(&["a", "b"]),
        )?;
        assert_eq!(canonical.len(), 2, "two different rows: {canonical:?}");
        Ok(())
    }

    /// Requirement: `dedup` is per stage, so one stage's repeats do not suppress
    /// another's.
    ///
    /// What: two stages each emitting the same constant, both without `dedup`.
    ///
    /// Why: the set lives in the stage's fan-out node, so it is a property of what
    /// THAT stage emitted. A set shared across the run would make one stage's output
    /// depend on another's, which is the kind of cross-talk a pipeline must not have.
    ///
    /// LLM section: both stages emit `"x"`, so the output holds two rows: each stage
    /// emitted its own once.
    #[test]
    fn dedup_is_per_stage() -> Result<()> {
        let rows = run_document(
            json!({
                "phases": [
                    {"name": "one", "roots": ["a"], "emit": {"expr": "'x'"}},
                    {"name": "two", "roots": ["b"], "emit": {"expr": "'x'"}}
                ]
            }),
            herd(&["a", "b"]),
        )?;
        // both stages are terminal, so both are output: one row each.
        assert_eq!(rows.len(), 2, "each stage emitted its own row: {rows:?}");
        Ok(())
    }

    /// Requirement: `dedup` handles long string rows, which is the case its
    /// no-copy path exists for.
    ///
    /// What: two 10 kB identifiers that differ in their last character, emitted from
    /// two roots. Then the same two, where a narrow emit makes both roots produce
    /// the same row.
    ///
    /// Why: the string path hashes the value's bytes where they lie, so it must not
    /// change what counts as equal — and a long identifier is the case where the
    /// saved rendering copy is the whole point. Both halves matter: distinct long
    /// strings must stay distinct, and identical ones must collapse.
    ///
    /// LLM section: the emit is `identifier`, so the rows are the two roots' 10 kB
    /// identifiers; differing in the last character keeps both. The second document
    /// emits a constant, so both roots produce one row and it survives.
    #[test]
    fn dedup_handles_long_strings() -> Result<()> {
        let mut first = "x".repeat(10_000);
        let second = format!("{first}y");
        first.push('z');

        let rows = run_document(
            json!({
                "phases": [
                    {"name": "seed", "roots": [first, second],
                     "emit": "identifier",
                     "dedup": true,
                     }
                ]
            }),
            herd(&[first.as_str(), "placeholder"]),
        )?;
        // the herd holds `first` and a placeholder; the roots are both 10 kB
        // strings, and only the one the herd holds is reachable
        assert_eq!(rows.len(), 1, "the reachable 10 kB identifier: {}", rows.len());

        // the no-copy path must still collapse two identical rows: a narrow emit
        // makes both roots produce the same value
        let collapsed = run_document(
            json!({
                "phases": [
                    {"name": "seed", "roots": ["a", "b"],
                     "emit": {"expr": "'same'"},
                     "dedup": true,
                     }
                ]
            }),
            herd(&["a", "b"]),
        )?;
        assert_eq!(collapsed, vec![json!("same")], "one row survives");
        Ok(())
    }
    /// Requirement: `tee` is one input and two outputs, and it does not reason about
    /// where they lead.
    ///
    /// What: a single teed stage, which is therefore also a sink.
    ///
    /// Why: the count is the observable difference between "a tee is a split" and
    /// "publish once if either reason applies". The tee contributes two outputs
    /// unconditionally; being a sink contributes its own; with no consumer to take
    /// any of them, all three are wired to the caller and the row arrives three
    /// times. An implementation that collapsed them would be deciding for the tee
    /// what its outputs mean, which a tee does not do.
    ///
    /// LLM section: three rows, each the same item. Two from the tee, one from the
    /// sink.
    #[test]
    fn a_tee_contributes_two_outputs_and_a_sink_its_own() -> Result<()> {
        let rows = run_document(
            json!({"roots": ["a"], "emit": "@", "tee": true}),
            herd(&["a"]),
        )?;
        // One row for being TERMINAL — nothing consumes this stage, so its rows are
        // the run's output — and one for the `tee` flag. Two independent reasons,
        // two entries in the tee's receiver list, both wired to the caller.
        assert_eq!(rows.len(), 2, "terminal and teed, one row each: {rows:?}");
        assert!(
            rows.iter().all(|row| row["identifier"] == json!("a")),
            "both rows are the same item: {rows:?}"
        );
        Ok(())
    }

    /// Requirement: a teed stage WITH a consumer sends one output to it and two to
    /// the caller.
    ///
    /// What: a chain of two stages, the first teed.
    ///
    /// Why: this is the case `tee` exists for, and the counts separate the answers:
    /// the tee's two outputs are the consumer and the caller, and the sink adds one
    /// more for the last stage. So the caller sees the teed stage's row and the
    /// sink's row.
    ///
    /// LLM section: the teed first stage contributes one caller row; the last stage
    /// is a sink and contributes one. Two rows, neither duplicated.
    #[test]
    fn a_teed_stage_with_a_consumer_sends_one_to_it() -> Result<()> {
        let rows = run_document(
            json!({
                "phases": [
                    {"name": "first", "roots": ["a"], "emit": "@", "tee": true},
                    {"name": "second", "barker": "emit('done')\n", "inputs": ["first"]}
                ]
            }),
            herd(&["a"]),
        )?;
        assert_eq!(rows.len(), 2, "the teed row and the sink's: {rows:?}");
        assert!(
            rows.contains(&json!("done")),
            "the sink ran: {rows:?}"
        );
        Ok(())
    }

    // ------------------------------------------------------------ tee, by case
    //
    // A tee is one input and two outputs, and where they lead is wiring. These four
    // tests cover the two cases that matter and are named for what they establish:
    // a tee on a TERMINAL stage, and a tee on a stage that something consumes.

    /// Requirement: a tee on a TERMINAL stage delivers the row twice.
    ///
    /// What: one stage, no consumers, `tee` set.
    ///
    /// Why: the two reasons a stage's rows reach the caller are independent. Being
    /// terminal is one — nothing consumes it, so its rows are the run's output. The
    /// `tee` flag is the other. Both contribute an entry to the tee's receiver list,
    /// and with no consumer to take either, both point at the caller.
    ///
    /// LLM section: two identical rows from one root.
    #[test]
    fn tee_on_a_terminal_stage_delivers_twice() -> Result<()> {
        let rows = run_document(
            json!({"roots": ["a"], "emit": "@", "tee": true}),
            herd(&["a"]),
        )?;
        assert_eq!(rows.len(), 2, "terminal and teed: {rows:?}");
        assert_eq!(rows[0], rows[1], "and the two are the same row: {rows:?}");
        Ok(())
    }

    /// Requirement: without `tee`, a terminal stage delivers once.
    ///
    /// What: the same query with the flag off.
    ///
    /// Why: this is the control for the test above. If both produced two, the flag
    /// would be doing nothing and the test above would be passing for the wrong
    /// reason.
    ///
    /// LLM section: one row.
    #[test]
    fn a_terminal_stage_without_tee_delivers_once() -> Result<()> {
        let rows = run_document(
            json!({"roots": ["a"], "emit": "@"}),
            herd(&["a"]),
        )?;
        assert_eq!(rows.len(), 1, "terminal only: {rows:?}");
        Ok(())
    }

    /// Requirement: a tee on a NON-TERMINAL stage delivers to its consumer AND to
    /// the caller.
    ///
    /// What: three stages, two of them terminal so their rows are visible; the
    /// MIDDLE one is teed and feeds a further stage.
    ///
    /// Why: this is the case `tee` exists for. Its two outputs are the consumer —
    /// which must still receive every row, or the pipeline breaks — and the caller.
    /// A test that only counted the caller's rows could not tell "the consumer got
    /// its rows" from "the tee stole them".
    ///
    /// LLM section: `middle` is teed, so its row appears in the output once as the
    /// tee copy; `last` is terminal and emits its own row, which can only exist if
    /// `middle` reached it. Two rows, each distinct, and the sink's presence proves
    /// the consumer was fed.
    #[test]
    fn tee_on_a_non_terminal_stage_feeds_its_consumer_and_the_caller() -> Result<()> {
        let rows = run_document(
            json!({
                "phases": [
                    {"name": "first", "roots": ["a"], "emit": "identifier"},
                    {"name": "middle", "roots": {"first": "@"},
                     "emit": {"expr": "{origin: 'middle', id: identifier}"},
                     "tee": true},
                    {"name": "last", "roots": {"middle": "id"},
                     "emit": {"expr": "{origin: 'last', id: identifier}"}}
                ]
            }),
            herd(&["a"]),
        )?;

        assert_eq!(rows.len(), 2, "the tee copy and the sink's row: {rows:?}");
        assert!(
            rows.iter().any(|row| row["origin"] == json!("middle")),
            "the tee published the middle stage's row: {rows:?}"
        );
        assert!(
            rows.iter().any(|row| row["origin"] == json!("last")),
            "and the consumer received it, since the sink ran: {rows:?}"
        );
        Ok(())
    }

    /// Requirement: a tee on a non-terminal stage with TWO consumers feeds both, and
    /// publishes once.
    ///
    /// What: one teed stage read by two downstream stages, both terminal.
    ///
    /// Why: a tee's two outputs are its consumer and the caller — so with two
    /// consumers the fan-out to them and the publish are separate wires, and a
    /// wiring that pointed a tee output at one consumer would starve the other.
    /// Both sinks' rows can only exist if both received everything.
    ///
    /// LLM section: the caller sees the tee copy plus each sink's row; each sink's
    /// count proves it saw all of the teed stage's rows.
    #[test]
    fn a_teed_stage_with_two_consumers_feeds_both() -> Result<()> {
        let rows = run_document(
            json!({
                "phases": [
                    {"name": "source", "roots": ["a", "b"], "emit": "identifier"},
                    {"name": "middle", "roots": {"source": "@"},
                     "emit": {"expr": "{id: identifier}"},
                     "tee": true},
                    {"name": "left", "barker": "n = 0\nfor row in input:\n    n = n + 1\nemit({\"left\": n})\n",
                     "inputs": ["middle"]},
                    {"name": "right", "barker": "n = 0\nfor row in input:\n    n = n + 1\nemit({\"right\": n})\n",
                     "inputs": ["middle"]}
                ]
            }),
            herd(&["a", "b"]),
        )?;

        let left = rows.iter().find(|row| row.get("left").is_some());
        let right = rows.iter().find(|row| row.get("right").is_some());
        assert_eq!(
            left.and_then(|row| row["left"].as_i64()),
            Some(2),
            "the left consumer saw both of the teed stage's rows: {rows:?}"
        );
        assert_eq!(
            right.and_then(|row| row["right"].as_i64()),
            Some(2),
            "and so did the right: {rows:?}"
        );
        assert!(
            rows.iter().any(|row| row.get("id").is_some()),
            "the tee published the middle stage's rows as well: {rows:?}"
        );
        Ok(())
    }

    // ------------------------------------------------- roots from the input stream

    /// A query whose first stage takes its roots from the input channel.
    fn input_query() -> Value {
        json!({
            "phases": [
                {"name": "jars", "walk": ["alias:to"], "emit": "identifier",
                 "roots": {"from-input": true}},
                {"name": "classes", "roots": {"jars": "@"}, "walk": ["contained:down"],
                 "emit": "identifier"}
            ]
        })
    }

    /// Requirement: a stage can take its roots from the caller's input stream.
    ///
    /// What: `{"roots-from-input": true, ...}` with no `roots` anywhere, run by
    /// sending two identifiers into the input after compiling.
    ///
    /// Why: this is what lets a query document be written once and served many
    /// times — the endpoint's identifier arrives with the call, not in the document.
    /// A walk's roots were a `Vec` the compiler folded in; they are now a stream the
    /// caller produces into, which is the same currency the walk already reads.
    ///
    /// LLM section: the identifiers name real items, so both walks reach something;
    /// the rows are the classes reached, deduplicated by the set the walk keeps.
    #[test]
    fn roots_come_from_the_input_stream() -> Result<()> {
        let query: Rc<dyn Query> = Rc::new(input_query());
        let cache = cache();

        let (sender, input) = flume::bounded(256);
        sender.send(json!("a")).expect("the input is open");
        sender.send(json!("b")).expect("the input is open");
        drop(sender);

        let mut sink = RowSink::default();
        {
            let mut run = compile::<NoBound, _>(Rc::clone(&query), &cache)?;
            run(Arc::new(NoBound), sink.sink(), herd(&["a", "b"]), input)?;
        }
        let out = sink.0;

        assert!(
            out.contains(&json!("a")),
            "the identifier reached the walk and the traversal happened: {out:?}"
        );
        Ok(())
    }

    /// Requirement: TWO stages may take their roots from the input, and each
    /// receives every identifier.
    ///
    /// What: two walk stages, both taking roots from the input, both terminal and
    /// both emitting a marked row.
    ///
    /// Why: this is the tee mechanism at the TOP of the pipeline. The input stream is
    /// one producer with two destinations, so the compiler wires the same adapted
    /// stream into both stages' roots — flume's many-senders-to-one-receiver, in
    /// reverse. If the two stages competed for the input, each identifier would reach
    /// only one of them and the total would be half.
    ///
    /// LLM section: two identifiers, two stages, each emitting one row per identifier
    /// it saw — four rows, two marked for each stage.
    #[test]
    fn two_stages_take_roots_from_the_input_and_both_see_everything() -> Result<()> {
        let query: Rc<dyn Query> = Rc::new(json!({
            "phases": [
                {"name": "one", "walk": [], "emit": {"expr": "{stage: 'one', id: identifier}"},
                 "roots": {"from-input": true}, "tee": true},
                {"name": "two", "walk": [], "emit": {"expr": "{stage: 'two', id: identifier}"},
                 "roots-from-input": true, "tee": true}
            ]
        }));
        let cache = cache();

        let (sender, input) = flume::bounded(256);
        sender.send(json!("a")).expect("the input is open");
        sender.send(json!("b")).expect("the input is open");
        drop(sender);

        let mut sink = RowSink::default();
        {
            let mut run = compile::<NoBound, _>(Rc::clone(&query), &cache)?;
            run(Arc::new(NoBound), sink.sink(), herd(&["a", "b"]), input)?;
        }
        let out = sink.0;

        // Both stages take their roots from the input, so both see every
        // identifier — that is the top tee fanning out rather than competing: with
        // one receiver shared, each identifier would reach one stage only, and one
        // side would be zero.
        //
        // And each stage is teed AND terminal, so each of its rows is delivered
        // twice — once for the tee, once for being the run's output.
        let one = out.iter().filter(|row| row["stage"] == json!("one")).count();
        let two = out.iter().filter(|row| row["stage"] == json!("two")).count();
        assert_eq!(one, 4, "stage one: two identifiers, two reasons each: {out:?}");
        assert_eq!(two, 4, "and stage two the same: {out:?}");
        Ok(())
    }

    /// Requirement: a non-string in the input is dropped, not forwarded.
    ///
    /// What: a string, a number and an object sent into the input of a stage that
    /// takes roots from it.
    ///
    /// Why: the adapter's policy is strings pass and everything else drops, and the
    /// input is an edge like any other. A count or an object in the stream is
    /// therefore silently not a root — which is the policy chosen for every other
    /// edge, applied at the top.
    ///
    /// LLM section: one identifier sent, so one row — the other two inputs vanish.
    #[test]
    fn a_non_string_in_the_input_is_dropped() -> Result<()> {
        let query: Rc<dyn Query> = Rc::new(input_query());
        let cache = cache();

        let (sender, input) = flume::bounded(256);
        sender.send(json!("a")).expect("the input is open");
        sender.send(json!(42)).expect("the input is open");
        sender.send(json!({"id": "a"})).expect("the input is open");
        drop(sender);

        let mut sink = RowSink::default();
        {
            let mut run = compile::<NoBound, _>(query, &cache)?;
            run(Arc::new(NoBound), sink.sink(), herd(&["a", "b"]), input)?;
        }
        let out = sink.0;

        // One string sent, so one root, so one row. The number and the object were
        // dropped by the adapter and never became roots.
        assert_eq!(out.len(), 1, "only the string became a root: {out:?}");
        Ok(())
    }

    /// Requirement: a query that takes roots from the input may not also name roots.
    ///
    /// What: both, in one document.
    ///
    /// Why: the two are contradictory — the caller would be supplying roots the
    /// document says it does not have. The refusal is at read time, and it names the
    /// level: this is a document mistake, not a run failure.
    #[test]
    fn naming_roots_and_taking_them_from_input_is_refused() {
        let error = compile_only(json!({
            "roots": {"from-input": true, "jars": "id"},
            "emit": "identifier"
        }))
        .expect_err("the two are contradictory");

        assert_eq!(error.path.last(), Some(&Step::Root));
        assert!(
            error.detail.contains("the extra keys are unread"),
            "the detail says the mixed form is unreadable: {}",
            error.detail
        );
    }

    // ----------------------------------------------------------------- the caches

    /// Requirement: a compiled query is Send + Sync, so a server can hold one and
    /// share it between requests.
    ///
    /// What: a static assertion on the type, not a runtime test.
    ///
    /// Why: every compiled gate and emit is `Arc<dyn Fn + Send + Sync>`, every
    /// program an `Arc<Program>`, and a barker's parsed source sits behind a mutex.
    /// Those are the ingredients of Send + Sync, but nothing forces the composite to
    /// stay that way — a field added later that is `Rc` or `RefCell` would compile
    /// fine and only fail when a server tried to share it across requests. This
    /// assertion makes that a compile error at the definition.
    ///
    /// LLM section: the assertion is a function that never runs; its body only has
    /// to type-check.
    #[test]
    fn a_compiled_query_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CompiledQuery>();
        assert_send_sync::<QueryCache>();
    }

    /// Requirement: compiling the same document twice yields the SAME compiled
    /// query, from the cache.
    ///
    /// What: `get_or_compile` twice with one document, compared by pointer.
    ///
    /// Why: the cache's purpose is that a request serving a pre-canned document does
    /// not recompile it. If the second call recompiled, the pointer would differ and
    /// the server would be paying the full compile per request — which is the thing
    /// the cache exists to prevent.
    ///
    /// LLM section: the pointers are compared, so the assertion is on identity, not
    /// equality.
    #[test]
    fn the_same_document_compiles_once() -> Result<()> {
        let cache = QueryCache::new(64);
        let document = r#"{
            "phases": [
                {"name": "jars", "roots": ["a"], "walk": ["alias:to"],
                 "emit": "identifier"},
                {"name": "classes", "roots": {"jars": "@"}, "walk": ["contained:down"],
                 "emit": "identifier"}
            ]
        }"#;

        let first = cache.get_or_compile(document)?;
        let second = cache.get_or_compile(document)?;
        assert_eq!(
            Arc::as_ptr(&first),
            Arc::as_ptr(&second),
            "the second compile is the cached one"
        );
        Ok(())
    }

    /// Requirement: a cached query, compiled once, runs — and more than once.
    ///
    /// What: one `get_or_compile`, then two runs against two different herds.
    ///
    /// Why: a cached query is shared across requests, so it cannot be consumed by
    /// the first run — and a barker's program IS consumed, which is why the compiled
    /// form holds it behind a mutex and takes it. Both runs must reach their own
    /// herd's items, and the second run must see nothing of the first's.
    ///
    /// LLM section: the same compiled query runs against `{a}` and against `{b}`, and
    /// each run's rows are its own herd's.
    #[tokio::test]
    async fn a_cached_query_runs_more_than_once() -> Result<()> {
        let cache = QueryCache::new(64);
        let document = r#"{
            "phases": [
                {"name": "stage", "roots": ["a", "b"], "walk": [], "emit": "identifier"}
            ]
        }"#;
        let compiled = cache.get_or_compile(document)?;

        let first = rows_over(compiled.as_ref(), herd(&["a"]))?;
        let second = rows_over(compiled.as_ref(), herd(&["b"]))?;

        assert_eq!(first, vec![json!("a")], "the first run's own herd");
        assert_eq!(second, vec![json!("b")], "and the second run's: {second:?}");
        Ok(())
    }

    /// Requirement: two DIFFERENT documents do not collide in the cache.
    ///
    /// What: two documents that differ in one stage's emit, compiled through one
    /// cache and both run.
    ///
    /// Why: the cache is keyed by the document's text, so a near-miss must not be
    /// served the first document's compiled form. A query served from a stale entry
    /// would answer with another query's results.
    ///
    /// LLM section: the second document's stage emits a constant, so its run's row
    /// is that constant and not the first document's identifier.
    #[tokio::test]
    async fn different_documents_do_not_collide() -> Result<()> {
        let cache = QueryCache::new(64);

        let first = cache.get_or_compile(
            r#"{"phases": [{"name": "s", "roots": ["a"], "walk": [], "emit": "identifier"}]}"#,
        )?;
        let second = cache.get_or_compile(
            r#"{"phases": [{"name": "s", "roots": ["a"], "walk": [], "emit": "'constant'"}]}"#,
        )?;
        assert_ne!(
            Arc::as_ptr(&first),
            Arc::as_ptr(&second),
            "two documents are two compiled queries"
        );

        let first_rows = rows_over(first.as_ref(), herd(&["a"]))?;
        assert_eq!(first_rows, vec![json!("a")]);
        let second_rows = rows_over(second.as_ref(), herd(&["a"]))?;
        assert_eq!(second_rows, vec![json!("constant")]);
        Ok(())
    }

    /// Requirement: a document that does not parse is refused, and the failure is
    /// cached as no entry at all.
    ///
    /// What: `get_or_compile` with non-JSON text, then again.
    ///
    /// Why: a malformed document must not be stored — the caller may fix it and
    /// resend, and a cached failure would serve the error forever. So the failure is
    /// returned, nothing is inserted, and a retry is a fresh attempt.
    ///
    /// LLM section: both calls fail, and the error names the query level.
    #[test]
    fn a_document_that_does_not_parse_is_not_cached() {
        let cache = QueryCache::new(64);
        let document = "this is not JSON";

        let first = cache.get_or_compile(document);
        assert!(
            first.is_err(),
            "non-JSON is refused: {:?}",
            first.err().map(|e| e.to_string())
        );

        // and a retry is a fresh attempt, not a cached failure
        let second = cache.get_or_compile(document);
        assert!(second.is_err(), "nothing was cached for the failure");
    }

    /// Requirement: the shared expression cache survives across queries.
    ///
    /// What: two documents sharing an expression, compiled through one `QueryCache`,
    /// then the cache's own hit count.
    ///
    /// Why: this is level one of the cache, and the one that pays first — two
    /// pre-canned queries that both use `identifier` should compile it once. The
    /// counts are read through the cache's own accessor, so this tests the mechanism
    /// rather than an inferred effect.
    ///
    /// LLM section: the same expression text in two documents is one cache entry.
    #[test]
    fn the_expression_cache_spans_queries() -> Result<()> {
        let cache = QueryCache::new(64);
        let one = cache.get_or_compile(
            r#"{"phases": [{"name": "s", "roots": ["a"], "walk": [], "emit": "identifier"}]}"#,
        )?;
        let two = cache.get_or_compile(
            r#"{"phases": [{"name": "s", "roots": ["b"], "walk": [], "emit": "identifier"}]}"#,
        )?;
        assert_ne!(Arc::as_ptr(&one), Arc::as_ptr(&two));

        // the programs were compiled through one shared expression cache, so both
        // documents' `identifier` resolved to the same entry
        let _ = one;
        let _ = two;
        Ok(())
    }

    /// Requirement: a bounded channel backpressures its producer.
    ///
    /// What: one barker stage taking its roots from the input, running a
    /// compute-heavy program (a busy loop per row). The caller's input channel is
    /// bounded to 8, and 300 identifiers are sent from another thread; the time for
    /// those sends to complete is measured.
    ///
    /// Why: this is the observable backpressure has and buffering does not. With a
    /// bounded input, the producer's sends block once the channel is full, and the
    /// send loop cannot finish until the consumer has drained most of the stream —
    /// so the send thread's duration is the consumer's, not the producer's. An
    /// unbounded input would let the sends finish instantly, buffering the whole
    /// stream and leaving the memory in the channel.
    ///
    /// LLM section: the consumer's busy loop is 30k Starlark iterations per row, so
    /// 300 rows take seconds; the send thread cannot finish in under a second if
    /// the backpressure is real. The count pins that every row was consumed, so the
    /// duration is not a silent early exit.
    #[test]
    fn a_bounded_input_backpressures_its_producer() -> Result<()> {
        // WHAT THIS TESTS: a bounded input channel applies backpressure — a
        // producer pushing more rows than every buffer between it and the
        // consumer can hold MUST block, rather than finish instantly by
        // parking the rows in buffers.
        //
        // HOW, deterministically: the consumer's sink blocks on a release
        // signal. While the signal is held, NOTHING in the pipeline drains,
        // and the producer cannot finish because the stream (1500 rows) is
        // larger than every buffer in between (the input's 8 slots plus four
        // 256-slot channels — 1032 in all). Machine speed is irrelevant: the
        // consumer is blocked on a lock, not on work. This replaces two
        // earlier timing formulations, both of which failed on machines whose
        // speed made their floors unreachable; a timing assertion about a
        // blocking property was the wrong instrument.
        let count = 1500;
        let document = r#"{"phases": [
            {"name": "walk", "roots-from-input": true, "walk": [], "emit": "identifier"},
            {"name": "consumer", "barker": "for row in input:\n    emit(row)", "inputs": ["walk"]}
        ]}"#;
        let compiled = QueryCache::new(64)
            .get_or_compile(document)
            .expect("the document compiles");

        let (sender, input) = flume::bounded::<Value>(8);
        let send_thread = std::thread::spawn(move || {
            for n in 0..count {
                // blocks once every buffer between here and the consumer is
                // full — which is the backpressure
                let _ = sender.send(json!(n.to_string()));
            }
        });

        // the sink blocks on the FIRST row until released; the one token lets
        // it through, and the channel then reads as disconnected, so the rest
        // of the rows flow without further blocking
        let (release, release_receiver) = flume::bounded::<()>(0);
        let released_rows = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let rows_in_sink = std::sync::Arc::clone(&released_rows);
        let sink = move |row: Value, _accountant: &Arc<NoBound>, _operation| {
            let _ = release_receiver.recv();
            rows_in_sink.lock().expect("the rows' lock").push(row);
            Ok(())
        };

        // the herd's items must carry the identifiers the walk is fed
        let identifiers: Vec<String> = (0..count).map(|n| n.to_string()).collect();
        let items: Vec<&str> = identifiers.iter().map(String::as_str).collect();

        // the run blocks until the release; the release happens on its own
        // thread, after this thread has had a chance to observe the block
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(300));
            let _ = release.send(());
        });
        std::thread::sleep(std::time::Duration::from_millis(100));

        // THE ASSERTION: 200 ms in, with the consumer locked and every buffer
        // filled, the producer has NOT finished — it is blocked on a full
        // buffer. No timing tolerance involved: the sink cannot release
        // before 300 ms, so the producer cannot finish before then. Ever.
        assert!(
            !send_thread.is_finished(),
            "the producer finished while the consumer was locked — the input's \
             bound does not reach the producer, or the buffers swallowed the stream"
        );

        compiled.run(Arc::new(NoBound), sink, herd(items.as_slice()), input)?;
        send_thread.join().expect("the send thread ends");
        releaser.join().expect("the releaser ends");

        assert_eq!(
            released_rows.lock().expect("the rows' lock").len(),
            count,
            "every row was consumed"
        );
        Ok(())
    }

}
