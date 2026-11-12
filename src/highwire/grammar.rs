//! The walk query's grammar, as one trait per semantic level.
//!
//! A walk query is not JSON. JSON is the first way a query happens to arrive,
//! and the shapes it uses — objects with named keys, arrays, strings — are
//! facts about JSON and about nothing else. What the grammar has is LEVELS, and
//! each level means something:
//!
//! ```text
//! Query
//!  └── phases: Vec<Rc<dyn Walk>>
//!       ├── name: Option<String>
//!       ├── roots: Vec<Rc<dyn Root>>
//!       ├── walk: Vec<Rc<dyn WalkConnection>>
//!       ├── filter: Option<Rc<dyn ItemFilter>>
//!       ├── emit: Rc<dyn ItemEmit>
//!       ├── stop: Option<Rc<dyn ItemStop>>
//!       ├── tee: bool
//!       └── emit_at_end: Option<Rc<dyn ItemEmit>>
//!
//! Root           — where a walk begins
//! WalkConnection — one connection type to traverse, and its gate
//! ItemFilter     — the gate an item must pass
//! ItemEmit       — the rows an item produces
//! EmitEntry      — one expression, its gate, its row rules
//! ItemStop       — the rules that end the walk
//! StopRule       — a gate, and what it reports when it fires
//! ExpressionBuilder — a gate, and the expressions a gate contains
//! ```
//!
//! Each trait expresses ITS level's semantic and nothing about the levels below
//! it. A level that owns sub-levels hands them back as `Rc<dyn _>` — behaviour,
//! not layout — so an implementation answers what its level means for whatever
//! data structure it is, and no implementation is ever required to agree on a
//! struct's fields.
//!
//! A query that is a single walk — the `{"walk": …}` form — answers `phases`
//! with itself, one element. A query that names phases answers with them, in
//! order. Nothing above the trait can tell the two apart.
//!
//! # What is deliberately absent
//!
//! Nothing here mentions the evaluation carrier. A walk query is configuration:
//! it is read once, before any item is looked at. Carriers appear in the
//! closures these levels PRODUCE, never in the document they are read from.

use std::rc::Rc;

use crate::highwire::walk::ExpressionBuilder;

/// What a level could not do, and where in the tree it was reading.
///
/// A level reader is handed a node and nothing else. It cannot know which phase
/// the node belongs to, nor which entry of which emit led to it — but it CAN
/// know what it was reading, and that is the step it reports. Each caller one
/// step up prepends its own step, so a path that comes out of a nested failure
/// reads from the outside in:
///
/// ```text
/// the query -> phase `classes` -> its emit -> entry 1 -> the gate
/// ```
///
/// The path is built by the code that descended, not reconstructed from a
/// message, which is what makes it right rather than merely plausible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrammarError {
    /// The route from the outermost reader to the node that failed. The last
    /// step is the reader that found the failure; the earlier steps are the
    /// readers that led to it.
    pub path: Vec<Step>,

    /// What was wrong, in the words of the level that found it.
    pub detail: String,
}

impl GrammarError {
    /// A failure at one step.
    pub fn at(step: Step, detail: impl Into<String>) -> Self {
        GrammarError {
            path: vec![step],
            detail: detail.into(),
        }
    }

    /// The same failure, one step further out.
    ///
    /// This is how a caller locates a failure it received: the caller knows the
    /// step it took to reach the reader that failed, and nothing below knows it.
    pub fn under(mut self, step: Step) -> Self {
        self.path.insert(0, step);
        self
    }
}

impl std::fmt::Display for GrammarError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("at ")?;
        let mut steps = self.path.iter();
        if let Some(first) = steps.next() {
            write!(f, "{first}")?;
        }
        for step in steps {
            write!(f, " -> {step}")?;
        }
        write!(f, ": {}", self.detail)
    }
}

impl std::error::Error for GrammarError {}

/// One step of the route to a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// The query's own structure: its phases, and the shape of their graph.
    Query,
    /// A phase, by name when it has one and by position otherwise.
    Phase { name: Option<String>, position: usize },
    /// The phase's name.
    Name,
    /// The phase's roots.
    Root,
    /// The nth predicate of a derived root.
    Predicate(usize),
    /// The phase's walk list.
    Connections,
    /// The nth connection of that list.
    Connection(usize),
    /// The gate on a connection.
    ConnectionGate,
    /// The phase's filter.
    Filter,
    /// The phase's emit.
    Emit,
    /// The gate on an emit.
    EmitGate,
    /// The nth entry of an emit.
    Entry(usize),
    /// A sansho expression.
    Expression,
    /// A gate.
    Gate,
    /// The phase's stop.
    Stop,
    /// The nth hard rule.
    Hard(usize),
    /// The nth soft rule.
    Soft(usize),
    /// A rule's gate.
    RuleGate,
    /// A rule's report.
    Report,
    /// The phase's tee.
    Tee,
    /// The phase's emit_at_end.
    EmitAtEnd,
}

impl std::fmt::Display for Step {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Step::Query => f.write_str("the query"),
            Step::Phase { name, position } => match name {
                Some(name) => write!(f, "phase `{name}`"),
                None => write!(f, "the phase at position {position}"),
            },
            Step::Name => f.write_str("its name"),
            Step::Root => f.write_str("its root"),
            Step::Predicate(index) => write!(f, "predicate {index}"),
            Step::Connections => f.write_str("its walk list"),
            Step::Connection(index) => write!(f, "connection {index}"),
            Step::ConnectionGate => f.write_str("the connection's gate"),
            Step::Filter => f.write_str("its filter"),
            Step::Emit => f.write_str("its emit"),
            Step::EmitGate => f.write_str("the emit's gate"),
            Step::Entry(index) => write!(f, "entry {index}"),
            Step::Expression => f.write_str("the expression"),
            Step::Gate => f.write_str("the gate"),
            Step::Stop => f.write_str("its stop"),
            Step::Hard(index) => write!(f, "hard rule {index}"),
            Step::Soft(index) => write!(f, "soft rule {index}"),
            Step::RuleGate => f.write_str("the rule's gate"),
            Step::Report => f.write_str("the rule's report"),
            Step::Tee => f.write_str("its tee"),
            Step::EmitAtEnd => f.write_str("its emit_at_end"),
        }
    }
}

/// The result of reading a level.
pub type LevelResult<T> = std::result::Result<T, GrammarError>;

/// One stage of a query: a graph traversal, or a program.
///
/// The two have the same SHAPE — rows or constants in, rows out — and different
/// guarantees: a walk is bounded by the graph and deterministic, a program is
/// computed code bounded only by its limits. So the trait says which one a stage
/// is, and hands back the one it is; asking for the other is a failure located
/// like any other mis-read.
///
/// `kind` exists so a caller never has to ask the wrong accessor: a scheduler
/// that must not trust a non-deterministic stage can see what it has without
/// running it.
pub trait Stage: ExpressionBuilder {
    /// Which kind of stage this is.
    fn kind(&self) -> LevelResult<StageKind>;

    /// The name other stages refer to this one by, when it has one.
    fn name(&self) -> LevelResult<Option<String>>;

    /// This stage as a walk, or a failure when it is not one.
    fn walk(&self) -> LevelResult<Rc<dyn Walk>>;

    /// This stage as a program, or a failure when it is not one.
    fn barker(&self) -> LevelResult<Rc<dyn Barker>>;
}

/// Which of the two kinds of stage a stage is.
#[derive(Debug)]
pub enum StageKind {
    /// A graph traversal.
    Walk,

    /// A program.
    Barker,
}

/// A program, as a stage of a query.
///
/// It differs from a walk in what it consumes: a walk starts from identifiers it
/// names or derives, while a program reads the ROWS of the stages named in
/// `inputs` and emits rows of its own. The currency on both sides is the same —
/// rows — which is what lets a program stand between two walks without a
/// conversion between them.
///
/// There is no "at the end" level here: a program runs once, so anything that
/// should happen after its last row is the last lines of the program.
pub trait Barker {
    /// The upstream stages whose rows this program reads, in the order it reads
    /// them. Empty is legal: a program that needs no input still emits.
    fn inputs(&self) -> LevelResult<Vec<String>>;

    /// The program's source.
    fn program(&self) -> LevelResult<&str>;

    /// Whether this stage's rows also go to the caller, or only to the stages
    /// downstream of it. Same meaning as a walk's `tee`: the middle of a
    /// pipeline is usually not wanted as output as well as consumed.
    fn tee(&self) -> LevelResult<bool>;

    /// Whether an array a row evaluates to produces one row per element.
    ///
    /// Same meaning as a walk's `unnest`: one row per element, ONE level only. A
    /// program that emits lists is usually emitting them because each element is
    /// a row.
    fn unnest(&self) -> LevelResult<bool>;

    /// Whether repeated rows are emitted once.
    ///
    /// Same meaning as a walk's `dedup`: a value this stage has already emitted is
    /// not emitted again.
    fn dedup(&self) -> LevelResult<bool>;
}

/// The query level: the top of a walk query.
///
/// The only level that is not owned by another. It exists so that "one stage"
/// and "a chain of stages" are the same thing to everything downstream: a
/// document that is a single stage is a query of one phase.
pub trait Query {
    /// The stages this query runs, in order.
    ///
    /// A query that names phases returns them. A query that is a single stage
    /// returns itself, as one element.
    fn phases(&self) -> LevelResult<Vec<Rc<dyn Stage>>>;
}

/// A walk: one rooted traversal, and the phases applied to the items it reaches.
pub trait Walk {
    /// The name this walk is referred to by, when it has one.
    ///
    /// A name is required for any walk that another walk draws its roots from,
    /// because that is how the reference is written. A single-walk query needs
    /// none.
    fn name(&self) -> LevelResult<Option<String>>;

    /// Where this walk begins.
    /// Where this walk starts, or `None` when it takes its roots from the
    /// query's input stream.
    fn root(&self) -> LevelResult<Option<Rc<dyn Root>>>;

    /// The connection types this walk traverses, each with the gate that admits
    /// an edge of that type.
    fn connections(&self) -> LevelResult<Vec<Rc<dyn WalkConnection>>>;

    /// The gate an item must pass to be processed, when this walk has one.
    ///
    /// This gates TRAVERSAL as well as emission: an item that fails it has its
    /// edges left unfollowed, so this level decides which subtrees are walked.
    fn filter(&self) -> LevelResult<Option<Rc<dyn ItemFilter>>>;

    /// The rows an item produces. A walk always emits something.
    fn emit(&self) -> LevelResult<Rc<dyn ItemEmit>>;

    /// The rules that end this walk, when it has any.
    fn stop(&self) -> LevelResult<Option<Rc<dyn ItemStop>>>;

    /// Whether this walk's rows also go to the collector, or only to the walks
    /// downstream of it.
    fn tee(&self) -> LevelResult<bool>;

    /// Whether repeated rows are emitted once.
    ///
    /// A row this stage has already emitted is not emitted again, compared by a
    /// 16-byte BLAKE3 digest of the row's canonical form. Intended for streams of
    /// identifiers, where "the same row" is unambiguous: a purl shared by two
    /// visited items appears once. For rows that are objects the comparison is on
    /// the canonical rendering, so two objects with equal fields dedupe whatever
    /// order they were built in.
    fn dedup(&self) -> LevelResult<bool>;

    /// What this walk emits once, after it finishes, over the statistics it
    /// gathered. Same semantics as `emit`, a different subject.
    fn emit_at_end(&self) -> LevelResult<Option<Rc<dyn ItemEmit>>>;
}

/// One connection type to traverse, with the gate that admits an edge of it.
///
/// The gate is SYNTAX — an expression — and never a compiled predicate. A node
/// that carried a compiled gate would make reading the walk the act of
/// preparing to run it: whoever builds the node would have to know how a gate
/// is compiled, and the walk could not be read at all without that machinery
/// present. Compiling a gate belongs to whoever runs the walk.
pub trait WalkConnection {
    /// The connection type, as the corpus names it — `alias:to`,
    /// `contained:down`, `tag:from`.
    fn connection_type(&self) -> LevelResult<String>;

    /// The gate over an edge of that type, or `None` when the connection names
    /// no gate, in which case every edge of the type is admitted.
    ///
    /// The leaves of this expression are the connection-name predicates — the
    /// `start:`, `end:` and `regex:` forms — and comparisons against the depth
    /// an edge would be followed at. What those leaves MEAN is the connection
    /// gate's business, not this level's.
    fn gate(&self) -> LevelResult<Option<Rc<dyn ExpressionBuilder>>>;
}

/// A root: where a walk begins.
///
/// A root is exactly one of two things, and they are not the same type:
///
/// - ENUMERATED: the identifiers are named outright;
/// - PREDICATES: the identifiers are derived from the rows of the walks
///   upstream of this one, through predicates.
///
/// The two are not two optional answers to one question. A root is one or the
/// other, and [`Root::kind`] is what says which — so a caller reads one method
/// to learn which accessor is meaningful, rather than checking two and hoping
/// they disagree.
pub trait Root {
    /// Which of the two kinds this root is.
    fn kind(&self) -> LevelResult<RootKind>;

    /// The identifiers this root names outright.
    ///
    /// Meaningful only when [`Root::kind`] answers [`RootKind::Enumerated`].
    fn identifiers(&self) -> LevelResult<Vec<String>>;

    /// The predicates that derive this root's identifiers.
    ///
    /// Meaningful only when [`Root::kind`] answers [`RootKind::Predicates`].
    /// More than one predicate is a union.
    fn predicates(&self) -> LevelResult<Vec<Rc<dyn RootPredicate>>>;
}

/// Which of the two kinds of root a root is.
pub enum RootKind {
    /// The identifiers are named outright.
    Enumerated,

    /// The identifiers come from predicates over upstream walks' rows.
    Predicates,

    /// The identifiers are the caller's input stream.
    FromInput,
}

/// A predicate that derives a walk's starting identifiers from the rows of a
/// walk upstream of it.
///
/// This is its own level because its representation is expected to change.
/// Today a predicate is a walk name and a sansho expression over that walk's
/// rows; it may become something else entirely, and nothing outside this level
/// should have to know which. Forcing "how a predicate is read" onto [`Root`]
/// would put that knowledge in the wrong place and make it unchangeable.
pub trait RootPredicate {
    /// The walk whose emitted rows this predicate reads.
    fn source(&self) -> LevelResult<String>;

    /// The sansho expression applied to that walk's rows to produce
    /// identifiers.
    ///
    /// This is a sansho expression, not a gate: it is evaluated against a ROW
    /// and produces a value, while a gate is evaluated against an item and the
    /// walk state and produces a boolean. Nothing about the gate level — its
    /// operators, its comparison vocabulary, its leaves — applies here.
    fn expression(&self) -> LevelResult<Rc<dyn SanshoExpression>>;
}

/// A sansho expression, as the AST holds it.
///
/// The AST carries the EXPRESSION, and never a compiled program. Compiling is
/// the engine's job: a node holding a compiled program would make building the
/// tree the act of compiling it, and would force anything that reads the tree
/// to have a compiler and a program cache in hand before it can so much as
/// look at it.
pub trait SanshoExpression {
    /// The expression itself — what a sansho expression is before anything
    /// compiles it.
    fn source(&self) -> LevelResult<&str>;
}

/// The item filter level: the gate an item must pass to be processed.
pub trait ItemFilter {
    /// The gate this filter expresses, as an expression node.
    fn gate(&self) -> LevelResult<Rc<dyn ExpressionBuilder>>;
}

/// The emit level: the rows an item produces.
pub trait ItemEmit {
    /// The gate the whole emit is subject to, when it has one. When it answers
    /// false no entry is examined and no row is produced.
    fn gate(&self) -> LevelResult<Option<Rc<dyn ExpressionBuilder>>>;

    /// Whether arrays produce one row per element, as the default for entries
    /// that do not say for themselves.
    fn unnest(&self) -> LevelResult<bool>;

    /// The entries, in the order they appear. Their rows are concatenated in
    /// that order.
    fn entries(&self) -> LevelResult<Vec<Rc<dyn EmitEntry>>>;
}

/// An emit entry: one expression, its gate, and its row rules.
pub trait EmitEntry {
    /// The sansho expression whose result becomes one or more rows.
    fn expression(&self) -> LevelResult<&str>;

    /// The gate this entry alone is subject to, or `None` when it has none.
    fn gate(&self) -> LevelResult<Option<Rc<dyn ExpressionBuilder>>>;

    /// Whether this entry's arrays produce one row per element, one level only.
    /// `None` means the entry does not say and the emit's answer is used.
    fn unnest(&self) -> LevelResult<Option<bool>>;

    /// Whether this entry produces a row for a null result. `None` means the
    /// entry does not say, and nulls are swallowed.
    fn emit_null(&self) -> LevelResult<Option<bool>>;
}

/// The stop level: the rules that end the walk.
///
/// Hard rules are tried before soft ones, the first match wins, and nothing
/// after a match is evaluated — so the two lists come back separately and the
/// engine keeps the ordering.
pub trait ItemStop {
    /// The hard rules, in order. A hard rule ends the walk at once.
    fn hard(&self) -> LevelResult<Vec<Rc<dyn StopRule>>>;

    /// The soft rules, in order. A soft rule ends the walk after the current
    /// round.
    fn soft(&self) -> LevelResult<Vec<Rc<dyn StopRule>>>;
}

/// A stop rule: a gate, and optionally what it reports when it fires.
pub trait StopRule {
    /// The gate that fires this rule.
    ///
    /// A rule without one is a build error: a rule that always fires would end
    /// every walk immediately, which is never what was meant.
    fn gate(&self) -> LevelResult<Rc<dyn ExpressionBuilder>>;

    /// The rows this rule reports when it fires, or `None` when it reports
    /// nothing.
    ///
    /// Reporting nothing is legal — a walk may stop silently — and a rule's
    /// report is the only way a caller can tell a stop from exhaustion.
    fn report(&self) -> LevelResult<Option<Rc<dyn ItemEmit>>>;
}
