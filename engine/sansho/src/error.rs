//! The Sansho error taxonomy.
//!
//! Every failure Sansho can produce is one of five structured categories, and every
//! category carries WHERE it happened and WHY — as structure, not as a sentence.
//!
//! * the location is a [`Span`] into the expression text (and, for evaluation
//!   failures, a [`Path`] into the document);
//! * the reason is a closed enum per category, so a caller branches on it instead of
//!   parsing prose;
//! * the error carries the expression text (`Arc<str>`), so a renderer can point at
//!   the span and a client can see the query it sent back.
//!
//! `Display` is one line: the short form for logs. The rich form is
//! [`miette::Diagnostic`], derived, which is what a terminal renders with a caret.
//!
//! Nothing in the public interface panics: a panic crossing the interface would abort
//! the caller's task, so all failure is carried in [`SanshoError`] values.

use serde::Serialize;
use std::fmt;
use std::sync::Arc;

/// A byte range in the expression text.
///
/// The parser knows it, the compiler inherits it, the evaluator reports it — which is
/// why it is carried in the node rather than looked up: a missing location has to be
/// impossible, not merely unlikely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct Span {
    /// First byte of the construct.
    pub start: usize,
    /// One past the last byte.
    pub end: usize,
}

impl Span {
    /// A span from `start` to `end`.
    pub fn new(start: usize, end: usize) -> Self {
        Span { start, end }
    }

    /// A span that covers exactly `text`, which begins at `start`.
    pub fn of(start: usize, text: &str) -> Self {
        Span {
            start,
            end: start + text.len(),
        }
    }

    /// The length in bytes.
    pub fn len(&self) -> usize {
        self.end.saturating_sub(self.start)
    }

    /// Whether the span covers nothing.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One navigation step into the document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Step {
    /// A member of an object.
    Field(String),
    /// An element of an array.
    Index(usize),
}

/// Where in the DOCUMENT an evaluation failed: the steps that reached the value.
///
/// The span says where in the *expression*; the path says where in the *document*.
/// An evaluation error answers both, because "where did it fail" has two answers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Path(pub Vec<Step>);

impl Path {
    /// The root of the document: nothing has been navigated yet.
    pub fn root() -> Self {
        Path(Vec::new())
    }

    /// This path with one more step.
    pub fn then(&self, step: Step) -> Self {
        let mut steps = self.0.clone();
        steps.push(step);
        Path(steps)
    }

    /// This path with a field appended.
    pub fn field(&self, name: impl Into<String>) -> Self {
        self.then(Step::Field(name.into()))
    }

    /// This path with an index appended.
    pub fn index(&self, at: usize) -> Self {
        self.then(Step::Index(at))
    }

    /// Whether this is the document root.
    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return write!(f, "@");
        }
        for step in &self.0 {
            match step {
                Step::Field(name) => {
                    if name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                        write!(f, ".{name}")?;
                    } else {
                        write!(f, ".\"{name}\"")?;
                    }
                }
                Step::Index(at) => write!(f, "[{at}]")?,
            }
        }
        Ok(())
    }
}

/// Why the bytes are not a document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InputFault {
    /// More than one document in the slice; the second starts at this offset.
    TrailingDocument { at: usize },
    /// An indefinite-length encoding, which the document contract forbids.
    IndefiniteLength { at: usize },
    /// Text that is not valid UTF-8.
    InvalidUtf8 { at: usize },
    /// The bytes do not decode at all.
    Decode { at: usize },
    /// A document could not be READ: the failure is the filesystem's, and its
    /// message is data the reader needs (permission, missing file, bad UTF-8 are
    /// different problems with different fixes).
    Read { at: usize, detail: String },
}

/// Why the expression does not parse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParseFault {
    /// A number was expected here.
    ExpectedDigit { found: String },
    /// A hexadecimal digit was expected here.
    ExpectedHexDigit { found: String },
    /// A specific token was expected here.
    ExpectedToken { found: String },
    /// An escape sequence was expected here.
    ExpectedEscape { found: String },
    /// A literal (backtick JSON) was expected here.
    ExpectedLiteral { found: String },
    /// The expression ended early.
    UnexpectedEnd { wanted: String },
}

/// Why an expression that parses is still invalid.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CompileFault {
    /// No JMESPath built-in has this name.
    UnknownFunction { name: String },
    /// The function exists; the number of arguments is wrong.
    WrongArity {
        name: String,
        minimum: usize,
        maximum: Option<usize>,
        found: usize,
    },
    /// The specification accepts no variables.
    RefusedVariable { name: String },
    /// The construct is not valid JMESPath.
    InvalidStructure { detail: String },
}

/// Why the document defeats evaluation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationFault {
    /// The value is not what the function requires.
    TypeMismatch {
        expected: String,
        found: String,
        function: Option<String>,
    },
    /// An unknown CBOR tag.
    UnknownTag { tag: u64 },
    /// A CBOR `undefined`.
    Undefined,
    /// A function name that is not callable here.
    NotAFunction { name: String },
    /// The document is malformed in a way the accessor cannot read: a truncated
    /// float, a byte string where text was required.
    Malformed { what: String },
    /// An argument is valid in type but invalid in value (a zero slice step).
    InvalidArgument { what: String },
    /// Two values cannot be ordered against each other (NaN, or unlike kinds).
    NotOrderable { left: String, right: String },
    /// A function was called with a shape of arguments it does not accept.
    BadArguments { function: String, detail: String },
    /// A construct the engine does not implement yet.
    NotImplemented,
}

/// Which bound was exceeded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitFault {
    /// The expression is longer than allowed.
    ExpressionLength { allowed: usize, observed: usize },
    /// The nesting is deeper than allowed.
    NestingDepth { allowed: usize, observed: usize },
    /// The tree is deeper than allowed.
    TreeDepth { allowed: usize, observed: usize },
    /// A structural comparison walked deeper than allowed.
    ComparisonDepth { allowed: usize, observed: usize },
    /// The evaluation ran more instructions than allowed.
    InstructionCount { allowed: usize, observed: usize },
    /// The result has more nodes than allowed.
    OutputNodes { allowed: usize, observed: usize },
    /// The result is larger than allowed.
    OutputBytes { allowed: u64, observed: u64 },
}

impl LimitFault {
    /// Which limit, as a stable name.
    pub fn name(&self) -> &'static str {
        match self {
            LimitFault::ExpressionLength { .. } => "expression length",
            LimitFault::NestingDepth { .. } => "nesting depth",
            LimitFault::TreeDepth { .. } => "tree depth",
            LimitFault::ComparisonDepth { .. } => "comparison depth",
            LimitFault::InstructionCount { .. } => "instruction count",
            LimitFault::OutputNodes { .. } => "output nodes",
            LimitFault::OutputBytes { .. } => "output bytes",
        }
    }
}

/// The five failure categories of the engine, in the order a document passes through
/// them: the bytes are read (input), the expression is parsed (parse), the parse tree
/// is lowered (compile), the program runs (evaluation), and every step is bounded
/// (limit).
///
/// Each carries WHERE (`span`, and for evaluation also `path`) and WHY (`reason`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "category", rename_all = "snake_case")]
pub enum SanshoError {
    /// The input byte slice does not satisfy the document contract.
    Input {
        /// Byte offset into the document.
        at: usize,
        /// Why the bytes are not a document.
        reason: InputFault,
    },
    /// The expression text is not valid JMESPath.
    Parse {
        /// Where in the expression, when the parser knew.
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<Span>,
        /// The expression, so a renderer can point at the span.
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<Arc<str>>,
        /// Why it does not parse.
        reason: ParseFault,
    },
    /// The expression parses but the specification declares it invalid.
    Compile {
        /// Where in the expression, when it was known.
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<Span>,
        /// The expression.
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<Arc<str>>,
        /// Why it is invalid.
        reason: CompileFault,
    },
    /// The document's contents defeat evaluation.
    Evaluation {
        /// Where in the expression, when the caller knew it. A carrier answering
        /// through the trait surface does not: the accessor has a value, not a
        /// position, and inventing `0..0` would be a location that lies.
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<Span>,
        /// Where in the document; absent only when nothing has been navigated.
        #[serde(skip_serializing_if = "Option::is_none")]
        path: Option<Path>,
        /// The expression.
        #[serde(skip_serializing_if = "Option::is_none")]
        source: Option<Arc<str>>,
        /// Why evaluation failed.
        reason: EvaluationFault,
    },
    /// A configured resource limit was exceeded.
    Limit {
        /// Where it tripped, when the expression was already in hand.
        #[serde(skip_serializing_if = "Option::is_none")]
        span: Option<Span>,
        /// Which limit, and by how much.
        reason: LimitFault,
    },
}

impl SanshoError {
    /// The category, as a stable, lowercase name.
    pub fn category(&self) -> &'static str {
        match self {
            SanshoError::Input { .. } => "input",
            SanshoError::Parse { .. } => "parse",
            SanshoError::Compile { .. } => "compile",
            SanshoError::Evaluation { .. } => "evaluation",
            SanshoError::Limit { .. } => "limit",
        }
    }

    /// The span, when the failure has one.
    pub fn span(&self) -> Option<Span> {
        match self {
            SanshoError::Parse { span, .. }
            | SanshoError::Compile { span, .. }
            | SanshoError::Evaluation { span, .. } => *span,
            SanshoError::Limit { span, .. } => *span,
            // an input failure's offset IS its span: it names one byte
            SanshoError::Input { at, .. } => Some(Span::new(*at, *at)),
        }
    }

    /// The document path, when the failure was an evaluation.
    pub fn path(&self) -> Option<&Path> {
        match self {
            SanshoError::Evaluation { path, .. } => path.as_ref(),
            _ => None,
        }
    }

    /// The expression text, when it was in hand.
    pub fn source(&self) -> Option<&Arc<str>> {
        match self {
            SanshoError::Parse { source, .. }
            | SanshoError::Compile { source, .. }
            | SanshoError::Evaluation { source, .. } => source.as_ref(),
            _ => None,
        }
    }

    /// An `Input` failure at a byte offset.
    pub fn input(at: usize, reason: InputFault) -> Self {
        SanshoError::Input { at, reason }
    }

    /// A `Parse` failure at a span.
    pub fn parse(span: Option<Span>, source: Option<Arc<str>>, reason: ParseFault) -> Self {
        SanshoError::Parse {
            span,
            source,
            reason,
        }
    }

    /// A `Compile` failure at a span.
    pub fn compile(span: Option<Span>, source: Option<Arc<str>>, reason: CompileFault) -> Self {
        SanshoError::Compile {
            span,
            source,
            reason,
        }
    }

    /// An `Evaluation` failure: where in the expression, where in the document, and why.
    pub fn evaluation(span: Option<Span>, path: Path, reason: EvaluationFault) -> Self {
        SanshoError::Evaluation {
            span,
            path: if path.is_root() { None } else { Some(path) },
            source: None,
            reason,
        }
    }

    /// The same failure with the expression text attached.
    pub fn with_source(mut self, source: Arc<str>) -> Self {
        match &mut self {
            SanshoError::Parse { source: slot, .. }
            | SanshoError::Compile { source: slot, .. }
            | SanshoError::Evaluation { source: slot, .. } => *slot = Some(source),
            _ => {}
        }
        self
    }

    /// An input failure with the reader's own message attached.
    pub fn with_detail(self, detail: impl Into<String>) -> Self {
        match self {
            SanshoError::Input { at, .. } => SanshoError::Input {
                at,
                reason: InputFault::Read {
                    at,
                    detail: detail.into(),
                },
            },
            other => other,
        }
    }

    /// The same failure with its span filled in, when it did not know one.
    ///
    /// The carrier accessors answer through the trait surface and have a value, not a
    /// position; the evaluator knows the node, so it supplies what they could not.
    pub fn with_span(mut self, known: Span) -> Self {
        match &mut self {
            SanshoError::Parse { span, .. }
            | SanshoError::Compile { span, .. }
            | SanshoError::Evaluation { span, .. } => {
                // fill in what the carrier could not know, and never overwrite a
                // location that IS known
                span.get_or_insert(known);
            }
            _ => {}
        }
        self
    }

    /// The same failure with the document path filled in, when it did not know one.
    pub fn with_path(mut self, known: &Path) -> Self {
        if let SanshoError::Evaluation { path, .. } = &mut self
            && path.is_none()
            && !known.is_root()
        {
            *path = Some(known.clone());
        }
        self
    }

    /// A `Limit` failure.
    pub fn limit(span: Option<Span>, reason: LimitFault) -> Self {
        SanshoError::Limit { span, reason }
    }
}

impl fmt::Display for SanshoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SanshoError::Input { at, reason } => write!(f, "input error at byte {at}: {reason:?}"),
            SanshoError::Parse { span, reason, .. } => match span {
                Some(span) => write!(f, "parse error at bytes {}..{}: {reason:?}", span.start, span.end),
                None => write!(f, "parse error: {reason:?}"),
            },
            SanshoError::Compile { span, reason, .. } => match span {
                Some(span) => write!(f, "compile error at bytes {}..{}: {reason:?}", span.start, span.end),
                None => write!(f, "compile error: {reason:?}"),
            },
            SanshoError::Evaluation { span, path, reason, .. } => {
                let at = match span {
                    Some(span) => format!(" at bytes {}..{}", span.start, span.end),
                    None => String::new(),
                };
                let where_ = path
                    .as_ref()
                    .map(|p| format!(" (document {p})"))
                    .unwrap_or_default();
                write!(f, "evaluation error{at}{where_}: {reason:?}")
            }
            SanshoError::Limit { reason, .. } => write!(
                f,
                "limit exceeded: {}",
                reason.name()
            ),
        }
    }
}

impl std::error::Error for SanshoError {}

impl miette::Diagnostic for SanshoError {
    fn code(&self) -> Option<Box<dyn std::fmt::Display + '_>> {
        Some(Box::new(self.category()))
    }

    fn help(&self) -> Option<Box<dyn std::fmt::Display + '_>> {
        match self {
            SanshoError::Compile {
                reason: CompileFault::UnknownFunction { name },
                ..
            } => Some(Box::new(format!(
                "`{name}` is not a JMESPath function"
            ))),
            _ => None,
        }
    }

    fn labels(&self) -> Option<Box<dyn Iterator<Item = miette::LabeledSpan> + '_>> {
        let span = self.span()?;
        Some(Box::new(std::iter::once(miette::LabeledSpan::new(
            Some(format!("{:?}", self.reason_name())),
            span.start,
            span.len(),
        ))))
    }
}

impl SanshoError {
    /// The reason, as a short stable name — for labels and for logs.
    pub fn reason_name(&self) -> &'static str {
        match self {
            SanshoError::Input { .. } => "not a document",
            SanshoError::Parse { .. } => "does not parse",
            SanshoError::Compile { .. } => "invalid expression",
            SanshoError::Evaluation { .. } => "evaluation failed",
            SanshoError::Limit { .. } => "limit exceeded",
        }
    }
}
