//! THE STRUCTURED ERRORS of the walk engine and the query compiler.
//!
//! Every run-time failure of a query is a KIND, matched rather than parsed:
//! - [`HighWireError::ConsumerGone`] — a stage's receiver was dropped. The
//!   client disconnected, or a downstream stage ended; the teardown is
//!   underway. Not a defect: the ordinary way a streaming request stops.
//! - [`HighWireError::Budget`] — the accountant ended the run (a cancellation,
//!   and later a resource budget).
//! - [`HighWireError::Wiring`] — the compiler produced a plan whose channels do
//!   not connect (a walk with no roots stream). A compiler defect or a
//!   malformed document, never an input condition.
//! - [`HighWireError::Build`] — an expression the grammar could not turn into a
//!   predicate. The compiler wraps these in the located [`crate::highwire::grammar::GrammarError`],
//!   so a caller sees the document's path plus this kind's detail.
//! - [`HighWireError::Nested`] — a nested query failed; the enclosing stage's
//!   error carries the description.
//!
//! The KIND is what an operator branches on: a `ConsumerGone` is expected
//! traffic, a `Wiring` is a bug, a `Build` is the caller's document.

use std::fmt;

/// A walk-engine or compiler failure, by kind.
#[derive(Debug, serde::Serialize, Clone, PartialEq, Eq)]
pub enum HighWireError {
    /// A stage's receiver was dropped: the consumer is gone, the teardown is
    /// underway.
    ConsumerGone {
        /// The stage whose send failed, as the query document names it.
        stage: String,
    },

    /// The accountant ended the run.
    Budget {
        /// What ended it.
        detail: String,
    },

    /// The plan does not wire: a stage has no stream where one is required.
    Wiring {
        /// Which connection is missing.
        detail: String,
    },

    /// The walk could not EVALUATE something it must evaluate: a connection
    /// target it could not read, a connection filter that failed. Structured
    /// rather than a formatted string so a caller can branch on the kind and read
    /// the detail, instead of parsing prose.
    Evaluation {
        /// The engine's own failure, kept STRUCTURED: not a formatted string, so a
        /// caller can match on the cause (its kind, its position) instead of
        /// parsing a message.
        cause: sansho::error::SanshoError,
    },

    /// An expression could not be built into a predicate.
    Build {
        /// What could not be built, and why.
        detail: String,
    },

    /// A nested query failed.
    Nested {
        /// The nested query's failure, as its stage described it.
        detail: String,
    },
}

impl HighWireError {
    /// A `ConsumerGone` for the named stage.
    pub fn consumer_gone(stage: impl Into<String>) -> Self {
        HighWireError::ConsumerGone {
            stage: stage.into(),
        }
    }

    /// A `Wiring` failure.
    pub fn wiring(detail: impl Into<String>) -> Self {
        HighWireError::Wiring {
            detail: detail.into(),
        }
    }

    /// A `Build` failure.
    pub fn build(detail: impl Into<String>) -> Self {
        HighWireError::Build {
            detail: detail.into(),
        }
    }

    /// An `Evaluation` failure: the engine could not evaluate something the walk
    /// must evaluate. The cause travels as itself.
    pub fn evaluation(cause: sansho::error::SanshoError) -> Self {
        HighWireError::Evaluation { cause }
    }

    /// A `Nested` failure.
    pub fn nested(detail: impl Into<String>) -> Self {
        HighWireError::Nested {
            detail: detail.into(),
        }
    }

    /// A stable, machine-readable code for the kind — what an error row
    /// carries so a client can branch without parsing prose.
    pub fn code(&self) -> &'static str {
        match self {
            HighWireError::ConsumerGone { .. } => "consumer_gone",
            HighWireError::Budget { .. } => "budget",
            HighWireError::Wiring { .. } => "wiring",
            HighWireError::Evaluation { .. } => "evaluation",
            HighWireError::Build { .. } => "build",
            HighWireError::Nested { .. } => "nested",
        }
    }
}

impl fmt::Display for HighWireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HighWireError::ConsumerGone { stage } => {
                write!(f, "the consumer of stage `{stage}` is gone")
            }
            HighWireError::Budget { detail } => write!(f, "the run was ended: {detail}"),
            HighWireError::Wiring { detail } => write!(f, "the plan does not wire: {detail}"),
            HighWireError::Evaluation { cause } => write!(f, "evaluation failed: {cause}"),
            HighWireError::Build { detail } => write!(f, "{detail}"),
            HighWireError::Nested { detail } => write!(f, "the nested query failed: {detail}"),
        }
    }
}

impl std::error::Error for HighWireError {}
