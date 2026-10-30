//! THE STRUCTURED ERRORS of the scripting surface.
//!
//! A namespace function's failure is a KIND, not a prose blob: a caller (a
//! test, an embedder, or a future error-code-mapping layer) matches the variant
//! instead of sniffing the message, and the message is generated in one place.
//!
//! At the Starlark boundary these become the NATIVE [`starlark::Error`] (via
//! [`From`]), so a script's failure arrives as any other Starlark error does —
//! with a span, a traceback, and the kind reachable by downcast.
//!
//! THE KINDS, and why exactly these:
//! - [`BarkerError::Type`] — an argument's type is wrong. Carries the function,
//!   the argument's name, what was expected, and what arrived: everything a
//!   script author needs without re-reading the docs.
//! - [`BarkerError::Value`] — the arguments are well-typed but meaningless
//!   together (an empty median, a clamp whose low exceeds its high, a
//!   percentile outside 0..=100).
//! - [`BarkerError::Range`] — the result does not exist (a date shift past the
//!   representable range, a log of a non-positive number).
//! - [`BarkerError::Pattern`] — a regular expression does not compile.
//! - [`BarkerError::Date`] — a string is not an RFC 3339 instant.
//! - [`BarkerError::Unrepresentable`] — a value has no text form where one is
//!   required (a dict key, a tally bucket). NEVER silent: the whole point of
//!   the variant is that the old code swallowed these into `""`.

use starlark::Error as StarlarkError;

/// A scripting-surface failure, by kind.
#[derive(Debug, Clone, thiserror::Error)]
pub enum BarkerError {
    /// An argument's type is wrong.
    #[error("{function}: `{argument}` must be {expected}, found {found}")]
    Type {
        /// The function that refused the call.
        function: &'static str,
        /// The argument's name, as the script writes it.
        argument: &'static str,
        /// What the function accepts.
        expected: &'static str,
        /// What arrived, as Starlark names it.
        found: String,
    },

    /// The arguments are well-typed but meaningless together.
    #[error("{function}: {detail}")]
    Value {
        /// The function that refused the call.
        function: &'static str,
        /// Why the arguments cannot be used together.
        detail: String,
    },

    /// The result does not exist.
    #[error("{function}: {detail}")]
    Range {
        /// The function whose result does not exist.
        function: &'static str,
        /// Which bound was exceeded, and by what.
        detail: String,
    },

    /// A regular expression does not compile.
    #[error("{function}: the pattern `{pattern}` is invalid: {reason}")]
    Pattern {
        /// The function that compiles patterns.
        function: &'static str,
        /// The pattern as the script wrote it.
        pattern: String,
        /// The compiler's reason.
        reason: String,
    },

    /// A string is not an RFC 3339 instant.
    #[error("{function}: `{text}` is not an RFC 3339 date: {reason}")]
    Date {
        /// The function that parses dates.
        function: &'static str,
        /// The text as the script wrote it.
        text: String,
        /// The parser's reason.
        reason: String,
    },

    /// A value has no text form where one is required.
    #[error("{function}: {detail}")]
    Unrepresentable {
        /// The function that needed a text form.
        function: &'static str,
        /// What could not be represented, and why.
        detail: String,
    },
}

impl BarkerError {
    /// A type failure, from the value that arrived.
    pub fn type_error(
        function: &'static str,
        argument: &'static str,
        expected: &'static str,
        found: starlark::values::Value,
    ) -> Self {
        BarkerError::Type {
            function,
            argument,
            expected,
            found: found.get_type().to_string(),
        }
    }

    /// A meaning failure: well-typed arguments that cannot be used together.
    pub fn value(function: &'static str, detail: impl Into<String>) -> Self {
        BarkerError::Value {
            function,
            detail: detail.into(),
        }
    }

    /// A range failure: the result does not exist.
    pub fn range(function: &'static str, detail: impl Into<String>) -> Self {
        BarkerError::Range {
            function,
            detail: detail.into(),
        }
    }

    /// An unrepresentable value.
    pub fn unrepresentable(function: &'static str, detail: impl Into<String>) -> Self {
        BarkerError::Unrepresentable {
            function,
            detail: detail.into(),
        }
    }
}

/// The NATIVE Starlark error, carrying the kind: a script sees a normal
/// Starlark failure (span, traceback), and our code can downcast back to the
/// variant.
impl From<BarkerError> for StarlarkError {
    fn from(error: BarkerError) -> Self {
        StarlarkError::new_other(anyhow::Error::new(error))
    }
}

/// The kind of a Starlark error, when it originated here.
///
/// Used by tests to assert the KIND rather than the message text: the error is
/// unwrapped to its `anyhow` payload, where our variant is the downcast target.
pub fn kind_of(error: StarlarkError) -> Option<BarkerError> {
    error.into_anyhow().downcast::<BarkerError>().ok()
}
