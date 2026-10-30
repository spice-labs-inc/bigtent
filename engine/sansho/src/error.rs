//! The Sansho error taxonomy.
//!
//! Every failure Sansho can produce is one of five structured categories.
//! Nothing in the public interface panics: a panic crossing the interface
//! would abort the caller's task, so all failure is carried in
//! [`SanshoError`] values.

use std::fmt;

/// The five failure categories of the engine, in the order a document
/// passes through them: the bytes are read (input), the expression is
/// parsed (parse), the parse tree is lowered (compile), the program runs
/// (evaluation), and every step is bounded (limit).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SanshoError {
    /// The input byte slice does not satisfy the document contract:
    /// it holds zero or more than one CBOR document, uses an
    /// indefinite-length encoding, contains text that is not valid
    /// UTF-8, or cannot be decoded at all.
    Input { message: String },
    /// The expression text is not valid JMESPath. Carries the byte
    /// position in the expression text where parsing failed.
    Parse { message: String, position: usize },
    /// The expression parses but the JMESPath specification declares it
    /// invalid (for example a wrong-arity function call pattern that the
    /// specification rejects statically).
    Compile { message: String },
    /// The document's contents defeat evaluation: an unknown CBOR tag, a
    /// CBOR `undefined` value, or a type mismatch the JMESPath
    /// specification defines as an evaluation error.
    Evaluation { message: String },
    /// A configured resource limit was exceeded: expression length,
    /// nesting depth, evaluation instruction count, output node count,
    /// or output byte size. Exceeding any limit aborts the whole
    /// evaluation; results are never partial.
    Limit { message: String },
}

impl SanshoError {
    /// The category, as a stable, lowercase name — for logs and for
    /// tests that assert on the category rather than the message.
    pub fn category(&self) -> &'static str {
        match self {
            SanshoError::Input { .. } => "input",
            SanshoError::Parse { .. } => "parse",
            SanshoError::Compile { .. } => "compile",
            SanshoError::Evaluation { .. } => "evaluation",
            SanshoError::Limit { .. } => "limit",
        }
    }
}

impl fmt::Display for SanshoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SanshoError::Input { message } => write!(f, "input error: {message}"),
            SanshoError::Parse { message, position } => {
                write!(f, "parse error at byte {position}: {message}")
            }
            SanshoError::Compile { message } => write!(f, "compile error: {message}"),
            SanshoError::Evaluation { message } => write!(f, "evaluation error: {message}"),
            SanshoError::Limit { message } => write!(f, "limit exceeded: {message}"),
        }
    }
}

impl std::error::Error for SanshoError {}

#[cfg(test)]
mod tests {
    use super::*;

    // Requirement: the error taxonomy is the single failure shape of the
    // public interface. What: every category renders a distinct,
    // human-readable message that names the category. Why: later phases
    // assert on category names and message shape; this pins both, so a
    // message change is a deliberate act, not an accident.
    //
    // LLM section: each variant must format with its category prefix
    // ("input error", "parse error at byte N", "compile error",
    // "evaluation error", "limit exceeded"); `category()` returns the
    // lowercase machine name of the variant.
    #[test]
    fn error_categories_render_distinct_messages() {
        let cases = [
            (
                SanshoError::Input {
                    message: "trailing bytes".into(),
                },
                "input error: trailing bytes",
                "input",
            ),
            (
                SanshoError::Parse {
                    message: "unexpected token".into(),
                    position: 7,
                },
                "parse error at byte 7: unexpected token",
                "parse",
            ),
            (
                SanshoError::Compile {
                    message: "wrong arity".into(),
                },
                "compile error: wrong arity",
                "compile",
            ),
            (
                SanshoError::Evaluation {
                    message: "unknown tag".into(),
                },
                "evaluation error: unknown tag",
                "evaluation",
            ),
            (
                SanshoError::Limit {
                    message: "output bytes".into(),
                },
                "limit exceeded: output bytes",
                "limit",
            ),
        ];
        for (error, expected_text, expected_category) in cases {
            assert_eq!(error.to_string(), expected_text);
            assert_eq!(error.category(), expected_category);
        }
    }

    // Requirement: the taxonomy carries the data its categories promise.
    // What: the parse variant preserves the byte position. Why: parse
    // errors are only actionable for humans and for tests if the position
    // survives; this guards the field against being dropped.
    //
    // LLM section: `SanshoError::Parse { position, .. }` must round-trip
    // the byte offset given at construction.
    #[test]
    fn parse_error_preserves_position() {
        let error = SanshoError::Parse {
            message: "unexpected token".into(),
            position: 42,
        };
        match error {
            SanshoError::Parse { position, .. } => assert_eq!(position, 42),
            other => panic!("wrong variant: {other:?}"),
        }
    }
}
