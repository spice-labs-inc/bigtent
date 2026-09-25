//! # Sansho — a JMESPath projection engine for single CBOR documents
//!
//! Sansho evaluates a JMESPath expression against one CBOR document and
//! produces a JSON value containing only the parts of the document the
//! expression selects.
//!
//! ## Interface
//!
//! - Input: a byte slice holding exactly one CBOR document (definite
//!   length encodings only), and a JMESPath expression, either as text or
//!   as a previously compiled [Program].
//! - Output: a JSON value — the projection result.
//!
//! ## Guarantees
//!
//! - Evaluation is a single forward pass: each input byte is consumed at
//!   most once.
//! - Parts of the document the expression does not select are never
//!   materialized; memory use tracks the output, not the input.
//! - Evaluation is deterministic: identical bytes and an identical
//!   expression always produce identical results.
//! - Sansho performs no input/output of its own — no network, no
//!   filesystem, no clock. It is a pure library.
//!
//! Sansho implements the complete JMESPath language as published at
//! jmespath.org; conformance is defined by the vendored JMESPath
//! compliance corpus.
//!
//! ## Phase status
//!
//! The parser, canonical form, walk program, and the evaluator over the
//! materialized backend are live: see [`compile`] and [`evaluate_json`].
//! The cursor backend over raw CBOR bytes — and with it the public
//! [`evaluate`] byte-slice entry point — arrives with the cursor phase.

pub mod ast;
pub mod cache;
pub mod cbor;
pub mod corpus;
pub mod error;
pub mod eval;
pub mod materialized;
pub mod parser;
pub mod program;
pub mod source;
pub mod view;

#[cfg(test)]
mod ast_tests;

pub use ast::canonical;
pub use cache::ProgramCache;
pub use corpus::{Driver, DriverOutcome, Unimplemented};
pub use error::SanshoError;
pub use eval::Stop;
pub use eval::{
    RealEngine, evaluate_cbor, evaluate_cbor_stopped, evaluate_cbor_with_stats,
    evaluate_json,
};
pub use materialized::MaterializedNode;
pub use parser::parse;
pub use program::{Program, compile};
pub use view::{Kind, Node};

/// Evaluate a [Program] against one CBOR document.
///
/// The `document` byte slice must contain exactly one CBOR document;
/// trailing bytes are an input error. Returns the projection result as a
/// JSON value.
///
/// The cursor backend decodes selectively: only the positions the
/// expression consumes materialize, each at most once per evaluation
/// (the memoized decode). [`evaluate_json`] evaluates against in-memory
/// documents.
pub fn evaluate(program: &Program, document: &[u8]) -> Result<serde_json::Value, SanshoError> {
    eval::evaluate_cbor(program, document)
}
