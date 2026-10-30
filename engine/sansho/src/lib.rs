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
//! - Navigation touches only the positions the program reads: the
//!   document is never decoded as a whole, and a leaf is decoded where it
//!   is read. (The decode-count instrumentation that used to demonstrate a
//!   byte-once walk was removed with the decode memo; navigation parses
//!   headers and does position arithmetic, so a repeated navigation walks
//!   again rather than sharing a decode.)
//! - Parts of the document the expression does not select are never
//!   materialized; memory use tracks the output, not the input.
//! - Evaluation is deterministic: identical bytes and an identical
//!   expression always produce the same VALUE — results that compare
//!   equal as JSON. This is value equality, not byte equality: an
//!   object's member order is not part of its value, so a result carried
//!   in a hash-backed container may serialize its members in a different
//!   order on the next call and still be the same result.
//! - Sansho performs no input/output of its own — no network, no
//!   filesystem, no clock. It is a pure library.
//!
//! Sansho implements the complete JMESPath language as published at
//! jmespath.org; conformance is defined by the vendored JMESPath
//! compliance corpus.
//!
//! ## Entry points
//!
//! [`parse`] and [`compile`] produce a [`Program`]; [`evaluate`] runs it
//! against any source that implements [`SanshoTrait`] — a CBOR byte slice or
//! an in-memory `serde_json::Value`; [`materialize`] converts the result to a
//! `serde_json::Value` at the boundary, after the evaluation has finished.
//! [`ProgramCache`] amortizes the parse and compile over repeated
//! expressions.

pub mod ast;
pub mod cache;
pub mod cbor;
pub mod cbor_value;
pub mod containers;
pub mod corpus;
pub mod error;
pub mod eval;
pub mod materialized;
pub mod parser;
pub mod program;
pub mod scalars;
pub mod source;
pub mod view;

#[cfg(test)]
mod ast_tests;

use std::rc::Rc;

pub use ast::{can_be_a_string, can_emit_a_string, canonical};
pub use cache::ProgramCache;
pub use corpus::{Driver, DriverOutcome, Unimplemented};
pub use error::SanshoError;
pub use eval::Stop;
pub use parser::parse;
pub use program::{Program, compile};

pub use crate::view::{SanshoCarrier, SanshoTrait};

use crate::view::{Kind, SanshoNumber};

/// Evaluate a [Program] against a document.
///
/// Generic over [`SanshoTrait`], so any Sansho source evaluates: a CBOR byte
/// slice, an in-memory `serde_json::Value`, a scalar, a container. The result
/// is a trait value rather than a JSON value — [`materialize`] converts it at
/// the boundary, after the evaluation has finished.
///
/// The byte source reads selectively: only the positions the expression
/// consumes are decoded, so an unselected payload is never visited. Bytes
/// after the document are not read.
pub fn evaluate<'a, T: SanshoTrait<'a>>(
    program: &Program,
    document: T,
) -> Result<SanshoCarrier<'a>, SanshoError> {
    eval::eval(program, Rc::new(document) as SanshoCarrier<'a>)
        .map(eval::flow_value)
        .map_err(|e| match e {
            Stop::Error(e) => e,
            Stop::NotImplemented => SanshoError::Evaluation {
                message: "Not implemented".to_string(),
            },
        })
}

/// The materialization boundary: the JSON view of a finished evaluation.
///
/// Generic over the trait, so any Sansho source materializes — a carrier, a
/// borrowed document, a scalar, a container. This is a SEPARATE operation
/// from evaluation: it runs after the evaluation completes and never inside
/// it. Octets become text only here, through the byte-string rendering.
pub fn materialize<'a, T: SanshoTrait<'a> + ?Sized>(
    value: &T,
) -> Result<serde_json::Value, SanshoError> {
    materialize_counted(
        value,
        &mut MaterializationBudget::default(),
    )
}

/// The materialization budget: how much one conversion may emit.
///
/// A carrier is a refcount-shared structure whose DENOTED tree can be
/// exponentially larger than the memory it holds — a chained `[@,@]` grows
/// the denoted tree 2x per stage while growing memory by one node. Nothing
/// upstream bounds that: the JMESPath spec has no resource semantics and
/// every implementation defers the cost to the consumer (verified by survey,
/// PR #171 review, finding 4). Sansho's consumer is a server, so the walk
/// that pays is ours — this budget is the boundary.
///
/// Per conversion, not per process: each `materialize` call gets a fresh
/// budget, so a pipeline's rows are individually bounded and one bad row
/// does not spend the next row's allowance. When the accountant lands, the
/// constants here become parameters it supplies.
#[derive(Clone, Copy, Default)]
pub struct MaterializationBudget {
    /// Nodes emitted so far, counting each occurrence (sharing is invisible
    /// by construction: the denoted size is the consumer's cost).
    nodes: u64,
    /// Rendered text bytes so far: string contents, byte encodings, keys.
    bytes: u64,
}

/// The node budget: the most nodes one conversion may emit.
pub const MAX_MATERIALIZED_NODES: u64 = 500_000;
/// The text budget: the most rendered string bytes one conversion may emit.
pub const MAX_MATERIALIZED_STRING_BYTES: u64 = 10_000_000;

impl MaterializationBudget {
    fn count_node(&mut self) -> Result<(), SanshoError> {
        self.nodes += 1;
        if self.nodes > MAX_MATERIALIZED_NODES {
            return Err(SanshoError::Limit {
                message: format!(
                    "materialization exceeds {} nodes (shared structures can denote \
                     exponentially many); narrow the expression",
                    MAX_MATERIALIZED_NODES
                ),
            });
        }
        Ok(())
    }

    fn count_bytes(&mut self, bytes: u64) -> Result<(), SanshoError> {
        self.bytes += bytes;
        if self.bytes > MAX_MATERIALIZED_STRING_BYTES {
            return Err(SanshoError::Limit {
                message: format!(
                    "materialization exceeds {} bytes of string text; narrow the expression",
                    MAX_MATERIALIZED_STRING_BYTES
                ),
            });
        }
        Ok(())
    }
}

fn materialize_counted<'a, T: SanshoTrait<'a> + ?Sized>(
    value: &T,
    budget: &mut MaterializationBudget,
) -> Result<serde_json::Value, SanshoError> {
    match value.kind() {
        Kind::Null => {
            budget.count_node()?;
            Ok(serde_json::Value::Null)
        }
        Kind::Bool => {
            budget.count_node()?;
            Ok(serde_json::Value::Bool(value.as_bool()?))
        }
        Kind::Number => {
            budget.count_node()?;
            // JSON has no spelling for a NON-FINITE number, so one answers
            // `null` — the JavaScript convention, and the same answer the
            // evaluator's text rendering gives. (The carrier still holds the
            // value; the policy is about JSON, not about evaluation.)
            let number = match value.as_sansho_number()? {
                SanshoNumber::I64(v) => serde_json::Number::from(v),
                SanshoNumber::U64(v) => serde_json::Number::from(v),
                SanshoNumber::F64(v) => match serde_json::Number::from_f64(v) {
                    Some(number) => number,
                    // NaN, +Infinity, -Infinity
                    None => return Ok(serde_json::Value::Null),
                },
            };
            Ok(serde_json::Value::Number(number))
        }
        Kind::String => {
            budget.count_node()?;
            let text = value.as_str()?;
            budget.count_bytes(text.len() as u64)?;
            Ok(serde_json::Value::String(text.to_string()))
        }
        Kind::Bytes => {
            budget.count_node()?;
            let encoded = cbor::base64url_encode(value.as_bytes()?);
            budget.count_bytes(encoded.len() as u64)?;
            Ok(serde_json::Value::String(encoded))
        }
        Kind::Array => {
            budget.count_node()?;
            let mut items = Vec::new();
            for element in value.as_elements()? {
                items.push(materialize_counted(&element, budget)?);
            }
            Ok(serde_json::Value::Array(items))
        }
        Kind::Object => {
            budget.count_node()?;
            let mut object = serde_json::Map::new();
            for (key, member) in value.as_object()? {
                budget.count_bytes(key.len() as u64)?;
                object.insert(key.to_string(), materialize_counted(&member, budget)?);
            }
            Ok(serde_json::Value::Object(object))
        }
    }
}
