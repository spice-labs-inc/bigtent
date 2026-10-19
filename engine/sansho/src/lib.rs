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

pub use ast::canonical;
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
    match value.kind() {
        Kind::Null => Ok(serde_json::Value::Null),
        Kind::Bool => Ok(serde_json::Value::Bool(value.as_bool()?)),
        Kind::Number => {
            let number = match value.as_sansho_number()? {
                SanshoNumber::I64(v) => serde_json::Number::from(v),
                SanshoNumber::U64(v) => serde_json::Number::from(v),
                SanshoNumber::F64(v) => {
                    serde_json::Number::from_f64(v).unwrap_or_else(|| serde_json::Number::from(0))
                }
            };
            Ok(serde_json::Value::Number(number))
        }
        Kind::String => Ok(serde_json::Value::String(value.as_str()?.to_string())),
        Kind::Bytes => Ok(serde_json::Value::String(cbor::base64url_encode(
            value.as_bytes()?,
        ))),
        Kind::Array => {
            let mut items = Vec::new();
            for element in value.as_elements()? {
                items.push(materialize(&element)?);
            }
            Ok(serde_json::Value::Array(items))
        }
        Kind::Object => {
            let mut object = serde_json::Map::new();
            for (key, member) in value.as_object()? {
                object.insert(key.to_string(), materialize(&member)?);
            }
            Ok(serde_json::Value::Object(object))
        }
    }
}
