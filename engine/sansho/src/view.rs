//! The read-only tree-view abstraction: the surface the evaluator uses to
//! walk a document, independent of how the document is physically stored.
//!
//! The abstraction is the specified surface (SPEC-0001 §5.3), not any
//! particular implementation. Two implementations are planned: the
//! materialized view over in-memory JSON values (this phase), and a
//! forward-only cursor over the raw CBOR byte slice (a later phase). Any
//! implementation must produce identical evaluation results — that
//! agreement is a normative requirement, tested differentially.

use std::borrow::Cow;
use std::fmt;

use crate::error::SanshoError;
use serde_json::Value as J;

/// The JSON-shaped kind of a node, as the evaluator sees it.
///
/// The underlying CBOR may be richer than JSON; the mapping in
/// SPEC-0001 §4 reduces it before it reaches this type: byte strings
/// arrive as [`Kind::String`] (base64url), bignums as [`Kind::Number`].
/// Anything the mapping rejects (unknown tags, `undefined`) never
/// becomes a node — it is an evaluation error at access time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Object,
    Array,
    String,
    Number,
    Bool,
    Null,
}

impl fmt::Display for Kind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Kind::Object => "object",
            Kind::Array => "array",
            Kind::String => "string",
            Kind::Number => "number",
            Kind::Bool => "bool",
            Kind::Null => "null",
        };
        f.write_str(name)
    }
}

/// A node of a document, positioned somewhere in it.
///
/// Nodes are cheap to copy for every implementation: a node either holds
/// a reference into an in-memory document or a small position inside a
/// byte slice. Cloning a node never copies document payload.
///
/// The lifetime `'d` is the lifetime of the document the node borrows
/// from; borrowed strings are returned with that lifetime, so a caller
/// may hold them while walking elsewhere in the document.
pub trait Node<'d>: Clone {
    /// The JSON-shaped kind of this node.
    fn kind(&self) -> Kind;

    /// The string content if this is a string node. Borrowed from the
    /// document when the format stores text (both the materialized and
    /// the CBOR text-string forms); computed (and owned) when the mapping
    /// transforms it — a CBOR byte string renders as base64url.
    fn as_str(&self) -> Option<Cow<'d, str>>;

    /// The numeric content as a double, if this is a number node.
    ///
    /// JMESPath defines numbers as IEEE-754 doubles; both backends
    /// reduce every integer and float to this representation so that
    /// comparison semantics agree exactly (SPEC-0001 §4 precision note
    /// accepts the documented loss beyond the exact-double range).
    fn as_f64(&self) -> Option<f64>;

    /// The boolean content, if this is a boolean node.
    fn as_bool(&self) -> Option<bool>;

    /// Whether this is the null node.
    fn is_null(&self) -> bool;

    /// The object member named `name`, if this is an object node that
    /// has one. For the cursor implementation this scans the object's
    /// entries in document order, comparing raw key bytes; it never
    /// materializes non-matching members.
    fn get_key(&self, name: &str) -> Option<Self>;

    /// The `index`-th array element, if this is an array node that has
    /// one.
    fn get_index(&self, index: usize) -> Option<Self>;

    /// All object members as `(key, node)` pairs.
    ///
    /// Ordering: entries come out ordered by key. The in-memory JSON
    /// map is order-insensitive per the JMESPath data model; both
    /// implementations sort keys, so backend agreement holds and object
    /// iteration is deterministic. The first corpus run is the referee
    /// if the compliance corpus ever demands a different order.
    fn entries(&self) -> Vec<(String, Self)>;

    /// All array elements, in document order.
    fn elements(&self) -> Vec<Self>;

    /// The number of members or elements, if this is an object or array
    /// node; `None` for scalars.
    fn container_len(&self) -> Option<usize>;

    /// Whether this backend's materializations count against the
    /// evaluation's aggregation ledger. The cursor DECODES (each
    /// materialization is memory the ENGINE creates — the aggregation
    /// cap's subject); the materialized backend's clones are the
    /// CALLER's pre-existing data (already fully materialized by
    /// definition — counting it would attribute the caller's memory to
    /// the engine).
    fn counts_toward_aggregation() -> bool;

    /// The node's full value: the JSON view of everything this node
    /// spans. For the materialized backend this is a clone; for the
    /// cursor it is the memoized decode of the item at the node's
    /// position — the fusion point that keeps every byte decoded at
    /// most once per evaluation (SPEC-0001 §5.1).
    fn materialize(&self) -> Result<J, SanshoError>;

    /// The output-boundary materialization: decode the position without
    /// storing it in the decode memo. For positions consumed EXACTLY
    /// ONCE by the output (a projection's kept elements), the memo's
    /// dedup is never hit — its per-position insert + clone is pure
    /// overhead; the unmemoized form decodes and returns. The default
    /// is the memoized materialize (sources that cannot avoid the memo
    /// keep the shared-decode guarantee).
    fn materialize_unmemoized(&self) -> Result<J, SanshoError> {
        self.materialize()
    }

    /// The raw bytes of a text string, WITHOUT the UTF-8 validation —
    /// the filter-predicate fast path's comparison input. The default
    /// is None (only the byte source implements it); byte strings
    /// (major 2) also return None and keep the base64url path.
    fn raw_text_bytes(&self) -> Option<&[u8]> {
        None
    }
}
