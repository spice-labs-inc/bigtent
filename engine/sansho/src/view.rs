//! The Sansho data-access trait — the ONLY thing an evaluation needs.
//!
//! At any place in a traversal it answers "what Sansho data type is
//! here, and what is the value of that type by reference?": kind,
//! borrowed values, navigation into members, and boundary
//! materialization. The trait is the thing that is CARRIED through
//! program execution: the evaluator's functions take
//! `&impl SanshoTrait<'a>`, and navigation RETURNS values bounded by
//! the trait (`Option<impl SanshoTrait<'a>>`, `Vec<impl SanshoTrait<'a>>`).
//! There is no other value carrier — no reference enum, no position
//! wrapper: a member IS a value implementing the trait, and the
//! evaluation's internal bookkeeping (a byte position's machinery,
//! the memos) lives inside the implementations' hidden types, visible
//! nowhere in the public surface.
//!
//! The trait is implemented directly on the Sansho types — the
//! "turtles all the way down" contract: the sources (`&[u8]` CBOR
//! bytes, `serde_json::Value` and `&serde_json::Value`,
//! `serde_cbor::Value` and `&serde_cbor::Value`, and — in the
//! embedding crate — `Item` and `&Item`), the scalar family (`i64`,
//! `u64`, `f64`, `bool`, `str`, `String`), and the container family
//! (`Option`, `Vec`, slices, `BTreeSet`, `HashSet`, `BTreeMap`,
//! `HashMap` — standard-library types, their impls in this crate).

use std::fmt;

use crate::error::SanshoError;
use serde_json::Value as J;

/// The JSON-shaped kind of a value, as the evaluator sees it.
///
/// The underlying CBOR may be richer than JSON; the mapping in
/// SPEC-0001 §4 reduces it before it reaches this type: byte strings
/// arrive as [`Kind::String`] (base64url), bignums as [`Kind::Number`].
/// Anything the mapping rejects (unknown tags, `undefined`) never
/// becomes a value — it is an evaluation error at access time.
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

/// A Sansho number, by value: register-sized scalars carry no payload,
/// so "the value of the type by reference" for a number is the Copy
/// value itself — the byte source decodes it from the CBOR header with
/// no allocation. Full-width: `i64`/`u64`/`f64` — no wrap-around, no
/// dropped f64 (R1.1).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SanshoNumber {
    I64(i64),
    U64(u64),
    F64(f64),
}

impl SanshoNumber {
    /// The spec-level view: JMESPath numbers are IEEE-754 doubles
    /// (SPEC-0001 §4 reduces comparison semantics to f64).
    pub fn to_f64(self) -> f64 {
        match self {
            SanshoNumber::I64(v) => v as f64,
            SanshoNumber::U64(v) => v as f64,
            SanshoNumber::F64(v) => v,
        }
    }

    /// The in-memory CBOR integer form: `serde_cbor::Value::Integer`
    /// is i128; the full-width mapping keeps everything that fits in
    /// i64/u64 exact and reduces only genuine bignums to f64 — the
    /// same shape the materialization path renders.
    pub fn from_i128(n: i128) -> Self {
        if let Ok(v) = i64::try_from(n) {
            return SanshoNumber::I64(v);
        }
        if n >= 0 {
            if let Ok(v) = u64::try_from(n) {
                return SanshoNumber::U64(v);
            }
        }
        SanshoNumber::F64(n as f64)
    }
}

/// The Sansho data-access trait — the only thing an evaluation needs.
///
/// `'a` is the lifetime of the underlying document. Values returned
/// "by reference" (`as_str`, the member values of navigation) are
/// values implementing the trait — a member IS a value — borrowed from
/// the document for `'a` where the format stores it, owned (computed)
/// where the mapping transforms it.
///
/// Navigation returns trait-bounded values: `get_key`/`get_index`
/// give `Option<impl SanshoTrait<'a>>`, `entries`/`elements` give
/// `Vec` of them. The hidden member type is per-implementation (a
/// byte position for the byte source — its machinery crate-private —
/// a borrowed value for the borrowed source forms, a computed value
/// for the value forms); the walk never names it, only the trait.
///
/// Evaluation state (the flow, a byte position's parsing and memo)
/// is internal bookkeeping: it lives in this crate's private modules
/// and in the hidden types of the implementations — nowhere in the
/// public surface.
pub trait SanshoTrait<'a>: 'a {
    /// The JSON-shaped kind of this value.
    fn kind(&self) -> Kind;

    /// The string content of this value, if it is a string. Borrowed
    /// from the document when the format stores text (the mmap'd CBOR
    /// text, the in-memory strings); computed (and owned) when the
    /// mapping transforms it — a CBOR byte string renders as base64url.
    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>>;

    /// The full-width number probe: the exact integer or float value,
    /// if this is a number. Backends with exact integer access (the
    /// byte header decode, the materialized JSON number, the in-memory
    /// CBOR integer) report the `I64`/`U64` arms — numbers are
    /// full-width, never narrowed.
    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        self.as_f64().map(SanshoNumber::F64)
    }

    /// The numeric content as a double, if this is a number: the
    /// spec-level reduction (SPEC-0001 §4) of the full-width probe.
    fn as_f64(&self) -> Option<f64> {
        self.as_sansho_number().map(SanshoNumber::to_f64)
    }

    /// The boolean content, if this is a boolean value.
    fn as_bool(&self) -> Option<bool>;

    /// Whether this is the null value.
    fn is_null(&self) -> bool;

    /// The member named `name`, if this is an object value that has
    /// one: a value bounded by the trait — the very thing the
    /// evaluator carries.
    fn get_key(&self, name: &str) -> Option<impl SanshoTrait<'a> + use<'a, Self>>;

    /// The `index`-th array element, if this is an array value that
    /// has one: a value bounded by the trait.
    fn get_index(&self, index: usize) -> Option<impl SanshoTrait<'a> + use<'a, Self>>;

    /// All object members as `(key, member)` pairs, ordered by key —
    /// iteration is deterministic and backends agree.
    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a, Self>)>;

    /// All array elements, in document order.
    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a, Self>>;

    /// The number of members or elements, if this is an object or
    /// array; `None` for scalars.
    fn container_len(&self) -> Option<usize>;

    /// Whether this value's materializations count against the
    /// evaluation's aggregation ledger. The byte source DECODES (each
    /// materialization is memory the ENGINE creates — the aggregation
    /// cap's subject); the materialized sources' clones are the
    /// CALLER's pre-existing data (already fully materialized by
    /// definition).
    fn counts_toward_aggregation() -> bool;

    /// This value's full JSON view — the materialization boundary: the
    /// row boundary for an emit. For the byte source it is the
    /// memoized decode of the position (every byte decoded at most
    /// once per evaluation, SPEC-0001 §5.1).
    fn materialize(&self) -> Result<J, SanshoError>;

    /// The output-boundary materialization: decode without storing in
    /// the memo. For positions consumed EXACTLY ONCE by the output,
    /// the memo's dedup is never hit; the default is the memoized
    /// form.
    fn materialize_unmemoized(&self) -> Result<J, SanshoError> {
        self.materialize()
    }

    /// The raw bytes of a text string, WITHOUT the UTF-8 validation —
    /// the filter-predicate fast path's comparison input. The default
    /// is None (only the byte source and borrowed-string forms
    /// implement it); byte strings (major 2) also return None and keep
    /// the base64url path.
    fn raw_text_bytes(&self) -> Option<&[u8]> {
        None
    }
}