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
//! evaluation's internal bookkeeping (a byte position's machinery)
//! lives inside the implementations' hidden types, visible nowhere in
//! the public surface.
//!
//! The trait is implemented directly on the Sansho types — the
//! "turtles all the way down" contract: the sources (`&[u8]` CBOR
//! bytes, `serde_json::Value` and `&serde_json::Value`,
//! `serde_cbor::Value` and `&serde_cbor::Value`, and — in the
//! embedding crate — `Item` and `&Item`), the scalar family (`i64`,
//! `u64`, `f64`, `bool`, `str`, `String`), and the container family
//! (`Option`, `Vec`, slices, `BTreeSet`, `HashSet`, `BTreeMap`,
//! `HashMap` — standard-library types, their impls in this crate).

use crate::error::SanshoError;
use std::collections::HashMap;
use std::fmt::{self};
use std::rc::Rc;
use std::borrow::Cow;

/// The kind of a value, as the evaluator sees it.
///
/// The underlying format may be richer than JSON. A CBOR byte string
/// (major type 2) is its own kind — [`Kind::Bytes`] — because it is not
/// text: it carries octets whose encoding the format does not specify,
/// and it is never validated as UTF-8. A bignum (tags 2/3) is
/// [`Kind::Number`]. Anything the mapping rejects (unknown tags,
/// `undefined`) never becomes a value — it is an evaluation error at
/// access time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Object,
    Array,
    String,
    Bytes,
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
            Kind::Bytes => "bytes",
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
    /// (comparison semantics reduce to f64).
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
        if n >= 0
            && let Ok(v) = u64::try_from(n)
        {
            return SanshoNumber::U64(v);
        }
        SanshoNumber::F64(n as f64)
    }
}

pub type SanshoCarrier<'a> = Rc<dyn SanshoTrait<'a> + 'a>;

/// The member view a container hands out.
///
/// `Member` is implemented per **concrete** member type — never for a
/// parameter, and never as a blanket over references. That is the point: a
/// container impl may require `&'a V: Member<'a>` for its own parameter `V`,
/// and because every `Member` impl names a concrete type, the trait solver
/// has exactly one step to unfold per member type. It can never re-enter the
/// container impl (which would unify `V` with a container of `V` and grow the
/// type forever), so a generic container impl cannot overflow the solver.
///
/// The hand-out itself is a refcount around the borrow for a
/// document-backed member, and the value itself for one that is already a
/// carrier — no member is ever copied.
pub trait Member<'a>: SanshoTrait<'a> {
    /// The member as a refcounted trait value.
    fn carrier(self) -> SanshoCarrier<'a>;
}

// pub struct OwnedCarrier<'a, T>
// where
//     &'a T: SanshoTrait<'a>,
// {
//     val: T,
//     reference: &'a T,
// }
//
// impl<'a, T> OwnedCarrier<'a, T>
// where
//     &'a T: SanshoTrait<'a>,
// {
//     pub fn new(v: T) -> OwnedCarrier<'a, T> {
//         OwnedCarrier {
//             val: v,
//             reference: &v,
//         }
//     }
// }
//
// impl<'a, T> SanshoTrait<'a> for OwnedCarrier<'a, T>
// where
//     &'a T: SanshoTrait<'a>,
// {
//     fn kind(&self) -> Kind {
//         self.reference.kind()
//     }
//
//     fn as_str(&self) -> Result<Cow<'_, str>, SanshoError> {
//         self.reference.as_str()
//     }
//
//     fn as_bytes(&self) -> Result<&[u8], SanshoError> {
//         self.reference.as_bytes()
//     }
//
//     fn as_sansho_number(&self) -> Result<SanshoNumber, SanshoError> {
//         self.reference.as_sansho_number()
//     }
//
//     fn as_bool(&self) -> Result<bool, SanshoError> {
//         self.reference.as_bool()
//     }
//
//     fn get_key(&self, name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
//         self.reference.get_key(name)
//     }
//
//     fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
//         self.reference.get_index(index)
//     }
//
//     fn as_object(&self) -> Result<HashMap<&str, SanshoCarrier<'a>>, SanshoError> {
//         self.reference.as_object()
//     }
//
//     fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
//         self.reference.as_elements()
//     }
//
//     fn container_len(&self) -> Result<usize, SanshoError> {
//         self.reference.container_len()
//     }
//
//     fn to_string_sansho(&self) -> String {
//         self.reference.to_string_sansho()
//     }
// }

/// The Sansho data-access trait — the only thing an evaluation needs.
///
/// `'a` is the lifetime of the underlying document. Values returned
/// "by reference" (`as_str`, the member values of navigation) are
/// values implementing the trait — a member IS a value — borrowed from
/// the document for `'a` where the format stores it, owned (computed)
/// where the mapping transforms it.
///
/// Navigation returns trait-bounded values: `get_key`/`get_index`
/// give `Option<impl SanshoTrait<'a>>`; `as_object` gives the object's
/// members keyed by name, and `as_elements` gives the array's elements
/// in document order. The hidden member type is per-implementation (a
/// byte position for the byte source — its machinery crate-private —
/// a borrowed value for the borrowed source forms, a computed value
/// for the value forms); the walk never names it, only the trait.
///
/// Evaluation state (the flow, a byte position's parsing)
/// is internal bookkeeping: it lives in this crate's private modules
/// and in the hidden types of the implementations — nowhere in the
/// public surface.
pub trait SanshoTrait<'a>: 'a {
    /// The JSON-shaped kind of this value.
    fn kind(&self) -> Kind;

    /// The string content of this value, if this is a TEXT string.
    /// Borrowed from the document when the format stores text (the
    /// mmap'd CBOR text, the in-memory strings); owned for a value form,
    /// which has no borrowable storage. A byte string is not text: it is
    /// [`Kind::Bytes`] and answers `None` here — see [`Self::as_bytes`].
    fn as_str(&self) -> Result<Cow<'_, str>, SanshoError> {
        Err(SanshoError::Evaluation {
            message: "Not a String type".to_string(),
        })
    }

    /// The octets of this value, if this is a BYTE string
    /// ([`Kind::Bytes`]) — borrowed from the document, so nothing is
    /// copied and no encoding is applied. The default is `None`: a value
    /// that is not a byte string has no octets of its own.
    fn as_bytes(&self) -> Result<&[u8], SanshoError> {
        Err(SanshoError::Evaluation {
            message: "Not a byte carrier".to_string(),
        })
    }

    /// The full-width number probe: the exact integer or float value,
    /// if this is a number. Backends with exact integer access (the
    /// byte header decode, the materialized JSON number, the in-memory
    /// CBOR integer) report the `I64`/`U64` arms — numbers are
    /// full-width, never narrowed.
    fn as_sansho_number(&self) -> Result<SanshoNumber, SanshoError> {
        Err(SanshoError::Evaluation {
            message: "Not a number".to_string(),
        })
    }

    /// The numeric content as a double, if this is a number: the
    /// spec-level reduction of the full-width probe.
    fn as_f64(&self) -> Result<f64, SanshoError> {
        self.as_sansho_number().map(SanshoNumber::to_f64)
    }

    /// The boolean content, if this is a boolean value.
    fn as_bool(&self) -> Result<bool, SanshoError> {
        Err(SanshoError::Evaluation {
            message: "Not a bool type".to_string(),
        })
    }

    /// Whether this is the null value.
    fn is_null(&self) -> bool {
        matches!(self.kind(), Kind::Null)
    }

    /// The member named `name`, if this is an object value that has
    /// one: a value bounded by the trait — the very thing the
    /// evaluator carries.
    fn get_key(&self, _name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        Err(SanshoError::Evaluation {
            message: "Not an object type".to_string(),
        })
    }

    /// The `index`-th array element, if this is an array value that
    /// has one: a value bounded by the trait.
    fn get_index(&self, _index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        Err(SanshoError::Evaluation {
            message: "Not an array type".to_string(),
        })
    }

    /// The object's members, keyed by name.
    ///
    /// Member order is not part of a value: a JSON object is an unordered
    /// collection of name/value pairs, so the iteration order of the
    /// returned map is not a contract and no caller may depend on it.
    fn as_object(&self) -> Result<HashMap<&str, SanshoCarrier<'a>>, SanshoError> {
        Err(SanshoError::Evaluation {
            message: format!("requires an object, found {}", self.kind()),
        })
    }

    /// All array elements, in document order.
    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        Err(SanshoError::Evaluation {
            message: format!("elements requires an array, found {}", self.kind()),
        })
    }

    /// The array's TEXT elements, in document order, skipping every element
    /// of any other kind — including byte strings, which are octets and are
    /// never text.
    ///
    /// Nothing is refcounted and nothing is allocated: each string borrows the
    /// source. This is the LENIENT read of an array — an element whose text is
    /// not valid UTF-8 is skipped rather than reported. The strict read is
    /// [`Self::as_elements`] followed by [`Self::as_str`], which answers an
    /// input error instead. The two exist deliberately: scanning for a prefix
    /// wants to pass over what it cannot read, while producing a value must
    /// not silently drop data.
    ///
    /// The default answers an error. A general-path default is not possible —
    /// it would have to borrow `&str` out of carriers that it owns — so each
    /// source whose storage holds the text itself supplies its own walk.
    fn as_array_str<'s>(
        &'s self,
    ) -> Result<Box<dyn Iterator<Item = Cow<'s, str>> + 's>, SanshoError> {
        Err(SanshoError::Evaluation {
            message: format!("as_array_str requires an array, found {}", self.kind()),
        })
    }

    /// The number of members or elements, if this is an object or
    /// array; `None` for scalars.
    fn container_len(&self) -> Result<usize, SanshoError> {
        Err(SanshoError::Evaluation {
            message: "Not an Array or Object".to_string(),
        })
    }

    /// Convert the Sansho Trait to a human friendly string. This operation may
    /// be expensive and it's okay to implement it using cloning and other copying
    /// operations. The method should be used sparingly... mostly for debugging
    /// not in the normal course of operations (i.e., during `eval`)
    fn to_string_sansho(&self) -> String;
}

// The boxed trait object IS a Sansho value: navigation hands members over
// as refcounted values, and the evaluator carries trait values, so a
// wrapper must answer the trait itself — by delegating to the value
// inside. `Box` and `Rc` are standard-library types, so these
// implementations live here with the others.
impl<'a> SanshoTrait<'a> for SanshoCarrier<'a> {
    fn kind(&self) -> Kind {
        (**self).kind()
    }

    fn as_str(&self) -> Result<Cow<'_, str>, SanshoError> {
        (**self).as_str()
    }

    fn as_bytes(&self) -> Result<&[u8], SanshoError> {
        (**self).as_bytes()
    }

    fn as_sansho_number(&self) -> Result<SanshoNumber, SanshoError> {
        (**self).as_sansho_number()
    }

    fn as_bool(&self) -> Result<bool, SanshoError> {
        (**self).as_bool()
    }

    fn get_key(&self, name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        (**self).get_key(name)
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        (**self).get_index(index)
    }

    fn as_object(&self) -> Result<HashMap<&str, SanshoCarrier<'a>>, SanshoError> {
        (**self).as_object()
    }

    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        (**self).as_elements()
    }

    fn as_array_str<'s>(
        &'s self,
    ) -> Result<Box<dyn Iterator<Item = Cow<'s, str>> + 's>, SanshoError> {
        (**self).as_array_str()
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        (**self).container_len()
    }

    fn to_string_sansho(&self) -> String {
        (**self).to_string_sansho()
    }
}
