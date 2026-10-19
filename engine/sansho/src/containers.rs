//! The container source family — the "turtles all the way down" layer for
//! the standard-library containers the evaluation builds and borrows.
//!
//! Every implementation here is a *borrowed view*: a container of trait
//! values answers the trait by handing out the members it already holds —
//! `Rc::clone` where the member is already a refcounted value, and the
//! [`Member`] view (a refcount around the borrow) where the member is the
//! document's own. Nothing is decoded and no member is rebuilt.
//!
//! The generic container impls require `Member` of their parameter rather
//! than `SanshoTrait`: every `Member` impl names a concrete member type, so
//! the solver has one step to unfold and can never re-enter the container
//! impl. That is what keeps a generic impl from overflowing the solver.
//!
//! Only the methods a container can answer are written: everything else
//! (a map is not a string, an array has no members, a scalar is not a
//! container) comes from the trait's defaults, which are the errors.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::rc::Rc;

use crate::error::SanshoError;
use crate::view::{Kind, Member, SanshoCarrier, SanshoNumber, SanshoTrait};

// ---------------------------------------------------------------------------
// The member views — one per concrete member type
// ---------------------------------------------------------------------------

/// A member that is the document's own text.
impl<'a> Member<'a> for &'a String {
    fn carrier(self) -> SanshoCarrier<'a> {
        Rc::new(self)
    }
}

/// A member that is a document-backed set of strings.
impl<'a> Member<'a> for &'a BTreeSet<String> {
    fn carrier(self) -> SanshoCarrier<'a> {
        Rc::new(self)
    }
}

/// A member that is an in-memory CBOR value.
impl<'a> Member<'a> for &'a serde_cbor::Value {
    fn carrier(self) -> SanshoCarrier<'a> {
        Rc::new(self)
    }
}

/// A member that is an in-memory JSON value.
impl<'a> Member<'a> for &'a serde_json::Value {
    fn carrier(self) -> SanshoCarrier<'a> {
        Rc::new(self)
    }
}

// Still to come, one by one: the remaining member views —
//
//   impl<'a> Member<'a> for SanshoCarrier<'a>                                  // already a carrier
//   impl<'a> Member<'a> for &'a HashSet<String>
//   impl<'a> Member<'a> for &'a BTreeMap<String, BTreeSet<String>>             // an item's connection map
//   impl<'a> Member<'a> for &'a Option<String>                                 // an item's MIME type
//   impl<'a> Member<'a> for &'a Option<serde_cbor::Value>                      // an item's body
//   impl<'a> Member<'a> for &'a [u8]                                           // a CBOR document's bytes
//   impl<'a> Member<'a> for &'a Vec<SanshoCarrier<'a>>                         // a computed array
//   impl<'a> Member<'a> for &'a SanshoCarrier<'a>                              // a reference to a carrier
//
// ---------------------------------------------------------------------------
// Arrays
// ---------------------------------------------------------------------------
impl<'a> SanshoTrait<'a> for Vec<SanshoCarrier<'a>> {
    fn kind(&self) -> Kind {
        Kind::Array
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        Ok(self.get(index).map(Rc::clone))
    }

    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        Ok((*self).clone())
    }
    fn as_array_str<'s>(&'s self) -> Result<Box<dyn Iterator<Item = &'s str> + 's>, SanshoError> {
        // the elements may be any kind; the text-bearing ones are yielded
        Ok(Box::new(
            self.iter().filter_map(|element| element.as_str().ok()),
        ))
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        Ok(self.len())
    }

    fn to_string_sansho(&self) -> String {
        let mut out = String::from("[");
        for (position, member) in self.iter().enumerate() {
            if position > 0 {
                out.push(',');
            }
            out.push_str(&member.to_string_sansho());
        }
        out.push(']');
        out
    }
}

impl<'a> SanshoTrait<'a> for &'a Vec<SanshoCarrier<'a>> {
    fn kind(&self) -> Kind {
        Kind::Array
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        Ok(self.get(index).map(Rc::clone))
    }

    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        Ok((*self).clone())
    }
    fn as_array_str<'s>(&'s self) -> Result<Box<dyn Iterator<Item = &'s str> + 's>, SanshoError> {
        // the elements may be any kind; the text-bearing ones are yielded
        Ok(Box::new(
            self.iter().filter_map(|element| element.as_str().ok()),
        ))
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        Ok(self.len())
    }

    fn to_string_sansho(&self) -> String {
        let mut out = String::from("[");
        for (position, member) in self.iter().enumerate() {
            if position > 0 {
                out.push(',');
            }
            out.push_str(&member.to_string_sansho());
        }
        out.push(']');
        out
    }
}

/// The borrowed set of strings (an item's connection targets): each member
/// is handed out through its [`Member`] view — a refcount around the borrow,
/// never a copy.
impl<'a> SanshoTrait<'a> for &'a BTreeSet<String> {
    fn kind(&self) -> Kind {
        Kind::Array
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        Ok(self.iter().nth(index).map(Member::carrier))
    }

    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        Ok(self.iter().map(Member::carrier).collect())
    }
    fn as_array_str<'s>(&'s self) -> Result<Box<dyn Iterator<Item = &'s str> + 's>, SanshoError> {
        // every element is text
        Ok(Box::new(self.iter().map(|text| text.as_str())))
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        Ok(self.len())
    }

    fn to_string_sansho(&self) -> String {
        let mut out = String::from("[");
        for (position, member) in self.iter().enumerate() {
            if position > 0 {
                out.push(',');
            }
            out.push_str(member);
        }
        out.push(']');
        out
    }
}

/// The borrowed set of strings, in the set's own order.
impl<'a> SanshoTrait<'a> for &'a HashSet<String> {
    fn kind(&self) -> Kind {
        Kind::Array
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        Ok(self.iter().nth(index).map(Member::carrier))
    }

    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        Ok(self.iter().map(Member::carrier).collect())
    }
    fn as_array_str<'s>(&'s self) -> Result<Box<dyn Iterator<Item = &'s str> + 's>, SanshoError> {
        // every element is text
        Ok(Box::new(self.iter().map(|text| text.as_str())))
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        Ok(self.len())
    }

    fn to_string_sansho(&self) -> String {
        let mut out = String::from("[");
        out.push_str(
            &self
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .join(", "),
        );
        out.push(']');
        out
    }
}

impl<'a> SanshoTrait<'a> for HashSet<String> {
    fn kind(&self) -> Kind {
        Kind::Array
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        Ok(self
            .iter()
            .nth(index)
            .map(|v| Rc::new(v.to_string()) as SanshoCarrier<'a>))
    }

    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        Ok(self
            .iter()
            .map(|v| Rc::new(v.to_string()) as SanshoCarrier<'a>)
            .collect())
    }
    fn as_array_str<'s>(&'s self) -> Result<Box<dyn Iterator<Item = &'s str> + 's>, SanshoError> {
        // every element is text
        Ok(Box::new(self.iter().map(|text| text.as_str())))
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        Ok(self.len())
    }

    fn to_string_sansho(&self) -> String {
        let mut out = String::from("[");
        out.push_str(
            &self
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>()
                .join(", "),
        );
        out.push(']');
        out
    }
}

// ---------------------------------------------------------------------------
// Objects
// ---------------------------------------------------------------------------

// A carrier-valued map needs no implementation of its own: the generic map
// impls below cover it through `Member for SanshoCarrier`, and a second impl
// for `&HashMap<String, SanshoCarrier>` would conflict with them.
//
// impl<'a> SanshoTrait<'a> for &'a HashMap<String, SanshoCarrier<'a>> { … }

/// The borrowed ordered map (an item's connection map): the map lives in the
/// document, so each member is handed out through its [`Member`] view and
/// each key is the map's own `&'a str`.
impl<'a, V> SanshoTrait<'a> for &'a BTreeMap<String, V>
where
    &'a V: Member<'a>,
{
    fn kind(&self) -> Kind {
        Kind::Object
    }

    fn get_key(&self, name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        Ok(self.get(name).map(Member::carrier))
    }

    fn as_object(&self) -> Result<HashMap<&'a str, SanshoCarrier<'a>>, SanshoError> {
        Ok(self
            .iter()
            .map(|(key, member)| (key.as_str(), member.carrier()))
            .collect())
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        Ok(self.len())
    }

    fn to_string_sansho(&self) -> String {
        let mut out = String::from("{");
        for (position, (key, member)) in self.iter().enumerate() {
            if position > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str(": ");
            out.push_str(&member.carrier().to_string_sansho());
        }
        out.push('}');
        out
    }
}

/// The borrowed hash map: each member is handed out through its [`Member`]
/// view and each key is the map's own `&'a str`.
impl<'a, V> SanshoTrait<'a> for &'a HashMap<String, V>
where
    &'a V: Member<'a>,
{
    fn kind(&self) -> Kind {
        Kind::Object
    }

    fn get_key(&self, name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        Ok(self.get(name).map(Member::carrier))
    }

    fn as_object(&self) -> Result<HashMap<&'a str, SanshoCarrier<'a>>, SanshoError> {
        Ok(self
            .iter()
            .map(|(key, member)| (key.as_str(), member.carrier()))
            .collect())
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        Ok(self.len())
    }

    fn to_string_sansho(&self) -> String {
        let mut out = String::from("{");
        for (position, (key, member)) in self.iter().enumerate() {
            if position > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str(": ");
            out.push_str(&member.carrier().to_string_sansho());
        }
        out.push('}');
        out
    }
}

/// The borrowed hash map: each member is handed out through its [`Member`]
/// view and each key is the map's own `&'a str`.
impl<'a> SanshoTrait<'a> for HashMap<String, SanshoCarrier<'a>> {
    fn kind(&self) -> Kind {
        Kind::Object
    }

    fn get_key(&self, name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        Ok(self.get(name).cloned())
    }

    fn as_object(&self) -> Result<HashMap<&str, SanshoCarrier<'a>>, SanshoError> {
        Ok(self
            .iter()
            .map(|(key, member)| (key.as_str(), member.clone()))
            .collect())
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        Ok(self.len())
    }

    fn to_string_sansho(&self) -> String {
        let mut out = String::from("{");

        out.push_str(
            &self
                .iter()
                .map(|(k, v)| format!("{k}: {}", v.to_string_sansho()))
                .collect::<Vec<String>>()
                .join(", "),
        );
        out.push('}');
        out
    }
}

// ---------------------------------------------------------------------------
// Optional members
// ---------------------------------------------------------------------------

/// The computed optional member: `null`, or the member's own surface.
impl<'a> SanshoTrait<'a> for &'a Option<SanshoCarrier<'a>> {
    fn kind(&self) -> Kind {
        match self {
            Some(member) => member.kind(),
            None => Kind::Null,
        }
    }

    fn as_str(&self) -> Result<&'a str, SanshoError> {
        match self {
            Some(member) => member.as_str(),
            None => Err(SanshoError::Evaluation {
                message: "Not a String type".to_string(),
            }),
        }
    }

    fn as_bytes(&self) -> Result<&'a [u8], SanshoError> {
        match self {
            Some(member) => member.as_bytes(),
            None => Err(SanshoError::Evaluation {
                message: "Not a byte carrier".to_string(),
            }),
        }
    }

    fn as_sansho_number(&self) -> Result<SanshoNumber, SanshoError> {
        match self {
            Some(member) => member.as_sansho_number(),
            None => Err(SanshoError::Evaluation {
                message: "Not a Number type".to_string(),
            }),
        }
    }

    fn as_bool(&self) -> Result<bool, SanshoError> {
        match self {
            Some(member) => member.as_bool(),
            None => Err(SanshoError::Evaluation {
                message: "Not a bool type".to_string(),
            }),
        }
    }

    fn is_null(&self) -> bool {
        self.is_none()
    }

    fn get_key(&self, name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        match self {
            Some(member) => member.get_key(name),
            None => Ok(None),
        }
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        match self {
            Some(member) => member.get_index(index),
            None => Err(SanshoError::Evaluation {
                message: "Not an array type".to_string(),
            }),
        }
    }

    fn as_object(&self) -> Result<HashMap<&'a str, SanshoCarrier<'a>>, SanshoError> {
        match self {
            Some(member) => member.as_object(),
            None => Err(SanshoError::Evaluation {
                message: "requires an object, found null".to_string(),
            }),
        }
    }

    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        match self {
            Some(member) => member.as_elements(),
            None => Err(SanshoError::Evaluation {
                message: "elements requires an array, found null".to_string(),
            }),
        }
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        match self {
            Some(member) => member.container_len(),
            None => Err(SanshoError::Evaluation {
                message: "Not an Array or Object".to_string(),
            }),
        }
    }

    fn to_string_sansho(&self) -> String {
        match self {
            Some(member) => member.to_string_sansho(),
            None => "null".to_string(),
        }
    }
}

/// The borrowed optional member (an item's MIME type, an item's body):
/// `null`, or the member's own surface, borrowed for `'a`.
/// The borrowed optional in-memory CBOR value (an item's body): `null`, or
/// the value's own surface.
///
/// Zero copy: every probe and every navigation delegates straight to the
/// inner `&'a serde_cbor::Value`, so the text, the octets and the map keys
/// are the document's own borrows, and a member is handed out as a refcount
/// around a borrow — the CBOR value is never cloned and never re-encoded.
impl<'a> SanshoTrait<'a> for &'a Option<serde_cbor::Value> {
    fn kind(&self) -> Kind {
        match self {
            Some(value) => value.kind(),
            None => Kind::Null,
        }
    }

    /// The text borrows the document: the binding comes from the place
    /// behind the `'a` reference, so the return is `'a`-bounded.
    fn as_str(&self) -> Result<&'a str, SanshoError> {
        match self {
            Some(serde_cbor::Value::Text(text)) => Ok(text.as_str()),
            _ => Err(SanshoError::Evaluation {
                message: "Not a String type".to_string(),
            }),
        }
    }

    /// The octets borrow the document: same reason as `as_str`.
    fn as_bytes(&self) -> Result<&'a [u8], SanshoError> {
        match self {
            Some(serde_cbor::Value::Bytes(bytes)) => Ok(bytes.as_slice()),
            _ => Err(SanshoError::Evaluation {
                message: "Not a byte carrier".to_string(),
            }),
        }
    }

    fn as_sansho_number(&self) -> Result<SanshoNumber, SanshoError> {
        match self {
            Some(value) => value.as_sansho_number(),
            None => Err(SanshoError::Evaluation {
                message: "Not a number".to_string(),
            }),
        }
    }

    fn as_bool(&self) -> Result<bool, SanshoError> {
        match self {
            Some(value) => value.as_bool(),
            None => Err(SanshoError::Evaluation {
                message: "Not a bool type".to_string(),
            }),
        }
    }

    fn is_null(&self) -> bool {
        self.is_none()
    }

    fn get_key(&self, name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        match self {
            Some(value) => value.get_key(name),
            None => Ok(None),
        }
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        match self {
            Some(value) => value.get_index(index),
            None => Err(SanshoError::Evaluation {
                message: "Not an array type".to_string(),
            }),
        }
    }

    /// The keys borrow the document and each member is a refcount around a
    /// borrow: matched here for the same reason as `as_str`.
    fn as_object(&self) -> Result<HashMap<&'a str, SanshoCarrier<'a>>, SanshoError> {
        match self {
            Some(serde_cbor::Value::Map(members)) => Ok(members
                .iter()
                .filter_map(|(key, member)| match key {
                    serde_cbor::Value::Text(text) => {
                        Some((text.as_str(), Rc::new(member) as SanshoCarrier<'a>))
                    }
                    _ => None,
                })
                .collect()),
            _ => Err(SanshoError::Evaluation {
                message: format!("entries requires an object, found {}", self.kind()),
            }),
        }
    }

    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        match self {
            Some(value) => value.as_elements(),
            None => Err(SanshoError::Evaluation {
                message: "elements requires an array, found null".to_string(),
            }),
        }
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        match self {
            Some(value) => value.container_len(),
            None => Err(SanshoError::Evaluation {
                message: "Not an Array or Object".to_string(),
            }),
        }
    }

    fn to_string_sansho(&self) -> String {
        match self {
            Some(value) => value.to_string_sansho(),
            None => "null".to_string(),
        }
    }
}

/// A borrowed-or-owned value is a value: every read delegates to whatever the
/// `Cow` carries, so a `Cow` and the value inside it answer identically.
///
/// The two bounds name the two implementations it delegates to. The borrowed
/// form carries a reference, and the trait is implemented on references for the
/// document-backed types, so that bound is on the reference and is
/// higher-ranked — a reborrow of any lifetime answers. The owned form carries
/// the value itself, so its bound is on `T::Owned`, the type the `Cow` holds.
///
/// Both arms are written out rather than going through `as_ref()`: that hands
/// back a temporary reference, and a value derived from a temporary cannot be
/// returned from a method.
impl<'a, T> SanshoTrait<'a> for Cow<'a, T>
where
    T: ToOwned,
    T::Owned: SanshoTrait<'a>,
    for<'x> &'x T: SanshoTrait<'x>,
{
    fn kind(&self) -> Kind {
        match self {
            Cow::Borrowed(reference) => reference.kind(),
            Cow::Owned(value) => value.kind(),
        }
    }

    fn as_str(&self) -> Result<&str, SanshoError> {
        match self {
            Cow::Borrowed(reference) => reference.as_str(),
            Cow::Owned(value) => value.as_str(),
        }
    }

    fn as_bytes(&self) -> Result<&[u8], SanshoError> {
        match self {
            Cow::Borrowed(reference) => reference.as_bytes(),
            Cow::Owned(value) => value.as_bytes(),
        }
    }

    fn as_sansho_number(&self) -> Result<SanshoNumber, SanshoError> {
        match self {
            Cow::Borrowed(reference) => reference.as_sansho_number(),
            Cow::Owned(value) => value.as_sansho_number(),
        }
    }

    fn as_f64(&self) -> Result<f64, SanshoError> {
        match self {
            Cow::Borrowed(reference) => reference.as_f64(),
            Cow::Owned(value) => value.as_f64(),
        }
    }

    fn as_bool(&self) -> Result<bool, SanshoError> {
        match self {
            Cow::Borrowed(reference) => reference.as_bool(),
            Cow::Owned(value) => value.as_bool(),
        }
    }

    fn is_null(&self) -> bool {
        match self {
            Cow::Borrowed(reference) => reference.is_null(),
            Cow::Owned(value) => value.is_null(),
        }
    }

    fn get_key(&self, name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        match self {
            Cow::Borrowed(reference) => reference.get_key(name),
            Cow::Owned(value) => value.get_key(name),
        }
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        match self {
            Cow::Borrowed(reference) => reference.get_index(index),
            Cow::Owned(value) => value.get_index(index),
        }
    }

    fn as_object(&self) -> Result<HashMap<&str, SanshoCarrier<'a>>, SanshoError> {
        match self {
            Cow::Borrowed(reference) => reference.as_object(),
            Cow::Owned(value) => value.as_object(),
        }
    }

    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        match self {
            Cow::Borrowed(reference) => reference.as_elements(),
            Cow::Owned(value) => value.as_elements(),
        }
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        match self {
            Cow::Borrowed(reference) => reference.container_len(),
            Cow::Owned(value) => value.container_len(),
        }
    }

    fn to_string_sansho(&self) -> String {
        match self {
            Cow::Borrowed(reference) => reference.to_string_sansho(),
            Cow::Owned(value) => value.to_string_sansho(),
        }
    }

    fn as_array_str<'s>(
        &'s self,
    ) -> Result<Box<dyn Iterator<Item = &'s str> + 's>, SanshoError> {
        match self {
            Cow::Borrowed(reference) => reference.as_array_str(),
            Cow::Owned(value) => value.as_array_str(),
        }
    }
}

impl<'a, T: SanshoTrait<'a>> SanshoTrait<'a> for &'a Option<T>
where
    &'a T: Member<'a>,
{
    fn kind(&self) -> Kind {
        match self {
            Some(member) => member.carrier().kind(),
            None => Kind::Null,
        }
    }

    fn as_str(&self) -> Result<&str, SanshoError> {
        match self {
            Some(v) => v.as_str(),
            None => Err(SanshoError::Evaluation {
                message: "Not a String type".to_string(),
            }),
        }
    }

    fn as_bytes(&self) -> Result<&[u8], SanshoError> {
        match self {
            Some(member) => member.as_bytes(),
            None => Err(SanshoError::Evaluation {
                message: "Not a byte carrier".to_string(),
            }),
        }
    }

    fn as_sansho_number(&self) -> Result<SanshoNumber, SanshoError> {
        match self {
            Some(member) => member.carrier().as_sansho_number(),
            None => Err(SanshoError::Evaluation {
                message: "Not a Number type".to_string(),
            }),
        }
    }

    fn as_bool(&self) -> Result<bool, SanshoError> {
        match self {
            Some(member) => member.carrier().as_bool(),
            None => Err(SanshoError::Evaluation {
                message: "Not a bool type".to_string(),
            }),
        }
    }

    fn is_null(&self) -> bool {
        self.is_none()
    }

    fn get_key(&self, name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        match self {
            Some(member) => member.carrier().get_key(name),
            None => Ok(None),
        }
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        match self {
            Some(member) => member.carrier().get_index(index),
            None => Err(SanshoError::Evaluation {
                message: "Not an array type".to_string(),
            }),
        }
    }

    fn as_object(&self) -> Result<HashMap<&str, SanshoCarrier<'a>>, SanshoError> {
        match self {
            Some(member) => member.as_object(),
            None => Err(SanshoError::Evaluation {
                message: "requires an object, found null".to_string(),
            }),
        }
    }

    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        match self {
            Some(member) => member.carrier().as_elements(),
            None => Err(SanshoError::Evaluation {
                message: "elements requires an array, found null".to_string(),
            }),
        }
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        match self {
            Some(member) => member.carrier().container_len(),
            None => Err(SanshoError::Evaluation {
                message: "Not an Array or Object".to_string(),
            }),
        }
    }

    fn to_string_sansho(&self) -> String {
        match self {
            Some(member) => member.carrier().to_string_sansho(),
            None => "null".to_string(),
        }
    }
}
