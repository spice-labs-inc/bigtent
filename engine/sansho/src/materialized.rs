//! The JSON value source: `&serde_json::Value` implemented directly on the
//! trait surface — references only.
//!
//! Members are BORROWED values (zero-copy): an `&'a serde_json::Value`'s
//! members are `&'a serde_json::Value`, handed out as a refcount around the
//! borrow, and an object's keys are the document's own `&'a str`.
//!
//! The OWNED form (`impl SanshoTrait for serde_json::Value`) is also live, and
//! it is required rather than optional: `J` is the carrier for everything the
//! evaluator CONSTRUCTS — a `Program::Literal` (`` `42` ``, `` `"text"` ``) wraps
//! its `serde_json::Value` in a carrier, and the engine's null value is
//! `Rc::new(serde_json::Value::Null)`. Removing it does not compile.
//!
//! The cost is the reason the reference forms exist: an owned value cannot lend
//! its content for `'a` from a `&self` receiver, so navigating an OWNED document
//! clones each member it steps through, and a string is copied. Reading a
//! borrowed document — a `&serde_json::Value`, a CBOR byte slice — pays neither.

use std::rc::Rc;

use crate::error::SanshoError;
use crate::view::{Kind, SanshoCarrier, SanshoNumber, SanshoTrait};
use serde_json::Value as J;
use std::borrow::Cow;

/// The borrowed form: members are BORROWED values (zero-copy).
impl<'a> SanshoTrait<'a> for J {
    fn kind(&self) -> Kind {
        match *self {
            J::Object(_) => Kind::Object,
            J::Array(_) => Kind::Array,
            J::String(_) => Kind::String,
            J::Number(_) => Kind::Number,
            J::Bool(_) => Kind::Bool,
            J::Null => Kind::Null,
        }
    }

    fn as_str(&self) -> Result<Cow<'_, str>, SanshoError> {
        match self {
            // the text borrows the document
            J::String(text) => Ok(Cow::Borrowed(text.as_str())),
            _ => Err(SanshoError::Evaluation {
                message: "Not a String type".to_string(),
            }),
        }
    }

    fn as_sansho_number(&self) -> Result<SanshoNumber, SanshoError> {
        let number = (*self).as_number().ok_or_else(|| SanshoError::Evaluation {
            message: "Not a Number type".to_string(),
        })?;
        // exact first: serde_json preserves i64/u64 exactly — report them
        // full-width, never narrowed through f64
        if let Some(v) = number.as_i64() {
            return Ok(SanshoNumber::I64(v));
        }
        if let Some(v) = number.as_u64() {
            return Ok(SanshoNumber::U64(v));
        }
        number
            .as_f64()
            .map(SanshoNumber::F64)
            .ok_or_else(|| SanshoError::Evaluation {
                message: "Not a Number type".to_string(),
            })
    }

    fn as_bool(&self) -> Result<bool, SanshoError> {
        match *self {
            J::Bool(value) => Ok(value),
            _ => Err(SanshoError::Evaluation {
                message: "Not a bool type".to_string(),
            }),
        }
    }

    fn is_null(&self) -> bool {
        matches!(*self, J::Null)
    }

    fn get_key(&self, name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        match self {
            J::Object(obj) => Ok(obj
                .get(name)
                .map(|member| Rc::new(member.clone()) as SanshoCarrier<'a>)),
            _ => Err(SanshoError::Evaluation {
                message: "Not an object type".to_string(),
            }),
        }
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        match self {
            J::Array(value) => Ok(value
                .get(index)
                .map(|member| Rc::new(member.clone()) as SanshoCarrier<'a>)),
            _ => Err(SanshoError::Evaluation {
                message: "Not an array type".to_string(),
            }),
        }
    }

    fn as_object(&self) -> Result<std::collections::HashMap<&str, SanshoCarrier<'a>>, SanshoError> {
        match self.as_object() {
            // the keys borrow the document: no key string is built
            Some(object) => Ok(object
                .iter()
                .map(|(key, member)| (key.as_str(), Rc::new(member.clone()) as SanshoCarrier<'a>))
                .collect()),
            None => Err(SanshoError::Evaluation {
                message: format!("entries requires an object, found {}", self.kind()),
            }),
        }
    }

    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        match self.as_array() {
            Some(items) => Ok(items
                .iter()
                .map(|member| Rc::new(member.clone()) as SanshoCarrier<'a>)
                .collect()),
            None => Err(SanshoError::Evaluation {
                message: format!("elements requires an array, found {}", self.kind()),
            }),
        }
    }

    fn as_array_str<'s>(
        &'s self,
    ) -> Result<Box<dyn Iterator<Item = Cow<'s, str>> + 's>, SanshoError> {
        let value: &'s J = self;
        match value.as_array() {
            Some(items) => Ok(Box::new(items.iter().filter_map(|item| match item {
                J::String(text) => Some(Cow::Borrowed(text.as_str())),
                _ => None,
            }))),
            None => Err(SanshoError::Evaluation {
                message: format!("as_array_str requires an array, found {}", value.kind()),
            }),
        }
    }
    fn container_len(&self) -> Result<usize, SanshoError> {
        match self {
            J::Object(map) => Ok(map.len()),
            J::Array(items) => Ok(items.len()),
            _ => Err(SanshoError::Evaluation {
                message: "Not an Array or Object".to_string(),
            }),
        }
    }

    /// The value is already materialized: print its JSON text.
    fn to_string_sansho(&self) -> String {
        (*self).to_string()
    }
}

/// The borrowed form: members are BORROWED values (zero-copy).
impl<'a> SanshoTrait<'a> for &'a J {
    fn kind(&self) -> Kind {
        match **self {
            J::Object(_) => Kind::Object,
            J::Array(_) => Kind::Array,
            J::String(_) => Kind::String,
            J::Number(_) => Kind::Number,
            J::Bool(_) => Kind::Bool,
            J::Null => Kind::Null,
        }
    }

    fn as_str(&self) -> Result<Cow<'a, str>, SanshoError> {
        let value: &'a J = self;
        match value {
            // the text borrows the document
            J::String(text) => Ok(Cow::Borrowed(text.as_str())),
            _ => Err(SanshoError::Evaluation {
                message: "Not a String type".to_string(),
            }),
        }
    }

    fn as_sansho_number(&self) -> Result<SanshoNumber, SanshoError> {
        let number = (**self)
            .as_number()
            .ok_or_else(|| SanshoError::Evaluation {
                message: "Not a Number type".to_string(),
            })?;
        // exact first: serde_json preserves i64/u64 exactly — report them
        // full-width, never narrowed through f64
        if let Some(v) = number.as_i64() {
            return Ok(SanshoNumber::I64(v));
        }
        if let Some(v) = number.as_u64() {
            return Ok(SanshoNumber::U64(v));
        }
        number
            .as_f64()
            .map(SanshoNumber::F64)
            .ok_or_else(|| SanshoError::Evaluation {
                message: "Not a Number type".to_string(),
            })
    }

    fn as_bool(&self) -> Result<bool, SanshoError> {
        match **self {
            J::Bool(value) => Ok(value),
            _ => Err(SanshoError::Evaluation {
                message: "Not a bool type".to_string(),
            }),
        }
    }

    fn is_null(&self) -> bool {
        matches!(**self, J::Null)
    }

    fn get_key(&self, name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        let value: &'a J = self;
        match value {
            J::Object(_) => Ok(value
                .get(name)
                .map(|member| Rc::new(member) as SanshoCarrier<'a>)),
            _ => Err(SanshoError::Evaluation {
                message: "Not an object type".to_string(),
            }),
        }
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        let value: &'a J = self;
        match value {
            J::Array(_) => Ok(value
                .get(index)
                .map(|member| Rc::new(member) as SanshoCarrier<'a>)),
            _ => Err(SanshoError::Evaluation {
                message: "Not an array type".to_string(),
            }),
        }
    }

    fn as_object(&self) -> Result<std::collections::HashMap<&str, SanshoCarrier<'a>>, SanshoError> {
        let value: &'a J = self;
        match value.as_object() {
            // the keys borrow the document: no key string is built
            Some(object) => Ok(object
                .iter()
                .map(|(key, member)| (key.as_str(), Rc::new(member) as SanshoCarrier<'a>))
                .collect()),
            None => Err(SanshoError::Evaluation {
                message: format!("entries requires an object, found {}", self.kind()),
            }),
        }
    }

    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        let value: &'a J = self;
        match value.as_array() {
            Some(items) => Ok(items
                .iter()
                .map(|member| Rc::new(member) as SanshoCarrier<'a>)
                .collect()),
            None => Err(SanshoError::Evaluation {
                message: format!("elements requires an array, found {}", self.kind()),
            }),
        }
    }

    fn as_array_str<'s>(
        &'s self,
    ) -> Result<Box<dyn Iterator<Item = Cow<'s, str>> + 's>, SanshoError> {
        let value: &'s J = self;
        match value.as_array() {
            Some(items) => Ok(Box::new(items.iter().filter_map(|item| match item {
                J::String(text) => Some(Cow::Borrowed(text.as_str())),
                _ => None,
            }))),
            None => Err(SanshoError::Evaluation {
                message: format!("as_array_str requires an array, found {}", value.kind()),
            }),
        }
    }
    fn container_len(&self) -> Result<usize, SanshoError> {
        match **self {
            J::Object(ref map) => Ok(map.len()),
            J::Array(ref items) => Ok(items.len()),
            _ => Err(SanshoError::Evaluation {
                message: "Not an Array or Object".to_string(),
            }),
        }
    }

    /// The value is already materialized: print its JSON text.
    fn to_string_sansho(&self) -> String {
        (**self).to_string()
    }
}

