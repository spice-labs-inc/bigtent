//! The in-memory CBOR value source: `&serde_cbor::Value` implemented
//! directly on the trait surface.
//!
//! Members are BORROWED values (zero-copy): an `&'a serde_cbor::Value`'s
//! members are `&'a serde_cbor::Value`, handed out as a refcount around the
//! borrow.
//!
//! The view matches the byte source's, item for item: byte strings are
//! [`Kind::Bytes`] (their octets borrowed, never encoded), maps expose text
//! keys, tags 2/3 reduce to doubles, other tags and `undefined` are
//! evaluation errors (reported by the probes that meet them, never silently
//! as a value).

use std::rc::Rc;

use crate::error::SanshoError;
use crate::view::{Kind, SanshoCarrier, SanshoNumber, SanshoTrait};

/// The borrowed form: members are BORROWED values (zero-copy).
impl<'a> SanshoTrait<'a> for &'a serde_cbor::Value {
    fn kind(&self) -> Kind {
        match **self {
            serde_cbor::Value::Null => Kind::Null,
            serde_cbor::Value::Bool(_) => Kind::Bool,
            serde_cbor::Value::Integer(_) | serde_cbor::Value::Float(_) => Kind::Number,
            serde_cbor::Value::Text(_) => Kind::String,
            // a byte string is its own kind: octets, not text
            serde_cbor::Value::Bytes(_) => Kind::Bytes,
            serde_cbor::Value::Array(_) => Kind::Array,
            serde_cbor::Value::Map(_) => Kind::Object,
            _ => Kind::Null,
        }
    }

    fn as_str(&self) -> Result<&'a str, SanshoError> {
        let value: &'a serde_cbor::Value = self;
        match value {
            // the text borrows the document
            serde_cbor::Value::Text(text) => Ok(text.as_str()),
            _ => Err(SanshoError::Evaluation {
                message: "Not a String type".to_string(),
            }),
        }
    }

    fn as_bytes(&self) -> Result<&'a [u8], SanshoError> {
        let value: &'a serde_cbor::Value = self;
        match value {
            // the octets are the document's own bytes: no copy, no encoding
            serde_cbor::Value::Bytes(bytes) => Ok(bytes.as_slice()),
            _ => Err(SanshoError::Evaluation {
                message: "Not a byte carrier".to_string(),
            }),
        }
    }

    fn as_sansho_number(&self) -> Result<SanshoNumber, SanshoError> {
        match **self {
            serde_cbor::Value::Integer(n) => Ok(SanshoNumber::from_i128(n)),
            serde_cbor::Value::Float(f) => Ok(SanshoNumber::F64(f)),
            _ => Err(SanshoError::Evaluation {
                message: "Not a Number type".to_string(),
            }),
        }
    }

    fn as_bool(&self) -> Result<bool, SanshoError> {
        match **self {
            serde_cbor::Value::Bool(value) => Ok(value),
            _ => Err(SanshoError::Evaluation {
                message: "Not a bool type".to_string(),
            }),
        }
    }

    fn is_null(&self) -> bool {
        matches!(**self, serde_cbor::Value::Null)
    }

    fn get_key(&self, name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        let value: &'a serde_cbor::Value = self;
        match value {
            serde_cbor::Value::Map(map) => Ok(map
                .get(&serde_cbor::Value::Text(name.to_string()))
                .map(|member| Rc::new(member) as SanshoCarrier<'a>)),
            _ => Err(SanshoError::Evaluation {
                message: "Not an object type".to_string(),
            }),
        }
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        let value: &'a serde_cbor::Value = self;
        match value {
            serde_cbor::Value::Array(items) => Ok(items
                .get(index)
                .map(|member| Rc::new(member) as SanshoCarrier<'a>)),
            _ => Err(SanshoError::Evaluation {
                message: "Not an array type".to_string(),
            }),
        }
    }

    fn entries(&self) -> Result<Vec<(&'a str, SanshoCarrier<'a>)>, SanshoError> {
        let value: &'a serde_cbor::Value = self;
        match value {
            // a non-text key is not a member: it is not exposed. The map's
            // own order is the member order.
            serde_cbor::Value::Map(members) => Ok(members
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

    fn elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        let value: &'a serde_cbor::Value = self;
        match value {
            serde_cbor::Value::Array(items) => Ok(items
                .iter()
                .map(|member| Rc::new(member) as SanshoCarrier<'a>)
                .collect()),
            _ => Err(SanshoError::Evaluation {
                message: format!("elements requires an array, found {}", self.kind()),
            }),
        }
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        match **self {
            serde_cbor::Value::Array(ref items) => Ok(items.len()),
            serde_cbor::Value::Map(ref map) => Ok(map.len()),
            _ => Err(SanshoError::Evaluation {
                message: "Not an Array or Object".to_string(),
            }),
        }
    }

    /// The value is already materialized: print its data structure.
    fn to_string_sansho(&self) -> String {
        format!("{:?}", **self)
    }
}
