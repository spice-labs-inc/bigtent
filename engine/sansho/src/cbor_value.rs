//! The in-memory CBOR value source: `serde_cbor::Value` and
//! `&serde_cbor::Value` implemented directly on the trait surface.
//!
//! Two forms, both on the trait:
//! - `serde_cbor::Value` (the value form): the natural call shape —
//!   `lookup(&value, …)`; members are OWNED values (clones).
//! - `&'a serde_cbor::Value` (the borrowed form): members are BORROWED
//!   values (zero-copy).
//!
//! The materialization is [`cbor_value_to_json`] — the exact JSON view
//! the byte source presents (byte strings render base64url, maps
//! expose text keys, tags 2/3 reduce to doubles, other tags and
//! undefined are evaluation errors) — so the two CBOR paths agree
//! byte for byte, refereed by the differential tests.

use crate::error::SanshoError;
use crate::view::{Kind, SanshoNumber, SanshoTrait};
use serde_json::Value as J;

/// The exact JSON view of an in-memory CBOR value — mirrors the byte
/// source's materialization exactly (the engine's `decode_item`):
/// exact integers, base64url byte strings, text-keyed objects
/// (non-negative integer keys render as decimal strings), bignum tags
/// 2/3 reduced to doubles, everything else an evaluation error. The
/// byte-string rendering uses the engine's encoder — one encoder, no
/// drift possible.
pub fn cbor_value_to_json(value: &serde_cbor::Value) -> Result<J, SanshoError> {
    use crate::cbor::base64url_encode;
    match value {
        serde_cbor::Value::Null => Ok(J::Null),
        serde_cbor::Value::Bool(b) => Ok(J::Bool(*b)),
        serde_cbor::Value::Integer(n) => {
            if *n >= 0 {
                u64::try_from(*n)
                    .map(|v| J::Number(serde_json::Number::from(v)))
                    .or_else(|_| {
                        // beyond u64: the byte path renders the bignum
                        // as a double (the documented precision loss)
                        Ok(J::Number(
                            serde_json::Number::from_f64(*n as f64)
                                .unwrap_or_else(|| serde_json::Number::from(0u64)),
                        ))
                    })
            } else {
                i64::try_from(*n)
                    .map(|v| J::Number(serde_json::Number::from(v)))
                    .or_else(|_| {
                        Ok(J::Number(
                            serde_json::Number::from_f64(*n as f64)
                                .unwrap_or_else(|| serde_json::Number::from(0u64)),
                        ))
                    })
            }
        }
        serde_cbor::Value::Float(f) => Ok(J::Number(
            serde_json::Number::from_f64(*f).unwrap_or_else(|| serde_json::Number::from(0u64)),
        )),
        serde_cbor::Value::Bytes(bytes) => Ok(J::String(base64url_encode(bytes))),
        serde_cbor::Value::Text(s) => Ok(J::String(s.clone())),
        serde_cbor::Value::Array(items) => Ok(J::Array(
            items.iter().map(cbor_value_to_json).collect::<Result<_, _>>()?,
        )),
        serde_cbor::Value::Map(map) => {
            let mut out = serde_json::Map::new();
            for (key, value) in map {
                let key = match key {
                    serde_cbor::Value::Text(s) => s.clone(),
                    serde_cbor::Value::Integer(n) if *n >= 0 => n.to_string(),
                    other => {
                        return Err(SanshoError::Evaluation {
                            message: format!("map key must be a text string, found {other:?}"),
                        })
                    }
                };
                out.insert(key, cbor_value_to_json(value)?);
            }
            Ok(J::Object(out))
        }
        // bignums: tags 2 (positive) and 3 (negative); the value is a
        // big-endian byte string, reduced to a double
        serde_cbor::Value::Tag(tag, inner) if *tag == 2 || *tag == 3 => {
            let negative = *tag == 3;
            match inner.as_ref() {
                serde_cbor::Value::Bytes(bytes) => {
                    if bytes.len() > 16 {
                        return Err(SanshoError::Evaluation {
                            message: "bignum exceeds 128 bits".to_string(),
                        });
                    }
                    let mut buffer = [0u8; 16];
                    buffer[16 - bytes.len()..].copy_from_slice(bytes);
                    let magnitude = u128::from_be_bytes(buffer);
                    let value = if negative {
                        magnitude
                            .checked_add(1)
                            .and_then(|m| i128::try_from(m).ok().map(|v| -v))
                            .ok_or_else(|| SanshoError::Evaluation {
                                message: "negative bignum exceeds 128 bits".to_string(),
                            })?
                    } else {
                        i128::try_from(magnitude).map_err(|_| SanshoError::Evaluation {
                            message: "bignum exceeds 128 bits".to_string(),
                        })?
                    };
                    Ok(J::Number(
                        serde_json::Number::from_f64(value as f64)
                            .unwrap_or_else(|| serde_json::Number::from(0u64)),
                    ))
                }
                _ => Err(SanshoError::Evaluation {
                    message: "bignum tag must wrap a byte string".to_string(),
                }),
            }
        }
        serde_cbor::Value::Tag(tag, _) => Err(SanshoError::Evaluation {
            message: format!("unsupported CBOR tag {tag}"),
        }),
        other => Err(SanshoError::Evaluation {
            message: format!("unsupported CBOR value: {other:?}"),
        }),
    }
}

/// The value form: members are owned computed values.
impl<'a> SanshoTrait<'a> for serde_cbor::Value {
    fn kind(&self) -> Kind {
        match self {
            serde_cbor::Value::Null => Kind::Null,
            serde_cbor::Value::Bool(_) => Kind::Bool,
            serde_cbor::Value::Integer(_) | serde_cbor::Value::Float(_) => Kind::Number,
            serde_cbor::Value::Bytes(_) | serde_cbor::Value::Text(_) => Kind::String,
            serde_cbor::Value::Array(_) => Kind::Array,
            serde_cbor::Value::Map(_) => Kind::Object,
            _ => Kind::Null,
        }
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        match self {
            serde_cbor::Value::Text(s) => Some(std::borrow::Cow::Borrowed(s)),
            _ => None,
        }
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        match self {
            serde_cbor::Value::Integer(n) => Some(SanshoNumber::from_i128(*n)),
            serde_cbor::Value::Float(f) => Some(SanshoNumber::F64(*f)),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            serde_cbor::Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    fn is_null(&self) -> bool {
        matches!(self, serde_cbor::Value::Null)
    }

    fn get_key(&self, name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
        match self {
            serde_cbor::Value::Map(map) => {
                map.get(&serde_cbor::Value::Text(name.to_string())).cloned()
            }
            _ => None,
        }
    }

    fn get_index(&self, index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
        match self {
            serde_cbor::Value::Array(items) => items.get(index).cloned(),
            _ => None,
        }
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
        match self {
            serde_cbor::Value::Map(map) => map
                .iter()
                .filter_map(|(key, value)| match key {
                    serde_cbor::Value::Text(s) => Some((s.clone(), value.clone())),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
        match self {
            serde_cbor::Value::Array(items) => items.to_vec(),
            _ => Vec::new(),
        }
    }

    fn container_len(&self) -> Option<usize> {
        match self {
            serde_cbor::Value::Array(items) => Some(items.len()),
            serde_cbor::Value::Map(map) => Some(map.len()),
            _ => None,
        }
    }

    fn counts_toward_aggregation() -> bool {
        // the in-memory value is the CALLER's pre-existing data —
        // nothing is engine-created
        false
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        cbor_value_to_json(self)
    }
}

/// The borrowed form: members are BORROWED values (zero-copy).
impl<'a> SanshoTrait<'a> for &'a serde_cbor::Value {
    fn kind(&self) -> Kind {
        match **self {
            serde_cbor::Value::Null => Kind::Null,
            serde_cbor::Value::Bool(_) => Kind::Bool,
            serde_cbor::Value::Integer(_) | serde_cbor::Value::Float(_) => Kind::Number,
            serde_cbor::Value::Bytes(_) | serde_cbor::Value::Text(_) => Kind::String,
            serde_cbor::Value::Array(_) => Kind::Array,
            serde_cbor::Value::Map(_) => Kind::Object,
            _ => Kind::Null,
        }
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        match **self {
            serde_cbor::Value::Text(ref s) => Some(std::borrow::Cow::Borrowed(s)),
            _ => None,
        }
    }

    fn as_str_ref(&self) -> Option<&'a str> {
        match **self {
            serde_cbor::Value::Text(ref text) => Some(text.as_str()),
            _ => None,
        }
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        match **self {
            serde_cbor::Value::Integer(n) => Some(SanshoNumber::from_i128(n)),
            serde_cbor::Value::Float(f) => Some(SanshoNumber::F64(f)),
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match **self {
            serde_cbor::Value::Bool(b) => Some(b),
            _ => None,
        }
    }

    fn is_null(&self) -> bool {
        matches!(**self, serde_cbor::Value::Null)
    }

    fn get_key(&self, name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
        match **self {
            serde_cbor::Value::Map(ref map) => {
                map.get(&serde_cbor::Value::Text(name.to_string()))
            }
            _ => None,
        }
    }

    fn get_index(&self, index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
        match **self {
            serde_cbor::Value::Array(ref items) => items.get(index),
            _ => None,
        }
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
        match **self {
            serde_cbor::Value::Map(ref map) => map
                .iter()
                .filter_map(|(key, value)| match key {
                    serde_cbor::Value::Text(s) => Some((s.clone(), value)),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
        match **self {
            serde_cbor::Value::Array(ref items) => items.iter().cloned().collect(),
            _ => Vec::new(),
        }
    }

    fn container_len(&self) -> Option<usize> {
        match **self {
            serde_cbor::Value::Array(ref items) => Some(items.len()),
            serde_cbor::Value::Map(ref map) => Some(map.len()),
            _ => None,
        }
    }

    fn counts_toward_aggregation() -> bool {
        false
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        cbor_value_to_json(*self)
    }
}