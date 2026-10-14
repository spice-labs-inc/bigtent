//! The byte source: `&[u8]` — one CBOR document, walked in place with
//! minicbor.
//!
//! A member IS a subslice of the document: navigation reads the header and
//! the key bytes where they lie and hands the member on as a refcount
//! around `&'a [u8]` (which implements this trait), so a traversal copies
//! nothing — no decoded values, no key strings, no member containers. The
//! only allocation is the refcount itself.
//!
//! `to_string_sansho` is the exception, and the trait's own comment says so:
//! it materializes the document's data structure with `serde_cbor` and
//! prints it. That is a debugging aid, not part of an evaluation.

use minicbor::Decoder;
use minicbor::data::Type;
use std::rc::Rc;

use crate::error::SanshoError;
use crate::view::{Kind, SanshoCarrier, SanshoNumber, SanshoTrait};

/// The minicbor type of the item at the start of `bytes`, or `None` when
/// the bytes are empty or the header cannot be read. Peeking does not
/// consume.
fn item_type(bytes: &[u8]) -> Option<Type> {
    Decoder::new(bytes).datatype().ok()
}

/// The member's extent, as a refcounted borrow of the document: the member
/// is the document's own bytes, so wrapping it in a refcount copies nothing.
fn member<'a>(bytes: &'a [u8], start: usize, end: usize) -> SanshoCarrier<'a> {
    Rc::new(&bytes[start..end])
}

/// The decoder's error, as the engine's Input error: a malformed or
/// truncated document.
fn decoder_error(error: minicbor::decode::Error) -> SanshoError {
    SanshoError::Input {
        message: error.to_string(),
    }
}

impl<'a> SanshoTrait<'a> for &'a [u8] {
    fn kind(&self) -> Kind {
        match item_type(self) {
            Some(Type::Map) | Some(Type::MapIndef) => Kind::Object,
            Some(Type::Array) | Some(Type::ArrayIndef) => Kind::Array,
            Some(Type::String) | Some(Type::StringIndef) => Kind::String,
            Some(Type::Bytes) | Some(Type::BytesIndef) => Kind::Bytes,
            Some(Type::Bool) => Kind::Bool,
            Some(Type::Null) | Some(Type::Undefined) => Kind::Null,
            Some(Type::U8) | Some(Type::U16) | Some(Type::U32) | Some(Type::U64)
            | Some(Type::I8) | Some(Type::I16) | Some(Type::I32) | Some(Type::I64)
            | Some(Type::Int) | Some(Type::F16) | Some(Type::F32) | Some(Type::F64)
            | Some(Type::Tag) => Kind::Number,
            _ => Kind::Null,
        }
    }

    fn as_str(&self) -> Result<&'a str, SanshoError> {
        match item_type(self) {
            // the text borrows the document
            Some(Type::String) => Decoder::new(self).str().map_err(decoder_error),
            _ => Err(SanshoError::Evaluation {
                message: "Not a String type".to_string(),
            }),
        }
    }

    fn as_bytes(&self) -> Result<&'a [u8], SanshoError> {
        match item_type(self) {
            // the octets are the document's own bytes: no copy, no encoding
            Some(Type::Bytes) => Decoder::new(self).bytes().map_err(decoder_error),
            _ => Err(SanshoError::Evaluation {
                message: "Not a byte carrier".to_string(),
            }),
        }
    }

    fn as_sansho_number(&self) -> Result<SanshoNumber, SanshoError> {
        let mut decoder = Decoder::new(self);
        match decoder.datatype().map_err(decoder_error)? {
            Type::U8 | Type::U16 | Type::U32 | Type::U64 => {
                decoder.u64().map(SanshoNumber::U64).map_err(decoder_error)
            }
            Type::I8 | Type::I16 | Type::I32 | Type::I64 => {
                decoder.i64().map(SanshoNumber::I64).map_err(decoder_error)
            }
            Type::Int => decoder
                .int()
                .map(|value| SanshoNumber::from_i128(i128::from(value)))
                .map_err(decoder_error),
            Type::F16 => {
                // the half-precision reader is behind minicbor's `half`
                // feature: read the two payload bytes in place
                match self
                    .get(1..3)
                    .and_then(|payload| <[u8; 2]>::try_from(payload).ok())
                {
                    Some(bits) => Ok(SanshoNumber::F64(crate::cbor::half_to_f64(
                        u16::from_be_bytes(bits),
                    ))),
                    None => Err(SanshoError::Evaluation {
                        message: "truncated half-precision float".to_string(),
                    }),
                }
            }
            Type::F32 => decoder
                .f32()
                .map(|value| SanshoNumber::F64(value as f64))
                .map_err(decoder_error),
            Type::F64 => decoder.f64().map(SanshoNumber::F64).map_err(decoder_error),
            _ => Err(SanshoError::Evaluation {
                message: "Not a Number type".to_string(),
            }),
        }
    }

    fn as_bool(&self) -> Result<bool, SanshoError> {
        let mut decoder = Decoder::new(self);
        match decoder.datatype().map_err(decoder_error)? {
            Type::Bool => decoder.bool().map_err(decoder_error),
            _ => Err(SanshoError::Evaluation {
                message: "Not a bool type".to_string(),
            }),
        }
    }

    fn is_null(&self) -> bool {
        matches!(item_type(self), Some(Type::Null))
    }

    fn get_key(&self, name: &str) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        let bytes: &'a [u8] = self;
        let mut decoder = Decoder::new(bytes);
        let count = match decoder.datatype().map_err(decoder_error)? {
            Type::Map => {
                decoder
                    .map()
                    .map_err(decoder_error)?
                    .ok_or_else(|| SanshoError::Input {
                        message: "indefinite-length map".to_string(),
                    })?
            }
            _ => {
                return Err(SanshoError::Evaluation {
                    message: "Not an object type".to_string(),
                });
            }
        };
        for _ in 0..count {
            let key = match decoder.datatype() {
                Ok(Type::String) => decoder.str().map_err(decoder_error)?,
                // a non-text key is not a member: skip it and its value
                _ => {
                    decoder.skip().map_err(decoder_error)?;
                    decoder.skip().map_err(decoder_error)?;
                    continue;
                }
            };
            let start = decoder.position();
            decoder.skip().map_err(decoder_error)?;
            let end = decoder.position();
            if key == name {
                return Ok(Some(member(bytes, start, end)));
            }
        }
        Ok(None)
    }

    fn get_index(&self, index: usize) -> Result<Option<SanshoCarrier<'a>>, SanshoError> {
        let bytes: &'a [u8] = self;
        let mut decoder = Decoder::new(bytes);
        let count = match decoder.datatype().map_err(decoder_error)? {
            Type::Array => {
                decoder
                    .array()
                    .map_err(decoder_error)?
                    .ok_or_else(|| SanshoError::Input {
                        message: "indefinite-length array".to_string(),
                    })?
            }
            _ => {
                return Err(SanshoError::Evaluation {
                    message: "Not an array type".to_string(),
                });
            }
        };
        for position in 0..count {
            let start = decoder.position();
            decoder.skip().map_err(decoder_error)?;
            let end = decoder.position();
            if position as usize == index {
                return Ok(Some(member(bytes, start, end)));
            }
        }
        Ok(None)
    }

    fn entries(&self) -> Result<Vec<(&'a str, SanshoCarrier<'a>)>, SanshoError> {
        let bytes: &'a [u8] = self;
        let mut decoder = Decoder::new(bytes);
        let count = match decoder.datatype().map_err(decoder_error)? {
            Type::Map => {
                decoder
                    .map()
                    .map_err(decoder_error)?
                    .ok_or_else(|| SanshoError::Input {
                        message: "indefinite-length map".to_string(),
                    })?
            }
            _ => {
                return Err(SanshoError::Evaluation {
                    message: format!("entries requires an object, found {}", self.kind()),
                });
            }
        };
        let mut members = Vec::with_capacity(count.min(1024) as usize);
        for _ in 0..count {
            // the key borrows the document: no key string is built
            let key = match decoder.datatype() {
                Ok(Type::String) => decoder.str().map_err(decoder_error)?,
                // a non-text key is not a member: skip it and its value
                _ => {
                    decoder.skip().map_err(decoder_error)?;
                    decoder.skip().map_err(decoder_error)?;
                    continue;
                }
            };
            let start = decoder.position();
            decoder.skip().map_err(decoder_error)?;
            let end = decoder.position();
            members.push((key, member(bytes, start, end)));
        }
        Ok(members)
    }

    fn elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        let bytes: &'a [u8] = self;
        let mut decoder = Decoder::new(bytes);
        let count = match decoder.datatype().map_err(decoder_error)? {
            Type::Array => {
                decoder
                    .array()
                    .map_err(decoder_error)?
                    .ok_or_else(|| SanshoError::Input {
                        message: "indefinite-length array".to_string(),
                    })?
            }
            _ => {
                return Err(SanshoError::Evaluation {
                    message: format!("elements requires an array, found {}", self.kind()),
                });
            }
        };
        let mut members = Vec::with_capacity(count.min(1024) as usize);
        for _ in 0..count {
            let start = decoder.position();
            decoder.skip().map_err(decoder_error)?;
            let end = decoder.position();
            members.push(member(bytes, start, end));
        }
        Ok(members)
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        let mut decoder = Decoder::new(self);
        match decoder.datatype().map_err(decoder_error)? {
            Type::Map => decoder
                .map()
                .map_err(decoder_error)?
                .map(|count| count as usize)
                .ok_or_else(|| SanshoError::Input {
                    message: "indefinite-length map".to_string(),
                }),
            Type::Array => decoder
                .array()
                .map_err(decoder_error)?
                .map(|count| count as usize)
                .ok_or_else(|| SanshoError::Input {
                    message: "indefinite-length array".to_string(),
                }),
            _ => Err(SanshoError::Evaluation {
                message: "Not an Array or Object".to_string(),
            }),
        }
    }

    /// The trait's comment sanctions this being expensive: it materializes
    /// the document's data structure with `serde_cbor` and prints it. This
    /// is a debugging aid, never part of an evaluation.
    fn to_string_sansho(&self) -> String {
        match serde_cbor::from_slice::<serde_cbor::Value>(self) {
            Ok(value) => format!("{value:?}"),
            Err(error) => format!("<undecodable document: {error}>"),
        }
    }
}
