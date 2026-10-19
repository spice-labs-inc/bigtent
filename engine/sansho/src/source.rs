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
use std::collections::HashMap;
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
        // Every minicbor type is named explicitly, with no wildcard: a
        // minicbor upgrade that adds a variant fails this build rather than
        // silently becoming a kind it is not. The three variants that are
        // not values — a break byte outside a container, a simple value with
        // no JMESPath type, and a byte minicbor cannot classify — answer
        // null, and every read of them answers an error, so the kind is the
        // least-wrong answer rather than a claim about the document.
        match item_type(self) {
            Some(Type::Map) | Some(Type::MapIndef) => Kind::Object,
            Some(Type::Array) | Some(Type::ArrayIndef) => Kind::Array,
            Some(Type::String) | Some(Type::StringIndef) => Kind::String,
            Some(Type::Bytes) | Some(Type::BytesIndef) => Kind::Bytes,
            Some(Type::Bool) => Kind::Bool,
            Some(Type::Null) | Some(Type::Undefined) => Kind::Null,
            Some(Type::U8)
            | Some(Type::U16)
            | Some(Type::U32)
            | Some(Type::U64)
            | Some(Type::I8)
            | Some(Type::I16)
            | Some(Type::I32)
            | Some(Type::I64)
            | Some(Type::Int)
            | Some(Type::F16)
            | Some(Type::F32)
            | Some(Type::F64)
            | Some(Type::Tag) => Kind::Number,
            Some(Type::Simple) | Some(Type::Break) | Some(Type::Unknown(_)) => Kind::Null,
            // a document with no decodable first item at all
            None => Kind::Null,
        }
    }

    fn as_str(&self) -> Result<&'a str, SanshoError> {
        match self.kind() {
            // a chunked text item has no single `&str` to lend: it is named
            // rather than reported as a type error
            Kind::String if item_type(self) == Some(Type::StringIndef) => Err(SanshoError::Input {
                message: "indefinite-length text".to_string(),
            }),
            // the text borrows the document
            Kind::String => Decoder::new(self).str().map_err(decoder_error),
            _ => Err(SanshoError::Evaluation {
                message: "Not a String type".to_string(),
            }),
        }
    }

    fn as_bytes(&self) -> Result<&'a [u8], SanshoError> {
        match self.kind() {
            // a chunked byte string has no single slice to lend
            Kind::Bytes if item_type(self) == Some(Type::BytesIndef) => Err(SanshoError::Input {
                message: "indefinite-length byte string".to_string(),
            }),
            // the octets are the document's own bytes: no copy, no encoding
            Kind::Bytes => Decoder::new(self).bytes().map_err(decoder_error),
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
        let count = match self.kind() {
            Kind::Object => {
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
        let count = match self.kind() {
            Kind::Array => {
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

    fn as_object(&self) -> Result<std::collections::HashMap<&str, SanshoCarrier<'a>>, SanshoError> {
        let bytes: &'a [u8] = self;
        let mut decoder = Decoder::new(bytes);
        let count = match self.kind() {
            Kind::Object => {
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
        let mut members = HashMap::new();
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
            members.insert(key, member(bytes, start, end));
        }
        Ok(members)
    }

    fn as_elements(&self) -> Result<Vec<SanshoCarrier<'a>>, SanshoError> {
        let bytes: &'a [u8] = self;
        let mut decoder = Decoder::new(bytes);
        let count = match self.kind() {
            Kind::Array => {
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

    fn as_array_str<'s>(
        &'s self,
    ) -> Result<Box<dyn Iterator<Item = &'s str> + 's>, SanshoError> {
        match self.kind() {
            Kind::Array => {
                let slice: &'s [u8] = self;
                let mut decoder = Decoder::new(slice);
                // a definite array gives its element count; an indefinite one
                // gives None and ends at its break byte
                let mut remaining = decoder.array().map_err(decoder_error)?;
                Ok(Box::new(std::iter::from_fn(move || loop {
                    if let Some(left) = remaining {
                        if left == 0 {
                            return None;
                        }
                        remaining = Some(left - 1);
                    }
                    let kind = decoder.datatype().ok()?;
                    if kind == Type::Break {
                        remaining = Some(0);
                        return None;
                    }
                    if kind == Type::String {
                        // an element whose text is not valid UTF-8 is
                        // skipped, not reported: this is the lenient read
                        if let Ok(text) = decoder.str() {
                            return Some(text);
                        }
                    } else if kind == Type::StringIndef {
                        // a chunked text item has no single `&str`: the
                        // lenient read passes over it
                    } else if decoder.skip().is_err() {
                        // a malformed element: there is nothing to resynchronize
                        // to, so the iteration ends
                        return None;
                    }
                })))
            }
            _ => Err(SanshoError::Evaluation {
                message: format!("as_array_str requires an array, found {}", self.kind()),
            }),
        }
    }

    fn container_len(&self) -> Result<usize, SanshoError> {
        let mut decoder = Decoder::new(self);
        match self.kind() {
            Kind::Object => decoder
                .map()
                .map_err(decoder_error)?
                .map(|count| count as usize)
                .ok_or_else(|| SanshoError::Input {
                    message: "indefinite-length map".to_string(),
                }),
            Kind::Array => decoder
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
