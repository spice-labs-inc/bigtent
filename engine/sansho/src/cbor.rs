//! The CBOR-to-JSON mapping (SPEC-0001 §4, per RFC 8949 §4.2) and the
//! item decode.
//!
//! One item = one CBOR value at a byte position. The decode is
//! recursive over a forward-only minicbor decoder; every length and
//! offset computation is checked (hostile headers produce input errors,
//! never overflow); indefinite-length encodings are rejected at input;
//! duplicate map keys resolve last-wins.

use crate::error::SanshoError;
use minicbor::data::Type;
use minicbor::Decoder;
use serde_json::Value as J;

pub(crate) fn decode_document(bytes: &[u8]) -> Result<J, SanshoError> {
    let mut decoder = Decoder::new(bytes);
    let value = decode_item(&mut decoder, bytes)?;
    // the input is EXACTLY one document: trailing bytes are an input
    // error
    if decoder.position() != bytes.len() {
        return Err(SanshoError::Input {
            message: format!(
                "trailing bytes after the CBOR document: {} unexpected at position {}",
                bytes.len() - decoder.position(),
                decoder.position()
            ),
        });
    }
    Ok(value)
}

pub(crate) fn decode_at_position(bytes: &[u8], position: usize) -> Result<J, SanshoError> {
    let mut decoder = Decoder::new(bytes);
    decoder.set_position(position);
    decode_item(&mut decoder, bytes)
}

fn input_error(position: usize, what: &str) -> SanshoError {
    SanshoError::Input {
        message: format!("at byte {position}: {what}"),
    }
}

fn decode_item(decoder: &mut Decoder, bytes: &[u8]) -> Result<J, SanshoError> {
    let position = decoder.position();
    let kind = decoder
        .datatype()
        .map_err(|e| input_error(position, &format!("undecodable item: {e}")))?;
    match kind {
        // definite-length containers only; the indefinite forms are
        // rejected at input per the mapping table
        Type::Array => {
            let len = decoder
                .array()
                .map_err(|e| input_error(position, &format!("array header: {e}")))?
                .ok_or_else(|| input_error(position, "indefinite-length array"))?;
            let mut items = Vec::new();
            for _ in 0..len {
                items.push(decode_item(decoder, bytes)?);
            }
            Ok(J::Array(items))
        }
        Type::Map => {
            let len = decoder
                .map()
                .map_err(|e| input_error(position, &format!("map header: {e}")))?
                .ok_or_else(|| input_error(position, "indefinite-length map"))?;
            let mut map = serde_json::Map::new();
            for _ in 0..len {
                let key = decode_map_key(decoder)?;
                let value = decode_item(decoder, bytes)?;
                // duplicate keys resolve last-wins (the mapping table)
                map.insert(key, value);
            }
            Ok(J::Object(map))
        }
        Type::String => {
            let text = decoder
                .str()
                .map_err(|e| input_error(position, &format!("text string: {e}")))?;
            Ok(J::String(text.to_owned()))
        }
        Type::Bytes => {
            let bytes = decoder
                .bytes()
                .map_err(|e| input_error(position, &format!("byte string: {e}")))?;
            Ok(J::String(base64url_encode(bytes)))
        }
        Type::Bool => Ok(J::Bool(
            decoder
                .bool()
                .map_err(|e| input_error(position, &format!("boolean: {e}")))?,
        )),
        Type::Null => {
            decoder
                .null()
                .map_err(|e| input_error(position, &format!("null: {e}")))?;
            Ok(J::Null)
        }
        Type::U8 | Type::U16 | Type::U32 | Type::U64 => {
            let value = decoder
                .u64()
                .map_err(|e| input_error(position, &format!("unsigned: {e}")))?;
            Ok(number_from_u64(value))
        }
        Type::I8 | Type::I16 | Type::I32 | Type::I64 => {
            let value = decoder
                .i64()
                .map_err(|e| input_error(position, &format!("negative integer: {e}")))?;
            Ok(number_from_i64(value))
        }
        // the half-precision form: minicbor exposes no f16 accessor, so
        // the two bytes after the 0xF9 header decode manually (IEEE 754
        // half)
        Type::F16 => {
            let after_header = position + 1;
            let raw = bytes
                .get(after_header..after_header + 2)
                .ok_or_else(|| input_error(position, "truncated half-float"))?;
            let bits = u16::from_be_bytes([raw[0], raw[1]]);
            decoder.set_position(after_header + 2);
            Ok(number_from_f64(half_to_f64(bits)))
        }
        Type::F32 => {
            let value = decoder
                .f32()
                .map_err(|e| input_error(position, &format!("float: {e}")))?;
            Ok(number_from_f64(value as f64))
        }
        Type::F64 => {
            let value = decoder
                .f64()
                .map_err(|e| input_error(position, &format!("float: {e}")))?;
            Ok(number_from_f64(value))
        }
        Type::Tag => {
            let tag = decoder
                .tag()
                .map_err(|e| input_error(position, &format!("tag: {e}")))?;
            match tag.as_u64() {
                // bignums: tags 2 (positive) and 3 (negative); the value
                // is a big-endian byte string, reduced to a double (the
                // documented precision loss)
                2 | 3 => {
                    let bytes = decoder
                        .bytes()
                        .map_err(|e| input_error(position, &format!("bignum: {e}")))?;
                    bignum_to_number(bytes, tag.as_u64() == 3, position)
                }
                other => Err(SanshoError::Evaluation {
                    message: format!("unsupported CBOR tag {other} at byte {position}"),
                }),
            }
        }
        // undefined has no JSON equivalent; the mapping rejects it
        Type::Undefined => Err(SanshoError::Evaluation {
            message: format!("CBOR undefined at byte {position} has no JSON equivalent"),
        }),
        // everything else (unassigned simple values, breaks, indefinite
        // prefixes) is an input rejection
        other => Err(input_error(
            position,
            &format!("unsupported CBOR type: {other:?}"),
        )),
    }
}

fn decode_map_key(decoder: &mut Decoder) -> Result<String, SanshoError> {
    let position = decoder.position();
    // keys are text strings in this mapping (the document model is JSON)
    match decoder.datatype() {
        Ok(Type::String) => {
            let key = decoder.str().map_err(|e| input_error(position, &format!("map key: {e}")))?;
            Ok(key.to_owned())
        }
        Ok(Type::U8 | Type::U16 | Type::U32 | Type::U64) => {
            let key = decoder.u64().map_err(|e| input_error(position, &format!("map key: {e}")))?;
            Ok(key.to_string())
        }
        Ok(other) => Err(input_error(
            position,
            &format!("map key must be a text string, found {other:?}"),
        )),
        Err(e) => Err(input_error(position, &format!("map key: {e}"))),
    }
}

fn number_from_u64(value: u64) -> J {
    // the representation stays the exact integer; the comparison-level
    // double reduction (as_f64) is where the mapping's documented
    // precision loss beyond the exact-double range applies (spec §4)
    J::Number(serde_json::Number::from(value))
}

fn number_from_i64(value: i64) -> J {
    J::Number(serde_json::Number::from(value))
}

fn number_from_f64(value: f64) -> J {
    serde_json::Value::Number(
        serde_json::Number::from_f64(value)
            .unwrap_or_else(|| serde_json::Number::from(0u64)),
    )
}

fn bignum_to_number(bytes: &[u8], negative: bool, position: usize) -> Result<J, SanshoError> {
    if bytes.len() > 16 {
        return Err(SanshoError::Evaluation {
            message: format!("bignum at byte {position} exceeds 128 bits"),
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
                message: format!("negative bignum at byte {position} exceeds 128 bits"),
            })?
    } else {
        i128::try_from(magnitude).map_err(|_| SanshoError::Evaluation {
            message: format!("bignum at byte {position} exceeds 128 bits"),
        })?
    };
    Ok(number_from_f64(value as f64))
}

/// base64url, no padding (RFC 4648 §5, as the mapping requires).
pub(crate) fn base64url_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[(triple >> 18) as usize & 0x3F] as char);
        out.push(ALPHABET[(triple >> 12) as usize & 0x3F] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[(triple >> 6) as usize & 0x3F] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[triple as usize & 0x3F] as char);
        }
    }
    out
}

/// IEEE 754 half-precision to double.
fn half_to_f64(bits: u16) -> f64 {
    let sign = if bits & 0x8000 != 0 { -1.0 } else { 1.0 };
    let exponent = ((bits >> 10) & 0x1F) as i32;
    let mantissa = (bits & 0x03FF) as f64;
    match exponent {
        0 => {
            if mantissa == 0.0 {
                sign * 0.0
            } else {
                sign * (mantissa / 1024.0) * (2.0f64).powi(-14)
            }
        }
        0x1F => {
            if mantissa == 0.0 {
                sign * f64::INFINITY
            } else {
                f64::NAN
            }
        }
        e => sign * (1.0 + mantissa / 1024.0) * (2.0f64).powi(e - 15),
    }
}
