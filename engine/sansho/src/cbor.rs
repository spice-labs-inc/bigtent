//! The CBOR-to-JSON mapping and the item decode, over minicbor.
//!
//! One item = one CBOR value at the start of a byte slice. The decode is
//! recursive over a forward-only minicbor decoder; indefinite-length
//! encodings are rejected at input; duplicate map keys resolve last-wins;
//! non-text map keys are not members (the mapping exposes text keys only).
//!
//! The mapping, per the trait's documented view:
//! - byte strings (major 2) render as base64url text;
//! - tags 2 and 3 (bignums) reduce to doubles; every other tag is an
//!   evaluation error;
//! - `undefined` is an evaluation error;
//! - integers keep their full width where they fit `i64`/`u64`, and only
//!   genuine bignums reduce to `f64`.

// use crate::error::SanshoError;
// use crate::view::SanshoNumber;
// use minicbor::Decoder;
// use minicbor::data::Type;
// use serde_json::Value as J;
//
// fn input_error(position: usize, what: &str) -> SanshoError {
//     SanshoError::Input {
//         message: format!("at byte {position}: {what}"),
//     }
// }
//
// fn evaluation_error(position: usize, what: &str) -> SanshoError {
//     SanshoError::Evaluation {
//         message: format!("at byte {position}: {what}"),
//     }
// }
//
/// IEEE 754 half-precision to double.
pub(crate) fn half_to_f64(bits: u16) -> f64 {
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
//
// /// RFC 4648 §5 base64url: the URL-safe alphabet, `=`-padded to a multiple
// /// of four. The engine's only byte-string encoder, so the byte source and
// /// every other backend render byte strings identically.
// pub fn base64url_encode(bytes: &[u8]) -> String {
//     const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
//     let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
//     for chunk in bytes.chunks(3) {
//         let b0 = chunk[0] as u32;
//         let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
//         let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
//         let n = (b0 << 16) | (b1 << 8) | b2;
//         out.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
//         out.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
//         if chunk.len() > 1 {
//             out.push(ALPHABET[((n >> 6) & 0x3F) as usize] as char);
//         } else {
//             out.push('=');
//         }
//         if chunk.len() > 2 {
//             out.push(ALPHABET[(n & 0x3F) as usize] as char);
//         } else {
//             out.push('=');
//         }
//     }
//     out
// }
//
// /// A full-width number as a JSON number: the exact `i64`/`u64` where it
// /// fits, `f64` otherwise (a genuine bignum).
// ///
// /// The mapping's number coercion: JSON has no infinity or NaN, so a
// /// non-finite float renders as `0`. `-0.0` is finite and survives.
// fn number_to_json(number: SanshoNumber) -> J {
//     let value = match number {
//         SanshoNumber::I64(v) => Some(serde_json::Number::from(v)),
//         SanshoNumber::U64(v) => Some(serde_json::Number::from(v)),
//         SanshoNumber::F64(v) => {
//             if v.is_finite() {
//                 serde_json::Number::from_f64(v)
//             } else {
//                 // the coercion yields the integer zero: JSON has no
//                 // infinity or NaN, and 0 is the pinned rendering
//                 Some(serde_json::Number::from(0))
//             }
//         }
//     };
//     value.map(J::Number).unwrap_or(J::Null)
// }
//
// /// The big-endian magnitude of a bignum's bytes as a double. Tags 2 and 3
// /// are the bignum encodings; beyond `u64` the value necessarily reduces.
// fn bignum_to_f64(magnitude: &[u8]) -> f64 {
//     magnitude
//         .iter()
//         .fold(0.0, |acc, byte| acc * 256.0 + *byte as f64)
// }
//
// /// One CBOR item at the decoder's current position, as its JSON view.
// fn decode_item(decoder: &mut Decoder<'_>) -> Result<J, SanshoError> {
//     let at = decoder.position();
//     let datatype = decoder
//         .datatype()
//         .map_err(|error| input_error(at, &error.to_string()))?;
//     match datatype {
//         Type::Null => {
//             decoder
//                 .skip()
//                 .map_err(|error| input_error(at, &error.to_string()))?;
//             Ok(J::Null)
//         }
//         Type::Undefined => Err(evaluation_error(at, "CBOR undefined is not a value")),
//         Type::Bool => decoder
//             .bool()
//             .map(J::Bool)
//             .map_err(|error| input_error(at, &error.to_string())),
//         Type::U8 | Type::U16 | Type::U32 | Type::U64 => decoder
//             .u64()
//             .map(|value| number_to_json(SanshoNumber::U64(value)))
//             .map_err(|error| input_error(at, &error.to_string())),
//         Type::I8 | Type::I16 | Type::I32 | Type::I64 => decoder
//             .i64()
//             .map(|value| number_to_json(SanshoNumber::I64(value)))
//             .map_err(|error| input_error(at, &error.to_string())),
//         Type::Int => decoder
//             .int()
//             .map(|value| number_to_json(SanshoNumber::from_i128(i128::from(value))))
//             .map_err(|error| input_error(at, &error.to_string())),
//         Type::F16 => {
//             // the half-precision reader is behind minicbor's `half`
//             // feature; the two payload bytes are read in place instead
//             let payload = decoder
//                 .input()
//                 .get(at + 1..at + 3)
//                 .ok_or_else(|| input_error(at, "truncated half-precision float"))?;
//             let bits = u16::from_be_bytes([payload[0], payload[1]]);
//             decoder.set_position(at + 3);
//             Ok(number_to_json(SanshoNumber::F64(half_to_f64(bits))))
//         }
//         Type::F32 => decoder
//             .f32()
//             .map(|value| number_to_json(SanshoNumber::F64(value as f64)))
//             .map_err(|error| input_error(at, &error.to_string())),
//         Type::F64 => decoder
//             .f64()
//             .map(|value| number_to_json(SanshoNumber::F64(value)))
//             .map_err(|error| input_error(at, &error.to_string())),
//         Type::Bytes => decoder
//             .bytes()
//             .map(|bytes| J::String(base64url_encode(bytes)))
//             .map_err(|error| input_error(at, &error.to_string())),
//         Type::String => decoder
//             .str()
//             .map(|text| J::String(text.to_string()))
//             .map_err(|error| input_error(at, &error.to_string())),
//         Type::Array => {
//             let count = decoder
//                 .array()
//                 .map_err(|error| input_error(at, &error.to_string()))?
//                 .ok_or_else(|| input_error(at, "indefinite-length array"))?;
//             let mut items = Vec::with_capacity(count.min(1024) as usize);
//             for _ in 0..count {
//                 items.push(decode_item(decoder)?);
//             }
//             Ok(J::Array(items))
//         }
//         Type::Map => {
//             let count = decoder
//                 .map()
//                 .map_err(|error| input_error(at, &error.to_string()))?
//                 .ok_or_else(|| input_error(at, "indefinite-length map"))?;
//             let mut object = serde_json::Map::new();
//             for _ in 0..count {
//                 // a non-text key is not a MEMBER (navigation drops it),
//                 // but the boundary materialization renders it as its JSON
//                 // text: an integer key 1 becomes the key "1"
//                 let key = match decoder.datatype() {
//                     Ok(Type::String) => decoder
//                         .str()
//                         .map_err(|error| input_error(at, &error.to_string()))?
//                         .to_string(),
//                     _ => match decode_item(decoder)? {
//                         J::String(text) => text,
//                         other => other.to_string(),
//                     },
//                 };
//                 let value = decode_item(decoder)?;
//                 // duplicate keys resolve last-wins
//                 object.insert(key, value);
//             }
//             Ok(J::Object(object))
//         }
//         Type::Tag => {
//             let tag = decoder
//                 .tag()
//                 .map_err(|error| input_error(at, &error.to_string()))?;
//             let number = tag.as_u64();
//             match number {
//                 2 | 3 => {
//                     let magnitude = decoder
//                         .bytes()
//                         .map_err(|error| input_error(at, &error.to_string()))?;
//                     let value = bignum_to_f64(magnitude);
//                     let value = if number == 3 { -(value + 1.0) } else { value };
//                     Ok(number_to_json(SanshoNumber::F64(value)))
//                 }
//                 other => Err(evaluation_error(at, &format!("CBOR tag {other} is not a value"))),
//             }
//         }
//         Type::BytesIndef | Type::StringIndef | Type::ArrayIndef | Type::MapIndef => {
//             Err(input_error(at, "indefinite-length encoding"))
//         }
//         Type::Break => Err(input_error(at, "unexpected break")),
//         // the simple values outside the standard set, and unknown headers,
//         // carry no JSON meaning: null, matching the kind probe
//         Type::Simple | Type::Unknown(_) => {
//             decoder
//                 .skip()
//                 .map_err(|error| input_error(at, &error.to_string()))?;
//             Ok(J::Null)
//         }
//     }
// }
//
// /// The JSON view of ONE CBOR document: the item at the start of `bytes`,
// /// with the exactly-one-document contract — trailing bytes are an input
// /// error. A member's subslice ends at its own extent, so the same rule
// /// holds uniformly for the root and for every member.
// pub fn cbor_to_json(bytes: &[u8]) -> Result<J, SanshoError> {
//     let mut decoder = Decoder::new(bytes);
//     let value = decode_item(&mut decoder)?;
//     if decoder.position() != bytes.len() {
//         return Err(input_error(
//             decoder.position(),
//             "trailing bytes after the document",
//         ));
//     }
//     Ok(value)
// }

/// RFC 4648 §5 base64url: the URL-safe alphabet, `=`-padded to a multiple of
/// four. The engine's only byte-string rendering — the one place octets
/// become text, used by the materialization boundary and by nothing else.
pub fn base64url_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((n >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(n & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}
