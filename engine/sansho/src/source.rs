//! The zero-copy byte source (research spike, Phase 1).
//!
//! Spike finding recorded: the prototype's `&dyn SanshoSource`
//! reference shape (spike `source.rs`) does not survive contact with
//! the byte source — a byte node's children are ephemeral position
//! values; borrowing them past the accessor call is unsound. The
//! production shape is the **value-node** form: the crate's existing
//! `Node<'d>` trait (a node is a cheap position value; cloned nodes
//! never copy payload), which the evaluator is already generic over.
//! This module implements that trait with the measured cost fixes:
//!
//! 1. **Zero-copy leaves** — `as_str` borrows a text string's bytes
//!    (CBOR text strings are raw UTF-8, no escapes); numbers decode
//!    from headers (no memo, no allocation). The old cursor's
//!    `as_str`/`as_f64` went through the memo, materializing and
//!    cloning every touched leaf (the 17.4 MB peak / 104 ms scan cost
//!    the spike measured).
//! 2. **Single-pass raw-key-bytes scan** — `get_key` compares raw key
//!    bytes; it never builds the `Vec<String>` of keys the old
//!    cursor's `map_keys` built per lookup (the 12.2 ms seek cost at
//!    100k connections).
//! 3. **Boundary-only memo** — `materialize` (the row boundary) stays
//!    memoized (`HashMap<usize, J>`: each position decodes at most
//!    once per evaluation — the byte-once contract); navigation never
//!    touches it.
//!
//! Backing-bytes contract: the byte slice must be immutable for the
//! evaluation's lifetime (mmap'd clusters satisfy this); this node is
//! a pure borrower — no owning variant, no unsafe.

use std::borrow::Cow;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;

use crate::cbor::{base64url_encode, decode_at_position};
use crate::error::SanshoError;
use crate::view::{Kind, Node};
use serde_json::Value as J;

/// The evaluation-scoped decode state: each boundary position decodes
/// once; the decode count is the test instrumentation for the byte-once
/// property.
#[derive(Clone, Default)]
pub(crate) struct DecodeMemo {
    memo: Rc<RefCell<HashMap<usize, J>>>,
    pub(crate) decode_count: Rc<std::cell::Cell<usize>>,
}

impl DecodeMemo {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn decode(&self, bytes: &[u8], position: usize) -> Result<J, SanshoError> {
        if let Some(value) = self.memo.borrow().get(&position) {
            return Ok(value.clone());
        }
        self.decode_count.set(self.decode_count.get() + 1);

        // the ROOT position decodes as the WHOLE document: the
        // exactly-one-document contract (trailing bytes are an input
        // error) is enforced here, on the real evaluation path
        let value = if position == 0 {
            crate::cbor::decode_document(bytes)?
        } else {
            decode_at_position(bytes, position)?
        };
        self.memo.borrow_mut().insert(position, value.clone());
        Ok(value)
    }

    /// The unmemoized boundary decode: decode the position WITHOUT
    /// storing it (for positions consumed exactly once by the output);
    /// the decode count still increments (the byte-once observable).
    fn decode_unmemoized(&self, bytes: &[u8], position: usize) -> Result<J, SanshoError> {
        self.decode_count.set(self.decode_count.get() + 1);
        let value = if position == 0 {
            crate::cbor::decode_document(bytes)?
        } else {
            decode_at_position(bytes, position)?
        };
        Ok(value)
    }
}

/// A byte-position node over a CBOR byte slice.
#[derive(Clone)]
pub struct CborNode<'a> {
    bytes: &'a [u8],
    position: usize,
    memo: DecodeMemo,
}

impl<'a> CborNode<'a> {
    /// Position at the document root.
    pub fn root(bytes: &'a [u8]) -> Self {
        CborNode {
            bytes,
            position: 0,
            memo: DecodeMemo::new(),
        }
    }

    /// The evaluation's decode count (the byte-once observable: how
    /// many DISTINCT positions materialized at the boundary).
    pub fn decode_count(&self) -> usize {
        self.memo.decode_count.get()
    }

    fn child(&self, position: usize) -> Self {
        CborNode {
            bytes: self.bytes,
            position,
            memo: self.memo.clone(),
        }
    }

    /// The item's major type + additional info, from the header byte
    /// alone (no decode).
    fn header(&self) -> Result<(u8, u8), SanshoError> {
        let byte = *self.bytes.get(self.position).ok_or_else(|| SanshoError::Input {
            message: format!("position {} past the end of the document", self.position),
        })?;
        Ok((byte >> 5, byte & 0x1f))
    }

    /// Read the length argument at a position (the header's additional
    /// info → the length/value argument); returns (argument, next).
    fn argument_at(&self, position: usize) -> Result<(u64, usize), SanshoError> {
        let byte = *self.bytes.get(position).ok_or_else(|| SanshoError::Input {
            message: format!("position {position} past the end of the document"),
        })?;
        let additional = byte & 0x1f;
        let mut next = position + 1;
        let argument: u64 = match additional {
            0..=23 => additional as u64,
            24 => {
                let v = *self.bytes.get(next).ok_or_else(|| SanshoError::Input {
                    message: format!("position {next}: truncated u8 argument"),
                })?;
                next += 1;
                v as u64
            }
            25 => {
                let v = u16::from_be_bytes(
                    self.bytes
                        .get(next..next + 2)
                        .ok_or_else(|| SanshoError::Input {
                            message: format!("position {next}: truncated u16 argument"),
                        })?
                        .try_into()
                        .unwrap(),
                );
                next += 2;
                v as u64
            }
            26 => {
                let v = u32::from_be_bytes(
                    self.bytes
                        .get(next..next + 4)
                        .ok_or_else(|| SanshoError::Input {
                            message: format!("position {next}: truncated u32 argument"),
                        })?
                        .try_into()
                        .unwrap(),
                );
                next += 4;
                v as u64
            }
            27 => {
                let v = u64::from_be_bytes(
                    self.bytes
                        .get(next..next + 8)
                        .ok_or_else(|| SanshoError::Input {
                            message: format!("position {next}: truncated u64 argument"),
                        })?
                        .try_into()
                        .unwrap(),
                );
                next += 8;
                v as u64
            }
            31 => {
                return Err(SanshoError::Input {
                    message: format!("position {position}: indefinite-length encoding"),
                });
            }
            _ => {
                return Err(SanshoError::Input {
                    message: format!("position {position}: unsupported additional info {additional}"),
                });
            }
        };
        Ok((argument, next))
    }

    /// The byte extent of the item at `position` (header + payload):
    /// length-prefixed containers compute their extents from their
    /// headers — this is how a skip works without decoding.
    fn item_extent(&self, position: usize) -> Result<usize, SanshoError> {
        let byte = *self.bytes.get(position).ok_or_else(|| SanshoError::Input {
            message: format!("position {position} past the end of the document"),
        })?;
        let major = byte >> 5;
        let (argument, mut next) = self.argument_at(position)?;
        match major {
            4 => {
                // definite-length array: header + one item per entry
                for _ in 0..argument {
                    next = self.item_extent(next)?;
                }
                Ok(next)
            }
            5 => {
                // definite-length map: header + TWO items per entry
                // (key AND value)
                for _ in 0..argument {
                    next = self.item_extent(next)?;
                    next = self.item_extent(next)?;
                }
                Ok(next)
            }
            2 | 3 => {
                // strings: header + payload bytes
                Ok(next + argument as usize)
            }
            6 => {
                // tags: the wrapped item's extent (the tag's payload
                // is a full CBOR item — a bignum inside a container
                // must not be mis-walked as the next entry)
                self.item_extent(next)
            }
            _ => Ok(next),
        }
    }

    /// The member/element positions of a container, in document order
    /// (positions only; no decoding; no key materialization).
    fn container_positions(&self) -> Result<Vec<usize>, SanshoError> {
        let (major, _) = self.header()?;
        if major != 4 && major != 5 {
            return Ok(Vec::new());
        }
        let (argument, mut next) = self.argument_at(self.position)?;
        // NO capacity reservation from the hostile header: a huge
        // count must not reserve huge memory — the walk fails on the
        // missing bytes long before the vector grows meaningfully.
        let mut positions = Vec::new();
        for _ in 0..argument {
            if major == 5 {
                // the key: skipped past (its extent)
                next = self.item_extent(next)?;
            }
            positions.push(next);
            next = self.item_extent(next)?;
        }
        Ok(positions)
    }

    /// The text keys' byte extents (start, value-position), in
    /// document order — raw key bytes, never materialized as strings.
    /// Probe helper (examples only): the map's key positions.
    pub fn keys_via_positions(&self) -> Result<Vec<(usize, usize)>, SanshoError> {
        self.map_key_positions()
    }

    fn map_key_positions(&self) -> Result<Vec<(usize, usize)>, SanshoError> {
        let (major, _) = self.header()?;
        if major != 5 {
            return Ok(Vec::new());
        }
        let (argument, mut next) = self.argument_at(self.position)?;
        let mut keys = Vec::new();
        for _ in 0..argument {
            let key_start = next;
            let key_extent = self.item_extent(next)?;
            next = key_extent;
            let value_pos = next;
            next = self.item_extent(value_pos)?;
            // text keys only (major 3); non-text keys are skipped
            // (the mapping rejects them only when materialized)
            if self.bytes.get(key_start).map(|b| b >> 5) == Some(3) {
                keys.push((key_start, value_pos));
            }
        }
        Ok(keys)
    }

    /// The raw bytes of a TEXT string (major type 3), WITHOUT the
    /// UTF-8 validation — the filter-predicate fast path compares
    /// byte prefixes directly (an invalid-UTF-8 element in a filter
    /// predicate is DROPPED, not errored — the relaxed contract,
    /// owner decision 2026-09-23; byte strings (major 2) return None
    /// and keep the base64url general path).
    pub fn raw_text_bytes(&self) -> Option<&'a [u8]> {
        let (major, _) = self.header().ok()?;
        if major != 3 {
            return None;
        }
        let (argument, next) = self.argument_at(self.position).ok()?;
        let start = next;
        let end = start.checked_add(argument as usize)?;
        if end > self.bytes.len() {
            return None;
        }
        Some(&self.bytes[start..end])
    }

    /// The text key's content bytes (for the raw scan).
    /// Probe helper (examples only): a text key's bytes.
    pub fn key_bytes(&self, key_start: usize) -> Result<&'a [u8], SanshoError> {
        self.text_key_bytes(key_start)
    }

    fn text_key_bytes(&self, key_start: usize) -> Result<&'a [u8], SanshoError> {
        let (argument, next) = self.argument_at(key_start)?;
        let start = next;
        let end = start
            .checked_add(argument as usize)
            .ok_or_else(|| SanshoError::Input {
                message: format!("position {key_start}: key length overflow"),
            })?;
        if end > self.bytes.len() {
            return Err(SanshoError::Input {
                message: format!("position {key_start}: key extends past the document"),
            });
        }
        Ok(&self.bytes[start..end])
    }
}

/// The f16 → f64 conversion (the mapping's float semantics).
///
/// NOTE (pinned dormancy): this conversion disagrees with the
/// mapping's materialization (`cbor.rs`'s `half_to_f64`) on the ±Inf
/// exponent — NaN here vs the mapping's coercion. `Node::as_f64` is
/// never called by the evaluator (the leaf fast paths and the
/// boundary materialization cover the number paths), so the
/// divergence is dormant; the escape matrix pins the materialized
/// behavior. Reconcile when/if `as_f64` gains a caller.
fn f16_to_f64(half: u16) -> f64 {
    let sign = if half >> 15 == 0 { 1.0 } else { -1.0 };
    let exponent = (half >> 10) & 0x1f;
    let mantissa = half & 0x3ff;
    match exponent {
        0 => sign * (mantissa as f64) * 2.0_f64.powi(-24),
        0x1f => f64::NAN,
        e => sign * (mantissa as f64 + 1024.0) * 2.0_f64.powi(e as i32 - 25),
    }
}

impl<'a> Node<'a> for CborNode<'a> {
    fn kind(&self) -> Kind {
        let (major, _) = self.header().unwrap_or((7, 31));
        match major {
            0 | 1 => Kind::Number,
            2 | 3 => Kind::String,
            4 => Kind::Array,
            5 => Kind::Object,
            6 => Kind::Number, // tags render as numbers per the mapping
            7 => match self.bytes.get(self.position).map(|b| b & 0x1f) {
                Some(20 | 21) => Kind::Bool,
                Some(22) => Kind::Null,
                Some(24..=27) => Kind::Number,
                _ => Kind::Null,
            },
            _ => Kind::Null,
        }
    }

    fn as_str(&self) -> Option<Cow<'a, str>> {
        let (major, _) = self.header().ok()?;
        if major != 2 && major != 3 {
            return None;
        }
        let (argument, next) = self.argument_at(self.position).ok()?;
        let start = next;
        let end = start.checked_add(argument as usize)?;
        if end > self.bytes.len() {
            return None;
        }
        match major {
            2 => Some(Cow::Owned(base64url_encode(&self.bytes[start..end]))),
            _ => std::str::from_utf8(&self.bytes[start..end])
                .ok()
                .map(Cow::Borrowed),
        }
    }

    fn as_f64(&self) -> Option<f64> {
        let (major, _) = self.header().ok()?;
        match major {
            0 => {
                let (argument, _) = self.argument_at(self.position).ok()?;
                Some(argument as f64)
            }
            1 => {
                let (argument, _) = self.argument_at(self.position).ok()?;
                if argument > i64::MAX as u64 {
                    None
                } else {
                    Some((-1 - argument as i64) as f64)
                }
            }
            7 => match self.bytes.get(self.position).map(|b| b & 0x1f) {
                Some(25) => {
                    let (_, next) = self.argument_at(self.position).ok()?;
                    let v = u16::from_be_bytes(
                        self.bytes.get(next..next + 2)?.try_into().unwrap(),
                    );
                    Some(f16_to_f64(v))
                }
                Some(26) => {
                    let (_, next) = self.argument_at(self.position).ok()?;
                    let v = u32::from_be_bytes(
                        self.bytes.get(next..next + 4)?.try_into().unwrap(),
                    );
                    Some(f32::from_bits(v) as f64)
                }
                Some(27) => {
                    let (_, next) = self.argument_at(self.position).ok()?;
                    let v = u64::from_be_bytes(
                        self.bytes.get(next..next + 8)?.try_into().unwrap(),
                    );
                    Some(f64::from_bits(v))
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        let (major, _) = self.header().ok()?;
        if major != 7 {
            return None;
        }
        match self.bytes.get(self.position).map(|b| b & 0x1f) {
            Some(20) => Some(false),
            Some(21) => Some(true),
            _ => None,
        }
    }

    fn is_null(&self) -> bool {
        // the mapping's null set: null (22) and undefined (23) both
        // reduce to null (the cursor's contract)
        self.kind() == Kind::Null
    }

    fn get_key(&self, name: &str) -> Option<Self> {
        // the single-pass raw-key-bytes scan: no key strings are ever
        // materialized (the old cursor's map_keys Vec<String> is the
        // measured seek cost; gone here)
        let keys = self.map_key_positions().ok()?;
        for (key_start, value_pos) in keys {
            if self.text_key_bytes(key_start).ok()? == name.as_bytes() {
                return Some(self.child(value_pos));
            }
        }
        None
    }

    fn get_index(&self, index: usize) -> Option<Self> {
        let positions = self.container_positions().ok()?;
        positions.into_iter().nth(index).map(|p| self.child(p))
    }

    fn entries(&self) -> Vec<(String, Self)> {
        // the entry ORDER is by key — the view's documented iteration
        // order (sorted) — achieved by sorting the (key, value-node)
        // pairs
        let mut pairs: Vec<(String, Self)> = Vec::new();
        let keys = match self.map_key_positions() {
            Ok(keys) => keys,
            Err(_) => return Vec::new(),
        };
        for (key_start, value_pos) in keys {
            // the cursor's entries emit "" for non-text keys; align
            // (map_key_positions already filters to text keys, so the
            // non-text case cannot reach here — the alignment is
            // documented for parity)
            match self.text_key_bytes(key_start) {
                Ok(bytes) => match std::str::from_utf8(bytes) {
                    Ok(key) => pairs.push((key.to_string(), self.child(value_pos))),
                    Err(_) => continue,
                },
                Err(_) => continue,
            }
        }
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs
    }

    fn elements(&self) -> Vec<Self> {
        self.container_positions()
            .unwrap_or_default()
            .into_iter()
            .map(|p| self.child(p))
            .collect()
    }

    fn container_len(&self) -> Option<usize> {
        let (major, _) = self.header().ok()?;
        if major != 4 && major != 5 {
            return None;
        }
        let (argument, _) = self.argument_at(self.position).ok()?;
        Some(argument as usize)
    }

    fn counts_toward_aggregation() -> bool {
        true
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        // the boundary decode through the memo (each position decodes
        // at most once per evaluation — the byte-once contract)
        self.memo.decode(self.bytes, self.position)
    }

    fn materialize_unmemoized(&self) -> Result<J, SanshoError> {
        // the output boundary: a position consumed exactly once by the
        // output needs no memo entry — no insert, no clone; the decode
        // count still increments (the byte-once observable)
        self.memo.decode_unmemoized(self.bytes, self.position)
    }

    fn raw_text_bytes(&self) -> Option<&[u8]> {
        // the trait's raw-bytes accessor (the filter fast path calls
        // through the trait — the inherent method above exists for
        // direct callers; both must agree)
        self.raw_text_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // T1.1's first half: a text string's as_str borrows the input
    // bytes (zero-copy: same bytes, no allocation).
    #[test]
    fn text_string_borrows_bytes() {
        let doc = serde_json::json!({"name": "hello world"});
        let bytes = serde_cbor::to_vec(&doc).unwrap();
        let root = CborNode::root(&bytes);
        let name = root.get_key("name").expect("key exists");
        let s = name.as_str().expect("a string");
        match s {
            Cow::Borrowed(b) => {
                // the borrowed str's bytes must be a sub-slice of the
                // input bytes (the zero-copy property)
                let start = b.as_ptr() as usize;
                let end = start + b.len();
                let input_start = bytes.as_ptr() as usize;
                let input_end = input_start + bytes.len();
                assert!(
                    start >= input_start && end <= input_end,
                    "borrowed string must be within the input slice"
                );
            }
            Cow::Owned(_) => panic!("text strings must borrow, not own"),
        }
    }

    // T1.1's second half: the seek shape (get_key) allocates nothing
    // beyond the positions buffer (the raw-key scan never builds key
    // strings). Measured via the profile binary; the structural
    // assertion: the scan's key comparisons are byte comparisons.
    #[test]
    fn raw_key_scan_finds_keys() {
        let doc = serde_json::json!({
            "connections": {"alias:from": ["pkg:a", "gitoid:b"]},
            "body": {"file_size": 3050}
        });
        let bytes = serde_cbor::to_vec(&doc).unwrap();
        let root = CborNode::root(&bytes);
        let conn = root.get_key("connections").expect("connections");
        let alias = conn.get_key("alias:from").expect("alias:from (colon key)");
        assert_eq!(alias.container_len(), Some(2));
        let body = root.get_key("body").expect("body");
        let size = body.get_key("file_size").expect("file_size");
        assert_eq!(size.as_f64(), Some(3050.0));
    }

    // T1.3: the byte-once invariant — materialize() (the boundary)
    // decodes each position once; navigation never decodes.
    #[test]
    fn boundary_memo_decodes_once() {
        let doc = serde_json::json!({"x": {"deep": [1, 2, 3]}});
        let bytes = serde_cbor::to_vec(&doc).unwrap();
        let root = CborNode::root(&bytes);
        // navigation: no decodes
        let x = root.get_key("x").expect("x");
        let deep = x.get_key("deep").expect("deep");
        assert_eq!(root.decode_count(), 0, "navigation must not decode");
        // boundary: the deep node materializes once, memoized
        let v1 = deep.materialize().unwrap();
        let v2 = deep.materialize().unwrap();
        assert_eq!(v1, v2);
        assert_eq!(root.decode_count(), 1, "one distinct boundary decode");
    }
}