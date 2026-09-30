//! The byte source: `&[u8]` implementing [`SanshoTrait`] — a CBOR
//! document's values are LAZY byte positions, whose machinery (header
//! parsing, single-pass raw-key scans, the decode memo) lives in the
//! crate-private [`CborNode`], the hidden type of this implementation's
//! navigation. Nothing of the position is part of a public signature:
//! the trait's navigation returns the positions as values implementing
//! the trait, and the walking code never names the type.
//!
//! Measured-cost fixes (the value-node spike's findings):
//! 1. **Zero-copy leaves** — a text string's bytes are the slice's
//!    bytes (CBOR text is raw UTF-8, no escapes); numbers decode from
//!    headers (no memo, no allocation).
//! 2. **Single-pass raw-key-bytes scan** — key lookup compares raw key
//!    bytes; no `Vec<String>` of keys is ever built.
//! 3. **Boundary-only memo** — `materialize` (the row boundary) is
//!    memoized (`HashMap<usize, J>`: each position decodes at most
//!    once per evaluation — the byte-once contract); navigation never
//!    touches it.
//!
//! Backing-bytes contract: the byte slice must be immutable for the
//! evaluation's lifetime (mmap'd clusters satisfy this); this source
//! is a pure borrower — no owning variant, no unsafe.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::cbor::{base64url_encode, decode_at_position};
use crate::error::SanshoError;
use crate::view::{Kind, SanshoNumber, SanshoTrait};
use serde_json::Value as J;

/// The evaluation-scoped decode state: each boundary position decodes
/// once; the decode count is the test instrumentation for the byte-once
/// property.
#[derive(Clone, Debug, Default)]
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

/// A byte-position value over a CBOR byte slice — the byte source's
/// member type and the hidden type of its navigation. CRATE-PRIVATE:
/// the machinery (header parsing, key scans, the memo) is internal
/// bookkeeping; values of this type reach walking code only as values
/// implementing [`SanshoTrait`].
#[derive(Clone, Debug)]
pub(crate) struct CborNode<'a> {
    bytes: &'a [u8],
    position: usize,
    memo: DecodeMemo,
}

impl<'a> CborNode<'a> {
    /// Position at the document root.
    pub(crate) fn root(bytes: &'a [u8]) -> Self {
        CborNode {
            bytes,
            position: 0,
            memo: DecodeMemo::new(),
        }
    }

    pub(crate) fn decode_count(&self) -> usize {
        self.memo.decode_count.get()
    }

    /// The raw bytes of a TEXT string (major type 3), WITHOUT the
    /// UTF-8 validation — the filter-predicate fast path compares
    /// byte prefixes directly (an invalid-UTF-8 element in a filter
    /// predicate is DROPPED, not errored — the relaxed contract,
    /// owner decision 2026-09-23; byte strings (major 2) return None
    /// and keep the base64url general path).
    pub(crate) fn raw_text_bytes(&self) -> Option<&'a [u8]> {
        let (major, _) = self.header()?;
        if major != 3 {
            return None;
        }
        let (argument, next) = self.argument_at(self.position)?;
        let start = next;
        let end = start.checked_add(argument as usize)?;
        if end > self.bytes.len() {
            return None;
        }
        Some(&self.bytes[start..end])
    }

    fn child(&self, position: usize) -> Self {
        CborNode {
            bytes: self.bytes,
            position,
            memo: self.memo.clone(),
        }
    }

    fn header(&self) -> Option<(u8, u8)> {
        let byte = *self.bytes.get(self.position)?;
        Some((byte >> 5, byte & 0x1f))
    }

    /// The argument (small form or the following big-endian integer)
    /// and the offset just past it.
    fn argument_at(&self, position: usize) -> Option<(u64, usize)> {
        let byte = *self.bytes.get(position)?;
        let small = (byte & 0x1f) as u64;
        match byte & 0x1f {
            0..=23 => Some((small, position + 1)),
            24 => Some((
                *self.bytes.get(position + 1)? as u64,
                position + 2,
            )),
            25 => {
                let raw: [u8; 2] = self.bytes.get(position + 1..position + 3)?.try_into().ok()?;
                Some((u16::from_be_bytes(raw) as u64, position + 3))
            }
            26 => {
                let raw: [u8; 4] = self.bytes.get(position + 1..position + 5)?.try_into().ok()?;
                Some((u32::from_be_bytes(raw) as u64, position + 5))
            }
            27 => {
                let raw: [u8; 8] = self.bytes.get(position + 1..position + 9)?.try_into().ok()?;
                Some((u64::from_be_bytes(raw), position + 9))
            }
            _ => None,
        }
    }

    /// The positions of an array's elements (or an object's values).
    fn container_positions(&self) -> Option<Vec<usize>> {
        let (major, small) = self.header()?;
        if major != 4 && major != 5 {
            return Some(Vec::new());
        }
        if small > 27 {
            // indefinite-length containers are not decomposable here
            return None;
        }
        let (length, mut cursor) = self.argument_at(self.position)?;
        let mut positions = Vec::with_capacity(length.min(1024) as usize);
        for _ in 0..length {
            if major == 5 {
                // the key: skipped past (its extent)
                cursor = self.item_extent_at(cursor)?;
            }
            positions.push(cursor);
            cursor = self.item_extent_at(cursor)?;
        }
        Some(positions)
    }

    fn map_key_positions(&self) -> Option<Vec<(usize, usize)>> {
        let (major, small) = self.header()?;
        if major != 5 {
            return Some(Vec::new());
        }
        if small > 27 {
            return None;
        }
        let (argument, mut cursor) = self.argument_at(self.position)?;
        let mut keys = Vec::with_capacity(argument.min(1024) as usize);
        for _ in 0..argument {
            let key_start = cursor;
            cursor = self.item_extent_at(cursor)?;
            let value_pos = cursor;
            cursor = self.item_extent_at(cursor)?;
            // text keys only (major 3); non-text keys are skipped
            // (the mapping rejects them only when materialized)
            if self.bytes.get(key_start).map(|b| b >> 5) == Some(3) {
                keys.push((key_start, value_pos));
            }
        }
        Some(keys)
    }

    /// The key bytes of a text key, at its start offset.
    fn text_key_bytes(&self, key_start: usize) -> Option<&'a [u8]> {
        let (_, small) = self.header_at(key_start)?;
        if small > 27 {
            return None;
        }
        let (arg, next) = self.argument_at(key_start)?;
        self.bytes.get(next..next + arg as usize)
    }

    fn header_at(&self, position: usize) -> Option<(u8, u8)> {
        let byte = *self.bytes.get(position)?;
        Some((byte >> 5, byte & 0x1f))
    }

    fn item_extent_at(&self, position: usize) -> Option<usize> {
        let (major, small) = self.header_at(position)?;
        let (argument, next) = self.argument_at(position)?;
        match major {
            // numbers (0/1) and simple values (7): header only
            0 | 1 | 7 => Some(next),
            // strings (2/3): header + payload
            2 | 3 => Some(next + argument as usize),
            // definite-length containers: header + one item per entry
            // (a map counts TWO per entry: key AND value)
            4 | 5 if small <= 27 => {
                let mut cursor = next;
                let entries = if major == 5 { argument * 2 } else { argument };
                for _ in 0..entries {
                    cursor = self.item_extent_at(cursor)?;
                }
                Some(cursor)
            }
            // tags: the wrapped item's extent
            6 => self.item_extent_at(next),
            _ => None,
        }
    }

    /// The f16 → f64 conversion (the mapping's float semantics).
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
}


/// The CBOR header argument (small form or the following big-endian
/// integer) and the offset just past it — the free form so the source
/// probe can parse against the `'a` slice directly.
fn argument_at(bytes: &[u8], position: usize) -> Option<(u64, usize)> {
    let byte = *bytes.get(position)?;
    let small = (byte & 0x1f) as u64;
    match byte & 0x1f {
        0..=23 => Some((small, position + 1)),
        24 => Some((*bytes.get(position + 1)? as u64, position + 2)),
        25 => {
            let raw: [u8; 2] = bytes.get(position + 1..position + 3)?.try_into().ok()?;
            Some((u16::from_be_bytes(raw) as u64, position + 3))
        }
        26 => {
            let raw: [u8; 4] = bytes.get(position + 1..position + 5)?.try_into().ok()?;
            Some((u32::from_be_bytes(raw) as u64, position + 5))
        }
        27 => {
            let raw: [u8; 8] = bytes.get(position + 1..position + 9)?.try_into().ok()?;
            Some((u64::from_be_bytes(raw), position + 9))
        }
        _ => None,
    }
}

impl<'a> SanshoTrait<'a> for &'a [u8] {
    fn kind(&self) -> Kind {
        CborNode::root(*self).kind()
    }

    fn as_str(&self) -> Option<Cow<'_, str>> {
        // inlined against the `'a` slice directly (a temporary root's
        // borrow would tie the returned string to the temporary)
        let bytes = *self;
        let position = 0usize;
        let byte = *bytes.get(position)?;
        let major = byte >> 5;
        if major != 2 && major != 3 {
            return None;
        }
        let (argument, next) = argument_at(bytes, position)?;
        let start = next;
        let end = start.checked_add(argument as usize)?;
        if end > bytes.len() {
            return None;
        }
        match major {
            2 => Some(Cow::Owned(base64url_encode(&bytes[start..end]))),
            _ => std::str::from_utf8(&bytes[start..end]).ok().map(Cow::Borrowed),
        }
    }

    fn as_str_ref(&self) -> Option<&'a str> {
        // text (major 3) only: a byte string (major 2) renders as
        // base64url, which is computed and cannot be borrowed
        let bytes = *self;
        if bytes.first().map(|byte| byte >> 5) != Some(3) {
            return None;
        }
        let (argument, next) = argument_at(bytes, 0)?;
        let end = next.checked_add(argument as usize)?;
        if end > bytes.len() {
            return None;
        }
        std::str::from_utf8(&bytes[next..end]).ok()
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        CborNode::root(*self).as_sansho_number()
    }

    fn as_bool(&self) -> Option<bool> {
        CborNode::root(*self).as_bool()
    }

    fn is_null(&self) -> bool {
        CborNode::root(*self).is_null()
    }

    fn get_key(&self, name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
        CborNode::root(*self).get_key(name)
    }

    fn get_index(&self, index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
        CborNode::root(*self).get_index(index)
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
        CborNode::root(*self).entries()
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
        CborNode::root(*self).elements()
    }

    fn container_len(&self) -> Option<usize> {
        CborNode::root(*self).container_len()
    }

    fn counts_toward_aggregation() -> bool {
        // the byte source's materializations are engine-created memory
        // (the boundary decode memo)
        true
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        CborNode::root(*self).materialize()
    }

    fn materialize_unmemoized(&self) -> Result<J, SanshoError> {
        CborNode::root(*self).materialize_unmemoized()
    }
}

impl<'a> SanshoTrait<'a> for CborNode<'a> {
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

    fn as_str(&self) -> Option<Cow<'_, str>> {
        let (major, _) = self.header()?;
        if major != 2 && major != 3 {
            return None;
        }
        let (argument, next) = self.argument_at(self.position)?;
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

    fn as_str_ref(&self) -> Option<&'a str> {
        // text (major 3) only: a byte string (major 2) renders as
        // base64url, which is computed and cannot be borrowed
        let (major, _) = self.header()?;
        if major != 3 {
            return None;
        }
        let (argument, next) = self.argument_at(self.position)?;
        let end = next.checked_add(argument as usize)?;
        if end > self.bytes.len() {
            return None;
        }
        std::str::from_utf8(&self.bytes[next..end]).ok()
    }

    fn as_sansho_number(&self) -> Option<SanshoNumber> {
        let (major, _) = self.header()?;
        match major {
            // unsigned (major 0): the full u64 — exact, no narrowing
            0 => {
                let (argument, _) = self.argument_at(self.position)?;
                Some(SanshoNumber::U64(argument))
            }
            // negative (major 1): the -1-argument form; magnitudes
            // beyond i64::MAX follow the materialization path (none)
            1 => {
                let (argument, _) = self.argument_at(self.position)?;
                if argument > i64::MAX as u64 {
                    None
                } else {
                    Some(SanshoNumber::I64(-1 - argument as i64))
                }
            }
            // floats (major 7, 25/26/27)
            7 => match self.bytes.get(self.position).map(|b| b & 0x1f) {
                Some(25) => {
                    let (_, next) = self.argument_at(self.position)?;
                    let v = u16::from_be_bytes(
                        self.bytes.get(next..next + 2)?.try_into().unwrap(),
                    );
                    Some(SanshoNumber::F64(CborNode::f16_to_f64(v)))
                }
                Some(26) => {
                    let (_, next) = self.argument_at(self.position)?;
                    let v = u32::from_be_bytes(
                        self.bytes.get(next..next + 4)?.try_into().unwrap(),
                    );
                    Some(SanshoNumber::F64(f32::from_bits(v) as f64))
                }
                Some(27) => {
                    let (_, next) = self.argument_at(self.position)?;
                    let v = u64::from_be_bytes(
                        self.bytes.get(next..next + 8)?.try_into().unwrap(),
                    );
                    Some(SanshoNumber::F64(f64::from_bits(v)))
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        let (major, _) = self.header()?;
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

    fn get_key(&self, name: &str) -> Option<impl SanshoTrait<'a> + use<'a>> {
        // the single-pass raw-key-bytes scan: no key strings are ever
        // materialized
        let keys = self.map_key_positions()?;
        for (key_start, value_pos) in keys {
            if self.text_key_bytes(key_start)? == name.as_bytes() {
                return Some(self.child(value_pos));
            }
        }
        None
    }

    fn get_index(&self, index: usize) -> Option<impl SanshoTrait<'a> + use<'a>> {
        let positions = self.container_positions()?;
        positions.into_iter().nth(index).map(|p| self.child(p))
    }

    fn entries(&self) -> Vec<(String, impl SanshoTrait<'a> + use<'a>)> {
        // the entry ORDER is by key — the view's documented iteration
        // order (sorted) — achieved by sorting the (key, value-node)
        // pairs
        let mut pairs: Vec<(String, CborNode<'a>)> = Vec::new();
        let Some(keys) = self.map_key_positions() else {
            return Vec::new();
        };
        for (key_start, value_pos) in keys {
            // the cursor's entries emit "" for non-text keys; align
            // (map_key_positions already filters to text keys, so the
            // non-text case cannot reach here — the alignment is
            // documented for parity)
            match self.text_key_bytes(key_start) {
                Some(bytes) => match std::str::from_utf8(bytes) {
                    Ok(key) => pairs.push((key.to_string(), self.child(value_pos))),
                    Err(_) => continue,
                },
                None => continue,
            }
        }
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        pairs
    }

    fn elements(&self) -> Vec<impl SanshoTrait<'a> + use<'a>> {
        self.container_positions()
            .unwrap_or_default()
            .into_iter()
            .map(|p| self.child(p))
            .collect()
    }

    fn container_len(&self) -> Option<usize> {
        let (major, _) = self.header()?;
        if major != 4 && major != 5 {
            return None;
        }
        let (argument, _) = self.argument_at(self.position)?;
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
        self.raw_text_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eval::lookup_value;
    use crate::{compile, parse};

    /// Requirement: `&[u8]` is a direct Sansho source. What: `lookup`
    /// over a `&[u8]` evaluates a CBOR document exactly like the
    /// existing byte entry, and the trait's probes report the root
    /// kind and full-width value. Why: the byte source must be on the
    /// trait surface (the owner's directive: the trait on `&[u8]`).
    #[test]
    fn byte_source_is_a_direct_sansho_source() {
        let bytes: &[u8] = &serde_cbor::to_vec(&serde_json::json!({
            "exact": 9007199254740993i64,
            "text": "hello",
        }))
        .unwrap();
        let program = compile(&parse("exact").unwrap()).unwrap();
        let projected = lookup_value(&bytes, &program).unwrap();
        assert_eq!(projected, serde_json::json!(9007199254740993i64));

        // the root probes: an object root, and the exact number probe
        assert_eq!(SanshoTrait::kind(&bytes), Kind::Object);
        assert!(!SanshoTrait::is_null(&bytes));

        // a text-string root borrows the slice (zero-copy)
        let text: &[u8] = &serde_cbor::to_vec(&serde_json::json!("hello")).unwrap();
        assert_eq!(SanshoTrait::as_str(&text), Some(Cow::Borrowed("hello")));

        // a number root reports the full-width value from the header
        let num: &[u8] = &serde_cbor::to_vec(&serde_json::json!(9007199254740993i64)).unwrap();
        assert_eq!(
            SanshoTrait::as_sansho_number(&num),
            Some(SanshoNumber::U64(9007199254740993))
        );
    }

    /// Requirement: a traversal can carry a string out of itself — the
    /// walk's connection finder returns `Vec<&'b str>` borrowed from the
    /// document, not from the member values it walked through. What:
    /// navigating a CBOR document and collecting `as_str_ref` into a
    /// `Vec<&str>` tied to the document compiles and yields the
    /// document's own strings; a borrowed JSON document lends the same
    /// way, the owned value forms do not, and a CBOR byte string (whose
    /// rendering is computed) does not. Why: `as_str` lends from the
    /// receiver, so a member — a value obtained inside the traversal —
    /// cannot be returned from it; this is the borrow that can.
    ///
    /// LLM section: `targets` is deliberately a function whose return
    /// type borrows from its document argument and from nothing local —
    /// compiling it is half the assertion.
    #[test]
    fn document_borrows_escape_the_traversal_that_found_them() {
        fn targets<'a>(document: &'a [u8], edge: &str) -> Vec<&'a str> {
            let Some(connections) = SanshoTrait::get_key(&document, "connections") else {
                return Vec::new();
            };
            let Some(edge_targets) = SanshoTrait::get_key(&connections, edge) else {
                return Vec::new();
            };
            SanshoTrait::elements(&edge_targets)
                .iter()
                .filter_map(|target| SanshoTrait::as_str_ref(target))
                .collect()
        }

        let bytes: &[u8] = &serde_cbor::to_vec(&serde_json::json!({
            "connections": {
                "alias:from": ["pkg:a", "gitoid:b"],
                "contained:up": ["p"]
            }
        }))
        .unwrap();
        assert_eq!(targets(bytes, "alias:from"), vec!["pkg:a", "gitoid:b"]);
        assert_eq!(targets(bytes, "contained:up"), vec!["p"]);
        assert_eq!(targets(bytes, "not-an-edge-type"), Vec::<&str>::new());

        // a borrowed JSON document lends its own strings...
        let document = serde_json::json!({"name": "hello"});
        let borrowed: &serde_json::Value = &document;
        let member = SanshoTrait::get_key(&borrowed, "name").expect("the member");
        assert_eq!(SanshoTrait::as_str_ref(&member), Some("hello"));

        // ...while the owned value form holds its text itself
        let owned_member = SanshoTrait::get_key(&document, "name").expect("the member");
        assert_eq!(SanshoTrait::as_str_ref(&owned_member), None);

        // a byte string renders as base64url — computed, never borrowed
        let encoded = serde_cbor::to_vec(&serde_cbor::Value::Bytes(vec![1, 2, 3])).unwrap();
        let byte_string: &[u8] = &encoded;
        assert_eq!(SanshoTrait::as_str_ref(&byte_string), None);
        assert!(
            SanshoTrait::as_str(&byte_string).is_some(),
            "the computed rendering still reads through `as_str`"
        );

        // the option forms delegate to the value they carry
        let carried: Option<std::borrow::Cow<'static, str>> =
            Some(std::borrow::Cow::Borrowed("carried"));
        assert_eq!(SanshoTrait::as_str_ref(&carried), Some("carried"));
    }

    /// Requirement: navigation must never decode (the byte-once
    /// boundary contract). What: over truncated slices at every prefix
    /// length, navigation through the trait never panics and the memo
    /// stays untouched. Why: a malformed/truncated input can only
    /// produce a boundary error, never a navigation crash.
    #[test]
    fn truncated_slices_never_panic_and_navigation_never_decodes() {
        let doc = serde_json::json!({
            "connections": {"alias:from": ["pkg:a", "gitoid:b"], "contained:up": ["p"]},
            "body": {"file_names": ["a.java", "b.java"], "file_size": 3050}
        });
        let full = serde_cbor::to_vec(&doc).unwrap();
        for len in 0..full.len() {
            let truncated = &full[..len];
            let root = CborNode::root(truncated);
            // navigation must never panic, and never decode (the
            // byte-once boundary contract)
            let _ = root.kind();
            let _ = root.get_key("connections");
            let _ = root.get_key("body");
            let _ = root.elements();
            let _ = root.container_len();
            assert_eq!(root.decode_count(), 0, "navigation must not decode");
        }
    }
}