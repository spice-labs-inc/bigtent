//! The cursor tree view: a [`Node`](crate::view::Node) implementation
//! over a raw CBOR byte slice.
//!
//! A node is a POSITION in the byte slice — cheap to copy, never
//! materialized until something consumes its value. Every materialization
//! goes through the evaluation's MEMO: a position decodes at most once
//! per evaluation, which is what makes the byte-once requirement
//! (SPEC-0001 §5.1) true even when a filter predicate and an emission
//! both consume the same subtree — the shared decode is the fusion.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::cbor::{base64url_encode, decode_at_position};
use crate::error::SanshoError;
use crate::view::{Kind, Node};
use serde_json::Value as J;

/// The evaluation-scoped decode state: each position decodes once; the
/// decode count is the test instrumentation for the byte-once property.
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
}

/// A node positioned in a CBOR byte slice.
#[derive(Clone)]
pub struct CursorNode<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) position: usize,
    pub(crate) memo: DecodeMemo,
}

impl<'a> CursorNode<'a> {
    /// Position at the document root.
    pub fn root(bytes: &'a [u8]) -> Self {
        CursorNode {
            bytes,
            position: 0,
            memo: DecodeMemo::new(),
        }
    }

    /// The evaluation's decode count (the test instrumentation for the
    /// byte-once property: how many DISTINCT positions decoded).
    pub fn decode_count(&self) -> usize {
        self.memo.decode_count.get()
    }

    fn child(&self, position: usize) -> Self {
        CursorNode {
            bytes: self.bytes,
            position,
            memo: self.memo.clone(),
        }
    }

    /// The item's major type, from the header byte alone (no decode).
    fn header_kind(&self) -> Result<Kind, SanshoError> {
        let byte = self
            .bytes
            .get(self.position)
            .ok_or_else(|| SanshoError::Input {
                message: format!("position {} past the end of the document", self.position),
            })?;
        Ok(match byte >> 5 {
            0 | 1 => Kind::Number, // unsigned / negative integer
            2 => Kind::String,     // byte string: base64url in the view
            3 => Kind::String,     // text string
            4 => Kind::Array,
            5 => Kind::Object,
            6 => Kind::Number, // bignum tags render as numbers
            7 => match byte & 0x1F {
                20 => Kind::Bool,
                21 => Kind::Bool,
                22 => Kind::Null,
                24..=27 => Kind::Number, // simple values / floats
                _ => Kind::Null,
            },
            _ => unreachable!("the major type is three bits"),
        })
    }

    /// Iterate a container's member/element POSITIONS without decoding
    /// the values. Returns the positions in document order.
    fn container_positions(&self) -> Result<Vec<usize>, SanshoError> {
        let mut decoder = minicbor::Decoder::new(self.bytes);
        decoder.set_position(self.position);
        let position = self.position;
        let (is_map, len) = match self.header_kind()? {
            Kind::Array => (
                false,
                decoder
                    .array()
                    .map_err(|e| SanshoError::Input {
                        message: format!("at byte {position}: array header: {e}"),
                    })?
                    .ok_or_else(|| SanshoError::Input {
                        message: format!("at byte {position}: indefinite-length array"),
                    })?,
            ),
            Kind::Object => (
                true,
                decoder
                    .map()
                    .map_err(|e| SanshoError::Input {
                        message: format!("at byte {position}: map header: {e}"),
                    })?
                    .ok_or_else(|| SanshoError::Input {
                        message: format!("at byte {position}: indefinite-length map"),
                    })?,
            ),
            _ => return Ok(Vec::new()),
        };
        // one iteration per entry (the map's loop reads the key AND
        // the value) — len entries for both maps and arrays
        let count = len;
        // NO capacity reservation from the hostile header: a huge count
        // must not reserve huge memory — the walk fails on the missing
        // bytes long before the vector grows meaningfully
        let mut positions = Vec::new();
        for _ in 0..count {
            if is_map {
                // the key: decoded (it names the member) and skipped past
                let key_type = decoder.datatype().map_err(|e| SanshoError::Input {
                    message: format!("at byte {}: map key: {e}", decoder.position()),
                })?;
                match key_type {
                    minicbor::data::Type::String => {
                        let _ = decoder.str().map_err(|e| SanshoError::Input {
                            message: format!("at byte {}: map key: {e}", decoder.position()),
                        })?;
                    }
                    _ => {
                        // a non-text key: skip it (the mapping rejects it
                        // only when the key's VALUE is materialized)
                        decoder.skip().map_err(|e| SanshoError::Input {
                            message: format!("at byte {}: map key: {e}", decoder.position()),
                        })?;
                    }
                }
            }
            positions.push(decoder.position());
            // the value (or element): skipped past, position recorded
            decoder.skip().map_err(|e| SanshoError::Input {
                message: format!("at byte {}: item: {e}", decoder.position()),
            })?;
        }
        Ok(positions)
    }

    /// The map's keys (text ones), in document order.
    fn map_keys(&self) -> Result<Vec<String>, SanshoError> {
        let mut decoder = minicbor::Decoder::new(self.bytes);
        decoder.set_position(self.position);
        let len = decoder
            .map()
            .map_err(|e| SanshoError::Input {
                message: format!("at byte {}: map header: {e}", self.position),
            })?
            .ok_or_else(|| SanshoError::Input {
                message: format!("at byte {}: indefinite-length map", self.position),
            })?;
        // no capacity reservation from the hostile header
        let mut keys = Vec::new();
        for _ in 0..len {
            let key_type = decoder.datatype().map_err(|e| SanshoError::Input {
                message: format!("at byte {}: map key: {e}", decoder.position()),
            })?;
            match key_type {
                minicbor::data::Type::String => {
                    keys.push(
                        decoder
                            .str()
                            .map_err(|e| SanshoError::Input {
                                message: format!("at byte {}: map key: {e}", decoder.position()),
                            })?
                            .to_string(),
                    );
                }
                _ => {
                    decoder.skip().map_err(|e| SanshoError::Input {
                        message: format!("at byte {}: map key: {e}", decoder.position()),
                    })?;
                    keys.push(String::new());
                }
            }
            decoder.skip().map_err(|e| SanshoError::Input {
                message: format!("at byte {}: item: {e}", decoder.position()),
            })?;
        }
        Ok(keys)
    }
}

impl<'a> Node<'a> for CursorNode<'a> {
    fn kind(&self) -> Kind {
        self.header_kind().unwrap_or(Kind::Null)
    }

    fn as_str(&self) -> Option<Cow<'a, str>> {
        let mut decoder = minicbor::Decoder::new(self.bytes);
        decoder.set_position(self.position);
        match self.header_kind() {
            Ok(Kind::String) => {
                // byte strings render base64url (owned); text borrows
                let byte = self.bytes[self.position];
                if byte >> 5 == 2 {
                    decoder
                        .bytes()
                        .ok()
                        .map(|b| Cow::Owned(base64url_encode(b)))
                } else {
                    decoder.str().ok().map(Cow::Borrowed)
                }
            }
            _ => None,
        }
    }

    fn as_f64(&self) -> Option<f64> {
        self.materialize().ok()?.as_f64()
    }

    fn as_bool(&self) -> Option<bool> {
        self.materialize().ok()?.as_bool()
    }

    fn is_null(&self) -> bool {
        self.header_kind() == Ok(Kind::Null)
    }

    fn get_key(&self, name: &str) -> Option<Self> {
        let positions = self.container_positions().ok()?;
        let keys = self.map_keys().ok()?;
        for (key, position) in keys.iter().zip(positions.into_iter()) {
            if key == name {
                return Some(self.child(position));
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
        let positions = match self.container_positions() {
            Ok(positions) => positions,
            Err(_) => return Vec::new(),
        };
        let keys = match self.map_keys() {
            Ok(keys) => keys,
            Err(_) => return Vec::new(),
        };
        for (key, position) in keys.into_iter().zip(positions.into_iter()) {
            pairs.push((key, self.child(position)));
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
        let mut decoder = minicbor::Decoder::new(self.bytes);
        decoder.set_position(self.position);
        match self.header_kind().ok()? {
            Kind::Array => decoder.array().ok()?.map(|len| len as usize),
            Kind::Object => decoder.map().ok()?.map(|len| len as usize),
            _ => None,
        }
    }

    fn counts_toward_aggregation() -> bool {
        true
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        self.memo.decode(self.bytes, self.position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The diagnostic probe: the body-map's key-lookup internals.
    #[test]
    fn probe_body_key_walk() {
        let item = serde_json::json!({
            "identifier": "x",
            "body": {"extra": {}, "file_names": (0..4000).map(|i| format!("gitoid:blob:sha256:{:064x}!$org/apache/logging/log4j/core/lookup/JndiLookup.java", i)).collect::<Vec<String>>(), "file_size": 3050, "mime_type": ["t"]}
        });
        let bytes = serde_cbor::to_vec(&item).unwrap();
        let root = CursorNode::root(&bytes);
        println!(
            "PROBE root kind={:?} root.get_key(body)={:?}",
            root.kind(),
            root.get_key("body").map(|n| n.position)
        );
        let body = root.get_key("body").expect("body exists");
        println!("PROBE body kind={:?} pos={}", body.kind(), body.position);
        let positions = body.container_positions();
        println!("PROBE body positions={positions:?}");
        let keys = body.map_keys();
        println!("PROBE body keys={keys:?}");
        println!(
            "PROBE body.get_key(extra)={:?}",
            body.get_key("extra").map(|n| n.position)
        );
        println!(
            "PROBE body.get_key(file_size)={:?}",
            body.get_key("file_size").map(|n| n.position)
        );
        // the eval-path probe: the same walk through the evaluator
        let parsed = crate::parser::parse("body.file_size").unwrap();
        let program = crate::program::compile(&parsed).unwrap();
        let flow = crate::eval::eval_program_for_tests(&program, &root);
        let value = match flow {
            Ok(f) => f.as_value(&crate::limits::EvalContext::new(
                crate::limits::Limits::default(),
            )),
            Err(stop) => Err(crate::SanshoError::Evaluation {
                message: format!("{stop:?}"),
            }),
        };
        println!("PROBE eval body.file_size={value:?}");
        // and the direct materialization at the found position:
        if let Some(node) = body.get_key("file_size") {
            println!("PROBE materialize(file_size)={:?}", node.materialize());
        }
    }
}
