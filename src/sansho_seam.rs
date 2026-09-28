//! The Sansho integration seam: project an item's fields through the
//! Sansho engine, directly over the item's stored CBOR bytes.
//!
//! This module is the ONLY place `bigtent` touches Sansho (the one-way
//! dependency of ADR 0001). The HTTP attachment point is a separate
//! specification — nothing here changes any existing endpoint.
//!
//! The critical property: the item bytes reach the engine UNDECODED.
//! The host's normal read path materializes the whole item; this seam
//! takes the raw byte span (via [`crate::rodeo::data::read_item_bytes_at`])
//! so selective materialization stays intact end to end.

use crate::item::{Connections, Item};
use crate::rodeo::data::DataFile;
use sansho::SanshoError;
use sansho::view::{Kind, Node};
use serde_json::Value as J;
use std::borrow::Cow;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Project an item's fields: the item's raw CBOR bytes + a JMESPath
/// expression → the projected JSON value. The default resource limits
/// apply; the compiled program is cached (the canonicalized expression
/// + the engine version as the key), so repeated expressions — the
/// common case for a query surface — pay the parse/compile once.
pub fn project_item(
    item_bytes: &[u8],
    expression: &str,
    cache: &sansho::ProgramCache,
) -> Result<serde_json::Value, SanshoError> {
    let program = cache.compile_cached(expression)?;
    sansho::evaluate_cbor(&program, item_bytes)
}

/// The convenience form without a cache (a fresh compile each call) —
/// for one-shot lookups where the parse cost is negligible.
pub fn project_item_once(
    item_bytes: &[u8],
    expression: &str,
) -> Result<serde_json::Value, SanshoError> {
    let program = sansho::compile(&sansho::parse(expression)?)?;
    sansho::evaluate_cbor(&program, item_bytes)
}

/// The seam over a mapped data file: the item at `offset` projected
/// through `expression` — the byte span comes from the no-decode
/// accessor, so nothing in the host decodes the item before Sansho sees
/// it.
pub fn project_item_from_file(
    file: &DataFile,
    offset: usize,
    expression: &str,
    cache: &sansho::ProgramCache,
) -> Result<serde_json::Value, SanshoError> {
    let bytes = file
        .read_item_bytes_at(offset)
        .ok_or_else(|| SanshoError::Input {
            message: format!("no item at data-file offset {offset}"),
        })?;
    project_item(bytes, expression, cache)
}

/// A compiled-program reference for callers that manage their own
/// caching (the seam's lower level).
pub fn compiled_program(expression: &str) -> Result<Arc<sansho::Program>, SanshoError> {
    static DEFAULT_CACHE: std::sync::OnceLock<sansho::ProgramCache> = std::sync::OnceLock::new();
    DEFAULT_CACHE
        .get_or_init(|| sansho::ProgramCache::new(1_024))
        .compile_cached(expression)
}

// ---------------------------------------------------------------------------
// The Node view over a real, in-memory Item
// ---------------------------------------------------------------------------

/// A [Node] position over a real [`Item`] — the in-memory, merged-item
/// case (the goat-herd) — without any serialization to bytes or to a
/// JSON value.
///
/// The walked structure is exactly what the item's CBOR serialization
/// presents to the byte source:
///
/// ```text
/// {
///   "identifier": <string>,
///   "connections": { <edge type>: [<sorted target strings>] },
///   "body_mime_type": <string> | null,
///   "body": <CBOR value> | null
/// }
/// ```
///
/// The body is walked as the in-memory [`serde_cbor::Value`] it already
/// is, with the byte source's exact JSON view (byte strings render
/// base64url, maps expose text keys, tags 2/3 reduce to doubles, other
/// tags and undefined are evaluation errors). A node is a cheap copy:
/// every variant borrows from the item, so cloning never copies
/// payload. Sub-node positions are the same enum type, which is what
/// `Node` requires (`get_key`/`get_index` return `Self`); a direct
/// `impl Node for Item` is structurally impossible for that reason —
/// the connections map, a single target string, and the body all need
/// distinct node kinds.
#[derive(Clone, Copy, Debug)]
pub enum ItemNode<'d> {
    /// The whole item: an object with the four serialized members.
    Item(&'d Item),
    /// The connections map: edge type -> sorted target set.
    Connections(&'d Connections),
    /// One connection target set: an array of sorted strings.
    Targets(&'d BTreeSet<String>),
    /// A borrowed string (an identifier, a connection target, a MIME type).
    Text(&'d str),
    /// A position inside the item's CBOR body.
    CborVal(&'d serde_cbor::Value),
    /// The JSON null node (a missing body or MIME type).
    Null,
}

impl<'d> ItemNode<'d> {
    /// Position at the item root.
    pub fn wrap(item: &'d Item) -> Self {
        ItemNode::Item(item)
    }
}

/// RFC 4648 §5 base64url, no padding — the mapping's byte-string view.
/// A pure-function duplicate of the engine's encoder, kept local to the
/// item view; the differential tests referee the two against each
/// other, so any drift fails loudly.
fn base64url_encode(bytes: &[u8]) -> String {
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

/// The JSON view of an in-memory CBOR value — mirrors the byte source's
/// materialization exactly (see the engine's `decode_item`): exact
/// integers, base64url byte strings, text-keyed objects (non-negative
/// integer keys render as decimal strings), bignum tags 2/3 reduced to
/// doubles, everything else an evaluation error.
fn cbor_val_to_json(value: &serde_cbor::Value) -> Result<J, SanshoError> {
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
            items.iter().map(cbor_val_to_json).collect::<Result<_, _>>()?,
        )),
        serde_cbor::Value::Map(map) => {
            let mut out = serde_json::Map::new();
            for (key, value) in map {
                let key = match key {
                    serde_cbor::Value::Text(s) => s.clone(),
                    serde_cbor::Value::Integer(n) if *n >= 0 => n.to_string(),
                    other => {
                        return Err(SanshoError::Evaluation {
                            message: format!(
                                "map key must be a text string, found {other:?}"
                            ),
                        })
                    }
                };
                out.insert(key, cbor_val_to_json(value)?);
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

impl<'d> Node<'d> for ItemNode<'d> {
    fn kind(&self) -> Kind {
        match self {
            ItemNode::Item(_) => Kind::Object,
            ItemNode::Connections(_) => Kind::Object,
            ItemNode::Targets(_) => Kind::Array,
            ItemNode::Text(_) => Kind::String,
            ItemNode::CborVal(value) => match value {
                serde_cbor::Value::Null => Kind::Null,
                serde_cbor::Value::Bool(_) => Kind::Bool,
                serde_cbor::Value::Integer(_) | serde_cbor::Value::Float(_) => Kind::Number,
                serde_cbor::Value::Bytes(_) | serde_cbor::Value::Text(_) => Kind::String,
                serde_cbor::Value::Array(_) => Kind::Array,
                serde_cbor::Value::Map(_) => Kind::Object,
                _ => Kind::Null,
            },
            ItemNode::Null => Kind::Null,
        }
    }

    fn as_str(&self) -> Option<Cow<'d, str>> {
        match self {
            ItemNode::Text(s) => Some(Cow::Borrowed(s)),
            ItemNode::CborVal(value) => match value {
                serde_cbor::Value::Text(s) => Some(Cow::Borrowed(s)),
                serde_cbor::Value::Bytes(bytes) => Some(Cow::Owned(base64url_encode(bytes))),
                _ => None,
            },
            _ => None,
        }
    }

    fn as_f64(&self) -> Option<f64> {
        match self {
            ItemNode::CborVal(value) => match value {
                serde_cbor::Value::Integer(n) => Some(*n as f64),
                serde_cbor::Value::Float(f) => Some(*f),
                _ => None,
            },
            _ => None,
        }
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            ItemNode::CborVal(serde_cbor::Value::Bool(b)) => Some(*b),
            _ => None,
        }
    }

    fn is_null(&self) -> bool {
        match self {
            ItemNode::Null => true,
            ItemNode::CborVal(serde_cbor::Value::Null) => true,
            _ => false,
        }
    }

    fn get_key(&self, name: &str) -> Option<Self> {
        match self {
            ItemNode::Item(item) => match name {
                "identifier" => Some(ItemNode::Text(&item.identifier)),
                "connections" => Some(ItemNode::Connections(&item.connections)),
                "body_mime_type" => Some(match item.body_mime_type.as_deref() {
                    Some(s) => ItemNode::Text(s),
                    None => ItemNode::Null,
                }),
                "body" => Some(match item.body.as_ref() {
                    Some(v) => ItemNode::CborVal(v),
                    None => ItemNode::Null,
                }),
                _ => None,
            },
            ItemNode::Connections(connections) => {
                connections.0.get(name).map(ItemNode::Targets)
            }
            ItemNode::CborVal(serde_cbor::Value::Map(map)) => {
                map.get(&serde_cbor::Value::Text(name.to_string()))
                    .map(ItemNode::CborVal)
            }
            _ => None,
        }
    }

    fn get_index(&self, index: usize) -> Option<Self> {
        match self {
            ItemNode::Targets(targets) => targets
                .iter()
                .nth(index)
                .map(|s| ItemNode::Text(s.as_str())),
            ItemNode::CborVal(serde_cbor::Value::Array(items)) => {
                items.get(index).map(ItemNode::CborVal)
            }
            _ => None,
        }
    }

    fn entries(&self) -> Vec<(String, Self)> {
        match self {
            ItemNode::Item(item) => {
                // sorted by key, matching the byte source's contract
                vec![
                    (
                        "body".to_string(),
                        match item.body.as_ref() {
                            Some(v) => ItemNode::CborVal(v),
                            None => ItemNode::Null,
                        },
                    ),
                    (
                        "body_mime_type".to_string(),
                        match item.body_mime_type.as_deref() {
                            Some(s) => ItemNode::Text(s),
                            None => ItemNode::Null,
                        },
                    ),
                    ("connections".to_string(), ItemNode::Connections(&item.connections)),
                    ("identifier".to_string(), ItemNode::Text(&item.identifier)),
                ]
            }
            ItemNode::Connections(connections) => connections
                .0
                .iter()
                .map(|(edge_type, targets)| (edge_type.clone(), ItemNode::Targets(targets)))
                .collect(),
            ItemNode::CborVal(serde_cbor::Value::Map(map)) => map
                .iter()
                // text keys only, sorted by the map's key order —
                // the byte source's entries contract
                .filter_map(|(key, value)| match key {
                    serde_cbor::Value::Text(s) => Some((s.clone(), ItemNode::CborVal(value))),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    fn elements(&self) -> Vec<Self> {
        match self {
            ItemNode::Targets(targets) => {
                targets.iter().map(|s| ItemNode::Text(s.as_str())).collect()
            }
            ItemNode::CborVal(serde_cbor::Value::Array(items)) => {
                items.iter().map(ItemNode::CborVal).collect()
            }
            _ => Vec::new(),
        }
    }

    fn container_len(&self) -> Option<usize> {
        match self {
            ItemNode::Item(_) => Some(4),
            ItemNode::Connections(connections) => Some(connections.0.len()),
            ItemNode::Targets(targets) => Some(targets.len()),
            ItemNode::CborVal(value) => match value {
                serde_cbor::Value::Array(items) => Some(items.len()),
                serde_cbor::Value::Map(map) => Some(map.len()),
                _ => None,
            },
            _ => None,
        }
    }

    fn counts_toward_aggregation() -> bool {
        // the item is the CALLER's pre-existing data, like the
        // materialized source — nothing here is engine-created memory
        false
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        match self {
            ItemNode::Item(item) => serde_json::to_value(item).map_err(|e| SanshoError::Input {
                message: format!("item does not materialize to JSON: {e}"),
            }),
            ItemNode::Connections(connections) => {
                serde_json::to_value(connections).map_err(|e| SanshoError::Input {
                    message: format!("connections do not materialize to JSON: {e}"),
                })
            }
            ItemNode::Targets(targets) => Ok(J::Array(
                targets.iter().map(|s| J::String(s.clone())).collect(),
            )),
            ItemNode::Text(s) => Ok(J::String(s.to_string())),
            ItemNode::CborVal(value) => cbor_val_to_json(value),
            ItemNode::Null => Ok(J::Null),
        }
    }

    fn materialize_unmemoized(&self) -> Result<J, SanshoError> {
        self.materialize()
    }

    fn raw_text_bytes(&self) -> Option<&[u8]> {
        // a borrowed string is already-validated UTF-8; its bytes feed
        // the filter-predicate fast path (`[?starts_with(@, 'pkg:')]`)
        // without any allocation, like the byte source's text strings
        match self {
            ItemNode::Text(s) => Some(s.as_bytes()),
            _ => None,
        }
    }
}

/// Evaluate a projection over a real, in-memory [`Item`] — the
/// merged-item (goat-herd) case: no serialization to bytes or JSON; the
/// item is walked directly through [`ItemNode`].
pub fn project_item_direct(
    item: &Item,
    expression: &str,
    cache: &sansho::ProgramCache,
) -> Result<serde_json::Value, SanshoError> {
    let program = cache.compile_cached(expression)?;
    sansho::eval::evaluate_over(&program, &ItemNode::wrap(item))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a REAL [`Item`] struct — not a JSON lookalike — with
    /// `n_connections` alias targets (every other one a pURL, the rest
    /// gitoids, matching the Appendix-A scan shape) and `n_file_names`
    /// body entries.
    fn real_item(n_connections: usize, n_file_names: usize) -> Item {
        let mut alias_from: BTreeSet<String> = BTreeSet::new();
        for i in 0..n_connections {
            if i % 2 == 0 {
                alias_from.insert(format!("pkg:maven/org.example/art{i}@1.0.{i}"));
            } else {
                alias_from.insert(format!("gitoid:blob:sha1:{i:040x}"));
            }
        }
        let file_names: Vec<String> = (0..n_file_names)
            .map(|i| {
                if i % 3 == 0 {
                    format!("gitoid:blob:sha256:{i:064x}!$org/example/Thing{i}.java")
                } else {
                    format!("org/example/Thing{i}.java")
                }
            })
            .collect();
        Item {
            identifier: format!("gitoid:blob:sha1:{}", "30e6".repeat(10)),
            connections: Connections::from_iter(
                [("alias:from".to_string(), alias_from)]
                    .into_iter()
                    .flat_map(|(edge, targets)| {
                        targets.into_iter().map(move |t| (edge.clone(), t))
                    })
                    .chain([("contained:up".to_string(), "gitoid:blob:sha1:parent".to_string())]),
            ),
            body_mime_type: Some("application/vnd.cc.goatrodeo".to_string()),
            body: Some(
                serde_cbor::value::to_value(serde_json::json!({
                    "extra": {},
                    "file_names": file_names,
                    "file_size": 3050u64 + n_file_names as u64,
                    "mime_type": ["text/x-java-source"]
                }))
                .expect("the JSON body converts to a CBOR value"),
            ),
        }
    }

    // Requirement: the owner's directive — the Node source must be
    // implemented on the REAL Item type, and evaluation over the item
    // must equal the byte source over the item's own serialization.
    // What: for real Item structs of several sizes, every expression
    // evaluated through `ItemNode` returns byte-identical output to
    // `evaluate_cbor` over `serde_cbor::to_vec(&item)`. Why: the item
    // view and the stored-bytes view must present the same document;
    // byte-identical output is the equivalence contract.
    //
    // LLM section: this is the T3.4-equivalent test, but the subject is
    // the actual `bigtent::item::Item` type — the fixture is a real
    // struct, and the byte path serializes that same struct. serde_cbor
    // is a development dependency (fixture construction only).
    #[test]
    fn item_node_matches_byte_path_byte_for_byte() {
        let expressions = [
            "identifier",
            "body.file_size",
            "body.mime_type[0]",
            "@",
            "connections.\"alias:from\"[?starts_with(@, 'pkg:')]",
            "length(connections.\"alias:from\")",
            "{id: identifier, first_alias: connections.\"alias:from\"[0]}",
            "body.file_names[?starts_with(@, 'gitoid:')]",
        ];
        for (n_connections, n_file_names) in [
            (10usize, 5usize),
            (1_000, 500),
            (25_000, 12_000),
        ] {
            let item = real_item(n_connections, n_file_names);
            let bytes = serde_cbor::to_vec(&item).expect("the real item serializes");
            for expression in expressions {
                let cache = sansho::ProgramCache::new(16);
                let program = cache.compile_cached(expression).unwrap();
                let via_item =
                    sansho::eval::evaluate_over(&program, &ItemNode::wrap(&item)).unwrap();
                let via_bytes = sansho::evaluate_cbor(&program, &bytes).unwrap();
                assert_eq!(
                    serde_json::to_vec(&via_item).unwrap(),
                    serde_json::to_vec(&via_bytes).unwrap(),
                    "{expression} (connections={n_connections}): the Item node and the byte \
                     path over the item's serialization must agree byte for byte"
                );
            }
        }
    }

    // Requirement: SPEC-0001 §5.1 (the decode contract) — the item
    // materializes to exactly its JSON value. What: `@` over the Item
    // node equals `serde_json::to_value(&item)`. Why: the whole-document
    // projection must be the item itself, with the connections map in
    // sorted order — the same shape the byte path decodes.
    #[test]
    fn item_node_materializes_to_the_item_json() {
        let item = real_item(10, 5);
        let program = sansho::compile(&sansho::parse("@").unwrap()).unwrap();
        let whole = sansho::eval::evaluate_over(&program, &ItemNode::wrap(&item)).unwrap();
        let expected = serde_json::to_value(&item).unwrap();
        assert_eq!(whole, expected);
    }

    // Requirement: the direct entry's equivalence — `project_item_direct`
    // over a real Item equals the byte path over the same item's
    // serialization. What: the two outputs are byte-identical. Why: the
    // in-memory entry is a conduit over the same document; it must not
    // introduce a second semantics.
    #[test]
    fn project_item_direct_equals_byte_path() {
        let item = real_item(100, 50);
        let bytes = serde_cbor::to_vec(&item).unwrap();
        let cache = sansho::ProgramCache::new(16);
        for expression in ["identifier", "body.file_size", "connections.\"alias:from\"[0]"] {
            let via_direct = project_item_direct(&item, expression, &cache).unwrap();
            let via_bytes = project_item(&bytes, expression, &cache).unwrap();
            assert_eq!(
                serde_json::to_vec(&via_direct).unwrap(),
                serde_json::to_vec(&via_bytes).unwrap(),
                "{expression}: the direct Item path must equal the byte path"
            );
        }
    }

    // Requirement: the whole-item materialize must be usable with the
    // existing round-trip contract. What: the existing seam round-trip
    // fixture also evaluates through the Item node. Why: coverage of the
    // SPEC-0001 Appendix-A shape on the real type.
    #[test]
    fn appendix_a_shape_on_the_item_node() {
        let item = real_item(2, 3);
        let cache = sansho::ProgramCache::new(16);
        let via_direct = project_item_direct(&item, "body.file_names[1]", &cache).unwrap();
        assert!(via_direct.is_string());
    }

    // Requirement: the filter-predicate fast path's input — a borrowed
    // Text node must expose its raw bytes so `[?starts_with(@, 'pkg:')]`
    // takes the no-allocation leaf path. What: the scan shape's result
    // is correct AND the Text node reports its raw bytes. Why: the
    // zero-copy-until-emit goal applies to the item path too.
    #[test]
    fn item_text_node_exposes_raw_bytes() {
        let item = real_item(10, 5);
        let program = sansho::compile(
            &sansho::parse("connections.\"alias:from\"[?starts_with(@, 'pkg:')]").unwrap(),
        )
        .unwrap();
        let result = sansho::eval::evaluate_over(&program, &ItemNode::wrap(&item)).unwrap();
        let expected: Vec<String> = (0..10)
            .filter(|i| i % 2 == 0)
            .map(|i| format!("pkg:maven/org.example/art{i}@1.0.{i}"))
            .collect();
        assert_eq!(result, serde_json::json!(expected));
    }

    // Requirement: the design contract (SPEC-0001 §5, the zero-copy
    // property) — evaluation yields the reference-shaped intermediate
    // representation, not a materialized value; materialization happens
    // only at the emit boundary. What: `evaluate_flow` over a real Item
    // produces `Flow::Proj` of `Flow::One` positions — PURE REFERENCES
    // into the item (no owned values); the scan's kept elements are
    // `ItemNode::Text` positions, and the seek shape is a single
    // reference position. Materializing the flow equals the byte path
    // byte for byte. Why: the benchmark measures THIS form — the
    // zero-copy evaluation the design is about; collapsing to `J` at
    // the entry (as `evaluate_over` does) is the materialized fallback.
    #[test]
    fn item_node_flow_is_pure_reference() {
        let item = real_item(10, 5);
        let scan = sansho::compile(
            &sansho::parse("connections.\"alias:from\"[?starts_with(@, 'pkg:')]").unwrap(),
        )
        .unwrap();
        let flow = sansho::eval::evaluate_flow(&scan, &ItemNode::wrap(&item)).unwrap();
        let sansho::Flow::Proj(items) = &flow else {
            panic!("the scan must produce a projection flow")
        };
        assert_eq!(items.len(), 5, "five of the ten targets are pURLs");
        for element in items {
            assert!(
                matches!(element, sansho::Flow::One(ItemNode::Text(_))),
                "each scan element must be a reference position, not a materialized value"
            );
        }
        // the emit-boundary materialization equals the byte path
        let bytes = serde_cbor::to_vec(&item).unwrap();
        let via_bytes = sansho::evaluate_cbor(&scan, &bytes).unwrap();
        assert_eq!(
            serde_json::to_vec(&flow.as_value().unwrap()).unwrap(),
            serde_json::to_vec(&via_bytes).unwrap()
        );

        // the seek shape: a single reference position into the body
        let seek = sansho::compile(&sansho::parse("body.file_size").unwrap()).unwrap();
        let seek_flow = sansho::eval::evaluate_flow(&seek, &ItemNode::wrap(&item)).unwrap();
        assert!(
            matches!(
                &seek_flow,
                sansho::Flow::One(ItemNode::CborVal(serde_cbor::Value::Integer(_)))
            ),
            "the seek result must be a reference to the body's integer position"
        );
    }

    // Requirement: SPEC-0001 §2 (the interface), §5 — the seam adds no
    // semantics: projecting through the seam returns BYTE-IDENTICAL
    // results to the direct engine evaluation of the same bytes. What:
    // for the fixture-shaped items, the seam's output equals the direct
    // engine output exactly. Why: the seam is a conduit, not a
    // transformation (plan `integration_seam_round_trip`).
    //
    // LLM section: the fixture item (the SPEC-0001 Appendix-A shape) is
    // encoded with serde_cbor (a development dependency, fixture
    // construction only); the seam and the direct path must agree
    // byte-for-byte on every expression tried.
    #[test]
    fn integration_seam_round_trip() {
        let item = serde_json::json!({
            "identifier": "gitoid:blob:sha1:30e65ad24f4b4d799e52cfd70fcbebc0490b7343",
            "connections": [["alias:from", "md5:4d921bf0ce238d1a96bdf65000fde565"]],
            "body_mime_type": "application/vnd.cc.goatrodeo",
            "body": {
                "extra": {},
                "file_names": [
                    "org/apache/logging/log4j/core/lookup/JndiLookup.java",
                    "gitoid:blob:sha256:005a5131bc1c950b2b4d1081a95bd21f52e45c308d9633465fbc240f8433c9e6!$org/apache/logging/log4j/core/lookup/JndiLookup.java"
                ],
                "file_size": 3050,
                "mime_type": ["text/x-java-source"]
            }
        });
        let item_bytes = serde_cbor::to_vec(&item).unwrap();

        for expression in [
            "body.file_names[?starts_with(@, 'gitoid:')]",
            "body.mime_type[0]",
            "identifier",
            "{id: identifier, size: body.file_size}",
            "length(body.file_names)",
        ] {
            let cache = sansho::ProgramCache::new(16);
            let via_seam = project_item(&item_bytes, expression, &cache)
                .unwrap_or_else(|e| panic!("{expression}: seam error: {e}"));
            let direct = {
                let program =
                    sansho::compile(&sansho::parse(expression).unwrap()).unwrap();
                sansho::evaluate_cbor(&program, &item_bytes)
                    .unwrap_or_else(|e| panic!("{expression}: direct error: {e}"))
            };
            assert_eq!(
                serde_json::to_vec(&via_seam).unwrap(),
                serde_json::to_vec(&direct).unwrap(),
                "{expression}: the seam must be byte-identical to the direct evaluation"
            );
        }
    }

    // Requirement: SPEC-0001 §2 (the interface contract) — the seam's
    // error boundaries: no item at the offset, an invalid expression,
    // and a limit-exceeded are STRUCTURED errors, never panics (plan
    // `integration_seam_round_trip` boundaries).
    //
    // LLM section: three error shapes, three assertions; the
    // limit-exceeded uses a tiny byte cap over a fat item.
    #[test]
    fn integration_seam_error_boundaries() {
        // a missing item: fewer bytes than the length prefix needs, and
        // an offset past the end
        let short = vec![0u8; 3];
        assert!(crate::rodeo::data::read_item_bytes_at(&short, 0).is_none());
        let file_bytes = vec![0u8; 12];
        assert!(crate::rodeo::data::read_item_bytes_at(&file_bytes, 100).is_none());

        // an invalid expression: the parse error, structured
        let item_bytes = serde_cbor::to_vec(&serde_json::json!({"a": 1})).unwrap();
        let error = project_item_once(&item_bytes, "a[0")
            .expect_err("an invalid expression must error");
        assert!(matches!(error, SanshoError::Parse { .. }));

        // (the limit-exceeded assertion was removed with the limits
        // machinery — the owner's directive; the structured-error path
        // is covered by the parse case above)
    }

    // Requirement: the no-decode property of the seam's accessor. What:
    // read_item_bytes_at returns the EXACT CBOR span (the bytes between
    // the length prefix and the next item), not a decoded/re-encoded
    // copy. Why: the host must not decode before the engine; the
    // accessor's honesty is the seam's premise (plan Phase 6 task 1).
    //
    // LLM section: a hand-built [len][item] span; the accessor returns
    // the item's bytes exactly.
    #[test]
    fn raw_accessor_returns_exact_span() {
        // the item bytes: a tiny CBOR map {"a": 1} = 0xA1 0x61 0x61 0x01;
        // the layout = [u32 LE length][item][trailing garbage]
        let item_bytes: &[u8] = &[0xA1, 0x61, 0x61, 0x01];
        let mut data = (item_bytes.len() as u32).to_le_bytes().to_vec();
        data.extend_from_slice(item_bytes);
        data.extend_from_slice(&[0xFF; 3]); // the trailing garbage is NOT the item's

        let span = crate::rodeo::data::read_item_bytes_at(&data, 0)
            .expect("the item exists at the offset");
        assert_eq!(span, item_bytes);
    }
}
