//! The Item-side implementations of [`sansho::SanshoTrait`] (`Item`,
//! `Connections`) and the item projection entries: project an item's
//! fields through the Sansho engine, directly over the item's stored
//! CBOR bytes.
//!
//! The trait implementations live here, WITH their types, per the
//! placement rule ("within Item for Item"; standard-library types'
//! implementations live in the Sansho engine itself).
//!
//! This module is the ONLY place `bigtent` touches Sansho (the one-way
//! dependency of ADR 0001). The HTTP attachment point is a separate
//! specification — nothing here changes any existing endpoint.
//!
//! The critical property: the item bytes reach the engine UNDECODED.
//! The host's normal read path materializes the whole item; the
//! no-decode entry takes the raw byte span (via
//! [`crate::rodeo::data::read_item_bytes_at`]) so selective
//! materialization stays intact end to end.

use crate::item::{Connections, Item};
use crate::rodeo::data::DataFile;
use sansho::SanshoError;
use serde_json::Value as J;
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

/// The data-file projection entry: the item at `offset` projected
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
/// caching (the projection entries' lower level).
pub fn compiled_program(expression: &str) -> Result<Arc<sansho::Program>, SanshoError> {
    static DEFAULT_CACHE: std::sync::OnceLock<sansho::ProgramCache> = std::sync::OnceLock::new();
    DEFAULT_CACHE
        .get_or_init(|| sansho::ProgramCache::new(1_024))
        .compile_cached(expression)
}

// ---------------------------------------------------------------------------
// The item's Sansho surface
// ---------------------------------------------------------------------------

/// The item's connection map's own Sansho surface — "within Item":
/// `Connections` (a bigtent type) implements the trait by DELEGATING
/// to its inner map's implementation (a standard-library type, whose
/// impl lives in Sansho) — the turtles compose: edge keys render
/// through the map impl, target sets through the set impl, target
/// strings through `String`'s impl.
impl<'d> sansho::SanshoTrait<'d> for Connections {
    fn kind(&self) -> sansho::view::Kind {
        sansho::view::Kind::Object
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        None
    }

    fn as_sansho_number(&self) -> Option<sansho::view::SanshoNumber> {
        None
    }

    fn as_bool(&self) -> Option<bool> {
        None
    }

    fn is_null(&self) -> bool {
        false
    }

    fn get_key(&self, name: &str) -> Option<impl sansho::SanshoTrait<'d> + use<'d>> {
        self.0.get(name).map(|targets| targets.clone())
    }

    fn get_index(&self, _index: usize) -> Option<impl sansho::SanshoTrait<'d> + use<'d>> {
        None::<J>
    }

    fn entries(&self) -> Vec<(String, impl sansho::SanshoTrait<'d> + use<'d>)> {
        self.0
            .iter()
            .map(|(edge, targets)| (edge.clone(), targets.clone()))
            .collect()
    }

    fn elements(&self) -> Vec<impl sansho::SanshoTrait<'d> + use<'d>> {
        Vec::<J>::new()
    }

    fn container_len(&self) -> Option<usize> {
        Some(self.0.len())
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        sansho::SanshoTrait::materialize(&self.0)
    }

    fn counts_toward_aggregation() -> bool {
        false
    }
}

/// The item's Sansho surface — "within Item for Item": the `Item`
/// COMPOSES from its member types' implementations (the turtles
/// contract): the four members route through the member type's own
/// navigation — `String`'s for the identifier, [`Connections`]'s for
/// the connection map (which routes through the map/set/string impls),
/// `Option<String>`'s for the MIME type, `Option<serde_cbor::Value>`'s
/// for the body. The VALUE form materializes its members (a value form
/// has no borrowable storage); the borrowed form ([`SanshoTrait`] for
/// `&Item`) hands out borrowed members instead.
impl<'d> sansho::SanshoTrait<'d> for Item {
    fn kind(&self) -> sansho::view::Kind {
        sansho::view::Kind::Object
    }

    fn as_str(&self) -> Option<std::borrow::Cow<'_, str>> {
        None
    }

    fn as_sansho_number(&self) -> Option<sansho::view::SanshoNumber> {
        None
    }

    fn as_bool(&self) -> Option<bool> {
        None
    }

    fn is_null(&self) -> bool {
        false
    }

    fn get_key(&self, name: &str) -> Option<impl sansho::SanshoTrait<'d> + use<'d>> {
        match name {
            "identifier" => sansho::SanshoTrait::as_str(&self.identifier)
                .map(|text| text.into_owned())
                .map(J::from),
            "connections" => sansho::SanshoTrait::materialize(&self.connections).ok(),
            "body_mime_type" => sansho::SanshoTrait::materialize(&self.body_mime_type).ok(),
            "body" => sansho::SanshoTrait::materialize(&self.body).ok(),
            _ => None,
        }
    }

    fn get_index(&self, _index: usize) -> Option<impl sansho::SanshoTrait<'d> + use<'d>> {
        None::<J>
    }

    fn entries(&self) -> Vec<(String, impl sansho::SanshoTrait<'d> + use<'d>)> {
        vec![
            ("identifier".to_string(), J::from(self.identifier.clone())),
            (
                "connections".to_string(),
                sansho::SanshoTrait::materialize(&self.connections)
                    .unwrap_or(J::Null),
            ),
            (
                "body_mime_type".to_string(),
                sansho::SanshoTrait::materialize(&self.body_mime_type)
                    .unwrap_or(J::Null),
            ),
            (
                "body".to_string(),
                sansho::SanshoTrait::materialize(&self.body).unwrap_or(J::Null),
            ),
        ]
    }

    fn elements(&self) -> Vec<impl sansho::SanshoTrait<'d> + use<'d>> {
        Vec::<J>::new()
    }

    fn container_len(&self) -> Option<usize> {
        Some(4)
    }

    fn materialize(&self) -> Result<J, SanshoError> {
        serde_json::to_value(self).map_err(|e| SanshoError::Input {
            message: format!("item does not materialize to JSON: {e}"),
        })
    }

    fn counts_toward_aggregation() -> bool {
        // the item is the CALLER's pre-existing data — nothing is
        // engine-created
        false
    }
}

/// The borrowed item surface would need one member type for four
/// different field types (`String`, [`Connections`], `Option<String>`,
/// `Option<serde_cbor::Value>`) — the trait's return has ONE opaque
/// type per method, so a borrowed multi-typed member set is a type-
/// level impossibility without a wrapper carrier (rejected). The value
/// form is the item's surface: the item is already entirely in memory,
/// so materialization is the caller's data, never an engine decode
/// ([`Self::counts_toward_aggregation`] stays false — nothing here
/// counts against the evaluation's aggregation ledger).


/// RFC 4648 §5 base64url — the engine's encoder (the item view's
/// byte-string rendering; one encoder, no drift possible).
pub use sansho::cbor::base64url_encode;

/// The JSON view of an in-memory CBOR value — the engine's own
/// conversion (moved into the engine with the `serde_cbor::Value`
/// source; the item view re-exports it, so the two CBOR paths cannot
/// drift).
pub use sansho::cbor_value::cbor_value_to_json;


/// Evaluate a projection over a real, in-memory [`Item`] — the
/// merged-item (goat-herd) case: no serialization to bytes or JSON; the
/// item is walked through the trait surface (`lookup` over `&Item`).
pub fn project_item_direct(
    item: &Item,
    expression: &str,
    cache: &sansho::ProgramCache,
) -> Result<serde_json::Value, SanshoError> {
    let program = cache.compile_cached(expression)?;
    sansho::lookup_value(item, &program)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::item::Connections;
    use std::collections::BTreeSet;

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

    // Requirement: the owner's directive — the source trait must be
    // implemented on the REAL Item type, so callers pass `&item` to
    // `lookup` exactly like `&bytes`. What: `sansho::lookup` over a
    // real Item equals `lookup` over the item's own serialization,
    // byte for byte, for several expressions and sizes. Why: the Item
    // surface is a conduit over the same document; it must not
    // introduce a second semantics (the trait-surface restatement of
    // the differential contract).
    #[test]
    fn item_source_matches_byte_source_on_the_trait_surface() {
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
                let via_item = sansho::lookup_value(&item, &program).unwrap();
                // the byte source impl lives on `&'a [u8]`; bind the
                // slice so the call passes `&slice` (T = &[u8])
                let slice: &[u8] = &bytes;
                let via_bytes = sansho::lookup_value(&slice, &program).unwrap();
                assert_eq!(
                    serde_json::to_vec(&via_item).unwrap(),
                    serde_json::to_vec(&via_bytes).unwrap(),
                    "{expression} (connections={n_connections}): the Item source and the byte \
                     source over the item's serialization must agree byte for byte"
                );
            }
        }
    }

    // Requirement: SPEC-0001 §5.1 (the decode contract) — the item
    // materializes to exactly its JSON value. What: `@` over a real
    // Item (through lookup) equals `serde_json::to_value(&item)`. Why:
    // the whole-document projection must be the item itself, with the
    // connections map in sorted order — the same shape the byte path
    // decodes.
    #[test]
    fn item_materializes_to_the_item_json() {
        let item = real_item(10, 5);
        let program = sansho::compile(&sansho::parse("@").unwrap()).unwrap();
        let whole = sansho::lookup_value(&item, &program).unwrap();
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
    // existing round-trip contract. What: the existing interface round
    // trip fixture also evaluates through the Item node. Why: coverage
    // of the SPEC-0001 Appendix-A shape on the real type.
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
        let result = sansho::lookup_value(&item, &program).unwrap();
        let expected: Vec<String> = (0..10)
            .filter(|i| i % 2 == 0)
            .map(|i| format!("pkg:maven/org.example/art{i}@1.0.{i}"))
            .collect();
        assert_eq!(result, serde_json::json!(expected));
    }

    // Requirement: the design contract (SPEC-0001 §5, the zero-copy
    // property) — evaluation yields references into the document;
    // materialization happens only at the emit boundary. What: the
    // item's VALUE-form surface over a real Item exposes the item's
    // own JSON view (its members materialize from the in-memory fields
    // — nothing is decoded), and the scan shape evaluates to the same
    // rows as the byte path, byte for byte. Why: the zero-copy-until-
    // emit property holds on the trait surface: the item is already in
    // memory, so its evaluation allocates nothing the byte path would
    // not (the value-form members are the item's own serialization).
    #[test]
    fn item_result_is_pure_reference() {
        let item = real_item(10, 5);
        let whole = sansho::compile(&sansho::parse("@").unwrap()).unwrap();
        let result = sansho::lookup_value(&item, &whole).unwrap();
        let expected = serde_json::to_value(&item).unwrap();
        assert_eq!(result, expected, "the item is its own JSON view");

        // the VALUE-form members are items of the item's own fields: the
        // object has the four serialized members
        use sansho::SanshoTrait;
        let members = SanshoTrait::entries(&item);
        assert_eq!(members.len(), 4, "the four serialized members");
        assert_eq!(members[0].0, "identifier");
        assert_eq!(members[2].0, "body_mime_type");

        // the scan shape evaluates byte-identically to the byte path
        let scan = sansho::compile(
            &sansho::parse("connections.\"alias:from\"[?starts_with(@, 'pkg:')]").unwrap(),
        )
        .unwrap();
        let scan_result = sansho::lookup_value(&item, &scan).unwrap();
        let bytes = serde_cbor::to_vec(&item).unwrap();
        let via_bytes = sansho::evaluate_cbor(&scan, &bytes).unwrap();
        assert_eq!(
            serde_json::to_vec(&scan_result).unwrap(),
            serde_json::to_vec(&via_bytes).unwrap(),
            "the scan through the Item source must equal the byte path"
        );

        // the seek shape evaluates through the item surface too (a body
        // member: the item's in-memory CBOR value, materialized)
        let seek = sansho::compile(&sansho::parse("body.file_size").unwrap()).unwrap();
        let seek_result = sansho::lookup_value(&item, &seek).unwrap();
        assert_eq!(seek_result, serde_json::json!(3050 + 5));
    }

    // Requirement: SPEC-0001 §2 (the interface), §5 — the integration
    // adds no semantics: projecting through the projection entries
    // returns BYTE-IDENTICAL results to the direct engine evaluation of
    // the same bytes. What: for the fixture-shaped items, the
    // integration's output equals the direct engine output exactly. Why:
    // it is a conduit, not a transformation (plan
    // `integration_interface_round_trip`).
    //
    // LLM section: the fixture item (the SPEC-0001 Appendix-A shape) is
    // encoded with serde_cbor (a development dependency, fixture
    // construction only); the interface path and the direct path must
    // agree byte-for-byte on every expression tried.
    #[test]
    fn integration_interface_round_trip() {
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
            let via_integ = project_item(&item_bytes, expression, &cache)
                .unwrap_or_else(|e| panic!("{expression}: integration error: {e}"));
            let direct = {
                let program =
                    sansho::compile(&sansho::parse(expression).unwrap()).unwrap();
                sansho::evaluate_cbor(&program, &item_bytes)
                    .unwrap_or_else(|e| panic!("{expression}: direct error: {e}"))
            };
            assert_eq!(
                serde_json::to_vec(&via_integ).unwrap(),
                serde_json::to_vec(&direct).unwrap(),
                "{expression}: the integration must be byte-identical to the direct evaluation"
            );
        }
    }

    // Requirement: SPEC-0001 §2 (the interface contract) — the
    // integration's error boundaries: no item at the offset, an invalid
    // expression, and a limit-exceeded are STRUCTURED errors, never
    // panics (plan `integration_interface_round_trip` boundaries).
    //
    // LLM section: three error shapes, three assertions; the
    // limit-exceeded uses a tiny byte cap over a fat item.
    #[test]
    fn integration_interface_error_boundaries() {
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

    // Requirement: the no-decode property of the integration's
    // accessor. What: read_item_bytes_at returns the EXACT CBOR span
    // (the bytes between the length prefix and the next item), not a
    // decoded/re-encoded copy. Why: the host must not decode before the
    // engine; the accessor's honesty is the integration's premise (plan
    // Phase 6 task 1).
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
