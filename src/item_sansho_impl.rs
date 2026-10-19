//! The Item-side implementations of [`sansho::SanshoTrait`] (`Item`)
//! and the item projection entries: project an item's
//! fields through the Sansho engine, directly over the item's stored
//! CBOR bytes.
//!
//! The trait implementations live here, WITH their types, per the
//! placement rule ("within Item for Item"; standard-library types'
//! implementations live in the Sansho engine itself).
//!
//! This module is the ONLY place `bigtent` touches Sansho. The HTTP attachment point is a separate
//! specification — nothing here changes any existing endpoint.
//!
//! The critical property: the item bytes reach the engine UNDECODED.
//! The host's normal read path materializes the whole item; the
//! no-decode entry takes the raw byte span (via
//! [`crate::rodeo::data::read_item_bytes_at`]) so selective
//! materialization stays intact end to end.

use std::{collections::HashMap, rc::Rc};

use sansho::SanshoCarrier;

use crate::item::Item;

// /// Project an item's fields: the item's raw CBOR bytes + a JMESPath
// /// expression → the projected JSON value. The default resource limits
// /// apply; the compiled program is cached (the canonicalized expression
// /// + the engine version as the key), so repeated expressions — the
// /// common case for a query surface — pay the parse/compile once.
// pub fn project_item<'a>(
//     item_bytes: &'a [u8],
//     expression: &str,
//     cache: &sansho::ProgramCache,
// ) -> Result<SanshoCarrier<'a>, SanshoError> {
//     let program = cache.compile_cached(expression)?;
//     sansho::eval::eval(&program, Rc::new(item_bytes)).map_err(|e| e.into())
// }
//
// /// The convenience form without a cache (a fresh compile each call) —
// /// for one-shot lookups where the parse cost is negligible.
// pub fn project_item_once(
//     item_bytes: &[u8],
//     expression: &str,
// ) -> Result<serde_json::Value, SanshoError> {
//     let program = sansho::compile(&sansho::parse(expression)?, None)?;
//     sansho::evaluate_cbor(&program, item_bytes)
// }
//
// /// The data-file projection entry: the item at `offset` projected
// /// through `expression` — the byte span comes from the no-decode
// /// accessor, so nothing in the host decodes the item before Sansho sees
// /// it.
// pub fn project_item_from_file(
//     file: &DataFile,
//     offset: usize,
//     expression: &str,
//     cache: &sansho::ProgramCache,
// ) -> Result<serde_json::Value, SanshoError> {
//     let bytes = file
//         .read_item_bytes_at(offset)
//         .ok_or_else(|| SanshoError::Input {
//             message: format!("no item at data-file offset {offset}"),
//         })?;
//     project_item(bytes, expression, cache)
// }

// ---------------------------------------------------------------------------
// The item's Sansho surface
// ---------------------------------------------------------------------------

impl<'d> sansho::SanshoTrait<'d> for &'d Item {
    fn kind(&self) -> sansho::view::Kind {
        sansho::view::Kind::Object
    }

    fn get_key(
        &self,
        name: &str,
    ) -> Result<Option<sansho::view::SanshoCarrier<'d>>, sansho::SanshoError> {
        let item: &'d Item = self;
        match name {
            // the member is the item's own field, borrowed: a refcount
            // around the borrow, never a copy
            "identifier" => Ok(Some(
                Rc::new(&item.identifier) as sansho::view::SanshoCarrier<'d>
            )),
            "connections" => Ok(Some(
                Rc::new(&item.connections) as sansho::view::SanshoCarrier<'d>
            )),
            "body_mime_type" => Ok(Some(
                Rc::new(&item.body_mime_type) as sansho::view::SanshoCarrier<'d>
            )),
            "body" => Ok(Some(Rc::new(&item.body) as sansho::view::SanshoCarrier<'d>)),
            _ => Ok(None),
        }
    }

    fn as_object(&self) -> Result<HashMap<&str, SanshoCarrier<'d>>, sansho::SanshoError> {
        let item: &'d Item = self;
        // the keys are the item's own names: no key string is built
        //
        let mut ret = HashMap::new();
        ret.insert(
            "identifier",
            Rc::new(&item.identifier) as sansho::view::SanshoCarrier<'d>,
        );
        ret.insert(
            "connections",
            Rc::new(&item.connections) as sansho::view::SanshoCarrier<'d>,
        );
        ret.insert(
            "body_mime_type",
            Rc::new(&item.body_mime_type) as sansho::view::SanshoCarrier<'d>,
        );
        ret.insert(
            "body",
            Rc::new(&item.body) as sansho::view::SanshoCarrier<'d>,
        );

        Ok(ret)
    }

    fn container_len(&self) -> Result<usize, sansho::SanshoError> {
        Ok(4)
    }

    fn to_string_sansho(&self) -> String {
        // the item's own four fields, each rendered by its own type
        let item: &'d Item = self;
        format!(
            "{{identifier: {}, connections: {}, body_mime_type: {}, body: {}}}",
            item.identifier.to_string_sansho(),
            (&item.connections).to_string_sansho(),
            (&item.body_mime_type).to_string_sansho(),
            (&item.body).to_string_sansho(),
        )
    }
}

// The JSON view of an in-memory CBOR value — the engine's own
// conversion (moved into the engine with the `serde_cbor::Value`
// source; the item view re-exports it, so the two CBOR paths cannot
// drift).
//
//
// Evaluate a projection over a real, in-memory [`Item`] — the
// merged-item (goat-herd) case: no serialization to bytes or JSON; the
// item is walked through the trait surface (`lookup` over `&Item`).
// pub fn project_item_direct(
//     item: &Item,
//     expression: &str,
//     cache: &sansho::ProgramCache,
// ) -> Result<serde_json::Value, SanshoError> {
//     let program = cache.compile_cached(expression)?;
//     sansho::lookup_value(item, &program)
// }

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// Build a REAL [`Item`] struct — not a JSON lookalike — with
    /// `n_connections` alias targets (every other one a pURL, the rest
    /// gitoids, matching the Appendix-A scan shape) and `n_file_names`
    /// body entries. Kept while the tests that build on it are commented
    /// out.
    #[allow(dead_code)]
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
            connections: {
                let mut map = std::collections::BTreeMap::new();
                map.insert("alias:from".to_string(), alias_from);
                map.insert(
                    "contained:up".to_string(),
                    ["gitoid:blob:sha1:parent".to_string()]
                        .into_iter()
                        .collect(),
                );
                map
            },
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
    // #[test]
    // fn item_source_matches_byte_source_on_the_trait_surface() {
    //     let expressions = [
    //         "identifier",
    //         "body.file_size",
    //         "body.mime_type[0]",
    //         "@",
    //         "connections.\"alias:from\"[?starts_with(@, 'pkg:')]",
    //         "length(connections.\"alias:from\")",
    //         "{id: identifier, first_alias: connections.\"alias:from\"[0]}",
    //         "body.file_names[?starts_with(@, 'gitoid:')]",
    //     ];
    //     for (n_connections, n_file_names) in [(10usize, 5usize), (1_000, 500), (25_000, 12_000)] {
    //         let item = real_item(n_connections, n_file_names);
    //         let bytes = serde_cbor::to_vec(&item).expect("the real item serializes");
    //         for expression in expressions {
    //             let cache = sansho::ProgramCache::new(16);
    //             let program = cache.compile_cached(expression).unwrap();
    //             let via_item = sansho::lookup_value(&item, &program).unwrap();
    //             // the byte source impl lives on `&'a [u8]`; bind the
    //             // slice so the call passes `&slice` (T = &[u8])
    //             let slice: &[u8] = &bytes;
    //             let via_bytes = sansho::lookup_value(&slice, &program).unwrap();
    //             assert_eq!(
    //                 serde_json::to_vec(&via_item).unwrap(),
    //                 serde_json::to_vec(&via_bytes).unwrap(),
    //                 "{expression} (connections={n_connections}): the Item source and the byte \
    //                  source over the item's serialization must agree byte for byte"
    //             );
    //         }
    //     }
    // }

    // Requirement: the decode contract — the item
    // materializes to exactly its JSON value. What: `@` over a real
    // Item (through lookup) equals `serde_json::to_value(&item)`. Why:
    // the whole-document projection must be the item itself, with the
    // connections map in sorted order — the same shape the byte path
    // decodes.
    // #[test]
    // fn item_materializes_to_the_item_json() {
    //     let item = real_item(10, 5);
    //     let program = sansho::compile(&sansho::parse("@").unwrap(), None).unwrap();
    //     let whole = sansho::lookup_value(&item, &program).unwrap();
    //     let expected = serde_json::to_value(&item).unwrap();
    //     assert_eq!(whole, expected);
    // }

    // Requirement: the direct entry's equivalence — `project_item_direct`
    // over a real Item equals the byte path over the same item's
    // serialization. What: the two outputs are byte-identical. Why: the
    // in-memory entry is a conduit over the same document; it must not
    // introduce a second semantics.
    // #[test]
    // fn project_item_direct_equals_byte_path() {
    //     let item = real_item(100, 50);
    //     let bytes = serde_cbor::to_vec(&item).unwrap();
    //     let cache = sansho::ProgramCache::new(16);
    //     for expression in [
    //         "identifier",
    //         "body.file_size",
    //         "connections.\"alias:from\"[0]",
    //     ] {
    //         let via_direct = project_item_direct(&item, expression, &cache).unwrap();
    //         let via_bytes = project_item(&bytes, expression, &cache).unwrap();
    //         assert_eq!(
    //             serde_json::to_vec(&via_direct).unwrap(),
    //             serde_json::to_vec(&via_bytes).unwrap(),
    //             "{expression}: the direct Item path must equal the byte path"
    //         );
    //     }
    // }

    // Requirement: the whole-item materialize must be usable with the
    // existing round-trip contract. What: the existing interface round
    // trip fixture also evaluates through the Item node. Why: coverage
    // #[test]
    // fn appendix_a_shape_on_the_item_node() {
    //     let item = real_item(2, 3);
    //     let cache = sansho::ProgramCache::new(16);
    //     let via_direct = project_item_direct(&item, "body.file_names[1]", &cache).unwrap();
    //     assert!(via_direct.is_string());
    // }

    // Requirement: the filter-predicate fast path's input — a borrowed
    // Text node must expose its raw bytes so `[?starts_with(@, 'pkg:')]`
    // takes the no-allocation leaf path. What: the scan shape's result
    // is correct AND the Text node reports its raw bytes. Why: the
    // zero-copy-until-emit goal applies to the item path too.
    // #[test]
    // fn item_text_node_exposes_raw_bytes() {
    //     let item = real_item(10, 5);
    //     let program = sansho::compile(
    //         &sansho::parse("connections.\"alias:from\"[?starts_with(@, 'pkg:')]").unwrap(),
    //         None,
    //     )
    //     .unwrap();
    //     let result = sansho::lookup_value(&item, &program).unwrap();
    //     let expected: Vec<String> = (0..10)
    //         .filter(|i| i % 2 == 0)
    //         .map(|i| format!("pkg:maven/org.example/art{i}@1.0.{i}"))
    //         .collect();
    //     assert_eq!(result, serde_json::json!(expected));
    // }

    // Requirement: the design contract (the zero-copy
    // property) — evaluation yields references into the document;
    // materialization happens only at the emit boundary. What: the
    // item's VALUE-form surface over a real Item exposes the item's
    // own JSON view (its members materialize from the in-memory fields
    // — nothing is decoded), and the scan shape evaluates to the same
    // rows as the byte path, byte for byte. Why: the zero-copy-until-
    // emit property holds on the trait surface: the item is already in
    // memory, so its evaluation allocates nothing the byte path would
    // not (the value-form members are the item's own serialization).
    //     #[test]
    //     fn item_result_is_pure_reference() {
    //         let item = real_item(10, 5);
    //         let whole = sansho::compile(&sansho::parse("@").unwrap(), None).unwrap();
    //         let result = sansho::lookup_value(&item, &whole).unwrap();
    //         let expected = serde_json::to_value(&item).unwrap();
    //         assert_eq!(result, expected, "the item is its own JSON view");
    //
    //         // the VALUE-form members are items of the item's own fields: the
    //         // object has the four serialized members
    //         use sansho::SanshoTrait;
    //         let members = SanshoTrait::entries(&item);
    //         assert_eq!(members.len(), 4, "the four serialized members");
    //         assert_eq!(members[0].0, "identifier");
    //         assert_eq!(members[2].0, "body_mime_type");
    //
    //         // the scan shape evaluates byte-identically to the byte path
    //         let scan = sansho::compile(
    //             &sansho::parse("connections.\"alias:from\"[?starts_with(@, 'pkg:')]").unwrap(),
    //             None,
    //         )
    //         .unwrap();
    //         let scan_result = sansho::lookup_value(&item, &scan).unwrap();
    //         let bytes = serde_cbor::to_vec(&item).unwrap();
    //         let via_bytes = sansho::evaluate_cbor(&scan, &bytes).unwrap();
    //         assert_eq!(
    //             serde_json::to_vec(&scan_result).unwrap(),
    //             serde_json::to_vec(&via_bytes).unwrap(),
    //             "the scan through the Item source must equal the byte path"
    //         );
    //
    //         // the seek shape evaluates through the item surface too (a body
    //         // member: the item's in-memory CBOR value, materialized)
    //         let seek = sansho::compile(&sansho::parse("body.file_size").unwrap(), None).unwrap();
    //         let seek_result = sansho::lookup_value(&item, &seek).unwrap();
    //         assert_eq!(seek_result, serde_json::json!(3050 + 5));
    //     }

    // Requirement: the interface — the integration
    // adds no semantics: projecting through the projection entries
    // returns BYTE-IDENTICAL results to the direct engine evaluation of
    // the same bytes. What: for the fixture-shaped items, the
    // integration's output equals the direct engine output exactly. Why:
    // it is a conduit, not a transformation (plan
    // `integration_interface_round_trip`).
    //
    // LLM section: the fixture item (the fixture shape) is
    // encoded with serde_cbor (a development dependency, fixture
    // construction only); the interface path and the direct path must
    // agree byte-for-byte on every expression tried.
    //     #[test]
    //     fn integration_interface_round_trip() {
    //         let item = serde_json::json!({
    //             "identifier": "gitoid:blob:sha1:30e65ad24f4b4d799e52cfd70fcbebc0490b7343",
    //             "connections": [["alias:from", "md5:4d921bf0ce238d1a96bdf65000fde565"]],
    //             "body_mime_type": "application/vnd.cc.goatrodeo",
    //             "body": {
    //                 "extra": {},
    //                 "file_names": [
    //                     "org/apache/logging/log4j/core/lookup/JndiLookup.java",
    //                     "gitoid:blob:sha256:005a5131bc1c950b2b4d1081a95bd21f52e45c308d9633465fbc240f8433c9e6!$org/apache/logging/log4j/core/lookup/JndiLookup.java"
    //                 ],
    //                 "file_size": 3050,
    //                 "mime_type": ["text/x-java-source"]
    //             }
    //         });
    //         let item_bytes = serde_cbor::to_vec(&item).unwrap();
    //
    //         for expression in [
    //             "body.file_names[?starts_with(@, 'gitoid:')]",
    //             "body.mime_type[0]",
    //             "identifier",
    //             "{id: identifier, size: body.file_size}",
    //             "length(body.file_names)",
    //         ] {
    //             let cache = sansho::ProgramCache::new(16);
    //             let via_integ = project_item(&item_bytes, expression, &cache)
    //                 .unwrap_or_else(|e| panic!("{expression}: integration error: {e}"));
    //             let direct = {
    //                 let program = sansho::compile(&sansho::parse(expression).unwrap(), None).unwrap();
    //                 sansho::evaluate_cbor(&program, &item_bytes)
    //                     .unwrap_or_else(|e| panic!("{expression}: direct error: {e}"))
    //             };
    //             assert_eq!(
    //                 serde_json::to_vec(&via_integ).unwrap(),
    //                 serde_json::to_vec(&direct).unwrap(),
    //                 "{expression}: the integration must be byte-identical to the direct evaluation"
    //             );
    //         }
    //     }

    // Requirement: the interface contract — the
    // integration's error boundaries: no item at the offset, an invalid
    // expression, and a limit-exceeded are STRUCTURED errors, never
    // panics (plan `integration_interface_round_trip` boundaries).
    //
    // LLM section: three error shapes, three assertions; the
    // limit-exceeded uses a tiny byte cap over a fat item.
    //     #[test]
    //     fn integration_interface_error_boundaries() {
    //         // a missing item: fewer bytes than the length prefix needs, and
    //         // an offset past the end
    //         let short = vec![0u8; 3];
    //         assert!(crate::rodeo::data::read_item_bytes_at(&short, 0).is_none());
    //         let file_bytes = vec![0u8; 12];
    //         assert!(crate::rodeo::data::read_item_bytes_at(&file_bytes, 100).is_none());
    //
    //         // an invalid expression: the parse error, structured
    //         let item_bytes = serde_cbor::to_vec(&serde_json::json!({"a": 1})).unwrap();
    //         let error =
    //             project_item_once(&item_bytes, "a[0").expect_err("an invalid expression must error");
    //         assert!(matches!(error, SanshoError::Parse { .. }));
    //
    //         // (the limit-exceeded assertion was removed with the limits
    //         // machinery — the owner's directive; the structured-error path
    //         // is covered by the parse case above)
    //     }

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
