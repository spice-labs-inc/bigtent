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

use crate::rodeo::data::DataFile;
use sansho::SanshoError;
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

#[cfg(test)]
mod tests {
    use super::*;

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

        // a limit-exceeded: the whole-evaluation structured error
        let fat = serde_json::json!({"payload": "x".repeat(2048)});
        let fat_bytes = serde_cbor::to_vec(&fat).unwrap();
        let limits = sansho::Limits { output_byte_cap: 128, ..Default::default() };
        let program = sansho::compile(&sansho::parse("payload").unwrap()).unwrap();
        let result = sansho::evaluate_cbor_with_limits(&program, &fat_bytes, &limits);
        assert!(matches!(
            result,
            Err(sansho::Stop::Error(SanshoError::Limit { .. }))
        ));
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
