//! CBOR tags: the one policy, in both source forms.
//!
//! REQUIREMENT (second review, finding 11, decided by the owner): the CBOR that
//! Goat Rodeo writes and the CBOR of merged items never contain a tag, so a tag
//! is not given semantics of its own — it is treated as `null` EVERYWHERE.
//! Before this, the two source forms disagreed: the borrowed `serde_cbor::Value`
//! form reported `Null` and the byte source reported `Number`, so the same
//! document answered differently depending on which form read it, and neither
//! answer said what it was doing.
//!
//! THEORY OF THE TEST: the byte source and the borrowed value must agree, and
//! both must say `null`. Each case is a tag over a different item type (text,
//! array, map, byte string) because the old bug's `Number` mapping was only
//! visible for some payloads.

use sansho::view::{Kind, SanshoTrait};

/// A tag over an inner item, hand-crafted: `serde_cbor`'s writer DROPS a tag
/// (it encodes the tagged item alone), so the head is written by hand — major
/// type 6, tag 0, then the inner item.
fn tagged_bytes(inner: &serde_cbor::Value) -> Vec<u8> {
    let mut bytes = vec![0xC0]; // tag 0
    bytes.extend(serde_cbor::to_vec(inner).expect("the inner item encodes"));
    bytes
}

/// THE POLICY: both forms report `Null` for a tag, whatever the tagged item is.
///
/// This is also the guard on `source.rs`'s `Some(Type::Tag) => Kind::Null`
/// arm: `datatype()` skips tags, so that arm cannot fire, and the `is_tagged`
/// pre-check is what answers. If someone reorders or removes the pre-check,
/// this test is what fails — a tag over text would report `String`, over bytes
/// `Bytes`, and the assertion below names which.
#[test]
fn a_tag_is_null_in_both_source_forms() {
    let inners: Vec<serde_cbor::Value> = vec![
        serde_cbor::Value::Text("2026-01-01T00:00:00Z".to_string()), // tag 0: a date
        serde_cbor::Value::Array(vec![serde_cbor::Value::Integer(1.into())]),
        serde_cbor::Value::Map(
            [(
                serde_cbor::Value::Text("k".to_string()),
                serde_cbor::Value::Integer(2.into()),
            )]
            .into_iter()
            .collect(),
        ),
        serde_cbor::Value::Bytes(vec![0x01, 0x02]), // tag 2: a bignum's magnitude
    ];

    for inner in inners {
        let bytes = tagged_bytes(&inner);

        // the BYTE source
        let byte_kind = (&bytes[..]).kind();
        assert_eq!(
            byte_kind,
            Kind::Null,
            "the byte source must report a tag as null, found {byte_kind} over {inner:?}"
        );

        // the BORROWED VALUE source. `serde_cbor` DROPS a tag on decode as
        // well as on encode, so the value form can only be handed a tag when
        // one is constructed directly — which is how the review checked it.
        let borrowed = serde_cbor::Value::Tag(0, Box::new(inner.clone()));
        let value_kind = (&borrowed).kind();
        assert_eq!(
            value_kind,
            Kind::Null,
            "the borrowed form must agree: a tag is null, found {value_kind}"
        );
    }
}

/// and `materialize` says `null` — one policy, not three.
#[test]
fn a_tag_materializes_as_null() {
    let bytes = tagged_bytes(&serde_cbor::Value::Integer(1_700_000_000.into()));
    let materialized = sansho::materialize(&(&bytes[..])).expect("materializes");
    assert_eq!(
        materialized,
        serde_json::Value::Null,
        "a tag is null: {materialized:?}"
    );
}

/// The tag's CONTENT is never silently substituted for it — the old byte-source
/// behaviour, which unwrapped the tag and answered with the inner item.
#[test]
fn a_tag_does_not_answer_with_its_content() {
    let bytes = tagged_bytes(&serde_cbor::Value::Text("the content".to_string()));
    let materialized = sansho::materialize(&(&bytes[..])).expect("materializes");
    assert_ne!(
        materialized,
        serde_json::json!("the content"),
        "a tag must not be unwrapped to its content: {materialized:?}"
    );
}
