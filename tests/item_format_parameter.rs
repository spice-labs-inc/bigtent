//! THE `item_format` PARAMETER IS PARSED BY A LIBRARY, NOT BY HAND.
//!
//! REQUIREMENT (invariant 7: use permissive open-source libraries rather than
//! rolling your own): `ItemFormat::from_raw_query` split the raw query on `&` and
//! on the first `=`, comparing the RAW text against `v3`/`v4`. A query parameter is
//! percent-encoded, so a hand-rolled comparison disagrees with every other endpoint
//! in the same server, which decodes through axum's extractor.
//!
//! THEORY OF THE TEST: the encoded spellings of the same parameter must be accepted,
//! because a client is entitled to send them and nothing documents otherwise. The
//! conflict and rejection cases must keep working, so this pins "now decodes"
//! without weakening the checks the hand parser did have. `%76%34` is `v4`; `%33`
//! is `v3`; `+` is a space, so `+v4` is NOT a value and must still be rejected.
use bigtent::server::ItemFormat;

/// The encoded spellings of the same parameter are the same parameter.
#[test]
fn an_encoded_item_format_is_accepted() {
    assert_eq!(
        ItemFormat::from_raw_query(Some("item_format=%76%34")),
        Ok(ItemFormat::V4),
        "`%76%34` IS `v4`: a percent-encoded value must decode like every other \
         endpoint decodes its parameters"
    );
    assert_eq!(
        ItemFormat::from_raw_query(Some("item_format=%76%33")),
        Ok(ItemFormat::V3)
    );
    // and the plain spelling still works
    assert_eq!(
        ItemFormat::from_raw_query(Some("item_format=v4")),
        Ok(ItemFormat::V4)
    );
}

/// The checks the hand parser had are not weakened.
#[test]
fn the_rejections_still_hold() {
    // an unknown value
    assert!(ItemFormat::from_raw_query(Some("item_format=v5")).is_err());
    // a conflicting duplicate
    assert!(
        ItemFormat::from_raw_query(Some("item_format=v3&item_format=v4")).is_err(),
        "conflicting duplicates must still be refused"
    );
    // an agreeing duplicate is accepted
    assert_eq!(
        ItemFormat::from_raw_query(Some("item_format=v4&item_format=v4")),
        Ok(ItemFormat::V4)
    );
    // no parameter at all keeps the default
    assert_eq!(ItemFormat::from_raw_query(None), Ok(ItemFormat::V3));
    assert_eq!(ItemFormat::from_raw_query(Some("")), Ok(ItemFormat::V3));
    // a parameter with no value is not a format
    assert!(ItemFormat::from_raw_query(Some("item_format")).is_err());
    // `+` is a space, so it is not a format either
    assert!(ItemFormat::from_raw_query(Some("item_format=+v4")).is_err());
}
