//! Phase 4, T4.3: the consumer harness — the engine-shaped shape the
//! High Wire plan's Phase 2 consumes: owned backing (an mmap-like byte
//! slice; an in-memory value) evaluated through the public trait
//! surface (`lookup` over the raw types), identical rows, with the
//! byte-once counter.

use sansho::view::SanshoTrait;

#[path = "../src/tests_common.rs"]
mod tests_common;

use tests_common::appendix_a;

/// The consumer's scan shape: the north_purls emit.
fn scan_program() -> sansho::Program {
    let parsed = sansho::parse("connections.\"alias:from\"[?starts_with(@, 'pkg:')]").unwrap();
    sansho::compile(&parsed).unwrap()
}

/// T4.3: the byte source (over the item's serialized bytes, as the
/// single-cluster consumer holds them) and the value source (over the
/// in-memory item, as the herd consumer holds it) produce identical
/// rows through the public `lookup`.
#[test]
fn consumer_backends_agree_through_lookup() {
    let item = appendix_a(1_000, 500);
    let bytes = serde_cbor::to_vec(&item).unwrap();
    let program = scan_program();

    // the byte source (an Arc<Mmap>-backed slice in the real consumer;
    // a plain Vec here — the source is a pure borrower)
    let byte_source: &[u8] = &bytes;
    let byte_value = sansho::lookup_value(&byte_source, &program).unwrap();

    // the value source (the in-memory merged item)
    let value_result = sansho::lookup_value(&item, &program).unwrap();

    assert_eq!(byte_value, value_result, "the backends agree");
    let rows = byte_value.as_array().unwrap();
    assert_eq!(rows.len(), 500, "one row per pkg: entry");
    assert!(rows[0].as_str().unwrap().starts_with("pkg:"));

    // the byte-once counter via the instrumented form: ONE evaluation
    let (result, count) = sansho::eval::evaluate_cbor_with_stats(&program, &bytes);
    assert!(result.is_ok());
    assert!(count <= 600, "the boundary decodes stay bounded: {count}");
}

/// T4.3-adjacent: the byte source's zero-copy contract through the
/// public surface: the trait's `as_str` on the source borrows within
/// the input slice.
#[test]
fn consumer_byte_source_borrows_in_place() {
    let item = appendix_a(10, 5);
    let bytes = serde_cbor::to_vec(&item).unwrap();
    let byte_source: &[u8] = &bytes;
    // the root is an object; the identifier member's string (via the
    // trait on the source) is borrowed from the input slice
    let ident = SanshoTrait::get_key(&byte_source, "identifier").unwrap();
    let s = ident.as_str().unwrap();
    let start = s.as_ptr() as usize;
    let input_start = bytes.as_ptr() as usize;
    assert!(
        start >= input_start && start + s.len() <= input_start + bytes.len(),
        "the borrowed string must be within the input slice"
    );
}