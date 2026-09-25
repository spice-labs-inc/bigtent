//! Phase 4, T4.3: the consumer harness — the engine-shaped shape the
//! High Wire plan's Phase 2 consumes: owned backing (an mmap-like byte
//! slice; an in-memory item) evaluated through the public generic
//! entry, identical rows, with the byte-once counter.

use sansho::source::CborNode;
use sansho::view::Node;

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
/// rows through the public `evaluate_over`.
#[test]
fn consumer_backends_agree_through_evaluate_over() {
    let item = appendix_a(1_000, 500);
    let bytes = serde_cbor::to_vec(&item).unwrap();
    let program = scan_program();

    // the byte source (an Arc<Mmap>-backed slice in the real consumer;
    // a plain Vec here — the CborNode is a pure borrower)
    let byte_root = CborNode::root(&bytes);
    let byte_result = sansho::eval::evaluate_over(&program, &byte_root).unwrap();

    // the value source (the in-memory merged item)
    let value_root = sansho::materialized::MaterializedNode::root(&item);
    let value_result = sansho::eval::evaluate_over(&program, &value_root).unwrap();

    assert_eq!(byte_result, value_result, "the backends agree");
    let rows = byte_result.as_array().unwrap();
    assert_eq!(rows.len(), 500, "one row per pkg: entry");
    assert!(rows[0].as_str().unwrap().starts_with("pkg:"));

    // the byte-once counter via the instrumented form: ONE evaluation
    // (the unmemoized output path re-decodes on a second evaluation of
    // the same memo — the byte-once contract is per evaluation)
    let fresh_root = CborNode::root(&bytes);
    let (result, count) = sansho::eval::evaluate_over_with_stats(
        &program,
        &fresh_root,
        |n| n.decode_count(),
    );
    assert!(result.is_ok());
    assert!(count <= 600, "the boundary decodes stay bounded: {count}");
}

/// T4.3-adjacent: the byte source's zero-copy contract through the
/// public surface (the text-string borrow is within the input slice).
#[test]
fn consumer_byte_source_borrows_in_place() {
    let item = appendix_a(10, 5);
    let bytes = serde_cbor::to_vec(&item).unwrap();
    let root = CborNode::root(&bytes);
    let ident = root.get_key("identifier").unwrap();
    let s = ident.as_str().unwrap();
    let start = s.as_ptr() as usize;
    let input_start = bytes.as_ptr() as usize;
    assert!(
        start >= input_start && start + s.len() <= input_start + bytes.len(),
        "the borrowed string must be within the input slice"
    );
}