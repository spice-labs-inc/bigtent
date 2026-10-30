//! Indefinite-length containers and chunked text.
//!
//! REQUIREMENT (second review, finding 18, decided by the owner): a document's
//! ENCODING must not change its meaning. CBOR can write an array, a map or a
//! text string either with a definite length or in chunks ended by a break
//! byte; both denote the same JSON value. Before this, the byte source refused
//! the indefinite forms in `container_len` and in `as_str`, and the evaluator
//! turned those refusals into `[]` and `""` — silently truncating real content.
//!
//! THEORY OF THE TEST: the same logical document, written both ways, must
//! evaluate identically — length, index, and slice — so the encoding is
//! invisible to a query. Each indefinite case has a definite twin asserting the
//! same answer, which is what makes "identical" a real comparison rather than
//! an expectation about one encoding.

use sansho::view::SanshoTrait;

/// Evaluate an expression over a document, as JSON out.
fn eval(bytes: &[u8], expression: &str) -> serde_json::Value {
    let parsed = sansho::parse(expression).expect("the expression parses");
    let program = sansho::compile(&parsed, None).expect("the expression compiles");
    let value = sansho::evaluate(&program, bytes).expect("the document evaluates");
    sansho::materialize(&value).expect("the result materializes")
}

// ---------------------------------------------------------------- the shapes

/// An indefinite-length array of 1, 2, 3: `0x9F`, the items, `0xFF`.
fn indefinite_array() -> Vec<u8> {
    vec![0x9F, 0x01, 0x02, 0x03, 0xFF]
}
/// The same array with a definite length: `0x83`.
fn definite_array() -> Vec<u8> {
    vec![0x83, 0x01, 0x02, 0x03]
}
/// An indefinite-length map {a: 1, b: 2}: `0xBF`, pairs, `0xFF`.
fn indefinite_map() -> Vec<u8> {
    vec![0xBF, 0x61, b'a', 0x01, 0x61, b'b', 0x02, 0xFF]
}
/// The same map with a definite length: `0xA2`.
fn definite_map() -> Vec<u8> {
    vec![0xA2, 0x61, b'a', 0x01, 0x61, b'b', 0x02]
}
/// Chunked text "abc": `0x7F`, "ab", "c", `0xFF`.
fn chunked_text() -> Vec<u8> {
    vec![0x7F, 0x62, b'a', b'b', 0x61, b'c', 0xFF]
}
/// The same text with a definite length: `0x63`.
fn definite_text() -> Vec<u8> {
    vec![0x63, b'a', b'b', b'c']
}

// ---------------------------------------------------------------- the lengths

/// `length(@)` counts an indefinite-length array's items.
#[test]
fn an_indefinite_array_has_its_length() {
    let indefinite = eval(&indefinite_array(), "length(@)");
    let definite = eval(&definite_array(), "length(@)");
    assert_eq!(indefinite, serde_json::json!(3), "the count is 3");
    assert_eq!(indefinite, definite, "the encoding does not change the length");
}

/// `length(@)` counts an indefinite-length map's members — the pairs are walked
/// and the count is halved.
#[test]
fn an_indefinite_map_has_its_length() {
    let indefinite = eval(&indefinite_map(), "length(@)");
    let definite = eval(&definite_map(), "length(@)");
    assert_eq!(indefinite, serde_json::json!(2), "two members");
    assert_eq!(indefinite, definite, "the encoding does not change the length");
}

/// `length(@)` counts a chunked text's CHARACTERS, across the chunks.
#[test]
fn chunked_text_has_its_length() {
    let indefinite = eval(&chunked_text(), "length(@)");
    let definite = eval(&definite_text(), "length(@)");
    assert_eq!(indefinite, serde_json::json!(3), "three characters");
    assert_eq!(indefinite, definite, "the encoding does not change the length");
}

// ------------------------------------------------------------ index and slice

/// Indexing reaches the items of an indefinite-length array.
#[test]
fn an_indefinite_array_can_be_indexed() {
    let indefinite = eval(&indefinite_array(), "[1]");
    let definite = eval(&definite_array(), "[1]");
    assert_eq!(indefinite, serde_json::json!(2), "the second item");
    assert_eq!(indefinite, definite);
}

/// Slicing an indefinite-length array gives the selected items — the case that
/// silently produced `[]` before `container_len` counted.
#[test]
fn an_indefinite_array_can_be_sliced() {
    let indefinite = eval(&indefinite_array(), "[0:2]");
    let definite = eval(&definite_array(), "[0:2]");
    assert_eq!(indefinite, serde_json::json!([1, 2]), "the first two");
    assert_eq!(indefinite, definite, "the encoding does not change the slice");
}

/// Slicing chunked text gives the selected characters — the case that silently
/// produced `""`.
#[test]
fn chunked_text_can_be_sliced() {
    let indefinite = eval(&chunked_text(), "[0:2]");
    let definite = eval(&definite_text(), "[0:2]");
    assert_eq!(indefinite, serde_json::json!("ab"), "the first two characters");
    assert_eq!(indefinite, definite, "the encoding does not change the slice");
}

/// The map's members are reachable by key, chunked or definite.
#[test]
fn an_indefinite_map_can_be_read_by_key() {
    let indefinite = eval(&indefinite_map(), "a");
    let definite = eval(&definite_map(), "a");
    assert_eq!(indefinite, serde_json::json!(1));
    assert_eq!(indefinite, definite);
}

// ------------------------------------------------------------- the controls

/// A DEFINITE document still lends its text: the `Cow` is `Borrowed`, so no
/// copy is made. This pins the zero-copy path that every document Goat Rodeo
/// writes takes — a regression here would be invisible in the answers.
#[test]
fn definite_text_is_still_borrowed() {
    let bytes = definite_text();
    let slice: &[u8] = &bytes;
    let text = slice.as_str().expect("the text reads");
    assert!(
        matches!(text, std::borrow::Cow::Borrowed(_)),
        "a definite text must be borrowed, not assembled"
    );
}

/// A CHUNKED text is assembled — the one case that must copy.
#[test]
fn chunked_text_is_assembled() {
    let bytes = chunked_text();
    let slice: &[u8] = &bytes;
    let text = slice.as_str().expect("the text reads");
    assert!(
        matches!(text, std::borrow::Cow::Owned(_)),
        "chunked text has no single run to lend, so it is assembled"
    );
    assert_eq!(text.as_ref(), "abc");
}

/// An empty definite array and a slice that selects nothing are `[]` — an
/// EMPTY answer, which is a different fact from an error, and the distinction
/// the old fallbacks conflated.
#[test]
fn an_empty_array_slice_is_empty_not_an_error() {
    assert_eq!(eval(&[0x80], "[0:2]"), serde_json::json!([]));
}
